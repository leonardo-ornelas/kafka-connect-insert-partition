package dev.ornelas.kafka.connect.smt;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public class InsertPartition<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final String PURPOSE = "partition number for the Kafka topic and field insertion using default partition logic";

    private String partitionField;

    private Integer numPartitions;

    private Cache<Schema, Schema> schemaUpdateCache;

    private interface ConfigName {
        String PARTITION_FIELD = "partition.field";
        String NUM_PARTITIONS = "num.partitions";
    }

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.PARTITION_FIELD, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
                    "Field name for Kafka partition.")
            .define(ConfigName.NUM_PARTITIONS, ConfigDef.Type.INT, null, ConfigDef.Importance.HIGH,
                    "Number of partitions for the record topic");

    private final StringSerializer stringSerializer = new StringSerializer();

    @Override
    public R apply(R record) {
        if (operatingValue(record) == null) {
            return record;
        } else if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }


    private R applySchemaless(R record) {
        final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);

        final Map<String, Object> updatedValue = new HashMap<>(value);

        Integer kafkaPartition = partition(record);

        if (partitionField != null) {
            updatedValue.put(partitionField, kafkaPartition);
        }

        return newRecord(record, kafkaPartition, null, updatedValue);
    }

    private R applyWithSchema(R record) {
        final Struct value = requireStruct(operatingValue(record), PURPOSE);

        Schema updatedSchema = schemaUpdateCache.get(value.schema());
        if (updatedSchema == null) {
            updatedSchema = makeUpdatedSchema(value.schema());
            schemaUpdateCache.put(value.schema(), updatedSchema);
        }

        final Struct updatedValue = new Struct(updatedSchema);

        for (Field field : value.schema().fields()) {
            updatedValue.put(field.name(), value.get(field));
        }

        Integer kafkaPartition = partition(record);

        updatedValue.put(partitionField, kafkaPartition);

        return newRecord(record, kafkaPartition, updatedSchema, updatedValue);
    }

    private Schema makeUpdatedSchema(Schema schema) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

        for (Field field : schema.fields()) {
            builder.field(field.name(), field.schema());
        }

        builder.field(partitionField, Schema.OPTIONAL_INT32_SCHEMA);

        return builder.build();
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        schemaUpdateCache = null;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        partitionField = config.getString(ConfigName.PARTITION_FIELD);

        if (partitionField == null) {
            throw new ConfigException("No field insertion configured");
        }

        numPartitions = config.getInt(ConfigName.NUM_PARTITIONS);

        if (numPartitions == null) {
            throw new ConfigException(ConfigName.NUM_PARTITIONS, "Number of partitions not configured");
        }

        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
    }

    @SuppressWarnings("unchecked")
    private Integer partition(R record) {

        if (record.key() != null) {

            Serde<Object> serde = Serdes.serdeFrom((Class<Object>) record.key().getClass());

            byte[] keyBytes = serde.serializer().serialize(record.topic(), record.key());

            return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
        }

        return record.kafkaPartition();
    }

    protected Schema operatingSchema(R record) {
        return record.valueSchema();
    }

    protected Object operatingValue(R record) {
        return record.value();
    }

    protected R newRecord(R record, Integer kafkaPartition, Schema updatedSchema, Object updatedValue) {
        return record.newRecord(record.topic(), Optional.ofNullable(kafkaPartition).orElse(record.kafkaPartition()), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
    }
}
