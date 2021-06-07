package dev.ornelas.kafka.connect.smt;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class InsertPartitionTest {

    private final InsertPartition<SourceRecord> xformValue = new InsertPartition<>();

    @AfterEach
    public void teardown() {
        xformValue.close();
    }

    @Test
    public void topLevelStructRequired() {
        xformValue.configure(Map.of("partition.field", "partition_field", "num.partitions", "1"));
        assertThrows(DataException.class,
                () -> xformValue.apply(new SourceRecord(null, null, "", 0, Schema.INT32_SCHEMA, 42)));
    }


    @Test
    public void copySchemaAndInsertConfiguredFields() {
        final Map<String, Object> props = new HashMap<>();
        props.put("partition.field", "partition_field");
        props.put("num.partitions", "1");

        xformValue.configure(props);

        final Schema simpleStructSchema = SchemaBuilder.struct().name("name").version(1).doc("doc").field("magic", Schema.OPTIONAL_INT64_SCHEMA).build();
        final Struct simpleStruct = new Struct(simpleStructSchema).put("magic", 42L);

        final SourceRecord record = new SourceRecord(null, null, "test", 0, null, 42, simpleStructSchema, simpleStruct, 789L);
        final SourceRecord transformedRecord = xformValue.apply(record);

        assertEquals(simpleStructSchema.name(), transformedRecord.valueSchema().name());
        assertEquals(simpleStructSchema.version(), transformedRecord.valueSchema().version());
        assertEquals(simpleStructSchema.doc(), transformedRecord.valueSchema().doc());

        assertEquals(Schema.OPTIONAL_INT64_SCHEMA, transformedRecord.valueSchema().field("magic").schema());
        assertEquals(42L, ((Struct) transformedRecord.value()).getInt64("magic").longValue());

        assertEquals(Schema.OPTIONAL_INT32_SCHEMA, transformedRecord.valueSchema().field("partition_field").schema());
        assertEquals(0, ((Struct) transformedRecord.value()).getInt32("partition_field").intValue());

        // Exercise caching
        final SourceRecord transformedRecord2 = xformValue.apply(
                new SourceRecord(null, null, "test", 1, simpleStructSchema, new Struct(simpleStructSchema)));
        assertSame(transformedRecord.valueSchema(), transformedRecord2.valueSchema());
    }

    @Test
    public void schemalessInsertConfiguredFields() {
        final Map<String, Object> props = new HashMap<>();
        props.put("partition.field", "partition_field");
        props.put("num.partitions", "1");

        xformValue.configure(props);

        final SourceRecord record = new SourceRecord(null, null, "test", 0,
                null, null, null, Collections.singletonMap("magic", 42L), 123L);

        final SourceRecord transformedRecord = xformValue.apply(record);

        assertEquals(42L, ((Map<?, ?>) transformedRecord.value()).get("magic"));
        assertEquals(0, ((Map<?, ?>) transformedRecord.value()).get("partition_field"));
    }

    @Test
    public void insertConfiguredFieldsIntoTombstoneEventWithoutSchemaLeavesValueUnchanged() {
        final Map<String, Object> props = new HashMap<>();
        props.put("partition.field", "partition_field");
        props.put("num.partitions", "1");

        xformValue.configure(props);

        final SourceRecord record = new SourceRecord(null, null, "test", 0,
                null, null);

        final SourceRecord transformedRecord = xformValue.apply(record);

        assertNull(transformedRecord.value());
        assertNull(transformedRecord.valueSchema());
    }

    @Test
    public void insertConfiguredFieldsIntoTombstoneEventWithSchemaLeavesValueUnchanged() {
        final Map<String, Object> props = new HashMap<>();
        props.put("partition.field", "partition_field");
        props.put("num.partitions", "1");

        xformValue.configure(props);

        final Schema simpleStructSchema = SchemaBuilder.struct().name("name").version(1).doc("doc").field("magic", Schema.OPTIONAL_INT64_SCHEMA).build();

        final SourceRecord record = new SourceRecord(null, null, "test", 0,
                simpleStructSchema, null);

        final SourceRecord transformedRecord = xformValue.apply(record);

        assertNull(transformedRecord.value());
        assertEquals(simpleStructSchema, transformedRecord.valueSchema());
    }


}
