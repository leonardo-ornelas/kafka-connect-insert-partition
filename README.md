Kafka Connect SMT to set partition base on default partitioner(if key is present) and to add a partition field 

This SMT supports inserting a partition into the record Value
Properties:

|Name|Description|Type|Default|Importance|
|---|---|---|---|---|
|`partition.field`| Field name for partition | String | `null` | High |
|`num.partitions`| Number of total partition, used to calculate the record partition. | String | `null` | High |

Example on how to add to your connector:
```
transforms=insertPartition
transforms.insertPartition.type=dev.ornelas.kafka.connect.smt.dev.ornelas.kafka.connect.smt.InsertPartition
transforms.insertPartition.partition.field="partition_field"
transforms.insertPartition.partition.num.partitions=50
```

Based on Apache Kafka® `InsertField` SMT