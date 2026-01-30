# Working with slowly changing dimensions

This section contains practical guides for implementing [slowly changing dimensions](https://en.wikipedia.org/wiki/Slowly_changing_dimension) (Slowly Changing Dimensions, SCD) — a popular approach to managing historical data in analytical stores.

The section describes the SCD1 and SCD2 variants:

* [SCD Type 1](#scd1): Old attribute values are replaced with new ones, keeping only the current state of data.
* [SCD Type 2](#scd2): The full history of changes is preserved by adding new records for each new attribute version.
  
## Features of SCD1 {#scd1}

[Slowly changing dimensions (type 1) or SCD1 (Type 1)](https://en.wikipedia.org/wiki/Slowly_changing_dimension#Type_1) is an approach where when an attribute of the dimension changes, the old value is replaced with a new one. Only the current state of data is stored. This approach is used when:

- historical information is not required;
- it is important to have only current data;
- it is required to minimize the size of the data store;
- it is required to have a simple data structure for analytics.

## Features of SCD2 and append-only approach {#scd2}

[Slowly changing dimensions (type 2) or SCD2 (Type 2)](https://en.wikipedia.org/wiki/Slowly_changing_dimension#Type_2) is an approach where when an attribute of the dimension changes, a new record is created, and the old one is marked as not current. Thus, the history of changes is preserved. This approach is used when:

- it is required to track the history of data changes;
- it is required to perform data analysis with regard to time periods;
- it is important to preserve the audit trail of changes;
- it is required to have the ability to restore the state of data at a specific point in time.

## Available guides

The section discusses different technical ways to implement these mechanisms:

* Using the combination of [Change Data Capture (CDC)](../../../concepts/cdc.md) and [Transfer](../../../concepts/transfer.md) for automatic streaming replication of changes from source tables.
    * [{#T}](scd1-transfer.md)
    * [{#T}](scd2-transfer.md)
* Using periodic YQL queries that process batches of changes from the intermediate table and merge them into the main SCD table.
    * [{#T}](scd2-merge.md)