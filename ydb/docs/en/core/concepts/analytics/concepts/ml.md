# Machine Learning

{{ydb-short-name}} serves as an effective platform for storing and processing data in ML pipelines. You can use familiar tools, such as Jupyter Notebook and Apache Spark, throughout all stages of the ML model lifecycle.

## Feature Engineering

Use {{ydb-short-name}} as an engine for feature engineering:

* SQL and [dbt](../../../integrations/migration/dbt.md): execute complex analytical queries to aggregate raw data and create new features. Materialize feature sets into row-based tables for fast access;
* Apache Spark: for more complex transformations that require Python or Scala logic, use the [Apache Spark connector](../../../integrations/ingestion/spark.md) to read data, process it, and save the results back to {{ydb-short-name}}.

## Model Training

{{ydb-short-name}} can serve as a fast and scalable data source for model training:

* Jupyter Integration: connect to {{ydb-short-name}} from [Jupyter Notebook](../../../integrations/gui/jupyter.md) for ad-hoc analysis and model prototyping;
* distributed training: the Apache Spark connector enables parallel reading of data from all cluster nodes directly into a Spark DataFrame. This allows you to load training sets for models in PySpark MLlib, CatBoost, Scikit-learn, and other libraries.

## Online Feature Store

The combination of [row-based](../../../concepts/datamodel/table.md#row-oriented-tables) (OLTP) and [columnar](../../../concepts/datamodel/table.md#column-oriented-tables) (OLAP) tables in {{ydb-short-name}} allows you to implement not only an analytical warehouse but also an [Online Feature Store](https://en.wikipedia.org/wiki/Feature_engineering#Feature_stores) on a single platform.

* Use row-based (OLTP) tables to store features that require low-latency point reads; this allows ML models to retrieve features in real time for inference.
* Use columnar (OLAP) tables to store historical data and for the batch calculation of these features.
