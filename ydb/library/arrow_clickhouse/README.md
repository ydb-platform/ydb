ClickHouse aggregate functions over Apache Arrow primitives
--------

This library is a modified ClickHouse (https://github.com/ClickHouse/ClickHouse/) code that uses Apache Arrow
(https://arrow.apache.org/) primitives instead of ClickHouse native ones. I.e. it uses arrow::RecordBatch
instead of DB::Block, arrow::Array and arrow::Builder instead of DB::IColumn and so on.
The redefinition of types is in arrow_clickhouse_types.h header.

The library uses DataStreams primitives that were replaced by processors in ClickHouse 20.3. It's not possible to
extract processors from ClickHouse code base. It's too monolithic and depends on specific multithreading model.

The core reason of library is posibility to use ClickHouse's GROUP BY code (Aggregator.cpp) and aggregate fucntions
(AggregateFunctions directory) with minimal modifications over data presented in Apache Arrow formats.

Original ClickHouse support 2-level aggregation and several optiumizations (LowCardinality, Sparse data, LLVM).
Also it allows to add functions combinators to aggregate functions. Such optimizations are not implemented here yet.
