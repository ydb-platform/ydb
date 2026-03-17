import uuid  # noqa: D100
from collections.abc import Iterable, Sized
from typing import TYPE_CHECKING, Any, NoReturn, Optional, Union

import duckdb

if TYPE_CHECKING:
    from pandas.core.frame import DataFrame as PandasDataFrame

    from .catalog import Catalog


from ..conf import SparkConf
from ..context import SparkContext
from ..errors import PySparkTypeError
from ..exception import ContributionsAcceptedError
from .conf import RuntimeConfig
from .dataframe import DataFrame
from .readwriter import DataFrameReader
from .streaming import DataStreamReader
from .types import StructType
from .udf import UDFRegistration

# In spark:
# SparkSession holds a SparkContext
# SparkContext gets created from SparkConf
# At this level the check is made to determine whether the instance already exists and just needs
# to be retrieved or it needs to be created.

# For us this is done inside of `duckdb.connect`, based on the passed in path + configuration
# SparkContext can be compared to our Connection class, and SparkConf to our ClientContext class


# data is a List of rows
# every value in each row needs to be turned into a Value
def _combine_data_and_schema(data: Iterable[Any], schema: StructType) -> list[duckdb.Value]:
    from duckdb import Value

    new_data = []
    for row in data:
        new_row = [Value(x, dtype.duckdb_type) for x, dtype in zip(row, [y.dataType for y in schema])]
        new_data.append(new_row)
    return new_data


class SparkSession:  # noqa: D101
    def __init__(self, context: SparkContext) -> None:  # noqa: D107
        self.conn = context.connection
        self._context = context
        self._conf = RuntimeConfig(self.conn)

    def _create_dataframe(self, data: Union[Iterable[Any], "PandasDataFrame"]) -> DataFrame:
        try:
            import pandas

            has_pandas = True
        except ImportError:
            has_pandas = False
        if has_pandas and isinstance(data, pandas.DataFrame):
            unique_name = f"pyspark_pandas_df_{uuid.uuid1()}"
            self.conn.register(unique_name, data)
            return DataFrame(self.conn.sql(f'select * from "{unique_name}"'), self)

        def verify_tuple_integrity(tuples: list[tuple]) -> None:
            if len(tuples) <= 1:
                return
            expected_length = len(tuples[0])
            for i, item in enumerate(tuples[1:]):
                actual_length = len(item)
                if expected_length == actual_length:
                    continue
                raise PySparkTypeError(
                    error_class="LENGTH_SHOULD_BE_THE_SAME",
                    message_parameters={
                        "arg1": f"data{i}",
                        "arg2": f"data{i + 1}",
                        "arg1_length": str(expected_length),
                        "arg2_length": str(actual_length),
                    },
                )

        if not isinstance(data, list):
            data = list(data)
        verify_tuple_integrity(data)

        def construct_query(tuples: Iterable) -> str:
            def construct_values_list(row: Sized, start_param_idx: int) -> str:
                parameter_count = len(row)
                parameters = [f"${x + start_param_idx}" for x in range(parameter_count)]
                parameters = "(" + ", ".join(parameters) + ")"
                return parameters

            row_size = len(tuples[0])
            values_list = [construct_values_list(x, 1 + (i * row_size)) for i, x in enumerate(tuples)]
            values_list = ", ".join(values_list)

            query = f"""
                select * from (values {values_list})
            """
            return query

        query = construct_query(data)

        def construct_parameters(tuples: Iterable) -> list[list]:
            parameters = []
            for row in tuples:
                parameters.extend(list(row))
            return parameters

        parameters = construct_parameters(data)

        rel = self.conn.sql(query, params=parameters)
        return DataFrame(rel, self)

    def _createDataFrameFromPandas(
        self, data: "PandasDataFrame", types: Union[list[str], None], names: Union[list[str], None]
    ) -> DataFrame:
        df = self._create_dataframe(data)

        # Cast to types
        if types:
            df = df._cast_types(*types)
        # Alias to names
        if names:
            df = df.toDF(*names)
        return df

    def createDataFrame(  # noqa: D102
        self,
        data: Union["PandasDataFrame", Iterable[Any]],
        schema: Optional[Union[StructType, list[str]]] = None,
        samplingRatio: Optional[float] = None,
        verifySchema: bool = True,
    ) -> DataFrame:
        if samplingRatio:
            raise NotImplementedError
        if not verifySchema:
            raise NotImplementedError
        types = None
        names = None

        if isinstance(data, DataFrame):
            raise PySparkTypeError(
                error_class="SHOULD_NOT_DATAFRAME",
                message_parameters={"arg_name": "data"},
            )

        if schema:
            if isinstance(schema, StructType):
                types, names = schema.extract_types_and_names()
            else:
                names = schema

        try:
            import pandas

            has_pandas = True
        except ImportError:
            has_pandas = False
        # Falsey check on pandas dataframe is not defined, so first check if it's not a pandas dataframe
        # Then check if 'data' is None or []
        if has_pandas and isinstance(data, pandas.DataFrame):
            return self._createDataFrameFromPandas(data, types, names)

        # Finally check if a schema was provided
        is_empty = False
        if not data and names:
            # Create NULLs for every type in our dataframe
            is_empty = True
            data = [tuple(None for _ in names)]

        if schema and isinstance(schema, StructType):
            # Transform the data into Values to combine the data+schema
            data = _combine_data_and_schema(data, schema)

        df = self._create_dataframe(data)
        if is_empty:
            rel = df.relation
            # Add impossible where clause
            rel = rel.filter("1=0")
            df = DataFrame(rel, self)

        # Cast to types
        if types:
            df = df._cast_types(*types)
        # Alias to names
        if names:
            df = df.toDF(*names)
        return df

    def newSession(self) -> "SparkSession":  # noqa: D102
        return SparkSession(self._context)

    def range(  # noqa: D102
        self,
        start: int,
        end: Optional[int] = None,
        step: int = 1,
        numPartitions: Optional[int] = None,
    ) -> "DataFrame":
        if numPartitions:
            raise ContributionsAcceptedError

        if end is None:
            end = start
            start = 0

        return DataFrame(self.conn.table_function("range", parameters=[start, end, step]), self)

    def sql(self, sqlQuery: str, **kwargs: Any) -> DataFrame:  # noqa: D102, ANN401
        if kwargs:
            raise NotImplementedError
        relation = self.conn.sql(sqlQuery)
        return DataFrame(relation, self)

    def stop(self) -> None:  # noqa: D102
        self._context.stop()

    def table(self, tableName: str) -> DataFrame:  # noqa: D102
        relation = self.conn.table(tableName)
        return DataFrame(relation, self)

    def getActiveSession(self) -> "SparkSession":  # noqa: D102
        return self

    @property
    def catalog(self) -> "Catalog":  # noqa: D102
        if not hasattr(self, "_catalog"):
            from duckdb.experimental.spark.sql.catalog import Catalog

            self._catalog = Catalog(self)
        return self._catalog

    @property
    def conf(self) -> RuntimeConfig:  # noqa: D102
        return self._conf

    @property
    def read(self) -> DataFrameReader:  # noqa: D102
        return DataFrameReader(self)

    @property
    def readStream(self) -> DataStreamReader:  # noqa: D102
        return DataStreamReader(self)

    @property
    def sparkContext(self) -> SparkContext:  # noqa: D102
        return self._context

    @property
    def streams(self) -> NoReturn:  # noqa: D102
        raise ContributionsAcceptedError

    @property
    def udf(self) -> UDFRegistration:  # noqa: D102
        return UDFRegistration(self)

    @property
    def version(self) -> str:  # noqa: D102
        return "1.0.0"

    class Builder:  # noqa: D106
        def __init__(self) -> None:  # noqa: D107
            pass

        def master(self, name: str) -> "SparkSession.Builder":  # noqa: D102
            # no-op
            return self

        def appName(self, name: str) -> "SparkSession.Builder":  # noqa: D102
            # no-op
            return self

        def remote(self, url: str) -> "SparkSession.Builder":  # noqa: D102
            # no-op
            return self

        def getOrCreate(self) -> "SparkSession":  # noqa: D102
            context = SparkContext("__ignored__")
            return SparkSession(context)

        def config(  # noqa: D102
            self,
            key: Optional[str] = None,
            value: Optional[Any] = None,  # noqa: ANN401
            conf: Optional[SparkConf] = None,
        ) -> "SparkSession.Builder":
            return self

        def enableHiveSupport(self) -> "SparkSession.Builder":  # noqa: D102
            # no-op
            return self

    builder = Builder()


__all__ = ["SparkSession"]
