import os
import tempfile
from io import BytesIO
import re
import pandas as pd
import pyarrow as pa
from chdb import query as chdb_query


class Table:
    """
    Table is a wrapper of multiple formats of data buffer, including parquet file path,
    parquet bytes, and pandas dataframe.
    if use_memfd is True, will try using memfd_create to create a temp file in memory, which is
    only available on Linux. If failed, will fallback to use tempfile.mkstemp to create a temp file
    """

    def __init__(
        self,
        parquet_path: str = None,
        temp_parquet_path: str = None,
        parquet_memoryview: memoryview = None,
        dataframe: pd.DataFrame = None,
        arrow_table: pa.Table = None,
        use_memfd: bool = False,
    ):
        """
        Initialize a Table object with one of parquet file path, parquet bytes, pandas dataframe or
        parquet table.
        """
        self._parquet_path = parquet_path
        self._temp_parquet_path = temp_parquet_path
        self._parquet_memoryview = parquet_memoryview
        self._dataframe = dataframe
        self._arrow_table = arrow_table
        self.use_memfd = use_memfd
        self._rows_read = 0
        self._bytes_read = 0
        self._elapsed = 0

    def __del__(self):
        if self._temp_parquet_path is not None:
            try:
                os.remove(self._temp_parquet_path)
            except OSError:
                pass

    def rows_read(self):
        return self._rows_read

    def bytes_read(self):
        return self._bytes_read

    def elapsed(self):
        return self._elapsed

    def to_pandas(self) -> pd.DataFrame:
        if self._dataframe is None:
            if self._arrow_table is not None:
                return self._arrow_table.to_pandas()
            elif self._parquet_memoryview is not None:
                # wrap bytes to ReadBuffer
                pq_reader = BytesIO(self._parquet_memoryview.tobytes())
                return pandas_read_parquet(pq_reader)
            elif self._parquet_path is not None:
                return pandas_read_parquet(self._parquet_path)
            elif self._temp_parquet_path is not None:
                return pandas_read_parquet(self._temp_parquet_path)
            else:
                raise ValueError("No data buffer in Table object")
        return self._dataframe

    def flush_to_disk(self):
        """
        Flush the data in memory to disk.
        """
        if self._parquet_path is not None or self._temp_parquet_path is not None:
            return

        if self._dataframe is not None:
            self._df_to_disk(self._dataframe)
            self._dataframe = None
        elif self._arrow_table is not None:
            self._arrow_table_to_disk(self._arrow_table)
            self._arrow_table = None
        elif self._parquet_memoryview is not None:
            self._memoryview_to_disk(self._parquet_memoryview)
            self._parquet_memoryview = None
        else:
            raise ValueError("No data in Table object")

    def _df_to_disk(self, df):
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            df.to_parquet(tmp)
            self._temp_parquet_path = tmp.name

    def _arrow_table_to_disk(self, arrow_table):
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            pa.parquet.write_table(arrow_table, tmp.name)
            self._temp_parquet_path = tmp.name

    def _memoryview_to_disk(self, memoryview):
        # copy memoryview to temp file
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            tmp.write(memoryview.tobytes())
            self._temp_parquet_path = tmp.name

    def __repr__(self):
        return repr(self.to_pandas())

    def __str__(self):
        return str(self.to_pandas())

    def query(self, sql: str, **kwargs) -> "Table":
        """
        Query on current Table object, return a new Table object.
        The `FROM` table name in SQL should always be `__table__`. eg:
            `SELECT * FROM __table__ WHERE ...`
        """
        self._validate_sql(sql)

        if (
            self._parquet_path is not None
        ):  # if we have parquet file path, run chdb query on it directly is faster
            return self._query_on_path(self._parquet_path, sql, **kwargs)
        elif self._temp_parquet_path is not None:
            return self._query_on_path(self._temp_parquet_path, sql, **kwargs)
        elif self._parquet_memoryview is not None:
            return self.queryParquetBuffer(sql, **kwargs)
        elif self._dataframe is not None:
            return self.queryDF(sql, **kwargs)
        elif self._arrow_table is not None:
            return self.queryArrowTable(sql, **kwargs)
        else:
            raise ValueError("Table object is not initialized correctly")

    # alias sql = query
    sql = query

    def show(self):
        print(self.to_pandas())

    def _query_on_path(self, path, sql, **kwargs):
        new_sql = sql.replace("__table__", f'file("{path}", Parquet)')
        res = chdb_query(new_sql, "Parquet", **kwargs)
        tbl = Table(parquet_memoryview=res.get_memview())
        tbl._rows_read = res.rows_read()
        tbl._bytes_read = res.bytes_read()
        tbl._elapsed = res.elapsed()
        return tbl

    def _validate_sql(self, sql):
        if "__table__" not in sql:
            raise ValueError("SQL should always contain `FROM __table__`")

    def queryParquetBuffer(self, sql: str, **kwargs) -> "Table":
        if "__table__" not in sql:
            raise ValueError("SQL should always contain `FROM __table__`")
        if self._parquet_memoryview is None:
            raise ValueError("Parquet buffer is None")

        temp_path = None
        parquet_fd = -1
        if self.use_memfd:
            parquet_fd = memfd_create("parquet_buffer")
        # if memfd_create failed, use tempfile to create a file descriptor for the memoryview
        if parquet_fd == -1:
            parquet_fd, temp_path = tempfile.mkstemp()
        ffd = os.fdopen(parquet_fd, "wb")
        ffd.write(self._parquet_memoryview.tobytes())
        ffd.flush()
        ret = self._run_on_temp(parquet_fd, temp_path, sql=sql, fmt="Parquet", **kwargs)
        ffd.close()
        if temp_path is not None:
            os.remove(temp_path)
        return ret

    def queryArrowTable(self, sql: str, **kwargs) -> "Table":
        if "__table__" not in sql:
            raise ValueError("SQL should always contain `FROM __table__`")
        if self._arrow_table is None:
            raise ValueError("Arrow table is None")

        temp_path = None
        arrow_fd = -1
        if self.use_memfd:
            arrow_fd = memfd_create("arrow_buffer")
        if arrow_fd == -1:
            arrow_fd, temp_path = tempfile.mkstemp()
        ffd = os.fdopen(arrow_fd, "wb")
        with pa.RecordBatchFileWriter(ffd, self._arrow_table.schema) as writer:
            writer.write_table(self._arrow_table)
        ffd.flush()
        ret = self._run_on_temp(arrow_fd, temp_path, sql=sql, fmt="Arrow", **kwargs)
        ffd.close()
        if temp_path is not None:
            os.remove(temp_path)
        return ret

    def queryDF(self, sql: str, **kwargs) -> "Table":
        if "__table__" not in sql:
            raise ValueError("SQL should always contain `FROM __table__`")
        if self._dataframe is None:
            raise ValueError("Dataframe is None")

        temp_path = None
        parquet_fd = -1
        if self.use_memfd:
            parquet_fd = memfd_create()
        if parquet_fd == -1:
            parquet_fd, temp_path = tempfile.mkstemp()
        ffd = os.fdopen(parquet_fd, "wb")
        self._dataframe.to_parquet(ffd, engine="pyarrow", compression=None)
        ffd.flush()
        ret = self._run_on_temp(parquet_fd, temp_path, sql=sql, fmt="Parquet", **kwargs)
        ffd.close()
        if temp_path is not None:
            os.remove(temp_path)
        return ret

    @staticmethod
    def queryStatic(sql: str, **kwargs) -> "Table":
        """
        Query on multiple Tables, use Table variables as the table name in SQL
        eg.
            table1 = Table(...)
            table2 = Table(...)
            query("SELECT * FROM __table1__ JOIN __table2__ ON ...", table1=table1, table2=table2)
        """
        ansiTablePattern = re.compile(r"__([a-zA-Z][a-zA-Z0-9_]*)__")
        temp_paths = []
        ffds = []

        def replace_table_name(match):
            tableName = match.group(1)
            if tableName not in kwargs:
                raise ValueError(f"Table {tableName} should be passed as a parameter")

            tbl = kwargs[tableName]
            # if tbl is DataFrame, convert it to Table
            if isinstance(tbl, pd.DataFrame):
                tbl = Table(dataframe=tbl)
            elif not isinstance(tbl, Table):
                raise ValueError(
                    f"Table {tableName} should be an instance of Table or DataFrame")

            if tbl._parquet_path is not None:
                return f'file("{tbl._parquet_path}", Parquet)'

            if tbl._temp_parquet_path is not None:
                return f'file("{tbl._temp_parquet_path}", Parquet)'

            temp_path = None
            data_fd = -1

            if tbl.use_memfd:
                data_fd = memfd_create()

            if data_fd == -1:
                data_fd, temp_path = tempfile.mkstemp()
                temp_paths.append(temp_path)

            ffd = os.fdopen(data_fd, "wb")
            ffds.append(ffd)

            if tbl._parquet_memoryview is not None:
                ffd.write(tbl._parquet_memoryview.tobytes())
                ffd.flush()
                os.lseek(data_fd, 0, os.SEEK_SET)
                return f'file("/dev/fd/{data_fd}", Parquet)'

            if tbl._dataframe is not None:
                ffd.write(tbl._dataframe.to_parquet(engine="pyarrow", compression=None))
                ffd.flush()
                os.lseek(data_fd, 0, os.SEEK_SET)
                return f'file("/dev/fd/{data_fd}", Parquet)'

            if tbl._arrow_table is not None:
                with pa.RecordBatchFileWriter(ffd, tbl._arrow_table.schema) as writer:
                    writer.write_table(tbl._arrow_table)
                ffd.flush()
                os.lseek(data_fd, 0, os.SEEK_SET)
                return f'file("/dev/fd/{data_fd}", Arrow)'

            raise ValueError(f"Table {tableName} is not initialized correctly")

        sql = ansiTablePattern.sub(replace_table_name, sql)
        res = chdb_query(sql, "Parquet")

        for fd in ffds:
            fd.close()

        for tmp_path in temp_paths:
            os.remove(tmp_path)

        tbl = Table(parquet_memoryview=res.get_memview())
        tbl._rows_read = res.rows_read()
        tbl._bytes_read = res.bytes_read()
        tbl._elapsed = res.elapsed()
        return tbl

    def _run_on_temp(
        self,
        fd: int,
        temp_path: str = None,
        sql: str = None,
        fmt: str = "Parquet",
        **kwargs,
    ) -> "Table":
        # replace "__table__" with file("temp_path", Parquet) or file("/dev/fd/{parquet_fd}", Parquet)
        if temp_path is not None:
            new_sql = sql.replace("__table__", f'file("{temp_path}", {fmt})')
        else:
            os.lseek(fd, 0, os.SEEK_SET)
            new_sql = sql.replace("__table__", f'file("/dev/fd/{fd}", {fmt})')
        res = chdb_query(new_sql, "Parquet", **kwargs)
        tbl = Table(parquet_memoryview=res.get_memview())
        tbl._rows_read = res.rows_read()
        tbl._bytes_read = res.bytes_read()
        tbl._elapsed = res.elapsed()
        return tbl


def pandas_read_parquet(path) -> pd.DataFrame:
    return pd.read_parquet(path)


def memfd_create(name: str = None) -> int:
    """
    Try to use memfd_create(2) to create a file descriptor with memory.
    Only available on Linux 3.17 or newer with glibc 2.27 or newer.
    """
    if hasattr(os, "memfd_create"):
        try:
            fd = os.memfd_create(name, flags=os.MFD_CLOEXEC)
            return fd
        except:  # noqa
            return -1
    return -1


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run SQL on parquet file")
    parser.add_argument("parquet_path", type=str, help="path to parquet file")
    parser.add_argument("sql", type=str, help="SQL to run")
    parser.add_argument(
        "--use-memfd",
        action="store_true",
        help="use memfd_create to create file descriptor",
    )
    args = parser.parse_args()

    table = Table(parquet_path=args.parquet_path, use_memfd=args.use_memfd)
    print(table.query(args.sql))
