import re
import time

from clickhouse_driver import client

from .escape import escape_params

insert_pattern = re.compile(r"^\s*insert\s+into.+?values\s*;?$", flags=re.IGNORECASE)


class Client(client.Client):
    def __init__(self, *args, **kwargs):
        # https://clickhouse.com/docs/en/sql-reference/data-types/datetime/#usage-remarks
        # The clickhouse-client applies the server time zone by default
        # if a time zone isn’t explicitly set when initializing the data type.
        # To use the client time zone, run clickhouse-client with the --use_client_time_zone parameter.
        settings = kwargs.pop("settings", None) or {}
        settings.setdefault("use_client_time_zone", True)
        kwargs.update(settings=settings)
        super().__init__(*args, **kwargs)

    def substitute_params(self, query, params, context):
        escaped = escape_params(params, context)
        return query % escaped

    def execute(
        self,
        query,
        params=None,
        with_column_types=False,
        external_tables=None,
        query_id=None,
        settings=None,
        types_check=False,
        columnar=False,
    ):
        """Support dict params for INSERT queries."""
        start_time = time.time()

        with self.disconnect_on_error(query, settings):
            is_insert = insert_pattern.match(query)

            if is_insert:
                rv = self.process_insert_query(
                    query,
                    params,
                    external_tables=external_tables,
                    query_id=query_id,
                    types_check=types_check,
                    columnar=columnar,
                )
            else:
                rv = self.process_ordinary_query(
                    query,
                    params=params,
                    with_column_types=with_column_types,
                    external_tables=external_tables,
                    query_id=query_id,
                    types_check=types_check,
                    columnar=columnar,
                )
            self.last_query.store_elapsed(time.time() - start_time)
            return rv
