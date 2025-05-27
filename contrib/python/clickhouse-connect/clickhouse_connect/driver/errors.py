from clickhouse_connect.driver.context import BaseQueryContext
from clickhouse_connect.driver.exceptions import DataError


#  Error codes used in the Cython API
NO_ERROR = 0
NONE_IN_NULLABLE_COLUMN = 1

error_messages = {NONE_IN_NULLABLE_COLUMN: 'Invalid None value in non-Nullable column'}


def handle_error(error_num: int, ctx: BaseQueryContext):
    if error_num > 0:
        msg = error_messages[error_num]
        if ctx.column_name:
            msg = f'{msg}, column name: `{ctx.column_name}`'
        raise DataError(msg)
