import logging
import re
import ydb

from typing import List, Any, Optional, Tuple, Dict
from ydb import issues, operation
from ydb.public.api.grpc.ydb_cms_v1_pb2_grpc import CmsServiceStub
from ydb.public.api.protos import ydb_cms_pb2


class ColorFormatter(logging.Formatter):
    COLORS = {
        logging.DEBUG: "\033[36m",  # Cyan
        logging.INFO: "\033[32m",  # Green
        logging.WARNING: "\033[33m",  # Yellow
        logging.ERROR: "\033[31m",  # Red
        logging.CRITICAL: "\033[35m",  # Magenta
    }
    RESET = "\033[0m"

    def format(self, record):
        color = self.COLORS.get(record.levelno, self.RESET)
        message = super().format(record)
        return f"{color}{message}{self.RESET}"


class CmsClient:
    def __init__(self, driver: ydb.Driver, logger: logging.Logger):
        self.logger = logger.getChild("cms_client")
        self.driver = driver

    def create_database(self, path: str, shared_database_path: str):
        self.logger.info(f"Creating serverless database {path} with shared database {shared_database_path}")

        request = ydb_cms_pb2.CreateDatabaseRequest()
        request.path = path
        request.serverless_resources.shared_database_path = shared_database_path
        self.driver(request, CmsServiceStub, "CreateDatabase", operation.Operation)

    def describe_database(self, path: str) -> ydb_cms_pb2.GetDatabaseStatusResult:
        self.logger.debug(f"Describing database {path}")

        def __wrap_describe_database_response(rpc_state, response) -> ydb_cms_pb2.GetDatabaseStatusResult:
            issues._process_response(response.operation)
            message = ydb_cms_pb2.GetDatabaseStatusResult()
            response.operation.result.Unpack(message)
            return message

        request = ydb_cms_pb2.GetDatabaseStatusRequest()
        request.path = path
        return self.driver(request, CmsServiceStub, "GetDatabaseStatus", __wrap_describe_database_response)

    def remove_database(self, path: str):
        self.logger.info(f"Removing database {path}")

        request = ydb_cms_pb2.RemoveDatabaseRequest()
        request.path = path
        self.driver(request, CmsServiceStub, "RemoveDatabase", operation.Operation)


def sanitize_identifier(name: str, fallback_prefix: str, max_length: int = 60) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_]", "_", name or "")
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    if not cleaned:
        cleaned = "x"
    if cleaned[0].isdigit():
        cleaned = fallback_prefix + cleaned
    return cleaned[:max_length]


def sanitize_column_names(names: List[str], fallback_prefix: str, max_length: int = 60) -> List[str]:
    seen: Dict[str, int] = {}
    result: List[str] = []
    for raw in names:
        base = sanitize_identifier(raw, fallback_prefix, max_length=max_length)
        count = seen.get(base, 0)
        seen[base] = count + 1
        result.append(base if count == 0 else f"{base}_{count}")
    return result


def build_upsert_query(
    table_id: str, columns: List[str], types: List[ydb.PrimitiveType], rows: List[List[Any]]
) -> Optional[Tuple[str, dict]]:
    if "__row_id" in columns:
        raise ValueError("`__row_id` column is not allowed")
    if len(columns) != len(types):
        raise ValueError("Number of columns and types must match")
    if len(columns) != len(set(columns)):
        raise ValueError("Columns names must be unique")

    struct_type = ydb.StructType()
    for col, t in zip(columns, types):
        struct_type.add_member(col, ydb.OptionalType(t))
    struct_type.add_member("__row_id", ydb.PrimitiveType.Uint64)
    list_type = ydb.ListType(struct_type)

    rows_data: List[Dict[str, Any]] = []
    for row_id, row in enumerate(rows):
        if len(row) != len(columns):
            raise ValueError("Number of values must match number of columns")

        record = {"__row_id": row_id}
        for col, value, t in zip(columns, row, types):
            if t == ydb.PrimitiveType.Utf8:
                record[col] = str(value)
            elif t == ydb.PrimitiveType.Double:
                record[col] = float(str(value).strip().replace(",", ""))
            else:
                raise ValueError(f"Unsupported type: {t}")
        rows_data.append(record)

    query = f"UPSERT INTO `{table_id}`\nSELECT * FROM AS_TABLE($rows);"
    return query, {"$rows": (rows_data, list_type)}


def build_create_query(table_id: str, columns: List[str], types: List[ydb.PrimitiveType]) -> str:
    _TYPE_MAPPING = {
        ydb.PrimitiveType.Utf8: "Utf8",
        ydb.PrimitiveType.Double: "Double",
    }

    if len(columns) != len(types):
        raise ValueError("Number of columns and types must match")
    if len(columns) != len(set(columns)):
        raise ValueError("Columns names must be unique")

    defs = []
    for col, t in zip(columns, types):
        type_name = _TYPE_MAPPING.get(t)
        if type_name is None:
            raise ValueError(f"Unsupported type: {t}")
        defs.append(f"`{col}` {type_name}")

    defs.append("`__row_id` Uint64 NOT NULL")
    defs_sql = ",\n\t".join(defs)
    return f"CREATE TABLE `{table_id}` (\n\t{defs_sql},\n\tPRIMARY KEY (`__row_id`)\n);"
