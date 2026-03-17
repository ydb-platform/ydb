class SQLLineageException(Exception):
    """Base Exception for SQLLineage"""


class UnsupportedStatementException(SQLLineageException):
    """Raised for SQL statement that SQLLineage doesn't support analyzing"""


class InvalidSyntaxException(SQLLineageException):
    """Raised for SQL statement that parser cannot parse"""


class MetaDataProviderException(SQLLineageException):
    """Raised for MetaDataProvider errors"""


class ConfigException(SQLLineageException):
    """Raised for configuration errors"""
