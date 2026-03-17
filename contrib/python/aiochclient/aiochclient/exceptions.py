class ChClientError(Exception):
    """Raised when:

    - type from user or from Clickhouse is unrecognized;

    - user tries to pass arguments to not insert queries;

    - Clickhouse returns some errors.
    """
