from sqlalchemy.sql.compiler import IdentifierPreparer

from clickhouse_connect.driver.binding import quote_identifier


class ChIdentifierPreparer(IdentifierPreparer):
    quote_identifier = staticmethod(quote_identifier)

    def __init__(self, dialect, **kwargs):
        super().__init__(dialect, **kwargs)
        if getattr(dialect, "server_side_params", False):
            self._double_percents = False

    def _requires_quotes(self, _value):
        return True
