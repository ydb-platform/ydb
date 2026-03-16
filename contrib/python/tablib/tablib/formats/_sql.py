"""Tablib - SQL INSERT Export Support."""
import datetime
import decimal

from ..exceptions import UnsupportedFormat


class SQLFormat:
    """Export Dataset rows as SQL INSERT statements."""
    title = 'sql'
    extensions = ('sql',)

    @staticmethod
    def _render_literal(value):
        """Render a Python value as an SQL literal."""
        if value is None:
            return 'NULL'
        if isinstance(value, bool):
            return 'TRUE' if value else 'FALSE'
        if isinstance(value, (int, decimal.Decimal)):
            return str(value)
        if isinstance(value, float):
            # Represent finite floats; non-finite as NULL
            try:
                if value == value and value not in (float('inf'), -float('inf')):
                    return repr(value)
            except Exception:
                pass
            return 'NULL'
        if isinstance(value, datetime.datetime):
            # ANSI SQL timestamp literal
            return f"TIMESTAMP '{value.isoformat(sep=' ')}'"
        if isinstance(value, datetime.date):
            # ANSI SQL date literal
            return f"DATE '{value.isoformat()}'"
        # Fallback for strings and others
        text = str(value).replace("'", "''")
        return f"'{text}'"

    @classmethod
    def export_set(cls, dataset, table=None, columns=None, commit=False):
        """
        Return SQL INSERT statements for Dataset rows.
        :param table: optional table name; defaults to dataset.title or 'data'
        """
        tbl = table or getattr(dataset, 'title', None) or 'export_table'
        tbl_ident = str(tbl)
        columns_headers = (','.join(
                columns if columns is not None else
                dataset.headers if dataset.headers is not None else []
            )
        )
        columns_headers = f' ({columns_headers})' if columns_headers else ''
        statements = []
        for row in dataset._data:
            values = ', '.join(cls._render_literal(v) for v in row)
            statements.append(
                f'INSERT INTO {tbl_ident}{columns_headers} VALUES ({values});'
            )
        return '\n'.join(statements) + '\n' + ('COMMIT;\n' if commit else '')

    @classmethod
    def import_set(cls, dataset, in_stream, **kwargs):
        """Importing SQL is not supported."""
        raise UnsupportedFormat('SQL import is not supported.')

    @classmethod
    def detect(cls, stream):
        """Always return False: no autodetect for SQL."""
        return False
