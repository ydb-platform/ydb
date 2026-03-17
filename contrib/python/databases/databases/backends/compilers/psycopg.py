from sqlalchemy.dialects.postgresql.psycopg import PGCompiler_psycopg


class APGCompiler_psycopg2(PGCompiler_psycopg):
    def construct_params(self, *args, **kwargs):
        pd = super().construct_params(*args, **kwargs)

        for column in self.prefetch:
            pd[column.key] = self._exec_default(column.default)

        return pd

    def _exec_default(self, default):
        if default.is_callable:
            return default.arg(self.dialect)
        else:
            return default.arg
