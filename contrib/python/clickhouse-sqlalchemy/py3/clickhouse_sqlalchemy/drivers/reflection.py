from sqlalchemy.engine import reflection

from clickhouse_sqlalchemy import Table, engines


class ClickHouseInspector(reflection.Inspector):
    def reflect_table(self, table, *args, **kwargs):
        # This check is necessary to support direct instantiation of
        # `clickhouse_sqlalchemy.Table` and then reflection of it.
        if not isinstance(table, Table):
            table.metadata.remove(table)
            ch_table = Table._make_from_standard(
                table, _extend_on=kwargs.get('_extend_on')
            )
        else:
            ch_table = table

        super(ClickHouseInspector, self).reflect_table(
            ch_table, *args, **kwargs
        )

        with self._operation_context() as conn:
            schema = conn.schema_for_object(ch_table)

            self._reflect_engine(ch_table.name, schema, ch_table)

    def _reflect_engine(self, table_name, schema, table):
        should_reflect = (
            self.dialect.supports_engine_reflection and
            self.dialect.engine_reflection
        )
        if not should_reflect:
            return

        engine_cls_by_name = {e.__name__: e for e in engines.__all__}

        e = self.get_engine(table_name, schema=table.schema)
        if not e:
            raise ValueError("Cannot find engine for table '%s'" % table_name)

        engine_cls = engine_cls_by_name.get(e['engine'])
        if engine_cls is not None:
            engine = engine_cls.reflect(table, **e)
            engine._set_parent(table)
        else:
            table.engine = None

    def get_engine(self, table_name, schema=None, **kw):
        with self._operation_context() as conn:
            return self.dialect.get_engine(
                conn, table_name, schema=schema, info_cache=self.info_cache,
                **kw
            )
