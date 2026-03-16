from sqlalchemy import util as sa_util, types as sqltypes, exc
from sqlalchemy.sql import compiler, ClauseElement, expression
from sqlalchemy.sql.ddl import CreateColumn
from sqlalchemy.sql.elements import TextClause
from sqlalchemy.util import to_list


class ClickHouseDDLCompiler(compiler.DDLCompiler):
    def _get_default_string(self, default, name):
        sa_util.assert_arg_type(
            default, (sa_util.string_types[0], ClauseElement, TextClause), name
        )

        if isinstance(default, sa_util.string_types):
            return self.sql_compiler.render_literal_value(
                default, sqltypes.STRINGTYPE
            )
        else:
            return self.sql_compiler.process(
                default, literal_binds=True, include_table=False
            )

    def get_column_specification(self, column, **kw):
        colspec = (
            self.preparer.format_column(column)
            + " "
            + self.dialect.type_compiler.process(
                column.type, type_expression=column
            )
        )

        opts = column.dialect_options['clickhouse']

        if column.server_default is not None:
            colspec += " DEFAULT " + self._get_default_string(
                column.server_default.arg, 'server_default'
            )
        elif opts['materialized'] is not None:
            colspec += " MATERIALIZED " + self._get_default_string(
                opts['materialized'], 'clickhouse_materialized'
            )
        elif opts['alias'] is not None:
            colspec += " ALIAS " + self._get_default_string(
                opts['alias'], 'clickhouse_alias'
            )

        if column.comment is not None:
            literal = self.sql_compiler.render_literal_value(
                column.comment, sqltypes.String()
            )
            colspec += " COMMENT " + literal

        codec = opts['codec']
        if codec is not None:
            if isinstance(codec, (list, tuple)):
                codec = ', '.join(codec)
            colspec += " CODEC({0})".format(codec)

        if opts['after'] is not None:
            colspec += " AFTER " + self._get_default_string(
                opts['after'], 'clickhouse_after'
            )

        return colspec

    def visit_create_column(self, create, **kw):
        column = create.element
        nullable = column.nullable

        # All columns including synthetic PKs must be 'nullable'
        column.nullable = True

        rv = super(ClickHouseDDLCompiler, self).visit_create_column(
            create, **kw
        )
        column.nullable = nullable

        return rv

    def visit_primary_key_constraint(self, constraint, **kw):
        # Do not render PKs.
        return None

    def visit_foreign_key_constraint(self, constraint, **kw):
        # Do not render FKs.
        return None

    def _compile_param(self, expr, opt_list=False):
        compiler = self.sql_compiler

        # Do not render unnecessary brackets.
        if isinstance(expr, (list, tuple)) and len(expr) == 1 and opt_list:
            expr = expr[0]

        if isinstance(expr, (list, tuple)):
            return '(' + ', '.join(
                self._compile_param(el) for el in expr
            ) + ')'
        if not isinstance(expr, expression.ColumnClause):
            if not hasattr(expr, 'self_group'):
                # assuming base type (int, string, etc.)
                return str(expr)
            else:
                expr = expr.self_group()
        return compiler.process(
            expr, include_table=False, literal_binds=True
        )

    def visit_merge_tree(self, engine):
        param = engine.get_parameters()
        if param is None:
            param = '()'
        else:
            param = self._compile_param(to_list(param))

        text = '{0}{1}\n'.format(engine.name, param)
        if engine.partition_by:
            text += ' PARTITION BY {0}\n'.format(
                self._compile_param(
                    engine.partition_by.get_expressions_or_columns(),
                    opt_list=True
                )
            )
        if engine.order_by:
            text += ' ORDER BY {0}\n'.format(
                self._compile_param(
                    engine.order_by.get_expressions_or_columns(),
                    opt_list=True
                )
            )
        if engine.primary_key:
            if not engine.order_by:
                text += ' ORDER BY {0}\n'.format(
                    self._compile_param(
                        engine.primary_key.get_expressions_or_columns(),
                        opt_list=True
                    )
                )

            text += ' PRIMARY KEY {0}\n'.format(
                self._compile_param(
                    engine.primary_key.get_expressions_or_columns(),
                    opt_list=True
                )
            )
        if engine.sample_by:
            text += ' SAMPLE BY {0}\n'.format(
                self._compile_param(
                    engine.sample_by.get_expressions_or_columns()[0]
                )
            )
        if engine.ttl:
            compile = self.sql_compiler.process
            text += ' TTL {0}\n'.format(
                ',\n     '.join(
                    compile(i, include_table=False, literal_binds=True)
                    for i in engine.ttl.get_expressions_or_columns()
                )
            )
        if engine.settings:
            text += ' SETTINGS ' + ', '.join(
                '{key}={value}'.format(
                    key=key,
                    value=value
                )
                for key, value in sorted(engine.settings.items())
            )
        return text

    def visit_engine(self, engine):
        engine_params = engine.get_parameters()
        text = engine.name
        if not engine_params:
            return text

        text += '('

        compiled_params = []
        for param in engine_params:
            if isinstance(param, tuple):
                compiled = (
                    '('
                    + ', '.join(self._compile_param(p) for p in param)
                    + ')'
                )
            else:
                compiled = self._compile_param(param)

            compiled_params.append(compiled)

        text += ', '.join(compiled_params)

        return text + ')'

    def create_table_suffix(self, table):
        cluster = table.dialect_options['clickhouse']['cluster']
        if cluster:
            return 'ON CLUSTER ' + self.preparer.quote(cluster)

        return ''

    def post_create_table(self, table):
        engine = getattr(table, 'engine', None)

        if not engine:
            raise exc.CompileError("No engine for table '%s'" % table.name)

        text = ' ENGINE = ' + self.process(engine)

        if table.comment is not None:
            literal = self.sql_compiler.render_literal_value(
                table.comment, sqltypes.String()
            )
            text += ' COMMENT ' + literal
        return text

    def visit_create_materialized_view(self, create):
        mv = create.element
        text = '\nCREATE MATERIALIZED VIEW '

        if create.if_not_exists:
            text += 'IF NOT EXISTS '

        text += self.preparer.format_table(mv)

        if mv.cluster:
            text += ' ON CLUSTER ' + self.preparer.quote(mv.cluster)

        if mv.to:
            text += ' TO ' + self.preparer.quote(mv.inner_table.name)

        else:
            text += ' ('

            separator = ''

            for column in mv.inner_table.columns:
                processed = self.process(CreateColumn(column))
                if processed is not None:
                    text += separator
                    separator = ', '
                    text += processed

            text += ')'

            text += self.post_create_table(mv.inner_table)

        text += ' '

        if mv.populate:
            text += 'POPULATE '

        text += 'AS ' + self.sql_compiler.process(
            mv.mv_selectable, literal_binds=True
        )

        return text

    def visit_drop_table(self, drop):
        text = '\nDROP TABLE '

        if getattr(drop, "if_exists", False):
            text += 'IF EXISTS '

        rv = text + self.preparer.format_table(drop.element)

        if getattr(drop, "on_cluster", False):
            rv += (
                ' ON CLUSTER ' +
                self.preparer.quote(drop.on_cluster)
            )

        return rv

    def visit_view(self, drop):
        text = '\nDROP VIEW '

        if getattr(drop, "if_exists", False):
            text += 'IF EXISTS '

        rv = text + self.preparer.format_table(drop.element)

        if getattr(drop, "on_cluster", False):
            rv += (
                ' ON CLUSTER ' +
                self.preparer.quote(drop.on_cluster)
            )

        return rv

    def visit_set_table_comment(self, create, **kw):
        return "ALTER TABLE %s MODIFY COMMENT %s" % (
            self.preparer.format_table(create.element),
            self.sql_compiler.render_literal_value(
                create.element.comment, sqltypes.String()
            )
        )

    def visit_drop_table_comment(self, create, **kw):
        return "ALTER TABLE %s MODIFY COMMENT ''" % (
            self.preparer.format_table(create.element)
        )
