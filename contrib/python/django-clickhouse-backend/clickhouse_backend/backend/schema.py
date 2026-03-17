import warnings

from django.db.backends.base.schema import (
    BaseDatabaseSchemaEditor,
    _related_non_m2m_objects,
)
from django.db.backends.ddl_references import (
    Columns,
    Expressions,
    IndexName,
    Statement,
    Table,
)
from django.db.models.constraints import CheckConstraint, UniqueConstraint
from django.db.models.expressions import Col, ExpressionList, F
from django.db.models.indexes import IndexExpression

from clickhouse_backend import compat
from clickhouse_backend.driver.escape import escape_param


class ChColumns(Columns):
    def __str__(self):
        def col_str(column, idx):
            col = self.quote_name(column)
            try:
                suffix = self.col_suffixes[idx]
                if suffix:
                    col = "-{}".format(col)
            except IndexError:
                pass
            return col

        return ", ".join(
            col_str(column, idx) for idx, column in enumerate(self.columns)
        )


class DatabaseSchemaEditor(BaseDatabaseSchemaEditor):
    sql_create_table = "CREATE TABLE %(table)s %(on_cluster)s (%(definition)s) ENGINE = %(engine)s %(extra)s"
    sql_rename_table = "RENAME TABLE %(old_table)s TO %(new_table)s %(on_cluster)s"
    sql_delete_table = "DROP TABLE %(table)s %(on_cluster)s"

    sql_create_column = (
        "ALTER TABLE %(table)s %(on_cluster)s ADD COLUMN %(column)s %(definition)s"
    )
    sql_alter_column = "ALTER TABLE %(table)s %(on_cluster)s %(changes)s"
    sql_alter_column_type = "MODIFY COLUMN %(column)s %(type)s"
    sql_alter_column_null = sql_alter_column_type
    sql_alter_column_not_null = sql_alter_column_type
    sql_alter_column_default = "MODIFY COLUMN %(column)s DEFAULT %(default)s"
    sql_alter_column_no_default = "MODIFY COLUMN %(column)s REMOVE DEFAULT"
    sql_alter_column_no_default_null = sql_alter_column_no_default
    sql_delete_column = "ALTER TABLE %(table)s %(on_cluster)s DROP COLUMN %(column)s"
    sql_rename_column = "ALTER TABLE %(table)s %(on_cluster)s RENAME COLUMN %(old_column)s TO %(new_column)s"
    sql_update_with_default = (
        "ALTER TABLE %(table)s %(on_cluster)s UPDATE %(column)s = %(default)s "
        "WHERE %(column)s IS NULL SETTINGS mutations_sync=1"
    )

    sql_create_check = (
        "ALTER TABLE %(table)s %(on_cluster)s ADD CONSTRAINT %(name)s CHECK (%(check)s)"
    )
    sql_delete_check = "ALTER TABLE %(table)s %(on_cluster)s DROP CONSTRAINT %(name)s"

    sql_index = "INDEX %(name)s (%(columns)s) TYPE %(type)s GRANULARITY %(granularity)s"
    sql_create_index = "ALTER TABLE %(table)s %(on_cluster)s ADD " + sql_index
    sql_delete_index = "ALTER TABLE %(table)s %(on_cluster)s DROP INDEX %(name)s"

    sql_create_constraint = "ALTER TABLE %(table)s %(on_cluster)s ADD %(constraint)s"

    sql_alter_column_comment = (
        "ALTER TABLE %(table)s %(on_cluster)s COMMENT COLUMN %(column)s %(comment)s"
    )
    sql_alter_table_comment = (
        "ALTER TABLE %(table)s %(on_cluster)s MODIFY COMMENT %(comment)s"
    )

    def delete_model(self, model):
        """Delete a model from the database."""
        # Handle auto-created intermediary models
        for field in model._meta.local_many_to_many:
            if field.remote_field.through._meta.auto_created:
                self.delete_model(field.remote_field.through)

        # Delete the table
        self.execute(
            self.sql_delete_table
            % {
                "table": self.quote_name(model._meta.db_table),
                "on_cluster": self._get_on_cluster(model),
            }
        )
        # Remove all deferred statements referencing the deleted table.
        for sql in list(self.deferred_sql):
            if isinstance(sql, Statement) and sql.references_table(
                model._meta.db_table
            ):
                self.deferred_sql.remove(sql)

    def _column_check_name(self, field):
        return "_check_%s" % field.column

    def _column_check_sql(self, field):
        db_params = field.db_parameters(connection=self.connection)
        if db_params["check"]:
            return self._check_sql(
                name=self._column_check_name(field),
                check=self.sql_check_constraint % db_params,
            )

    def table_sql(self, model):
        # https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree/#table_engine-mergetree-creating-a-table
        # TODO: Support SAMPLE/TTL/SETTINGS
        """Take a model and return its table definition."""
        # Create column SQL, include constraint and index.
        column_sqls = []
        params = []
        constraints = []
        for field in model._meta.local_fields:
            definition, extra_params = self.column_sql(model, field)
            if definition is None:
                continue
            params.extend(extra_params)
            # Add the SQL to our big list.
            column_sqls.append(
                "%s %s"
                % (
                    self.quote_name(field.column),
                    definition,
                )
            )
            constraints.append(self._column_check_sql(field))
        for constraint in model._meta.constraints:
            if isinstance(constraint, CheckConstraint):
                constraints.append(constraint.constraint_sql(model, self))
        if any(isinstance(c, UniqueConstraint) for c in model._meta.constraints):
            warnings.warn("Clickhouse does not support unique constraint.")

        engine = self._get_engine(model)
        extra_parts = self._model_extra_sql(model, engine)
        sql = self.sql_create_table % {
            "table": self.quote_name(model._meta.db_table),
            "on_cluster": self._get_on_cluster(model),
            "definition": ", ".join(
                str(constraint)
                for constraint in (*column_sqls, *constraints)
                if constraint
            ),
            "engine": self._get_engine_expression(model, engine),
            "extra": " ".join(extra_parts),
        }
        return sql, params

    def _get_on_cluster(self, model, sync=False):
        cluster = getattr(model._meta, "cluster", None)
        if cluster:
            if sync:
                return f"ON CLUSTER {self.quote_name(cluster)} SYNC"
            return f"ON CLUSTER {self.quote_name(cluster)}"
        return ""

    def _get_engine(self, model):
        from clickhouse_backend.models import MergeTree

        return getattr(
            model._meta, "engine", MergeTree(order_by=model._meta.pk.attname)
        )

    def _get_engine_expression(self, model, engine):
        from clickhouse_backend.models.sql import Query

        compiler = Query(model, alias_cols=False).get_compiler(
            connection=self.connection
        )
        engine.source_expressions = tuple(
            Col("", model._meta.get_field(e.name)) if isinstance(e, F) else e
            for e in engine.source_expressions
        )
        return Expressions(model._meta.db_table, engine, compiler, self.quote_value)

    def column_sql(self, model, field, include_default=False):
        # https://clickhouse.com/docs/en/sql-reference/statements/create/table/#create-table-query
        # TODO: Support [MATERIALIZED|EPHEMERAL|ALIAS expr1] [compression_codec] [TTL expr1]
        """
        Take a field and return its column definition.
        The field must already have had set_attributes_from_name() called.
        """
        # Get the column's type and use that as the basis of the SQL
        db_params = field.db_parameters(connection=self.connection)
        sql = db_params["type"]
        params = []
        # Check for fields that aren't actually columns (e.g. M2M)
        if sql is None:
            return None, None
        if field.null and "Nullable" not in sql:  # Compatible with django fields.
            sql = f"Nullable({sql})"
        # Add database default.
        if compat.field_has_db_default(field):
            default_sql, default_params = self.db_default_sql(field)
            sql = f"{sql} DEFAULT {default_sql}"
            params.extend(default_params)
        elif include_default:
            default_value = self.effective_default(field)
            if default_value is not None:
                sql = f"{sql} DEFAULT {self._column_default_sql(field)}"
                params.append(default_value)
        if compat.field_db_comment(field):
            sql = f"{sql} COMMENT %s"
            params.append(field.db_comment)
        return sql, params

    def _model_indexes_sql(self, model):
        """
        Return a list of all index SQL statements for the specified model.
        field indexes and index_together are ignored, only Meta.indexes is considered.
        """
        if not model._meta.managed or model._meta.proxy or model._meta.swapped:
            return []
        output = []

        msg = (
            "Because index requires extra params, such as TYPE and GRANULARITY, "
            "so field level index=True and Meta level index_together is ignored. "
            "Refer to https://clickhouse.com/docs/en/engines/table-engines/"
            "mergetree-family/mergetree/#table_engine-mergetree-data_skipping-indexes"
        )
        if any(field.db_index for field in model._meta.local_fields) or getattr(
            model._meta, "index_together", None
        ):
            warnings.warn(msg)

        for index in model._meta.indexes:
            output.append(index.create_sql(model, self))

        from clickhouse_backend.models.engines import BaseMergeTree

        engine = self._get_engine(model)
        if output and not isinstance(engine, BaseMergeTree):
            raise ValueError(
                "Index manipulation is supported only for tables with "
                "*MergeTree engine (including replicated variants). Refer to "
                "https://clickhouse.com/docs/en/sql-reference/statements/alter/index."
            )
        return output

    def _get_expression(self, model, *expressions):
        if not expressions:
            return ""
        index_expressions = []
        for expression in expressions:
            index_expression = IndexExpression(expression)
            index_expression.set_wrapper_classes(self.connection)
            index_expressions.append(index_expression)
        from clickhouse_backend.models.sql import Query

        query = Query(model, alias_cols=False)
        expression_list = ExpressionList(*index_expressions).resolve_expression(query)
        compiler = query.get_compiler(connection=self.connection)
        return Expressions(
            model._meta.db_table, expression_list, compiler, self.quote_value
        )

    def _model_extra_sql(self, model, engine):
        from clickhouse_backend.models.engines import BaseMergeTree

        if isinstance(engine, BaseMergeTree):
            order_by = engine.order_by
            partition_by = engine.partition_by
            primary_key = engine.primary_key

            if order_by is not None:
                yield "ORDER BY (%s)" % self._get_expression(model, *order_by)
            if partition_by:
                yield "PARTITION BY (%s)" % self._get_expression(model, *partition_by)
            if primary_key is not None:
                yield "PRIMARY KEY (%s)" % self._get_expression(model, *primary_key)
        if engine.settings:
            result = []
            for setting, value in engine.settings.items():
                result.append(f"{setting}={self.quote_value(value)}")
            yield "SETTINGS %s" % ", ".join(result)

    def alter_db_table_comment(self, model, old_db_table_comment, new_db_table_comment):
        self.execute(
            self.sql_alter_table_comment
            % {
                "table": self.quote_name(model._meta.db_table),
                "comment": self.quote_value(new_db_table_comment or ""),
                "on_cluster": self._get_on_cluster(model),
            }
        )

    def add_field(self, model, field):
        """
        Create a field on a model. Usually involves adding a column, but may
        involve adding a table instead (for M2M fields).
        """
        # Special-case implicit M2M tables
        if field.many_to_many and field.remote_field.through._meta.auto_created:
            return self.create_model(field.remote_field.through)
        # Get the column's definition
        definition, params = self.column_sql(model, field, include_default=False)
        # It might not actually have a column behind it
        if definition is None:
            return

        check_sql = self._column_check_sql(field)
        if check_sql:
            self.deferred_sql.append(
                self.sql_create_constraint
                % {
                    "table": self.quote_name(model._meta.db_table),
                    "constraint": check_sql,
                    "on_cluster": self._get_on_cluster(model),
                }
            )

        # Build the SQL and run it
        sql = self.sql_create_column % {
            "table": self.quote_name(model._meta.db_table),
            "column": self.quote_name(field.column),
            "definition": definition,
            "on_cluster": self._get_on_cluster(model),
        }
        self.execute(sql, params)
        # Drop the default if we need to
        # (Django usually does not use in-database defaults)
        if (
            not compat.field_has_db_default(field)
            and self.effective_default(field) is not None
        ):
            # Update existing rows with default value
            sql_update_default = (
                "ALTER TABLE %(table)s %(on_cluster)s UPDATE %(column)s = %(default)s "
                "WHERE 1 SETTINGS mutations_sync=1"
            )
            from clickhouse_backend import models

            if not isinstance(self._get_engine(model), models.Distributed):
                self.execute(
                    sql_update_default
                    % {
                        "table": self.quote_name(model._meta.db_table),
                        "column": self.quote_name(field.column),
                        "default": "%s",
                        "on_cluster": self._get_on_cluster(model),
                    },
                    [self.effective_default(field)],
                )

    def remove_field(self, model, field):
        """
        Remove a field from a model. Usually involves deleting a column,
        but for M2Ms may involve deleting a table.
        """
        # Special-case implicit M2M tables
        if field.many_to_many and field.remote_field.through._meta.auto_created:
            return self.delete_model(field.remote_field.through)
        # It might not actually have a column behind it
        db_params = field.db_parameters(connection=self.connection)
        if db_params["type"] is None:
            return
        # Delete the column
        sql = self.sql_delete_column % {
            "table": self.quote_name(model._meta.db_table),
            "column": self.quote_name(field.column),
            "on_cluster": self._get_on_cluster(model),
        }
        self.execute(sql)
        if db_params["check"]:
            constraint_name = self._column_check_name(field)
            self.execute(self._delete_check_sql(model, constraint_name))
        # Reset connection if required
        if self.connection.features.connection_persists_old_columns:
            self.connection.close()
        # Remove all deferred statements referencing the deleted column.
        for sql in list(self.deferred_sql):
            if isinstance(sql, Statement) and sql.references_column(
                model._meta.db_table, field.column
            ):
                self.deferred_sql.remove(sql)

    def quote_value(self, value):
        if isinstance(value, str):
            value = value.replace("%", "%%")
        return escape_param(value, {})

    def _field_indexes_sql(self, model, field):
        return []

    def _field_data_type(self, field):
        if field.is_relation:
            return field.rel_db_type(self.connection)
        return self.connection.data_types.get(
            field.get_internal_type(),
            field.db_type(self.connection),
        )

    def _field_base_data_types(self, field):
        # Yield base data types for array fields.
        if field.base_field.get_internal_type() == "ArrayField":
            yield from self._field_base_data_types(field.base_field)
        else:
            yield self._field_data_type(field.base_field)

    def _field_should_be_altered(self, old_field, new_field):
        if (not (old_field.concrete or old_field.many_to_many)) and (
            not (new_field.concrete or new_field.many_to_many)
        ):
            return False
        _, old_path, old_args, old_kwargs = old_field.deconstruct()
        _, new_path, new_args, new_kwargs = new_field.deconstruct()
        # Don't alter when:
        # - changing only a field name
        # - changing an attribute that doesn't affect the schema
        # - adding only a db_column and the column name is not changed
        non_database_attrs = (
            "verbose_name",
            # field primary_key is an internal concept of django,
            # clickhouse MergeTree primary_key is a different concept.
            "primary_key",
            # Clickhouse dont have unique constraint
            "unique",
            "blank",
            # Clickhouse don't support inline index, use meta index instead
            "db_index",
            "editable",
            "serialize",
            "unique_for_date",
            "unique_for_month",
            "unique_for_year",
            "help_text",
            "db_column",
            "db_tablespace",
            "auto_created",
            "validators",
            "error_messages",
            "on_delete",
            "related_name",
            "related_query_name",
            "db_collation",
            "limit_choices_to",
            "size",
        )

        from clickhouse_backend.models import EnumField

        if not isinstance(old_field, EnumField):
            old_kwargs.pop("choices", None)
        if not isinstance(new_field, EnumField):
            new_kwargs.pop("choices", None)

        for attr in non_database_attrs:
            old_kwargs.pop(attr, None)
            new_kwargs.pop(attr, None)

        if (
            not new_field.many_to_many
            and old_field.remote_field
            and new_field.remote_field
            and old_field.remote_field.model._meta.db_table
            == new_field.remote_field.model._meta.db_table
        ):
            old_kwargs.pop("to", None)
            new_kwargs.pop("to", None)
        # db_default can take many form but result in the same SQL.
        if (
            compat.dj_ge5
            and old_kwargs.get("db_default")
            and new_kwargs.get("db_default")
            and self.db_default_sql(old_field) == self.db_default_sql(new_field)
        ):
            old_kwargs.pop("db_default")
            new_kwargs.pop("db_default")
        if (
            old_field.concrete
            and new_field.concrete
            and (self.quote_name(old_field.column) != self.quote_name(new_field.column))
        ):
            return True
        if (
            old_field.many_to_many
            and new_field.many_to_many
            and old_field.name != new_field.name
        ):
            return True
        return (old_path, old_args, old_kwargs) != (new_path, new_args, new_kwargs)

    def _get_column_type(self, field):
        column_type = field.db_type(connection=self.connection)
        if field.null and "Nullable" not in column_type:
            column_type = f"Nullable({column_type})"
        return column_type

    def _alter_column_null_sql(self, model, old_field, new_field):
        """
        Hook to specialize column null alteration.

        Return a (sql, params) fragment to set a column to null or non-null
        as required by new_field, or None if no changes are required.
        """
        # Compatible for django fields.
        new_type = self._get_column_type(new_field)
        return (
            self.sql_alter_column_type
            % {
                "column": self.quote_name(new_field.column),
                "type": new_type,
            },
            [],
        )

    def _alter_field(
        self,
        model,
        old_field,
        new_field,
        old_type,
        new_type,
        old_db_params,
        new_db_params,
        strict=False,
    ):
        old_type = self._get_column_type(old_field)
        new_type = self._get_column_type(new_field)
        # Change check constraints?
        if old_db_params["check"] and (
            old_db_params["check"] != new_db_params["check"]
            or old_field.column != new_field.column
        ):
            constraint_name = self._column_check_name(old_field)
            self.execute(self._delete_check_sql(model, constraint_name))
        # Have they renamed the column?
        if old_field.column != new_field.column:
            self.execute(
                self.sql_rename_column
                % {
                    "table": self.quote_name(model._meta.db_table),
                    "old_column": self.quote_name(old_field.column),
                    "new_column": self.quote_name(new_field.column),
                    "type": new_type,
                    "on_cluster": self._get_on_cluster(model),
                }
            )
            # Rename all references to the renamed column.
            for sql in self.deferred_sql:
                if isinstance(sql, Statement):
                    sql.rename_column_references(
                        model._meta.db_table, old_field.column, new_field.column
                    )
        # Next, start accumulating actions to do
        actions = []
        post_actions = []
        # Only if we have a default and there is a change from NULL to NOT NULL
        four_way_default_alteration = (
            new_field.has_default() or compat.field_has_db_default(new_field)
        ) and (old_field.null and not new_field.null)
        # Type or comment change?
        if old_type != new_type or compat.field_db_comment(
            old_field
        ) != compat.field_db_comment(new_field):
            # Should not alter nullable before null values updated.
            if four_way_default_alteration:
                new_field.null = True
                new_type_backup = new_type
                new_type = self._get_column_type(new_field)
            fragment, other_actions = self._alter_column_type_sql(
                model, old_field, new_field, new_type
            )
            if four_way_default_alteration:
                new_field.null = False
                new_type = new_type_backup
            actions.append(fragment)
            post_actions.extend(other_actions)

        if compat.field_has_db_default(new_field):
            if (
                not compat.field_has_db_default(old_field)
                or new_field.db_default != old_field.db_default
            ):
                actions.append(
                    self._alter_column_database_default_sql(model, old_field, new_field)
                )
        elif compat.field_has_db_default(old_field):
            actions.append(
                self._alter_column_database_default_sql(
                    model, old_field, new_field, drop=True
                )
            )
        # When changing a column NULL constraint to NOT NULL with a given
        # default value, we need to perform 4 steps:
        #  1. Add a default for new incoming writes
        #  2. Update existing NULL rows with new default
        #  3. Replace NULL constraint with NOT NULL
        #  4. Drop the default again.
        # Default change?
        needs_database_default = False
        if (
            old_field.null
            and not new_field.null
            and not compat.field_has_db_default(new_field)
        ):
            old_default = self.effective_default(old_field)
            new_default = self.effective_default(new_field)
            if old_default != new_default and new_default is not None:
                needs_database_default = True
                actions.append(
                    self._alter_column_default_sql(model, old_field, new_field)
                )
        if actions:
            sql, params = tuple(zip(*actions))
            sql, params = (", ".join(sql), sum(params, []))
            self.execute(
                self.sql_alter_column
                % {
                    "table": self.quote_name(model._meta.db_table),
                    "changes": sql,
                    "on_cluster": self._get_on_cluster(model),
                },
                params,
            )
        if four_way_default_alteration:
            from clickhouse_backend.models import Distributed

            if not isinstance(self._get_engine(model), Distributed):
                if not compat.field_has_db_default(new_field):
                    default_sql = "%s"
                    params = [new_default]
                else:
                    default_sql, params = self.db_default_sql(new_field)
                # Update existing rows with default value
                self.execute(
                    self.sql_update_with_default
                    % {
                        "table": self.quote_name(model._meta.db_table),
                        "column": self.quote_name(new_field.column),
                        "default": default_sql,
                        "on_cluster": self._get_on_cluster(model),
                    },
                    params,
                )
            # Since we didn't run a NOT NULL change before we need to do it now
            sql, params = self._alter_column_null_sql(model, old_field, new_field)
            self.execute(
                self.sql_alter_column
                % {
                    "table": self.quote_name(model._meta.db_table),
                    "changes": sql,
                    "on_cluster": self._get_on_cluster(model),
                },
                params,
            )
        if post_actions:
            for sql, params in post_actions:
                self.execute(sql, params)
        # Type alteration on primary key? Then we need to alter the column
        # referring to us.
        drop_foreign_keys = (old_field.primary_key and new_field.primary_key) and (
            old_type != new_type
        )
        rels_to_update = []
        if drop_foreign_keys:
            rels_to_update.extend(_related_non_m2m_objects(old_field, new_field))
        # Changed to become primary key?
        if self._field_became_primary_key(old_field, new_field):
            # Update all referencing columns
            rels_to_update.extend(_related_non_m2m_objects(old_field, new_field))
        # Handle our type alters on the other end of rels from the PK stuff above
        for old_rel, new_rel in rels_to_update:
            rel_db_params = new_rel.field.db_parameters(connection=self.connection)
            rel_type = rel_db_params["type"]
            if (
                new_rel.field.null and "Nullable" not in rel_type
            ):  # Compatible with django fields.
                rel_type = "Nullable(%s)" % rel_type
            fragment, other_actions = self._alter_column_type_sql(
                new_rel.related_model, old_rel.field, new_rel.field, rel_type
            )
            self.execute(
                self.sql_alter_column
                % {
                    "table": self.quote_name(new_rel.related_model._meta.db_table),
                    "changes": fragment[0],
                    "on_cluster": self._get_on_cluster(model),
                },
                fragment[1],
            )
            for sql, params in other_actions:
                self.execute(sql, params)
        # Does it have check constraints we need to add?
        if new_db_params["check"] and (
            old_db_params["check"] != new_db_params["check"]
            or old_field.column != new_field.column
        ):
            check_sql = self._column_check_sql(new_field)
            if check_sql:
                self.execute(
                    self.sql_create_constraint
                    % {
                        "table": self.quote_name(model._meta.db_table),
                        "constraint": check_sql,
                        "on_cluster": self._get_on_cluster(model),
                    }
                )
        # Drop the default if we need to
        # (Django usually does not use in-database defaults)
        if needs_database_default:
            changes_sql, params = self._alter_column_default_sql(
                model, old_field, new_field, drop=True
            )
            sql = self.sql_alter_column % {
                "table": self.quote_name(model._meta.db_table),
                "changes": changes_sql,
                "on_cluster": self._get_on_cluster(model),
            }
            self.execute(sql, params)

    def _alter_column_type_sql(self, model, old_field, new_field, new_type):
        if compat.dj_ge42:
            return super()._alter_column_type_sql(
                model, old_field, new_field, new_type, "", ""
            )
        else:
            return super()._alter_column_type_sql(model, old_field, new_field, new_type)

    def _alter_column_comment_sql(self, model, new_field, new_type, new_db_comment):
        return (
            self.sql_alter_column_comment
            % {
                "table": self.quote_name(model._meta.db_table),
                "column": self.quote_name(new_field.column),
                "comment": self._comment_sql(new_db_comment),
                "on_cluster": self._get_on_cluster(model),
            },
            [],
        )

    def _create_index_sql(
        self,
        model,
        *,
        fields=None,
        name=None,
        sql=None,
        suffix="",
        col_suffixes=None,
        type=None,
        granularity=None,
        expressions=None,
        inline=False,
        **kwargs,
    ):
        """
        Return the SQL statement to create the index for one or several fields
        or expressions. `sql` can be specified if the syntax differs from the
        standard (GIS indexes, ...).
        """
        fields = fields or []
        expressions = expressions or []
        from clickhouse_backend.models.sql import Query

        compiler = Query(model, alias_cols=False).get_compiler(
            connection=self.connection,
        )
        columns = [field.column for field in fields]
        sql_create_index = sql or (self.sql_index if inline else self.sql_create_index)
        table = model._meta.db_table

        def create_index_name(*args, **kwargs):
            nonlocal name
            if name is None:
                name = self._create_index_name(*args, **kwargs)
            return self.quote_name(name)

        return Statement(
            sql_create_index,
            table=Table(table, self.quote_name),
            on_cluster=self._get_on_cluster(model),
            name=IndexName(table, columns, suffix, create_index_name),
            columns=(
                Columns(table, columns, self.quote_name, col_suffixes)
                if columns
                else Expressions(table, expressions, compiler, self.quote_value)
            ),
            type=Expressions(table, type, compiler, self.quote_value),
            granularity=granularity,
        )

    def _delete_index_sql(self, model, name, sql=None):
        return Statement(
            sql or self.sql_delete_index,
            table=Table(model._meta.db_table, self.quote_name),
            name=self.quote_name(name),
            on_cluster=self._get_on_cluster(model),
        )

    def alter_unique_together(self, model, old_unique_together, new_unique_together):
        """for django or other third party app, ignore unique constraint.
        User defined app should never use UniqueConstraint"""
        pass

    def alter_db_table(self, model, old_db_table, new_db_table):
        """Rename the table a model points to."""
        if old_db_table == new_db_table:
            return
        self.execute(
            self.sql_rename_table
            % {
                "old_table": self.quote_name(old_db_table),
                "new_table": self.quote_name(new_db_table),
                "on_cluster": self._get_on_cluster(model),
            }
        )
        # Rename all references to the old table name.
        for sql in self.deferred_sql:
            if isinstance(sql, Statement):
                sql.rename_table_references(old_db_table, new_db_table)

    def _create_check_sql(self, model, name, check):
        return Statement(
            self.sql_create_check,
            table=Table(model._meta.db_table, self.quote_name),
            name=self.quote_name(name),
            check=check,
            on_cluster=self._get_on_cluster(model),
        )

    def _delete_check_sql(self, model, name):
        return self._delete_constraint_sql(self.sql_delete_check, model, name)

    def _delete_constraint_sql(self, template, model, name):
        return Statement(
            template,
            table=Table(model._meta.db_table, self.quote_name),
            name=self.quote_name(name),
            on_cluster=self._get_on_cluster(model),
        )

    def add_constraint(self, model, constraint):
        """Add a constraint to a model."""
        from clickhouse_backend import models

        if isinstance(self._get_engine(model), models.Distributed):
            raise TypeError("Distributed table engine does not support constraints.")
        sql = constraint.create_sql(model, self)
        if sql:
            # Constraint.create_sql returns interpolated SQL which makes
            # params=None a necessity to avoid escaping attempts on execution.
            self.execute(sql, params=None)

    def remove_constraint(self, model, constraint):
        """Remove a constraint from a model."""
        from clickhouse_backend import models

        if isinstance(self._get_engine(model), models.Distributed):
            raise TypeError("Distributed table engine does not support constraints.")
        sql = constraint.remove_sql(model, self)
        if sql:
            self.execute(sql)
