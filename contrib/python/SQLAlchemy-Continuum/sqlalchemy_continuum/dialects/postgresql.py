import sqlalchemy as sa

from sqlalchemy_continuum.plugins import PropertyModTrackerPlugin

trigger_sql = """
CREATE TRIGGER {trigger_name}
AFTER INSERT OR UPDATE OR DELETE ON {table_name}
FOR EACH ROW EXECUTE PROCEDURE {procedure_name}()
"""

upsert_cte_sql = """
WITH upsert as
(
    UPDATE {version_table_name}
    SET {update_values}
    WHERE
        {transaction_column} = transaction_id_value
        AND
        {primary_key_criteria}
    RETURNING *
)
INSERT INTO {version_table_name}
({transaction_column}, {operation_type_column}, {column_names})
SELECT
    transaction_id_value,
    {operation_type},
    {insert_values}
WHERE NOT EXISTS (SELECT 1 FROM upsert);
"""

temporary_transaction_sql = """
CREATE TEMP TABLE IF NOT EXISTS {temporary_transaction_table}
({transaction_table_columns})
ON COMMIT DELETE ROWS;
"""

insert_temporary_transaction_sql = """
INSERT INTO {temporary_transaction_table} ({transaction_table_columns})
VALUES ({transaction_values});
"""

temp_transaction_trigger_sql = """
CREATE TRIGGER transaction_trigger
AFTER INSERT ON {transaction_table}
FOR EACH ROW EXECUTE PROCEDURE transaction_temp_table_generator()
"""

procedure_sql = """
CREATE OR REPLACE FUNCTION {procedure_name}() RETURNS TRIGGER AS $$
DECLARE transaction_id_value INT;
BEGIN
    BEGIN
        transaction_id_value = (SELECT id FROM temporary_transaction);
    EXCEPTION WHEN others THEN
        RETURN NEW;
    END;
    IF transaction_id_value IS NULL THEN
        RETURN NEW;
    END IF;

    IF (TG_OP = 'INSERT') THEN
        {after_insert}
        {upsert_insert}
    ELSIF (TG_OP = 'UPDATE') THEN
        IF (hstore(NEW.*) - hstore(OLD.*) - ARRAY[{excluded_columns}]::text[])
            = hstore('')
        THEN
            RETURN NULL;
        END IF;
        {after_update}
        {upsert_update}
    ELSIF (TG_OP = 'DELETE') THEN
        {after_delete}
        {upsert_delete}
    END IF;
    RETURN NEW;
END;
$$
LANGUAGE plpgsql
"""

validity_sql = """
UPDATE {version_table_name}
SET {end_transaction_column} = transaction_id_value
WHERE
    {transaction_column} = (
        SELECT MIN({transaction_column}) FROM {version_table_name}
        WHERE {end_transaction_column} IS NULL AND {primary_key_criteria}
    ) AND
    {primary_key_criteria};
"""


def uses_property_mod_tracking(manager):
    return any(
        isinstance(plugin, PropertyModTrackerPlugin) for plugin in manager.plugins
    )


class SQLConstruct:
    def __init__(
        self,
        table,
        transaction_column_name,
        operation_type_column_name,
        version_table_name_format,
        excluded_columns=None,
        update_validity_for_tables=None,
        use_property_mod_tracking=False,
        end_transaction_column_name=None,
    ):
        self.update_validity_for_tables = update_validity_for_tables
        self.operation_type_column_name = operation_type_column_name
        self.transaction_column_name = transaction_column_name
        self.end_transaction_column_name = end_transaction_column_name
        self.version_table_name_format = version_table_name_format
        self.use_property_mod_tracking = use_property_mod_tracking
        self.table = table
        self.excluded_columns = excluded_columns
        if update_validity_for_tables is None:
            self.update_validity_for_tables = []
        if self.excluded_columns is None:
            self.excluded_columns = []

    @property
    def table_name(self):
        if self.table.schema:
            return f'{self.table.schema}."{self.table.name}"'
        else:
            return '"' + self.table.name + '"'

    @property
    def transaction_table_name(self):
        if self.table.schema:
            return f'{self.table.schema}.transaction'
        else:
            return 'transaction'

    @property
    def temporary_transaction_table_name(self):
        return 'temporary_transaction'

    @property
    def version_table_name(self):
        version_table_name = self.version_table_name_format % self.table.name
        if self.table.schema:
            version_table_name = f'{self.table.schema}.{version_table_name}'
        return version_table_name

    @classmethod
    def for_manager(self, manager, cls):
        strategy = manager.option(cls, 'strategy')
        operation_type_column = manager.option(cls, 'operation_type_column_name')
        excluded_columns = [
            c.name
            for c in sa.inspect(cls).columns
            if manager.is_excluded_column(cls, c)
        ]
        return self(
            update_validity_for_tables=(
                sa.inspect(cls).tables if strategy == 'validity' else []
            ),
            version_table_name_format=manager.option(cls, 'table_name'),
            operation_type_column_name=operation_type_column,
            transaction_column_name=manager.option(cls, 'transaction_column_name'),
            end_transaction_column_name=manager.option(
                cls, 'end_transaction_column_name'
            ),
            use_property_mod_tracking=uses_property_mod_tracking(manager),
            excluded_columns=excluded_columns,
            table=cls.__table__,
        )

    @property
    def columns(self):
        return [c for c in self.table.c if c.name not in self.excluded_columns]

    @property
    def columns_without_pks(self):
        return [c for c in self.columns if not c.primary_key]

    @property
    def pk_columns(self):
        return [c for c in self.columns if c.primary_key]

    def copy_args(self):
        return {k: v for k, v in self.__dict__.items() if not k.startswith('__')}


class UpsertSQL(SQLConstruct):
    builders = {
        'update_values': ', ',
        'insert_values': ', ',
        'column_names': ', ',
        'primary_key_criteria': ' AND ',
    }

    def __init__(self, *args, **kwargs):
        SQLConstruct.__init__(self, *args, **kwargs)

        for key in self.builders:
            setattr(self, key, getattr(self, f'build_{key}')())

    def build_column_names(self):
        column_names = [f'"{c.name}"' for c in self.columns]
        if self.use_property_mod_tracking:
            column_names += [f'{c.name}_mod' for c in self.columns_without_pks]
        return column_names

    def build_primary_key_criteria(self):
        return [f'"{c.name}" = NEW."{c.name}"' for c in self.columns if c.primary_key]

    def build_update_values(self):
        parent_columns = [f'"{c.name}" = NEW."{c.name}"' for c in self.columns]
        mod_columns = []
        if self.use_property_mod_tracking:
            mod_columns = [
                f'{c.name}_mod = {c.name}_mod OR OLD."{c.name}" IS DISTINCT FROM NEW."{c.name}"'
                for c in self.columns_without_pks
            ]

        return [f'{self.operation_type_column_name} = 1'] + parent_columns + mod_columns

    def build_insert_values(self):
        values = self.build_values()
        if self.use_property_mod_tracking:
            values += self.build_mod_tracking_values()
        return values

    def build_values(self):
        return [f'NEW."{c.name}"' for c in self.columns]

    def build_mod_tracking_values(self):
        return []

    def __str__(self):
        params = {
            'version_table_name': self.version_table_name,
            'transaction_column': self.transaction_column_name,
            'operation_type': self.operation_type,
            'operation_type_column': self.operation_type_column_name,
            'transaction_table_name': self.transaction_table_name,
        }
        for key, join_operator in self.builders.items():
            params[key] = join_operator.join(getattr(self, key))

        sql = upsert_cte_sql.format(**params)
        return sql


class DeleteUpsertSQL(UpsertSQL):
    operation_type = 2

    def build_primary_key_criteria(self):
        return [f'"{c.name}" = OLD."{c.name}"' for c in self.pk_columns]

    def build_mod_tracking_values(self):
        return ['True'] * len(self.columns_without_pks)

    def build_update_values(self):
        return [f'"{c.name}" = OLD."{c.name}"' for c in self.columns]

    def build_values(self):
        return [f'OLD."{c.name}"' for c in self.columns]


class InsertUpsertSQL(UpsertSQL):
    operation_type = 0

    def build_mod_tracking_values(self):
        return ['True'] * len(self.columns_without_pks)


class UpdateUpsertSQL(UpsertSQL):
    operation_type = 1

    def build_mod_tracking_values(self):
        return [
            f'OLD."{c.name}" IS DISTINCT FROM NEW."{c.name}"'
            for c in self.columns_without_pks
        ]


class ValiditySQL(SQLConstruct):
    @property
    def primary_key_criteria(self):
        return ' AND '.join(f'"{c.name}" = NEW."{c.name}"' for c in self.pk_columns)

    def __str__(self):
        params = {
            'version_table_name': self.version_table_name,
            'transaction_table_name': self.transaction_table_name,
            'transaction_column': self.transaction_column_name,
            'end_transaction_column': self.end_transaction_column_name,
            'primary_key_criteria': self.primary_key_criteria,
        }
        return validity_sql.format(**params)


class InsertValiditySQL(ValiditySQL):
    pass


class UpdateValiditySQL(ValiditySQL):
    pass


class DeleteValiditySQL(ValiditySQL):
    @property
    def primary_key_criteria(self):
        return ' AND '.join(f'{c.name} = OLD."{c.name}"' for c in self.pk_columns)


def get_validity_sql(class_, tables, params):
    params = params.copy()
    del params['table']
    return ''.join(str(class_(table, **params)) for table in tables)


class CreateTriggerSQL(SQLConstruct):
    def __str__(self):
        return trigger_sql.format(
            trigger_name=f'{self.table.name}_trigger',
            table_name=self.table_name,
            procedure_name=f'{self.table.name}_audit',
        )


class TransactionSQLConstruct:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class CreateTemporaryTransactionTableSQL(TransactionSQLConstruct):
    table_name = 'temporary_transaction'

    def __str__(self):
        return temporary_transaction_sql.format(
            temporary_transaction_table=self.table_name,
            transaction_table_columns='id BIGINT, PRIMARY KEY(id)',
        )


class InsertTemporaryTransactionSQL(TransactionSQLConstruct):
    table_name = 'temporary_transaction'
    transaction_values = 'transaction_id_value'

    def __str__(self):
        return insert_temporary_transaction_sql.format(
            temporary_transaction_table=self.table_name,
            transaction_table_columns='id',
            transaction_values=self.transaction_values,
        )


class CreateTriggerFunctionSQL(SQLConstruct):
    def __str__(self):
        args = self.copy_args()
        tables = self.update_validity_for_tables
        after_insert = get_validity_sql(InsertValiditySQL, tables, args)
        after_update = get_validity_sql(UpdateValiditySQL, tables, args)
        after_delete = get_validity_sql(DeleteValiditySQL, tables, args)

        sql = procedure_sql.format(
            procedure_name=f'{self.table.name}_audit',
            excluded_columns=', '.join(f"'{c}'" for c in self.excluded_columns),
            transaction_table_name=self.transaction_table_name,
            after_insert=after_insert,
            after_update=after_update,
            after_delete=after_delete,
            temporary_transaction_sql=(CreateTemporaryTransactionTableSQL()),
            insert_temporary_transaction_sql=(InsertTemporaryTransactionSQL()),
            upsert_insert=InsertUpsertSQL(**args),
            upsert_update=UpdateUpsertSQL(**args),
            upsert_delete=DeleteUpsertSQL(**args),
        )
        return sql


class TransactionTriggerSQL:
    def __init__(self, tx_class):
        self.table = tx_class.__table__

    @property
    def transaction_table_name(self):
        if self.table.schema:
            return f'{self.table.schema}.transaction'
        else:
            return 'transaction'

    def __str__(self):
        return temp_transaction_trigger_sql.format(
            transaction_table=self.transaction_table_name
        )


def create_versioning_trigger_listeners(manager, cls):
    sa.event.listen(
        cls.__table__,
        'after_create',
        sa.schema.DDL(str(CreateTriggerFunctionSQL.for_manager(manager, cls))),
    )
    sa.event.listen(
        cls.__table__,
        'after_create',
        sa.schema.DDL(str(CreateTriggerSQL.for_manager(manager, cls))),
    )
    sa.event.listen(
        cls.__table__,
        'after_drop',
        sa.schema.DDL(
            f'DROP FUNCTION IF EXISTS {cls.__table__.name}_audit()',
        ),
    )


def sync_trigger(session, table_name, **kwargs):
    """
    Synchronizes versioning trigger for given table with given session.

    ::


        sync_trigger(session, 'my_table')
        session.commit()


    :param session: SQLAlchemy session object
    :param table_name: Name of the table to synchronize versioning trigger for
    :params **kwargs: kwargs to pass to create_trigger

    .. versionadded: 1.1.0
    """
    meta = sa.MetaData()
    version_table = sa.Table(table_name, meta, autoload_with=session.connection())
    parent_table = sa.Table(
        table_name[0 : -len('_version')], meta, autoload_with=session.connection()
    )
    excluded_columns = {c.name for c in parent_table.c} - {
        c.name for c in version_table.c if not c.name.endswith('_mod')
    }
    drop_trigger(session, parent_table.name)
    create_trigger(
        session, table=parent_table, excluded_columns=excluded_columns, **kwargs
    )


def create_trigger(
    session,
    table,
    transaction_column_name='transaction_id',
    operation_type_column_name='operation_type',
    version_table_name_format='%s_version',
    excluded_columns=None,
    use_property_mod_tracking=True,
    end_transaction_column_name=None,
):
    params = {
        'table': table,
        'update_validity_for_tables': [],
        'transaction_column_name': transaction_column_name,
        'operation_type_column_name': operation_type_column_name,
        'version_table_name_format': version_table_name_format,
        'excluded_columns': excluded_columns,
        'use_property_mod_tracking': use_property_mod_tracking,
        'end_transaction_column_name': end_transaction_column_name,
    }
    session.execute(sa.text(str(CreateTriggerFunctionSQL(**params))))
    session.execute(sa.text(str(CreateTriggerSQL(**params))))


def drop_trigger(session, table_name):
    session.execute(
        sa.text(f'DROP TRIGGER IF EXISTS {table_name}_trigger ON "{table_name}"')
    )
    session.execute(sa.text(f'DROP FUNCTION IF EXISTS {table_name}_audit()'))
