import sqlalchemy as sa


class ColumnReflector:
    def __init__(self, manager, parent_table, model=None):
        self.parent_table = parent_table
        self.model = model
        self.manager = manager

    def option(self, name):
        try:
            return self.manager.option(self.model, name)
        except TypeError:
            return self.manager.options[name]

    def reflect_column(self, column):
        """
        Make a copy of parent table column and some alterations to it.

        :param column: SQLAlchemy Column object of parent table
        """
        # Make a copy of the column so that it does not point to wrong table.
        column_copy = column._copy()
        column_copy.unique = False
        column_copy.onupdate = None
        if column_copy.autoincrement:
            column_copy.autoincrement = False
        if column_copy.name == self.option('transaction_column_name'):
            column_copy.nullable = False

        if not column_copy.primary_key:
            column_copy.nullable = True

        # Find the right column key
        if self.model is not None:
            for key, value in sa.inspect(self.model).columns.items():
                if value is column:
                    column_copy.key = key
        return column_copy

    @property
    def operation_type_column(self):
        """
        Return the operation type column. By default the name of this column
        is 'operation_type'.
        """
        return sa.Column(
            self.option('operation_type_column_name'),
            sa.SmallInteger,
            nullable=False,
            index=True,
        )

    @property
    def transaction_column(self):
        """
        Returns transaction column. By default the name of this column is
        'transaction_id'.
        """
        return sa.Column(
            self.option('transaction_column_name'),
            sa.BigInteger,
            primary_key=True,
            index=True,
            autoincrement=False,  # This is needed for MySQL
        )

    @property
    def end_transaction_column(self):
        """
        Returns end_transaction column. By default the name of this column is
        'end_transaction_id'.
        """
        return sa.Column(
            self.option('end_transaction_column_name'), sa.BigInteger, index=True
        )

    @property
    def reflected_parent_columns(self):
        for column in self.parent_table.c:
            if self.model and self.manager.is_excluded_column(self.model, column):
                continue
            reflected_column = self.reflect_column(column)
            yield reflected_column

    def __iter__(self):
        yield from self.reflected_parent_columns

        # Only yield internal version columns if parent model is not using
        # single table inheritance
        if not self.model or not sa.inspect(self.model).single:
            yield self.transaction_column
            if self.option('strategy') == 'validity':
                yield self.end_transaction_column
            yield self.operation_type_column


class TableBuilder:
    """
    TableBuilder handles the building of version tables based on parent
    table's structure and versioning configuration options.
    """

    def __init__(self, versioning_manager, parent_table, model=None):
        self.manager = versioning_manager
        self.parent_table = parent_table
        self.model = model

    def option(self, name):
        try:
            return self.manager.option(self.model, name)
        except TypeError:
            return self.manager.options[name]

    @property
    def table_name(self):
        """
        Returns the version table name for current parent table.
        """
        return self.option('table_name') % self.parent_table.name

    @property
    def columns(self):
        return list(ColumnReflector(self.manager, self.parent_table, self.model))

    def _build_composite_indexes(self, table):
        """
        Build composite indexes for efficient version queries.

        Creates indexes on (primary_key_columns, transaction_id DESC) which
        significantly speeds up common query patterns like:
        - Finding a specific version of an entity
        - Iterating through version history
        - Point-in-time queries

        The index is only created if create_composite_index option is True
        (default: True).
        """
        if not self.option('create_composite_index'):
            return []

        indexes = []
        tx_column_name = self.option('transaction_column_name')

        # Get primary key columns from the version table itself, excluding
        # the transaction_id column. We use the version table's columns
        # directly because column aliases may cause the column key to differ
        # from the parent table's column name (e.g., id vs _id).
        entity_pk_columns = [
            col for col in table.primary_key.columns if col.name != tx_column_name
        ]

        if not entity_pk_columns:
            return []

        # Build index columns: (pk1, pk2, ..., transaction_id DESC)
        index_columns = list(entity_pk_columns)
        index_columns.append(table.c[tx_column_name].desc())

        # Create a unique index name based on table name
        index_name = f'ix_{table.name}_pk_transaction_id'

        indexes.append(sa.Index(index_name, *index_columns))

        # For validity strategy, also create index for end_transaction_id queries
        if self.option('strategy') == 'validity':
            end_tx_column_name = self.option('end_transaction_column_name')
            validity_index_columns = list(entity_pk_columns)
            validity_index_columns.append(table.c[tx_column_name])
            validity_index_columns.append(table.c[end_tx_column_name])

            validity_index_name = f'ix_{table.name}_pk_validity'
            indexes.append(sa.Index(validity_index_name, *validity_index_columns))

        return indexes

    def __call__(self, extends=None):
        """
        Builds version table.
        """
        columns = self.columns if extends is None else []
        self.manager.plugins.after_build_version_table_columns(self, columns)
        table = sa.schema.Table(
            extends.name if extends is not None else self.table_name,
            self.parent_table.metadata,
            *columns,
            schema=self.parent_table.schema,
            extend_existing=extends is not None,
        )

        # Add composite indexes for efficient version queries
        if extends is None:
            for index in self._build_composite_indexes(table):
                # Indexes are automatically associated with the table when created
                pass

        return table
