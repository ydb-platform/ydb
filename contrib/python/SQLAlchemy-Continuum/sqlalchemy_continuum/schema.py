import sqlalchemy as sa


def get_end_tx_column_query(
    table, end_tx_column_name='end_transaction_id', tx_column_name='transaction_id'
):
    v1 = sa.alias(table, name='v')
    v2 = sa.alias(table, name='v2')
    v3 = sa.alias(table, name='v3')

    primary_keys = [c.name for c in table.c if c.primary_key]

    tx_criterion = sa.select(sa.func.min(getattr(v3.c, tx_column_name))).where(
        sa.and_(
            getattr(v3.c, tx_column_name) > getattr(v1.c, tx_column_name),
            *[
                getattr(v3.c, pk) == getattr(v1.c, pk)
                for pk in primary_keys
                if pk != tx_column_name
            ],
        )
    )
    tx_criterion = tx_criterion.scalar_subquery()

    columns = [getattr(v1.c, column) for column in primary_keys]
    columns += [getattr(v2.c, tx_column_name).label(end_tx_column_name)]

    return (
        sa.select(*columns)
        .select_from(
            v1.outerjoin(v2, sa.and_(getattr(v2.c, tx_column_name) == tx_criterion))
        )
        .order_by(getattr(v1.c, tx_column_name))
    )


def update_end_tx_column(
    table,
    end_tx_column_name='end_transaction_id',
    tx_column_name='transaction_id',
    conn=None,
):
    """
    Calculates end transaction columns and updates the version table with the
    calculated values. This function can be used for migrating between subquery
    versioning strategy and validity versioning strategy.

    :param table: SQLAlchemy table object
    :param end_tx_column_name: Name of the end transaction column
    :param tx_column_name: Transaction column name
    :param conn:
        Either SQLAlchemy Connection, Engine, Session or Alembic
        Operations object. Basically this should be an object that can execute
        the queries needed to update the end transaction column values.

        If no object is given then this function tries to use alembic.op for
        executing the queries.
    """
    if conn is None:
        from alembic import op

        conn = op.get_bind()

    query = get_end_tx_column_query(
        table, end_tx_column_name=end_tx_column_name, tx_column_name=tx_column_name
    )
    result = conn.execute(query)
    primary_keys = [c.name for c in table.c if c.primary_key]
    for row in result:
        if getattr(row, end_tx_column_name):
            criteria = [getattr(table.c, pk) == getattr(row, pk) for pk in primary_keys]

            update_stmt = (
                table.update()
                .where(sa.and_(*criteria))
                .values({end_tx_column_name: getattr(row, end_tx_column_name)})
            )
            conn.execute(update_stmt)


def get_property_mod_flags_query(
    table,
    tracked_columns,
    mod_suffix='_mod',
    end_tx_column_name='end_transaction_id',
    tx_column_name='transaction_id',
):
    v1 = sa.alias(table, name='v')
    v2 = sa.alias(table, name='v2')
    primary_keys = [c.name for c in table.c if c.primary_key]

    columns = [getattr(v1.c, column) for column in primary_keys]
    columns += [
        (
            sa.or_(
                getattr(v1.c, column) != getattr(v2.c, column),
                getattr(v2.c, tx_column_name).is_(None),
            )
        ).label(column + mod_suffix)
        for column in tracked_columns
    ]
    return (
        sa.select(*columns)
        .select_from(
            v1.outerjoin(
                v2,
                sa.and_(
                    getattr(v2.c, end_tx_column_name) == getattr(v1.c, tx_column_name),
                    *[
                        getattr(v2.c, pk) == getattr(v1.c, pk)
                        for pk in primary_keys
                        if pk != tx_column_name
                    ],
                ),
            )
        )
        .order_by(getattr(v1.c, tx_column_name))
    )


def update_property_mod_flags(
    table,
    tracked_columns,
    mod_suffix='_mod',
    end_tx_column_name='end_transaction_id',
    tx_column_name='transaction_id',
    conn=None,
):
    """
    Update property modification flags for given table and given columns. This
    function can be used for migrating an existing schema to use property mod
    flags (provided by PropertyModTracker plugin).

    :param table: SQLAlchemy table object
    :param mod_suffix: Modification tracking columns suffix
    :param end_tx_column_name: Name of the end transaction column
    :param tx_column_name: Transaction column name
    :param conn:
        Either SQLAlchemy Connection, Engine, Session or Alembic
        Operations object. Basically this should be an object that can execute
        the queries needed to update the property modification flags.

        If no object is given then this function tries to use alembic.op for
        executing the queries.
    """
    if conn is None:
        from alembic import op

        conn = op.get_bind()

    query = get_property_mod_flags_query(
        table,
        tracked_columns,
        mod_suffix=mod_suffix,
        end_tx_column_name=end_tx_column_name,
        tx_column_name=tx_column_name,
    )
    result = conn.execute(query)

    primary_keys = [c.name for c in table.c if c.primary_key]
    for row in result:
        values = {
            column + mod_suffix: getattr(row, column + mod_suffix)
            for column in tracked_columns
            if getattr(row, column + mod_suffix)
        }
        if values:
            criteria = [getattr(table.c, pk) == getattr(row, pk) for pk in primary_keys]
            query = table.update().where(sa.and_(*criteria)).values(values)
            conn.execute(query)
