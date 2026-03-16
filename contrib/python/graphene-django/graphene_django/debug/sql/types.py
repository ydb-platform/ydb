from graphene import Boolean, Float, ObjectType, String


class DjangoDebugSQL(ObjectType):
    class Meta:
        description = "Represents a single database query made to a Django managed DB."

    vendor = String(
        required=True,
        description=(
            "The type of database being used (e.g. postrgesql, mysql, sqlite)."
        ),
    )
    alias = String(
        required=True, description="The Django database alias (e.g. 'default')."
    )
    sql = String(description="The actual SQL sent to this database.")
    duration = Float(
        required=True, description="Duration of this database query in seconds."
    )
    raw_sql = String(
        required=True, description="The raw SQL of this query, without params."
    )
    params = String(
        required=True, description="JSON encoded database query parameters."
    )
    start_time = Float(required=True, description="Start time of this database query.")
    stop_time = Float(required=True, description="Stop time of this database query.")
    is_slow = Boolean(
        required=True,
        description="Whether this database query took more than 10 seconds.",
    )
    is_select = Boolean(
        required=True, description="Whether this database query was a SELECT."
    )

    # Postgres
    trans_id = String(description="Postgres transaction ID if available.")
    trans_status = String(description="Postgres transaction status if available.")
    iso_level = String(description="Postgres isolation level if available.")
    encoding = String(description="Postgres connection encoding if available.")
