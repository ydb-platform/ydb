from .. import Vector, HalfVector, SparseVector


async def register_vector(conn, schema='public'):
    await conn.set_type_codec(
        'vector',
        schema=schema,
        encoder=Vector._to_db_binary,
        decoder=Vector._from_db_binary,
        format='binary'
    )

    try:
        await conn.set_type_codec(
            'halfvec',
            schema=schema,
            encoder=HalfVector._to_db_binary,
            decoder=HalfVector._from_db_binary,
            format='binary'
        )

        await conn.set_type_codec(
            'sparsevec',
            schema=schema,
            encoder=SparseVector._to_db_binary,
            decoder=SparseVector._from_db_binary,
            format='binary'
        )
    except ValueError as e:
        if not str(e).startswith('unknown type:'):
            raise e
