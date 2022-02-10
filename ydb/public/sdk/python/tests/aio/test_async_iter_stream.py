import pytest
import ydb


@pytest.mark.asyncio
async def test_read_table(driver, database):

    description = (
        ydb.TableDescription()
        .with_primary_keys("key1")
        .with_columns(
            ydb.Column("key1", ydb.OptionalType(ydb.PrimitiveType.Uint64)),
            ydb.Column("value", ydb.OptionalType(ydb.PrimitiveType.Utf8)),
        )
    )

    session = await driver.table_client.session().create()
    await session.create_table(database + "/test_read_table", description)

    await session.transaction(ydb.SerializableReadWrite()).execute(
        """INSERT INTO `test_read_table` (`key1`, `value`) VALUES (1, "hello_world"), (2, "2")""",
        commit_tx=True,
    )

    expected_res = [[{"key1": 1, "value": "hello_world"}], [{"key1": 2, "value": "2"}]]

    i = 0
    async for resp in await session.read_table(
        database + "/test_read_table", row_limit=1
    ):
        assert resp.rows == expected_res[i]
        i += 1


@pytest.mark.asyncio
async def test_read_shard_table(driver, database):
    session: ydb.ISession = await driver.table_client.session().create()
    test_name = "read_shard_table"
    table_name = database + "/" + test_name

    await session.create_table(
        table_name,
        ydb.TableDescription()
        .with_primary_keys("Key1", "Key2")
        .with_columns(
            ydb.Column("Key1", ydb.OptionalType(ydb.PrimitiveType.Uint64)),
            ydb.Column("Key2", ydb.OptionalType(ydb.PrimitiveType.Uint64)),
            ydb.Column("Value", ydb.OptionalType(ydb.PrimitiveType.Utf8)),
        )
        .with_profile(
            ydb.TableProfile().with_partitioning_policy(
                ydb.PartitioningPolicy().with_uniform_partitions(8)
            )
        ),
    )

    prepared = await session.prepare(
        "DECLARE $data as List<Struct<Key1: Uint64, Key2: Uint64, Value: Utf8>>; "
        "UPSERT INTO {} "
        "SELECT Key1, Key2, Value FROM "
        "AS_TABLE($data);".format(test_name)
    )

    data_by_shard_id = {}
    with session.transaction() as tx:
        max_value = 2 ** 64
        shard_key_bound = max_value >> 3
        data = []

        for shard_id in range(8):
            data_by_shard_id[shard_id] = []

        for idx in range(8 * 8):
            shard_id = idx % 8
            table_row = {
                "Key1": shard_id * shard_key_bound + idx,
                "Key2": idx + 1000,
                "Value": str(idx ** 4),
            }
            data_by_shard_id[shard_id].append(table_row)
            data.append(table_row)

        await tx.execute(prepared, commit_tx=True, parameters={"$data": data})

    iter = await session.read_table(table_name)
    read_data = [item async for t in iter for item in t.rows]
    assert sorted(read_data, key=lambda x: (x["Key1"], x["Key2"])) == sorted(
        data, key=lambda x: (x["Key1"], x["Key2"])
    )
