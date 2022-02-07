Examples
===============

ydb
^^^

Create table
------------

::

              ... create an instance of Driver ...

              description = (
                  ydb.TableDescription()
                  .with_primary_keys('key1', 'key2')
                  .with_columns(
                      ydb.Column('key1', ydb.OptionalType(ydb.PrimitiveType.Uint64)),
                      ydb.Column('key2', ydb.OptionalType(ydb.PrimitiveType.Uint64)),
                      ydb.Column('value', ydb.OptionalType(ydb.PrimitiveType.Utf8))
                  )
                  .with_profile(
                      ydb.TableProfile()
                      .with_partitioning_policy(
                          ydb.PartitioningPolicy()
                          .with_explicit_partitions(
                              ydb.ExplicitPartitions(
                                  (
                                      ydb.KeyBound((100, )),
                                      ydb.KeyBound((300, 100)),
                                      ydb.KeyBound((400, )),
                                  )
                              )
                          )
                      )
                  )
              )

              session = driver.table_client.session().create()
              session.create_table('/my/table/', description)


Read table
----------
::

            .... initialize driver and session ....

            key_prefix_type = ydb.TupleType().add_element(
                ydb.OptionalType(ydb.PrimitiveType.Uint64).add_element(
                ydb.OptionalType(ydb.PrimitiveType.Utf8))
            async_table_iterator = session.read_table(
                '/my/table',
                columns=('KeyColumn0', 'KeyColumn1', 'ValueColumn'),
                ydb.KeyRange(
                    ydb.KeyBound((100, 'hundred'), key_prefix_type)
                    ydb.KeyBound((200, 'two-hundreds'), key_prefix_type)
                )
            )

            while True:
                try:
                    chunk_future = next(table_iterator)
                    chunk = chunk_future.result()  # or any other way to await
                    ... additional data processing ...
                except StopIteration:
                    break
