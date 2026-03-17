class _Break(BaseException):
    def __init__(self, tx, commit):
        super().__init__()
        self.tx = tx
        self.commit = commit


class GinoTransaction:
    """
    Represents an underlying database transaction and its connection, offering
    methods to manage this transaction.

    :class:`.GinoTransaction` is supposed to be created by either
    :meth:`gino.engine.GinoConnection.transaction`, or
    :meth:`gino.engine.GinoEngine.transaction`, or
    :meth:`gino.api.Gino.transaction`, shown as follows::

        async with db.transaction() as tx:
            ...

        async with engine.transaction() as tx:
            ...

        async with conn.transaction() as tx:
            ...

        tx = await conn.transaction()
        try:
            ...
            await tx.commit()
        except Exception:
            await tx.rollback()
            raise

    When in use with asynchronous context manager, :class:`.GinoTransaction`
    will be in **managed** mode, while the last example with ``await`` will put
    the :class:`.GinoTransaction` in **manual** mode where you have to call
    the :meth:`.commit` or :meth:`.rollback` to manually close the transaction.

    In **managed** mode the transaction will be automatically committed or
    rolled back on exiting the ``async with`` block depending on whether there
    is an exception or not. Meanwhile, you can explicitly exit the transaction
    early by :meth:`.raise_commit` or :meth:`.raise_rollback` which will raise
    an internal exception managed by the asynchronous context manager and
    interpreted as a commit or rollback action. In a nested transaction
    situation, the two exit-early methods always close up the very transaction
    which the two methods are referenced upon - all children transactions are
    either committed or rolled back correspondingly, while no parent
    transaction was ever touched. For example::

        async with db.transaction() as tx1:
            async with db.transaction() as tx2:
                async with db.transaction() as tx3:
                    tx2.raise_rollback()
                    # Won't reach here
                # Won't reach here
            # Continues here with tx1, with both tx2 and tx3 rolled back.

            # For PostgreSQL, tx1 can still be committed successfully because
            # tx2 and tx3 are just SAVEPOINTs in transaction tx1

    .. tip::

        The internal exception raised from :meth:`.raise_commit` and
        :meth:`.raise_rollback` is a subclass of :exc:`BaseException`, so
        normal ``try ... except Exception:`` can't trap the commit or rollback.

    """

    def __init__(self, conn, args, kwargs):
        self._conn = conn
        self._args = args
        self._kwargs = kwargs
        self._tx = None
        self._managed = None

    async def _begin(self):
        raw_conn = await self._conn.get_raw_connection()
        self._tx = self._conn.dialect.transaction(raw_conn, self._args, self._kwargs)
        await self._tx.begin()
        return self

    @property
    def connection(self):
        """
        Accesses to the :class:`~gino.engine.GinoConnection` of this
        transaction. This is useful if when the transaction is started from
        ``db`` or ``engine`` where the connection is implicitly acquired for
        you together with the transaction.

        """
        return self._conn

    @property
    def raw_transaction(self):
        """
        Accesses to the underlying transaction object, whose type depends on
        the dialect in use.

        """
        return self._tx.raw_transaction

    def raise_commit(self):
        """
        Only available in managed mode: skip rest of the code in this
        transaction and commit immediately by raising an internal exception,
        which will be caught and handled by the asynchronous context manager::

            async with db.transaction() as tx:
                await user.update(age=64).apply()
                tx.raise_commit()
                await user.update(age=32).apply()  # won't reach here

            assert user.age == 64  # no exception raised before

        """
        if not self._managed:
            raise AssertionError("Illegal in manual mode, use `commit` instead.")
        raise _Break(self, True)

    async def commit(self):
        """
        Only available in manual mode: manually commit this transaction.

        """
        if self._managed:
            raise AssertionError(
                "Illegal in managed mode, " "use `raise_commit` instead."
            )
        await self._tx.commit()

    def raise_rollback(self):
        """
        Only available in managed mode: skip rest of the code in this
        transaction and rollback immediately by raising an internal exception,
        which will be caught and handled by the asynchronous context manager::

            assert user.age == 64  # assumption

            async with db.transaction() as tx:
                await user.update(age=32).apply()
                tx.raise_rollback()
                await user.update(age=128).apply()  # won't reach here

            assert user.age == 64  # no exception raised before

        """
        if not self._managed:
            raise AssertionError("Illegal in manual mode, use `rollback` instead.")
        raise _Break(self, False)

    async def rollback(self):
        """
        Only available in manual mode: manually rollback this transaction.

        """
        if self._managed:
            raise AssertionError(
                "Illegal in managed mode, " "use `raise_rollback` instead."
            )
        await self._tx.rollback()

    def __await__(self):
        if self._managed is not None:
            raise AssertionError("Cannot start the same transaction twice")
        self._managed = False
        return self._begin().__await__()

    async def __aenter__(self):
        if self._managed is not None:
            raise AssertionError("Cannot start the same transaction twice")
        self._managed = True
        await self._begin()
        return self

    async def __aexit__(self, ex_type, ex, ex_tb):
        is_break = ex_type is _Break
        if is_break and ex.commit or ex_type is None:
            await self._tx.commit()
        else:
            await self._tx.rollback()
        if is_break and ex.tx is self:
            return True
