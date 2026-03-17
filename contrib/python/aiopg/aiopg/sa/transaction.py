from . import exc


class Transaction:
    """Represent a database transaction in progress.

    The Transaction object is procured by
    calling the SAConnection.begin() method of
    SAConnection:

        async with engine as conn:
            trans = await conn.begin()
            try:
                await conn.execute("insert into x (a, b) values (1, 2)")
            except Exception:
                await trans.rollback()
            else:
                await trans.commit()

    The object provides .rollback() and .commit()
    methods in order to control transaction boundaries.

    See also:  SAConnection.begin(), SAConnection.begin_twophase(),
    SAConnection.begin_nested().
    """

    __slots__ = ("_connection", "_parent", "_is_active")

    def __init__(self, connection, parent):
        self._connection = connection
        self._parent = parent or self
        self._is_active = True

    @property
    def is_active(self):
        """Return ``True`` if a transaction is active."""
        return self._is_active

    @property
    def connection(self):
        """Return transaction's connection (SAConnection instance)."""
        return self._connection

    async def close(self):
        """Close this transaction.

        If this transaction is the base transaction in a begin/commit
        nesting, the transaction will rollback().  Otherwise, the
        method returns.

        This is used to cancel a Transaction without affecting the scope of
        an enclosing transaction.
        """
        if not self._parent._is_active:
            return
        if self._parent is self:
            await self.rollback()
        else:
            self._is_active = False

    async def rollback(self):
        """Roll back this transaction."""
        if not self._parent._is_active:
            return
        await self._do_rollback()
        self._is_active = False

    async def _do_rollback(self):
        await self._parent.rollback()

    async def commit(self):
        """Commit this transaction."""

        if not self._parent._is_active:
            raise exc.InvalidRequestError("This transaction is inactive")
        await self._do_commit()
        self._is_active = False

    async def _do_commit(self):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            await self.rollback()
        elif self._is_active:
            await self.commit()


class RootTransaction(Transaction):
    __slots__ = ()

    def __init__(self, connection):
        super().__init__(connection, None)

    async def _do_rollback(self):
        await self._connection._rollback_impl()

    async def _do_commit(self):
        await self._connection._commit_impl()


class NestedTransaction(Transaction):
    """Represent a 'nested', or SAVEPOINT transaction.

    A new NestedTransaction object may be procured
    using the SAConnection.begin_nested() method.

    The interface is the same as that of Transaction class.
    """

    __slots__ = ("_savepoint",)

    def __init__(self, connection, parent):
        super().__init__(connection, parent)
        self._savepoint = None

    async def _do_rollback(self):
        assert self._savepoint is not None, "Broken transaction logic"
        if self._is_active:
            await self._connection._rollback_to_savepoint_impl(
                self._savepoint, self._parent
            )

    async def _do_commit(self):
        assert self._savepoint is not None, "Broken transaction logic"
        if self._is_active:
            await self._connection._release_savepoint_impl(
                self._savepoint, self._parent
            )


class TwoPhaseTransaction(Transaction):
    """Represent a two-phase transaction.

    A new TwoPhaseTransaction object may be procured
    using the SAConnection.begin_twophase() method.

    The interface is the same as that of Transaction class
    with the addition of the .prepare() method.
    """

    __slots__ = ("_is_prepared", "_xid")

    def __init__(self, connection, xid):
        super().__init__(connection, None)
        self._is_prepared = False
        self._xid = xid

    @property
    def xid(self):
        """Returns twophase transaction id."""
        return self._xid

    async def prepare(self):
        """Prepare this TwoPhaseTransaction.

        After a PREPARE, the transaction can be committed.
        """

        if not self._parent.is_active:
            raise exc.InvalidRequestError("This transaction is inactive")
        await self._connection._prepare_twophase_impl(self._xid)
        self._is_prepared = True

    async def _do_rollback(self):
        await self._connection._rollback_twophase_impl(
            self._xid, is_prepared=self._is_prepared
        )

    async def _do_commit(self):
        await self._connection._commit_twophase_impl(
            self._xid, is_prepared=self._is_prepared
        )
