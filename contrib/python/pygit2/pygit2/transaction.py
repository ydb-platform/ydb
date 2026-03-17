# Copyright 2010-2025 The pygit2 contributors
#
# This file is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License, version 2,
# as published by the Free Software Foundation.
#
# In addition to the permissions in the GNU General Public License,
# the authors give you unlimited permission to link the compiled
# version of this file into combinations with other programs,
# and to distribute those combinations without any restriction
# coming from the use of this file.  (The General Public License
# restrictions do apply in other respects; for example, they cover
# modification of the file, and distribution when not linked into
# a combined executable.)
#
# This file is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; see the file COPYING.  If not, write to
# the Free Software Foundation, 51 Franklin Street, Fifth Floor,
# Boston, MA 02110-1301, USA.

from __future__ import annotations

import threading
from typing import TYPE_CHECKING

from .errors import check_error
from .ffi import C, ffi
from .utils import to_bytes

if TYPE_CHECKING:
    from ._pygit2 import Oid, Signature
    from .repository import BaseRepository


class ReferenceTransaction:
    """Context manager for transactional reference updates.

    A transaction allows multiple reference updates to be performed atomically.
    All updates are applied when the transaction is committed, or none are applied
    if the transaction is rolled back.

    Example:
        with repo.transaction() as txn:
            txn.lock_ref('refs/heads/master')
            txn.set_target('refs/heads/master', new_oid, message='Update master')
            # Changes committed automatically on context exit
    """

    def __init__(self, repository: BaseRepository) -> None:
        self._repository = repository
        self._transaction = ffi.new('git_transaction **')
        self._tx = None
        self._thread_id = threading.get_ident()

        err = C.git_transaction_new(self._transaction, repository._repo)
        check_error(err)
        self._tx = self._transaction[0]

    def _check_thread(self) -> None:
        """Verify transaction is being used from the same thread that created it."""
        current_thread = threading.get_ident()
        if current_thread != self._thread_id:
            raise RuntimeError(
                f'Transaction created in thread {self._thread_id} '
                f'but used in thread {current_thread}. '
                'Transactions must be used from the thread that created them.'
            )

    def lock_ref(self, refname: str) -> None:
        """Lock a reference in preparation for updating it.

        Args:
            refname: Name of the reference to lock (e.g., 'refs/heads/master')
        """
        self._check_thread()
        if self._tx is None:
            raise ValueError('Transaction already closed')

        c_refname = ffi.new('char[]', to_bytes(refname))
        err = C.git_transaction_lock_ref(self._tx, c_refname)
        check_error(err)

    def set_target(
        self,
        refname: str,
        target: Oid | str,
        signature: Signature | None = None,
        message: str | None = None,
    ) -> None:
        """Set the target of a direct reference.

        The reference must be locked first via lock_ref().

        Args:
            refname: Name of the reference to update
            target: Target OID or hex string
            signature: Signature for the reflog (None to use repo identity)
            message: Message for the reflog
        """
        self._check_thread()
        if self._tx is None:
            raise ValueError('Transaction already closed')

        from ._pygit2 import Oid

        c_refname = ffi.new('char[]', to_bytes(refname))

        # Convert target to OID
        if isinstance(target, str):
            target = Oid(hex=target)

        c_oid = ffi.new('git_oid *')
        ffi.buffer(c_oid)[:] = target.raw

        c_sig = signature._pointer if signature else ffi.NULL
        c_msg = ffi.new('char[]', to_bytes(message)) if message else ffi.NULL

        err = C.git_transaction_set_target(self._tx, c_refname, c_oid, c_sig, c_msg)
        check_error(err)

    def set_symbolic_target(
        self,
        refname: str,
        target: str,
        signature: Signature | None = None,
        message: str | None = None,
    ) -> None:
        """Set the target of a symbolic reference.

        The reference must be locked first via lock_ref().

        Args:
            refname: Name of the reference to update
            target: Target reference name (e.g., 'refs/heads/master')
            signature: Signature for the reflog (None to use repo identity)
            message: Message for the reflog
        """
        self._check_thread()
        if self._tx is None:
            raise ValueError('Transaction already closed')

        c_refname = ffi.new('char[]', to_bytes(refname))
        c_target = ffi.new('char[]', to_bytes(target))
        c_sig = signature._pointer if signature else ffi.NULL
        c_msg = ffi.new('char[]', to_bytes(message)) if message else ffi.NULL

        err = C.git_transaction_set_symbolic_target(
            self._tx, c_refname, c_target, c_sig, c_msg
        )
        check_error(err)

    def remove(self, refname: str) -> None:
        """Remove a reference.

        The reference must be locked first via lock_ref().

        Args:
            refname: Name of the reference to remove
        """
        self._check_thread()
        if self._tx is None:
            raise ValueError('Transaction already closed')

        c_refname = ffi.new('char[]', to_bytes(refname))
        err = C.git_transaction_remove(self._tx, c_refname)
        check_error(err)

    def commit(self) -> None:
        """Commit the transaction, applying all queued updates."""
        self._check_thread()
        if self._tx is None:
            raise ValueError('Transaction already closed')

        err = C.git_transaction_commit(self._tx)
        check_error(err)

    def __enter__(self) -> ReferenceTransaction:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self._check_thread()
        # Only commit if no exception occurred
        if exc_type is None and self._tx is not None:
            self.commit()

        # Always free the transaction
        if self._tx is not None:
            C.git_transaction_free(self._tx)
            self._tx = None

    def __del__(self) -> None:
        if self._tx is not None:
            C.git_transaction_free(self._tx)
            self._tx = None
