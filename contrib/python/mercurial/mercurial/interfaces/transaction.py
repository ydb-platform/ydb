# transaction.py - simple journaling scheme for mercurial
#
# This transaction scheme is intended to gracefully handle program
# errors and interruptions. More serious failures like system crashes
# can be recovered with an fsck-like tool. As the whole repository is
# effectively log-structured, this should amount to simply truncating
# anything that isn't referenced in the changelog.
#
# Copyright 2005, 2006 Olivia Mackall <olivia@selenic.com>
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

from __future__ import annotations

import abc

from typing import (
    Callable,
    Collection,
    ContextManager,
    Protocol,
)

from ._basetypes import (
    CallbackCategoryT,
    HgPathT,
    VfsKeyT,
)

JournalEntryT = tuple[HgPathT, int]


class ITransaction(ContextManager, Protocol):
    @property
    @abc.abstractmethod
    def finalized(self) -> bool:
        ...

    @abc.abstractmethod
    def startgroup(self) -> None:
        """delay registration of file entry

        This is used by strip to delay vision of strip offset. The transaction
        sees either none or all of the strip actions to be done."""

    @abc.abstractmethod
    def endgroup(self) -> None:
        """apply delayed registration of file entry.

        This is used by strip to delay vision of strip offset. The transaction
        sees either none or all of the strip actions to be done."""

    @abc.abstractmethod
    def add(self, file: HgPathT, offset: int) -> None:
        """record the state of an append-only file before update"""

    @abc.abstractmethod
    def addbackup(
        self,
        file: HgPathT,
        hardlink: bool = True,
        location: VfsKeyT = b'',
        for_offset: bool | int = False,
    ) -> None:
        """Adds a backup of the file to the transaction

        Calling addbackup() creates a hardlink backup of the specified file
        that is used to recover the file in the event of the transaction
        aborting.

        * `file`: the file path, relative to .hg/store
        * `hardlink`: use a hardlink to quickly create the backup

        If `for_offset` is set, we expect a offset for this file to have been
        previously recorded
        """

    @abc.abstractmethod
    def registertmp(self, tmpfile: HgPathT, location: VfsKeyT = b'') -> None:
        """register a temporary transaction file

        Such files will be deleted when the transaction exits (on both
        failure and success).
        """

    @abc.abstractmethod
    def addfilegenerator(
        self,
        genid: bytes,
        filenames: Collection[HgPathT],
        genfunc: Callable,
        order: int = 0,
        location: VfsKeyT = b'',
        post_finalize: bool = False,
    ) -> None:
        """add a function to generates some files at transaction commit

        The `genfunc` argument is a function capable of generating proper
        content of each entry in the `filename` tuple.

        At transaction close time, `genfunc` will be called with one file
        object argument per entries in `filenames`.

        The transaction itself is responsible for the backup, creation and
        final write of such file.

        The `genid` argument is used to ensure the same set of file is only
        generated once. Call to `addfilegenerator` for a `genid` already
        present will overwrite the old entry.

        The `order` argument may be used to control the order in which multiple
        generator will be executed.

        The `location` arguments may be used to indicate the files are located
        outside of the the standard directory for transaction. It should match
        one of the key of the `transaction.vfsmap` dictionary.

        The `post_finalize` argument can be set to `True` for file generation
        that must be run after the transaction has been finalized.
        """

    @abc.abstractmethod
    def removefilegenerator(self, genid: bytes) -> None:
        """reverse of addfilegenerator, remove a file generator function"""

    @abc.abstractmethod
    def findoffset(self, file: HgPathT) -> int | None:
        ...

    @abc.abstractmethod
    def readjournal(self) -> list[JournalEntryT]:
        ...

    @abc.abstractmethod
    def replace(self, file: HgPathT, offset: int) -> None:
        """
        replace can only replace already committed entries
        that are not pending in the queue
        """

    @abc.abstractmethod
    def nest(self, name: bytes = b'<unnamed>') -> ITransaction:
        ...

    @abc.abstractmethod
    def release(self) -> None:
        ...

    @abc.abstractmethod
    def running(self) -> bool:
        ...

    @abc.abstractmethod
    def addpending(
        self,
        category: CallbackCategoryT,
        callback: Callable[[ITransaction], None],
    ) -> None:
        """add a callback to be called when the transaction is pending

        The transaction will be given as callback's first argument.

        Category is a unique identifier to allow overwriting an old callback
        with a newer callback.
        """

    @abc.abstractmethod
    def writepending(self) -> bool:
        """write pending file to temporary version

        This is used to allow hooks to view a transaction before commit"""

    @abc.abstractmethod
    def hasfinalize(self, category: CallbackCategoryT) -> bool:
        """check is a callback already exist for a category"""

    @abc.abstractmethod
    def addfinalize(
        self,
        category: CallbackCategoryT,
        callback: Callable[[ITransaction], None],
    ) -> None:
        """add a callback to be called when the transaction is closed

        The transaction will be given as callback's first argument.

        Category is a unique identifier to allow overwriting old callbacks with
        newer callbacks.
        """

    @abc.abstractmethod
    def addpostclose(
        self,
        category: CallbackCategoryT,
        callback: Callable[[ITransaction], None],
    ) -> None:
        """add or replace a callback to be called after the transaction closed

        The transaction will be given as callback's first argument.

        Category is a unique identifier to allow overwriting an old callback
        with a newer callback.
        """

    @abc.abstractmethod
    def getpostclose(
        self, category: CallbackCategoryT
    ) -> Callable[[ITransaction], None] | None:
        """return a postclose callback added before, or None"""

    @abc.abstractmethod
    def addabort(
        self,
        category: CallbackCategoryT,
        callback: Callable[[ITransaction], None],
    ) -> None:
        """add a callback to be called when the transaction is aborted.

        The transaction will be given as the first argument to the callback.

        Category is a unique identifier to allow overwriting an old callback
        with a newer callback.
        """

    @abc.abstractmethod
    def addvalidator(
        self,
        category: CallbackCategoryT,
        callback: Callable[[ITransaction], None],
    ) -> None:
        """adds a callback to be called when validating the transaction.

        The transaction will be given as the first argument to the callback.

        callback should raise exception if to abort transaction"""

    @abc.abstractmethod
    def close(self) -> None:
        '''commit the transaction'''

    @abc.abstractmethod
    def abort(self) -> None:
        """abort the transaction (generally called on error, or when the
        transaction is not explicitly committed before going out of
        scope)"""

    @abc.abstractmethod
    def add_journal(self, vfs_id: VfsKeyT, path: HgPathT) -> None:
        ...
