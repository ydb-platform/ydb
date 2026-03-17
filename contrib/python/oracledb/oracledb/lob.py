# -----------------------------------------------------------------------------
# Copyright (c) 2021, 2024, Oracle and/or its affiliates.
#
# This software is dual-licensed to you under the Universal Permissive License
# (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl and Apache License
# 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose
# either license.
#
# If you elect to accept the software under the Apache License, Version 2.0,
# the following applies:
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# lob.py
#
# Contains the LOB class for managing BLOB, CLOB, NCLOB and BFILE data.
# -----------------------------------------------------------------------------

from typing import Any, Union

from .base_impl import DB_TYPE_BFILE, DB_TYPE_BLOB
from . import __name__ as MODULE_NAME
from . import errors


class BaseLOB:
    __module__ = MODULE_NAME

    def __del__(self):
        self._impl.free_lob()

    def _check_is_bfile(self):
        if self._impl.dbtype is not DB_TYPE_BFILE:
            errors._raise_err(errors.ERR_OPERATION_ONLY_SUPPORTED_ON_BFILE)

    def _check_not_bfile(self):
        if self._impl.dbtype is DB_TYPE_BFILE:
            errors._raise_err(errors.ERR_OPERATION_NOT_SUPPORTED_ON_BFILE)

    def _check_value_to_write(self, value):
        """
        Check the value to write and return the actual value to write.
        Character LOBs must write strings but can accept UTF-8 encoded bytes
        (which will be decoded to strings). Binary LOBs must write bytes but
        can accept strings (which will be encoded in UTF-8).
        """
        if self.type is DB_TYPE_BLOB:
            if isinstance(value, str):
                return value.encode()
            elif isinstance(value, bytes):
                return value
        else:
            if isinstance(value, str):
                return value
            elif isinstance(value, bytes):
                return value.decode()
        raise TypeError("expecting string or bytes")

    @classmethod
    def _from_impl(cls, impl):
        lob = cls.__new__(cls)
        lob._impl = impl
        return lob

    def getfilename(self) -> tuple:
        """
        Return a two-tuple consisting of the directory alias and file name for
        a BFILE type LOB.
        """
        self._check_is_bfile()
        return self._impl.get_file_name()

    def setfilename(self, dir_alias: str, name: str) -> None:
        """
        Set the directory alias and name of a BFILE type LOB.
        """
        self._check_is_bfile()
        self._impl.set_file_name(dir_alias, name)

    @property
    def type(self) -> Any:
        """
        Returns the type of the LOB as one of the database type constants.
        """
        return self._impl.dbtype


class LOB(BaseLOB):
    __module__ = MODULE_NAME

    def __reduce__(self):
        value = self.read()
        return (type(value), (value,))

    def __str__(self):
        return self.read()

    def close(self) -> None:
        """
        Close the LOB. Call this when writing is completed so that the indexes
        associated with the LOB can be updated -– but only if open() was called
        first.
        """
        self._impl.close()

    def fileexists(self) -> bool:
        """
        Return a boolean indicating if the file referenced by a BFILE type LOB
        exists.
        """
        self._check_is_bfile()
        return self._impl.file_exists()

    def getchunksize(self) -> int:
        """
        Return the chunk size for the LOB. Reading and writing to the LOB in
        chunks of multiples of this size will improve performance.
        """
        self._check_not_bfile()
        return self._impl.get_chunk_size()

    def isopen(self) -> bool:
        """
        Return a boolean indicating if the LOB has been opened using the method
        open().
        """
        return self._impl.get_is_open()

    def open(self) -> None:
        """
        Open the LOB for writing. This will improve performance when writing to
        the LOB in chunks and there are functional or extensible indexes
        associated with the LOB. If this method is not called, each write will
        perform an open internally followed by a close after the write has been
        completed.
        """
        self._impl.open()

    def read(self, offset: int = 1, amount: int = None) -> Union[str, bytes]:
        """
        Return a portion (or all) of the data in the LOB. Note that the amount
        and offset are in bytes for BLOB and BFILE type LOBs and in UCS-2 code
        points for CLOB and NCLOB type LOBs. UCS-2 code points are equivalent
        to characters for all but supplemental characters. If supplemental
        characters are in the LOB, the offset and amount will have to be chosen
        carefully to avoid splitting a character.
        """
        if amount is None:
            amount = self._impl.get_max_amount()
            if amount >= offset:
                amount = amount - offset + 1
            else:
                amount = 1
        elif amount <= 0:
            errors._raise_err(errors.ERR_INVALID_LOB_AMOUNT)
        if offset <= 0:
            errors._raise_err(errors.ERR_INVALID_LOB_OFFSET)
        return self._impl.read(offset, amount)

    def size(self) -> int:
        """
        Returns the size of the data in the LOB. For BLOB and BFILE type LOBs
        this is the number of bytes. For CLOB and NCLOB type LOBs this is the
        number of UCS-2 code points. UCS-2 code points are equivalent to
        characters for all but supplemental characters.
        """
        return self._impl.get_size()

    def trim(self, new_size: int = 0, *, newSize: int = None) -> None:
        """
        Trim the LOB to the new size (the second parameter is deprecated and
        should not be used).
        """
        self._check_not_bfile()
        if newSize is not None:
            if new_size != 0:
                errors._raise_err(
                    errors.ERR_DUPLICATED_PARAMETER,
                    deprecated_name="newSize",
                    new_name="new_size",
                )
            new_size = newSize
        self._impl.trim(new_size)

    def write(self, data: Union[str, bytes], offset: int = 1) -> None:
        """
        Write the data to the LOB at the given offset. The offset is in bytes
        for BLOB type LOBs and in UCS-2 code points for CLOB and NCLOB type
        LOBs. UCS-2 code points are equivalent to characters for all but
        supplemental characters. If supplemental characters are in the LOB, the
        offset will have to be chosen carefully to avoid splitting a character.
        Note that if you want to make the LOB value smaller, you must use the
        trim() function.
        """
        self._check_not_bfile()
        self._impl.write(self._check_value_to_write(data), offset)


class AsyncLOB(BaseLOB):
    __module__ = MODULE_NAME

    async def close(self) -> None:
        """
        Close the LOB. Call this when writing is completed so that the indexes
        associated with the LOB can be updated -– but only if open() was called
        first.
        """
        await self._impl.close()

    async def fileexists(self) -> bool:
        """
        Return a boolean indicating if the file referenced by a BFILE type LOB
        exists.
        """
        self._check_is_bfile()
        return await self._impl.file_exists()

    async def getchunksize(self) -> int:
        """
        Return the chunk size for the LOB. Reading and writing to the LOB in
        chunks of multiples of this size will improve performance.
        """
        self._check_not_bfile()
        return await self._impl.get_chunk_size()

    async def isopen(self) -> bool:
        """
        Return a boolean indicating if the LOB has been opened using the method
        open().
        """
        return await self._impl.get_is_open()

    async def open(self) -> None:
        """
        Open the LOB for writing. This will improve performance when writing to
        the LOB in chunks and there are functional or extensible indexes
        associated with the LOB. If this method is not called, each write will
        perform an open internally followed by a close after the write has been
        completed.
        """
        await self._impl.open()

    async def read(
        self, offset: int = 1, amount: int = None
    ) -> Union[str, bytes]:
        """
        Return a portion (or all) of the data in the LOB. Note that the amount
        and offset are in bytes for BLOB and BFILE type LOBs and in UCS-2 code
        points for CLOB and NCLOB type LOBs. UCS-2 code points are equivalent
        to characters for all but supplemental characters. If supplemental
        characters are in the LOB, the offset and amount will have to be chosen
        carefully to avoid splitting a character.
        """
        if amount is None:
            amount = self._impl.get_max_amount()
            if amount >= offset:
                amount = amount - offset + 1
            else:
                amount = 1
        if offset <= 0:
            errors._raise_err(errors.ERR_INVALID_LOB_OFFSET)
        return await self._impl.read(offset, amount)

    async def size(self) -> int:
        """
        Returns the size of the data in the LOB. For BLOB and BFILE type LOBs
        this is the number of bytes. For CLOB and NCLOB type LOBs this is the
        number of UCS-2 code points. UCS-2 code points are equivalent to
        characters for all but supplemental characters.
        """
        return await self._impl.get_size()

    async def trim(self, new_size: int = 0, *, newSize: int = None) -> None:
        """
        Trim the LOB to the new size (the second parameter is deprecated and
        should not be used).
        """
        self._check_not_bfile()
        if newSize is not None:
            if new_size != 0:
                errors._raise_err(
                    errors.ERR_DUPLICATED_PARAMETER,
                    deprecated_name="newSize",
                    new_name="new_size",
                )
            new_size = newSize
        await self._impl.trim(new_size)

    async def write(self, data: Union[str, bytes], offset: int = 1) -> None:
        """
        Write the data to the LOB at the given offset. The offset is in bytes
        for BLOB type LOBs and in UCS-2 code points for CLOB and NCLOB type
        LOBs. UCS-2 code points are equivalent to characters for all but
        supplemental characters. If supplemental characters are in the LOB, the
        offset will have to be chosen carefully to avoid splitting a character.
        Note that if you want to make the LOB value smaller, you must use the
        trim() function.
        """
        self._check_not_bfile()
        await self._impl.write(self._check_value_to_write(data), offset)
