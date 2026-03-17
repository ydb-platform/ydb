#------------------------------------------------------------------------------
# Copyright (c) 2020, 2024, Oracle and/or its affiliates.
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
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
# lob.pyx
#
# Cython file defining the thin Lob implementation class (embedded in
# thin_impl.pyx).
#------------------------------------------------------------------------------

cdef class BaseThinLobImpl(BaseLobImpl):

    cdef:
        BaseThinConnImpl _conn_impl
        bytes _locator
        bint _has_metadata
        uint64_t _size
        uint32_t _chunk_size

    cdef LobOpMessage _create_close_message(self):
        """
        Create the message needed to close a LOB.
        """
        cdef LobOpMessage message
        message = self._conn_impl._create_message(LobOpMessage)
        message.operation = TNS_LOB_OP_CLOSE
        message.source_lob_impl = self
        return message

    cdef LobOpMessage _create_create_temp_message(self):
        """
        Create the message needed to create a temp LOB.
        """
        cdef LobOpMessage message
        self._locator = bytes(40)
        message = self._conn_impl._create_message(LobOpMessage)
        message.operation = TNS_LOB_OP_CREATE_TEMP
        message.amount = TNS_DURATION_SESSION
        message.send_amount = True
        message.source_lob_impl = self
        message.source_offset = self.dbtype._csfrm
        message.dest_offset = self.dbtype._ora_type_num
        return message

    cdef LobOpMessage _create_get_chunk_size_message(self):
        """
        Create the message needed to return the chunk size for a LOB.
        """
        cdef LobOpMessage message
        message = self._conn_impl._create_message(LobOpMessage)
        message.operation = TNS_LOB_OP_GET_CHUNK_SIZE
        message.source_lob_impl = self
        message.send_amount = True
        return message

    cdef LobOpMessage _create_get_is_open_message(self):
        """
        Create the message needed to return if a LOB is open.
        """
        cdef LobOpMessage message
        message = self._conn_impl._create_message(LobOpMessage)
        message.operation = TNS_LOB_OP_IS_OPEN
        message.source_lob_impl = self
        return message

    cdef LobOpMessage _create_get_size_message(self):
        """
        Create the message needed to return the size of a LOB.
        """
        cdef LobOpMessage message
        message = self._conn_impl._create_message(LobOpMessage)
        message.operation = TNS_LOB_OP_GET_LENGTH
        message.source_lob_impl = self
        message.send_amount = True
        return message

    cdef LobOpMessage _create_open_message(self):
        """
        Create the message needed to open a LOB.
        """
        cdef LobOpMessage message
        message = self._conn_impl._create_message(LobOpMessage)
        message.operation = TNS_LOB_OP_OPEN
        message.source_lob_impl = self
        message.amount = TNS_LOB_OPEN_READ_WRITE
        message.send_amount = True
        return message

    cdef LobOpMessage _create_read_message(self, uint64_t offset,
                                           uint64_t amount):
        """
        Create the message needed to read data from a LOB.
        """
        cdef LobOpMessage message
        message = self._conn_impl._create_message(LobOpMessage)
        message.operation = TNS_LOB_OP_READ
        message.source_lob_impl = self
        message.source_offset = offset
        message.amount = amount
        message.send_amount = True
        return message

    cdef LobOpMessage _create_trim_message(self, uint64_t new_size):
        """
        Create the message needed to trim a LOB.
        """
        cdef LobOpMessage message
        message = self._conn_impl._create_message(LobOpMessage)
        message.operation = TNS_LOB_OP_TRIM
        message.source_lob_impl = self
        message.amount = new_size
        message.send_amount = True
        return message

    cdef LobOpMessage _create_write_message(self, object value,
                                            uint64_t offset):
        """
        Create the message needed to write data to a LOB.
        """
        cdef LobOpMessage message
        message = self._conn_impl._create_message(LobOpMessage)
        message.operation = TNS_LOB_OP_WRITE
        message.source_lob_impl = self
        message.source_offset = offset
        if self.dbtype._ora_type_num == TNS_DATA_TYPE_BLOB:
            if not isinstance(value, bytes):
                raise TypeError("only bytes can be written to BLOBs")
            message.data = value
        else:
            if not isinstance(value, str):
                raise TypeError(
                    "only strings can be written to CLOBs and NCLOBS"
                )
            message.data = (<str> value).encode(self._get_encoding())
        return message

    cdef LobOpMessage _create_file_exists_message(self):
        """
        Create a message needed to return if BFILE exists.
        """
        cdef LobOpMessage message
        message = self._conn_impl._create_message(LobOpMessage)
        message.operation = TNS_LOB_OP_FILE_EXISTS
        message.source_lob_impl = self
        return message

    cdef LobOpMessage _create_file_close_message(self):
        """
        Create a message needed to close a file
        """
        cdef LobOpMessage message
        message = self._conn_impl._create_message(LobOpMessage)
        message.operation = TNS_LOB_OP_FILE_CLOSE
        message.source_lob_impl = self
        return message

    cdef LobOpMessage _create_file_open_message(self):
        """
        Create a message needed to open a file
        """
        cdef LobOpMessage message
        message = self._conn_impl._create_message(LobOpMessage)
        message.operation = TNS_LOB_OP_FILE_OPEN
        message.source_lob_impl = self
        message.amount = TNS_LOB_OPEN_READ_ONLY
        message.send_amount = True
        return message

    cdef LobOpMessage _create_get_file_is_open_message(self):
        """
        Create a message needed to return if a file is open
        """
        cdef LobOpMessage message
        message = self._conn_impl._create_message(LobOpMessage)
        message.operation = TNS_LOB_OP_FILE_ISOPEN
        message.source_lob_impl = self
        return message

    cdef const char* _get_encoding(self):
        """
        Return the encoding used by the LOB.
        """
        if self.dbtype._csfrm == CS_FORM_NCHAR \
                or self._locator[TNS_LOB_LOC_OFFSET_FLAG_3] & \
                TNS_LOB_LOC_FLAGS_VAR_LENGTH_CHARSET:
            return ENCODING_UTF16
        return ENCODING_UTF8

    def free_lob(self):
        """
        Internal method for closing a temp LOB during the next piggyback.
        """
        cdef:
            uint8_t flags1 = self._locator[TNS_LOB_LOC_OFFSET_FLAG_1]
            uint8_t flags4 = self._locator[TNS_LOB_LOC_OFFSET_FLAG_4]
        if flags1 & TNS_LOB_LOC_FLAGS_ABSTRACT \
                or flags4 & TNS_LOB_LOC_FLAGS_TEMP:
            if self._conn_impl._temp_lobs_to_close is None:
                self._conn_impl._temp_lobs_to_close = []
            self._conn_impl._temp_lobs_to_close.append(self._locator)
            self._conn_impl._temp_lobs_total_size += len(self._locator)
            self._conn_impl = None

    def get_max_amount(self):
        """
        Internal method for returning the maximum amount that can be read.
        """
        return 2**32 - 1

    def get_file_name(self):
        """
        Internal method for returning a 2-tuple constaining the directory.
        """
        cdef:
            const char_type *ptr = self._locator
            uint16_t dir_name_offset, file_name_offset
            uint16_t dir_name_len, file_name_len
        dir_name_offset = TNS_LOB_LOC_FIXED_OFFSET + 2
        dir_name_len = unpack_uint16(&ptr[TNS_LOB_LOC_FIXED_OFFSET],
                                     BYTE_ORDER_MSB)
        file_name_offset = dir_name_offset + dir_name_len + 2
        file_name_len = unpack_uint16(&ptr[dir_name_offset + dir_name_len],
                                      BYTE_ORDER_MSB)
        return (
            ptr[dir_name_offset:dir_name_offset + dir_name_len].decode(),
            ptr[file_name_offset:file_name_offset + file_name_len].decode()
        )

    def set_file_name(self, str dir_alias, str name):
        """
        Internal method for setting the directory alias and file name
        associated with a BFILE LOB.
        """
        cdef char_type dir_length[2]
        cdef char_type name_length[2]
        pack_uint16(dir_length, len(dir_alias), BYTE_ORDER_MSB)
        pack_uint16(name_length, len(name), BYTE_ORDER_MSB)
        self._locator = self._locator[:TNS_LOB_LOC_FIXED_OFFSET] + \
                dir_length[:2] + dir_alias.encode() + name_length[:2] + \
                name.encode()


cdef class ThinLobImpl(BaseThinLobImpl):

    cdef inline int _process_message(self, LobOpMessage message) except -1:
        """
        Process the message.
        """
        cdef Protocol protocol = <Protocol> self._conn_impl._protocol
        protocol._process_single_message(message)

    def close(self):
        """
        Internal method for closing a LOB that was opened earlier.
        """
        if self.dbtype._ora_type_num == TNS_DATA_TYPE_BFILE:
            self._process_message(self._create_file_close_message())
        else:
            self._process_message(self._create_close_message())

    def create_temp(self):
        """
        Internal method for creating a temporary LOB.
        """
        self._process_message(self._create_create_temp_message())

    def file_exists(self):
        """
        Internal method for returning whether the file referenced by a BFILE
        exists.
        """
        cdef LobOpMessage message
        message = self._create_file_exists_message()
        self._process_message(message)
        return message.bool_flag

    def get_chunk_size(self):
        """
        Internal method for returning the chunk size of the LOB.
        """
        cdef LobOpMessage message
        if self._has_metadata:
            return self._chunk_size
        message = self._create_get_chunk_size_message()
        self._process_message(message)
        return message.amount

    def get_is_open(self):
        """
        Internal method for returning whether the LOB is open or not.
        """
        cdef LobOpMessage message
        if self.dbtype._ora_type_num == TNS_DATA_TYPE_BFILE:
            message = self._create_get_file_is_open_message()
        else:
            message = self._create_get_is_open_message()
        self._process_message(message)
        return message.bool_flag

    def get_size(self):
        """
        Internal method for returning the size of a LOB.
        """
        cdef LobOpMessage message
        if self._has_metadata:
            return self._size
        message = self._create_get_size_message()
        self._process_message(message)
        return message.amount

    def open(self):
        """
        Internal method for opening a LOB.
        """
        if self.dbtype._ora_type_num == TNS_DATA_TYPE_BFILE:
            self._process_message(self._create_file_open_message())
        else:
            self._process_message(self._create_open_message())

    def read(self, uint64_t offset, uint64_t amount):
        """
        Internal method for reading a portion (or all) of the data in the LOB.
        """
        cdef:
            bint should_close = False
            LobOpMessage message
        if self.dbtype._ora_type_num == TNS_DATA_TYPE_BFILE:
            if not self.get_is_open():
                should_close = True
                self.open()
        message = self._create_read_message(offset, amount)
        self._process_message(message)
        if should_close:
            self.close()
        if message.data is None:
            if self.dbtype._ora_type_num in (TNS_DATA_TYPE_BLOB,
                                             TNS_DATA_TYPE_BFILE):
                return b""
            return ""
        return message.data

    def trim(self, uint64_t new_size):
        """
        Internal method for trimming the data in the LOB to the new size.
        """
        self._process_message(self._create_trim_message(new_size))
        self._has_metadata = False

    def write(self, object value, uint64_t offset):
        """
        Internal method for writing data to the LOB object.
        """
        self._process_message(self._create_write_message(value, offset))
        self._has_metadata = False


cdef class AsyncThinLobImpl(BaseThinLobImpl):

    async def _process_message(self, LobOpMessage message):
        """
        Process the message.
        """
        cdef BaseAsyncProtocol protocol
        protocol = <BaseAsyncProtocol> self._conn_impl._protocol
        await protocol._process_single_message(message)

    async def close(self):
        """
        Internal method for closing a LOB that was opened earlier.
        """
        if self.dbtype._ora_type_num == TNS_DATA_TYPE_BFILE:
            await self._process_message(self._create_file_close_message())
        else:
            await self._process_message(self._create_close_message())

    async def create_temp(self):
        """
        Internal method for creating a temporary LOB.
        """
        await self._process_message(self._create_create_temp_message())

    async def get_chunk_size(self):
        """
        Internal method for returning the chunk size of the LOB.
        """
        cdef LobOpMessage message
        if self._has_metadata:
            return self._chunk_size
        message = self._create_get_chunk_size_message()
        await self._process_message(message)
        return message.amount

    async def file_exists(self):
        """
        Internal method returning a boolean indicating if file referenced by a
        BFILE exists.
        """
        cdef LobOpMessage message
        message = self._create_file_exists_message()
        await self._process_message(message)
        return message.bool_flag

    async def get_is_open(self):
        """
        Internal method for returning whether the LOB is open or not.
        """
        cdef LobOpMessage message
        if self.dbtype._ora_type_num == TNS_DATA_TYPE_BFILE:
            message = self._create_get_file_is_open_message()
        else:
            message = self._create_get_is_open_message()
        await self._process_message(message)
        return message.bool_flag

    async def get_size(self):
        """
        Internal method for returning the size of a LOB.
        """
        cdef LobOpMessage message
        if self._has_metadata:
            return self._size
        message = self._create_get_size_message()
        await self._process_message(message)
        return message.amount

    async def open(self):
        """
        Internal method for opening a LOB.
        """
        if self.dbtype._ora_type_num == TNS_DATA_TYPE_BFILE:
            await self._process_message(self._create_file_open_message())
        else:
            await self._process_message(self._create_open_message())

    async def read(self, uint64_t offset, uint64_t amount):
        """
        Internal method for reading a portion (or all) of the data in the LOB.
        """
        cdef:
            bint should_close = False
            LobOpMessage message
        if self.dbtype._ora_type_num == TNS_DATA_TYPE_BFILE:
            if not await self.get_is_open():
                should_close = True
                await self.open()
        message = self._create_read_message(offset, amount)
        await self._process_message(message)
        if should_close:
            await self.close()
        if message.data is None:
            if self.dbtype._ora_type_num in (TNS_DATA_TYPE_BLOB,
                                             TNS_DATA_TYPE_BFILE):
                return b""
            return ""
        return message.data

    async def trim(self, uint64_t new_size):
        """
        Internal method for trimming the data in the LOB to the new size
        """
        await self._process_message(self._create_trim_message(new_size))
        self._has_metadata = False

    async def write(self, object value, uint64_t offset):
        """
        Internal method for writing data to the LOB object.
        """
        await self._process_message(self._create_write_message(value, offset))
        self._has_metadata = False
