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
# buffer.pyx
#
# Cython file defining the low-level network buffer read and write classes and
# methods for reading and writing low-level data from those buffers (embedded
# in thin_impl.pyx).
#------------------------------------------------------------------------------

cdef enum:
    PACKET_HEADER_SIZE = 8
    CHUNKED_BYTES_CHUNK_SIZE = 65536

cdef struct BytesChunk:
    char_type *ptr
    uint32_t length
    uint32_t allocated_length

cdef struct Rowid:
    uint32_t rba
    uint16_t partition_id
    uint32_t block_num
    uint16_t slot_num

@cython.final
@cython.freelist(20)
cdef class Packet:

    cdef:
        uint32_t packet_size
        uint8_t packet_type
        uint8_t packet_flags
        bytes buf

    cdef inline bint has_end_of_response(self):
        """
        Returns a boolean indicating if the end of request byte is found at the
        end of the packet.
        """
        cdef:
            uint16_t flags
            char *ptr
        ptr = cpython.PyBytes_AS_STRING(self.buf)
        flags = unpack_uint16(<const char_type*> &ptr[PACKET_HEADER_SIZE],
                              BYTE_ORDER_MSB)
        if flags & TNS_DATA_FLAGS_END_OF_RESPONSE:
            return True
        if self.packet_size == PACKET_HEADER_SIZE + 3 \
                and ptr[PACKET_HEADER_SIZE + 2] == TNS_MSG_TYPE_END_OF_RESPONSE:
            return True
        return False


@cython.final
cdef class ChunkedBytesBuffer:

    cdef:
        uint32_t _num_chunks
        uint32_t _allocated_chunks
        BytesChunk *_chunks

    def __dealloc__(self):
        cdef uint32_t i
        for i in range(self._allocated_chunks):
            if self._chunks[i].ptr is not NULL:
                cpython.PyMem_Free(self._chunks[i].ptr)
                self._chunks[i].ptr = NULL
        if self._chunks is not NULL:
            cpython.PyMem_Free(self._chunks)
            self._chunks = NULL

    cdef int _allocate_chunks(self) except -1:
        """
        Allocates a new set of chunks and copies data from the original set of
        chunks if needed.
        """
        cdef:
            BytesChunk *chunks
            uint32_t allocated_chunks
        allocated_chunks = self._allocated_chunks + 8
        chunks = <BytesChunk*> \
                cpython.PyMem_Malloc(sizeof(BytesChunk) * allocated_chunks)
        memset(chunks, 0, sizeof(BytesChunk) * allocated_chunks)
        if self._num_chunks > 0:
            memcpy(chunks, self._chunks, sizeof(BytesChunk) * self._num_chunks)
            cpython.PyMem_Free(self._chunks)
        self._chunks = chunks
        self._allocated_chunks = allocated_chunks

    cdef BytesChunk* _get_chunk(self, uint32_t num_bytes) except NULL:
        """
        Return the chunk that can be used to write the number of bytes
        requested.
        """
        cdef:
            uint32_t num_allocated_bytes
            BytesChunk *chunk
        if self._num_chunks > 0:
            chunk = &self._chunks[self._num_chunks - 1]
            if chunk.allocated_length >= chunk.length + num_bytes:
                return chunk
        if self._num_chunks >= self._allocated_chunks:
            self._allocate_chunks()
        self._num_chunks += 1
        chunk = &self._chunks[self._num_chunks - 1]
        chunk.length = 0
        if chunk.allocated_length < num_bytes:
            num_allocated_bytes = self._get_chunk_size(num_bytes)
            if chunk.ptr:
                cpython.PyMem_Free(chunk.ptr)
            chunk.ptr = <char_type*> cpython.PyMem_Malloc(num_allocated_bytes)
            chunk.allocated_length = num_allocated_bytes
        return chunk

    cdef inline uint32_t _get_chunk_size(self, uint32_t size):
        """
        Returns the size to allocate aligned on a 64K boundary.
        """
        return (size + CHUNKED_BYTES_CHUNK_SIZE - 1) & \
               ~(CHUNKED_BYTES_CHUNK_SIZE - 1)

    cdef char_type* end_chunked_read(self) except NULL:
        """
        Called when a chunked read has ended. Since a chunked read is never
        started until at least some bytes are being read, it is assumed that at
        least one chunk is in use. If one chunk is in use, those bytes are
        returned directly, but if more than one chunk is in use, the first
        chunk is resized to include all of the bytes in a contiguous section of
        memory first.
        """
        cdef:
            uint32_t i, num_allocated_bytes, total_num_bytes = 0, pos = 0
            char_type *ptr
        if self._num_chunks > 1:
            for i in range(self._num_chunks):
                total_num_bytes += self._chunks[i].length
            num_allocated_bytes = self._get_chunk_size(total_num_bytes)
            ptr = <char_type*> cpython.PyMem_Malloc(num_allocated_bytes)
            for i in range(self._num_chunks):
                memcpy(&ptr[pos], self._chunks[i].ptr, self._chunks[i].length)
                pos += self._chunks[i].length
                cpython.PyMem_Free(self._chunks[i].ptr)
                self._chunks[i].ptr = NULL
                self._chunks[i].allocated_length = 0
                self._chunks[i].length = 0
            self._num_chunks = 1
            self._chunks[0].ptr = ptr
            self._chunks[0].length = total_num_bytes
            self._chunks[0].allocated_length = num_allocated_bytes
        return self._chunks[0].ptr

    cdef char_type* get_chunk_ptr(self, uint32_t size_required) except NULL:
        """
        Called when memory is required for a chunked read.
        """
        cdef:
            BytesChunk *chunk
            char_type *ptr
        chunk = self._get_chunk(size_required)
        ptr = &chunk.ptr[chunk.length]
        chunk.length += size_required
        return ptr

    cdef inline void start_chunked_read(self):
        """
        Called when a chunked read is started and simply indicates that no
        chunks are in use. The memory is retained in order to reduce the
        overhead in freeing and reallocating memory for each chunked read.
        """
        self._num_chunks = 0


@cython.final
cdef class ReadBuffer(Buffer):

    cdef:
        ssize_t _saved_packet_pos, _next_packet_pos, _saved_pos
        ChunkedBytesBuffer _chunked_bytes_buf
        const char_type _split_data[255]
        uint32_t _pending_error_num
        Packet _current_packet
        Transport _transport
        list _saved_packets
        Capabilities _caps
        bint _check_request_boundary
        bint _in_pipeline
        object _waiter
        object _loop

    def __cinit__(self, Transport transport, Capabilities caps):
        self._transport = transport
        self._caps = caps
        self._chunked_bytes_buf = ChunkedBytesBuffer()

    cdef int _check_connected(self):
        """
        Checks to see if the transport is connected and throws the appropriate
        exception if not.
        """
        if self._pending_error_num not in (
            0, TNS_ERR_SESSION_SHUTDOWN, TNS_ERR_INBAND_MESSAGE
        ):
            if self._transport is not None:
                self._transport.disconnect()
                self._transport = None
            if self._pending_error_num == TNS_ERR_EXCEEDED_IDLE_TIME:
                errors._raise_err(errors.ERR_EXCEEDED_IDLE_TIME)
            else:
                errors._raise_err(errors.ERR_UNSUPPORTED_INBAND_NOTIFICATION,
                                  err_num=self._pending_error_num)
        elif self._transport is None:
            errors._raise_err(errors.ERR_NOT_CONNECTED)

    cdef int _get_int_length_and_sign(self, uint8_t *length,
                                      bint *is_negative,
                                      uint8_t max_length) except -1:
        """
        Returns the length of an integer sent on the wire. A check is also made
        to ensure the integer does not exceed the maximum length. If the
        is_negative pointer is NULL, negative integers will result in an
        exception being raised.
        """
        cdef const char_type *ptr = self._get_raw(1)
        if ptr[0] & 0x80:
            if is_negative == NULL:
                errors._raise_err(errors.ERR_UNEXPECTED_NEGATIVE_INTEGER)
            is_negative[0] = True
            length[0] = ptr[0] & 0x7f
        else:
            if is_negative != NULL:
                is_negative[0] = False
            length[0] = ptr[0]
        if length[0] > max_length:
            errors._raise_err(errors.ERR_INTEGER_TOO_LARGE, length=length[0],
                              max_length=max_length)

    cdef const char_type* _get_raw(self, ssize_t num_bytes,
                                   bint in_chunked_read=False) except NULL:
        """
        Returns a pointer to a buffer containing the requested number of bytes.
        This may be split across multiple packets in which case a chunked bytes
        buffer is used.
        """
        cdef:
            ssize_t num_bytes_left, num_bytes_split, max_split_data
            const char_type *source_ptr
            char_type *dest_ptr

        # if no bytes are left in the buffer, a new packet needs to be fetched
        # before anything else can take place
        if self._pos == self._size:
            self.wait_for_packets_sync()

        # if there is enough room in the buffer to satisfy the number of bytes
        # requested, return a pointer to the current location and advance the
        # offset the required number of bytes
        source_ptr = &self._data[self._pos]
        num_bytes_left = self._size - self._pos
        if num_bytes <= num_bytes_left:
            if in_chunked_read:
                dest_ptr = self._chunked_bytes_buf.get_chunk_ptr(num_bytes)
                memcpy(dest_ptr, source_ptr, num_bytes)
            self._pos += num_bytes
            return source_ptr

        # the requested bytes are split across multiple packets; if a chunked
        # read is in progress, a chunk is acquired that will accommodate the
        # remainder of the bytes in the current packet; otherwise, the split
        # buffer will be used instead (after first checking to see if there is
        # sufficient room available within it)
        if in_chunked_read:
            dest_ptr = self._chunked_bytes_buf.get_chunk_ptr(num_bytes_left)
        else:
            max_split_data = sizeof(self._split_data)
            if max_split_data < num_bytes:
                errors._raise_err(errors.ERR_BUFFER_LENGTH_INSUFFICIENT,
                                  actual_buffer_len=max_split_data,
                                  required_buffer_len=num_bytes)
            dest_ptr = <char_type*> self._split_data
        memcpy(dest_ptr, source_ptr, num_bytes_left)

        # acquire packets until the requested number of bytes is satisfied
        num_bytes -= num_bytes_left
        while num_bytes > 0:

            # advance to next packet
            self.wait_for_packets_sync()

            # copy data into the chunked buffer or split buffer, as appropriate
            source_ptr = &self._data[self._pos]
            num_bytes_split = min(num_bytes, self._size - self._pos)
            if in_chunked_read:
                dest_ptr = \
                        self._chunked_bytes_buf.get_chunk_ptr(num_bytes_split)
            else:
                dest_ptr = <char_type*> &self._split_data[num_bytes_left]
            memcpy(dest_ptr, source_ptr, num_bytes_split)
            self._pos += num_bytes_split
            num_bytes -= num_bytes_split

        # return the split buffer unconditionally; if performing a chunked read
        # the return value is ignored anyway
        return self._split_data

    cdef int _process_control_packet(self, Packet packet) except -1:
        """
        Processes a control packet.
        """
        cdef:
            uint16_t control_type
            Buffer buf
        buf = Buffer.__new__(Buffer)
        buf._populate_from_bytes(packet.buf)
        buf.skip_raw_bytes(8)               # skip packet header
        buf.read_uint16(&control_type)
        if control_type == TNS_CONTROL_TYPE_RESET_OOB:
            self._caps.supports_oob = False
        elif control_type == TNS_CONTROL_TYPE_INBAND_NOTIFICATION:
            buf.skip_raw_bytes(4)           # skip first integer
            buf.read_uint32(&self._pending_error_num)

    cdef int _process_packet(self, Packet packet,
                             bint *notify_waiter,
                             bint check_connected) except -1:
        """
        Processes a packet. If the packet is a control packet it is processed
        immediately and discarded; if the packet is a marker packet and a
        pipeline is being processed, the packet is discarded; otherwise, it is
        added to the list of saved packets for this response. If the protocol
        supports sending the end of request notification we wait for that
        message type to be returned at the end of the packet.
        """
        if packet.packet_type == TNS_PACKET_TYPE_CONTROL:
            self._process_control_packet(packet)
            notify_waiter[0] = False
            if check_connected:
                self._check_connected()
        elif self._in_pipeline \
                and packet.packet_type == TNS_PACKET_TYPE_MARKER:
            notify_waiter[0] = False
        else:
            self._saved_packets.append(packet)
            notify_waiter[0] = \
                    packet.packet_type != TNS_PACKET_TYPE_DATA \
                    or not self._check_request_boundary \
                    or packet.has_end_of_response()

    cdef int _read_raw_bytes_and_length(self, const char_type **ptr,
                                        ssize_t *num_bytes) except -1:
        """
        Helper function that processes the length. If the length is defined as
        TNS_LONG_LENGTH_INDICATOR, a chunked read is performed.
        """
        cdef uint32_t temp_num_bytes
        if num_bytes[0] != TNS_LONG_LENGTH_INDICATOR:
            return Buffer._read_raw_bytes_and_length(self, ptr, num_bytes)
        self._chunked_bytes_buf.start_chunked_read()
        num_bytes[0] = 0
        while True:
            self.read_ub4(&temp_num_bytes)
            if temp_num_bytes == 0:
                break
            num_bytes[0] += temp_num_bytes
            self._get_raw(temp_num_bytes, in_chunked_read=True)
        ptr[0] = self._chunked_bytes_buf.end_chunked_read()

    cdef int _start_packet(self) except -1:
        """
        Starts a packet. This prepares the current packet for processing.
        """
        cdef uint16_t data_flags
        self._current_packet = self._saved_packets[self._next_packet_pos]
        self._next_packet_pos += 1
        self._populate_from_bytes(self._current_packet.buf)
        self._pos = PACKET_HEADER_SIZE
        if self._current_packet.packet_type == TNS_PACKET_TYPE_DATA:
            self.read_uint16(&data_flags)
            if data_flags == TNS_DATA_FLAGS_EOF:
                self._pending_error_num = TNS_ERR_SESSION_SHUTDOWN

    async def discard_pipeline_responses(self, ssize_t num_responses):
        """
        Discards the specified number of responses after the pipeline has
        encountered an exception.
        """
        while num_responses > 0:
            if not self.has_response():
                await self.wait_for_response_async()
            while True:
                self._start_packet()
                if self._current_packet.has_end_of_response():
                    break
            num_responses -= 1
        self.reset_packets()

    cdef int notify_packet_received(self) except -1:
        """
        Notify the registered waiter that a packet has been received. This is
        only used by the asyncio implementation.
        """
        if self._waiter is not None:
            self._waiter.set_result(None)
            self._waiter = None

    cdef ThinDbObjectImpl read_dbobject(self, BaseDbObjectTypeImpl typ_impl):
        """
        Read a database object from the buffer and return a DbObject object
        containing it.
        it.
        """
        cdef:
            bytes oid = None, toid = None
            ThinDbObjectImpl obj_impl
            uint32_t num_bytes
        self.read_ub4(&num_bytes)
        if num_bytes > 0:                   # type OID
            toid = self.read_bytes()
        self.read_ub4(&num_bytes)
        if num_bytes > 0:                   # OID
            oid = self.read_bytes()
        self.read_ub4(&num_bytes)
        if num_bytes > 0:                   # snapshot
            self.read_bytes()
        self.skip_ub2()                     # version
        self.read_ub4(&num_bytes)           # length of data
        self.skip_ub2()                     # flags
        if num_bytes > 0:
            obj_impl = ThinDbObjectImpl.__new__(ThinDbObjectImpl)
            obj_impl.type = typ_impl
            obj_impl.toid = toid
            obj_impl.oid = oid
            obj_impl.packed_data = self.read_bytes()
            return obj_impl

    cdef object read_oson(self):
        """
        Read an OSON value from the buffer and return the converted value. OSON
        is sent as a LOB value with all of the data prefetched. Since the LOB
        locator is not required it is simply discarded.
        it.
        """
        cdef:
            OsonDecoder decoder
            uint32_t num_bytes
            bytes data
        self.read_ub4(&num_bytes)
        if num_bytes > 0:
            self.skip_ub8()             # size (unused)
            self.skip_ub4()             # chunk size (unused)
            data = self.read_bytes()
            self.read_bytes()           # LOB locator (unused)
            decoder = OsonDecoder.__new__(OsonDecoder)
            return decoder.decode(data)

    cdef object read_lob_with_length(self, BaseThinConnImpl conn_impl,
                                     DbType dbtype):
        """
        Read a LOB locator from the buffer and return a LOB object containing
        it.
        """
        cdef:
            uint32_t chunk_size, num_bytes
            BaseThinLobImpl lob_impl
            uint64_t size
            type cls
        self.read_ub4(&num_bytes)
        if num_bytes > 0:
            if dbtype._ora_type_num == TNS_DATA_TYPE_BFILE:
                size = chunk_size = 0
            else:
                self.read_ub8(&size)
                self.read_ub4(&chunk_size)
            lob_impl = conn_impl._create_lob_impl(dbtype, self.read_bytes())
            lob_impl._size = size
            lob_impl._chunk_size = chunk_size
            lob_impl._has_metadata = \
                    dbtype._ora_type_num != TNS_DATA_TYPE_BFILE
            cls = PY_TYPE_ASYNC_LOB \
                    if conn_impl._protocol._transport._is_async \
                    else PY_TYPE_LOB
            return cls._from_impl(lob_impl)

    cdef const char_type* read_raw_bytes(self, ssize_t num_bytes) except NULL:
        """
        Read the specified number of bytes from the packet and return them.
        """
        self._chunked_bytes_buf.start_chunked_read()
        self._get_raw(num_bytes, in_chunked_read=True)
        return self._chunked_bytes_buf.end_chunked_read()

    cdef int read_rowid(self, Rowid *rowid) except -1:
        """
        Reads a rowid from the buffer and populates the rowid structure.
        """
        self.read_ub4(&rowid.rba)
        self.read_ub2(&rowid.partition_id)
        self.skip_ub1()
        self.read_ub4(&rowid.block_num)
        self.read_ub2(&rowid.slot_num)

    cdef object read_urowid(self):
        """
        Read a universal rowid from the buffer and return the Python object
        representing its value.
        """
        cdef:
            ssize_t output_len, input_len, remainder, pos
            int input_offset = 1, output_offset = 0
            const char_type *input_ptr
            bytearray output_value
            uint32_t num_bytes
            uint8_t length
            Rowid rowid

        # get data (first buffer contains the length, which can be ignored)
        self.read_raw_bytes_and_length(&input_ptr, &input_len)
        if input_ptr == NULL:
            return None
        self.read_raw_bytes_and_length(&input_ptr, &input_len)

        # handle physical rowid
        if input_ptr[0] == 1:
            rowid.rba = unpack_uint32(&input_ptr[1], BYTE_ORDER_MSB)
            rowid.partition_id = unpack_uint16(&input_ptr[5], BYTE_ORDER_MSB)
            rowid.block_num = unpack_uint32(&input_ptr[7], BYTE_ORDER_MSB)
            rowid.slot_num = unpack_uint16(&input_ptr[11], BYTE_ORDER_MSB)
            return _encode_rowid(&rowid)

        # handle logical rowid
        output_len = (input_len // 3) * 4
        remainder = input_len % 3
        if remainder == 1:
            output_len += 1
        elif remainder == 2:
            output_len += 3
        output_value = bytearray(output_len)
        input_len -= 1
        output_value[0] = 42            # '*'
        output_offset += 1
        while input_len > 0:

            # produce first byte of quadruple
            pos = input_ptr[input_offset] >> 2
            output_value[output_offset] = TNS_BASE64_ALPHABET_ARRAY[pos]
            output_offset += 1

            # produce second byte of quadruple, but if only one byte is left,
            # produce that one byte and exit
            pos = (input_ptr[input_offset] & 0x3) << 4
            if input_len == 1:
                output_value[output_offset] = TNS_BASE64_ALPHABET_ARRAY[pos]
                break
            input_offset += 1
            pos |= ((input_ptr[input_offset] & 0xf0) >> 4)
            output_value[output_offset] = TNS_BASE64_ALPHABET_ARRAY[pos]
            output_offset += 1

            # produce third byte of quadruple, but if only two bytes are left,
            # produce that one byte and exit
            pos = (input_ptr[input_offset] & 0xf) << 2
            if input_len == 2:
                output_value[output_offset] = TNS_BASE64_ALPHABET_ARRAY[pos]
                break
            input_offset += 1
            pos |= ((input_ptr[input_offset] & 0xc0) >> 6)
            output_value[output_offset] = TNS_BASE64_ALPHABET_ARRAY[pos]
            output_offset += 1

            # produce final byte of quadruple
            pos = input_ptr[input_offset] & 0x3f
            output_value[output_offset] = TNS_BASE64_ALPHABET_ARRAY[pos]
            output_offset += 1
            input_offset += 1
            input_len -= 3

        return bytes(output_value).decode()

    cdef object read_vector(self):
        """
        Read a VECTOR value from the buffer and return the converted value.
        VECTOR is sent as a LOB value with all of the data prefetched. Since
        the LOB locator is not required it is simply discarded.
        it.
        """
        cdef:
            VectorDecoder decoder
            uint32_t num_bytes
            bytes data
        self.read_ub4(&num_bytes)
        if num_bytes > 0:
            self.skip_ub8()             # size (unused)
            self.skip_ub4()             # chunk size (unused)
            data = self.read_bytes()
            self.read_bytes()           # LOB locator (unused)
            if data:
                decoder = VectorDecoder.__new__(VectorDecoder)
                return decoder.decode(data)

    cdef object read_xmltype(self, BaseThinConnImpl conn_impl):
        """
        Reads an XMLType value from the buffer and returns the string value.
        The XMLType object is a special DbObjectType and is handled separately
        since the structure is a bit different.
        """
        cdef:
            DbObjectPickleBuffer buf
            uint32_t num_bytes
        self.read_ub4(&num_bytes)
        if num_bytes > 0:                   # type OID
            self.read_bytes()
        self.read_ub4(&num_bytes)
        if num_bytes > 0:                   # OID
            self.read_bytes()
        self.read_ub4(&num_bytes)
        if num_bytes > 0:                   # snapshot
            self.read_bytes()
        self.skip_ub2()                     # version
        self.read_ub4(&num_bytes)           # length of data
        self.skip_ub2()                     # flags
        if num_bytes > 0:
            buf = DbObjectPickleBuffer.__new__(DbObjectPickleBuffer)
            buf._populate_from_bytes(self.read_bytes())
            return buf.read_xmltype(conn_impl)

    cdef int check_control_packet(self) except -1:
        """
        Checks for a control packet or final close packet from the server.
        """
        cdef:
            bint notify_waiter
            Packet packet
        packet = self._transport.read_packet()
        self._process_packet(packet, &notify_waiter, False)
        if notify_waiter:
            self._start_packet()

    cdef bint has_response(self):
        """
        Returns a boolean indicating if the list of saved packets contains all
        of the packets for a response from the database. This method can only
        be called if support for the end of response bit is present.
        """
        cdef:
            Packet packet
            ssize_t i, max_pos
        for i in range(self._next_packet_pos, len(self._saved_packets)):
            packet = <Packet> self._saved_packets[i]
            if packet.has_end_of_response():
                return True
        return False

    cdef int reset_packets(self) except -1:
        """
        Resets the list of saved packets and the saved position (called when a
        request has been sent to the database and a response is expected).
        """
        self._saved_packets = []
        self._next_packet_pos = 0
        self._saved_packet_pos = 0
        self._saved_pos = 0

    cdef int restore_point(self) except -1:
        """
        Restores the position in the packets to the last saved point. This is
        needed by asyncio where an ansychronous wait for more packets is
        required so the processing of the response must be restarted at a known
        position.
        """
        if self._saved_packet_pos != self._next_packet_pos - 1:
            self._current_packet = self._saved_packets[self._saved_packet_pos]
            self._populate_from_bytes(self._current_packet.buf)
            self._next_packet_pos = self._saved_packet_pos + 1
        self._pos = self._saved_pos

    cdef int save_point(self) except -1:
        """
        Saves the current position in the packets. This is needed by asyncio
        where an asynchronous wait for more packets is required so the
        processing of the response must be restarted at a known position.
        """
        self._saved_packet_pos = self._next_packet_pos - 1
        self._saved_pos = self._pos

    cdef int skip_raw_bytes_chunked(self) except -1:
        """
        Skip a number of bytes that may or may not be chunked in the buffer.
        The first byte gives the length. If the length is
        TNS_LONG_LENGTH_INDICATOR, however, chunks are read and discarded.
        """
        cdef:
            uint32_t temp_num_bytes
            uint8_t length
        self.read_ub1(&length)
        if length != TNS_LONG_LENGTH_INDICATOR:
            self.skip_raw_bytes(length)
        else:
            while True:
                self.read_ub4(&temp_num_bytes)
                if temp_num_bytes == 0:
                    break
                self.skip_raw_bytes(temp_num_bytes)

    async def wait_for_packets_async(self):
        """
        Wait for packets to arrive in response to the request that was sent to
        the database (using asyncio).
        """
        if self._next_packet_pos >= len(self._saved_packets):
            self._waiter = self._loop.create_future()
            await self._waiter
        self._start_packet()

    cdef int wait_for_packets_sync(self) except -1:
        """
        Wait for packets to arrive in response to the request that was sent to
        the database (synchronously). If no packets are available and we are
        using asyncio, raise an exception so that processing can be restarted
        once packets have arrived.
        """
        cdef:
            bint notify_waiter
            Packet packet
        if self._next_packet_pos >= len(self._saved_packets):
            if self._transport._is_async:
                raise OutOfPackets()
            while True:
                packet = self._transport.read_packet()
                self._process_packet(packet, &notify_waiter, True)
                if notify_waiter:
                    break
        self._start_packet()

    async def wait_for_response_async(self):
        """
        Wait for packets to arrive in response to the request that was sent to
        the database (using asyncio). This method will not return until the
        complete response has been received. This requires the "end of
        response" capability available in Oracle Database 23ai and higher. This
        method also assumes that the current list of saved packets does not
        contain a full response.
        """
        try:
            self._check_request_boundary = True
            self._waiter = self._loop.create_future()
            await self._waiter
        finally:
            self._check_request_boundary = False


@cython.final
cdef class WriteBuffer(Buffer):

    cdef:
        uint8_t _packet_type
        uint8_t _packet_flags
        uint16_t _data_flags
        Capabilities _caps
        Transport _transport
        uint8_t _seq_num
        bint _packet_sent

    def __cinit__(self, Transport transport, Capabilities caps):
        self._transport = transport
        self._caps = caps
        self._size_for_sdu()

    cdef int _send_packet(self, bint final_packet) except -1:
        """
        Write the packet header and then send the packet. Once sent, reset the
        pointers back to an empty packet.
        """
        cdef ssize_t size = self._pos
        self._pos = 0
        if self._caps.protocol_version >= TNS_VERSION_MIN_LARGE_SDU:
            self.write_uint32(size)
        else:
            self.write_uint16(size)
            self.write_uint16(0)
        self.write_uint8(self._packet_type)
        self.write_uint8(self._packet_flags)
        self.write_uint16(0)
        if self._packet_type == TNS_PACKET_TYPE_DATA:
            self.write_uint16(self._data_flags)
        self._pos = size
        self._transport.write_packet(self)
        self._packet_sent = True
        self._pos = PACKET_HEADER_SIZE
        if not final_packet and self._packet_type == TNS_PACKET_TYPE_DATA:
            self._pos += sizeof(uint16_t)   # allow space for data flags

    cdef int _size_for_sdu(self) except -1:
        """
        Resizes the buffer based on the SDU size of the capabilities.
        """
        self._initialize(self._caps.sdu)

    cdef int _write_more_data(self, ssize_t num_bytes_available,
                              ssize_t num_bytes_wanted) except -1:
        """
        Called when the amount of buffer available is less than the amount of
        data requested. This sends the packet to the server and then resets the
        buffer for further writing.
        """
        self._send_packet(final_packet=False)

    cdef int end_request(self) except -1:
        """
        Indicates that the request from the client is completing and will send
        any packet remaining, if necessary.
        """
        if self._pos > PACKET_HEADER_SIZE:
            self._send_packet(final_packet=True)

    cdef inline ssize_t max_payload_bytes(self):
        """
        Return the maximum number of bytes that can be sent in a packet. This
        is the maximum size of the entire packet, less the bytes in the header
        and 2 bytes for the data flags.
        """
        return self._max_size - PACKET_HEADER_SIZE - 2

    cdef void start_request(self, uint8_t packet_type, uint8_t packet_flags=0,
                            uint16_t data_flags=0):
        """
        Indicates that a request from the client is starting. The packet type
        is retained just in case a request spans multiple packets. The packet
        header (8 bytes in length) is written when a packet is actually being
        sent and so is skipped at this point.
        """
        self._packet_sent = False
        self._packet_type = packet_type
        self._packet_flags = packet_flags
        self._pos = PACKET_HEADER_SIZE
        if packet_type == TNS_PACKET_TYPE_DATA:
            self._data_flags = data_flags
            self._pos += sizeof(uint16_t)

    cdef object write_dbobject(self, ThinDbObjectImpl obj_impl):
        """
        Writes a database object to the buffer.
        """
        cdef:
            ThinDbObjectTypeImpl typ_impl = obj_impl.type
            uint32_t num_bytes
            bytes packed_data
        self.write_ub4(len(obj_impl.toid))
        self.write_bytes_with_length(obj_impl.toid)
        if obj_impl.oid is None:
            self.write_ub4(0)
        else:
            self.write_ub4(len(obj_impl.oid))
            self.write_bytes_with_length(obj_impl.oid)
        self.write_ub4(0)                   # snapshot
        self.write_ub4(0)                   # version
        packed_data = obj_impl._get_packed_data()
        self.write_ub4(len(packed_data))
        self.write_ub4(obj_impl.flags)      # flags
        self.write_bytes_with_length(packed_data)

    cdef int write_lob_with_length(self, BaseThinLobImpl lob_impl) except -1:
        """
        Writes a LOB locator to the buffer.
        """
        self.write_ub4(len(lob_impl._locator))
        self.write_bytes_with_length(lob_impl._locator)

    cdef int write_qlocator(self, uint64_t data_length) except -1:
        """
        Writes a QLocator. QLocators are always 40 bytes in length.
        """
        self.write_ub4(40)                  # QLocator length
        self.write_uint8(40)                # chunk length
        self.write_uint16(38)               # QLocator length less 2 bytes
        self.write_uint16(TNS_LOB_QLOCATOR_VERSION)
        self.write_uint8(TNS_LOB_LOC_FLAGS_VALUE_BASED | \
                         TNS_LOB_LOC_FLAGS_BLOB | \
                         TNS_LOB_LOC_FLAGS_ABSTRACT)
        self.write_uint8(TNS_LOB_LOC_FLAGS_INIT)
        self.write_uint16(0)                # additional flags
        self.write_uint16(1)                # byt1
        self.write_uint64(data_length)
        self.write_uint16(0)                # unused
        self.write_uint16(0)                # csid
        self.write_uint16(0)                # unused
        self.write_uint64(0)                # unused
        self.write_uint64(0)                # unused

    cdef object write_oson(self, value, ssize_t max_fname_size):
        """
        Encodes the given value to OSON and then writes that to the buffer.
        it.
        """
        cdef OsonEncoder encoder = OsonEncoder.__new__(OsonEncoder)
        encoder.encode(value, max_fname_size)
        self.write_qlocator(encoder._pos)
        self._write_raw_bytes_and_length(encoder._data, encoder._pos)

    cdef int write_seq_num(self) except -1:
        self._seq_num += 1
        if self._seq_num == 0:
            self._seq_num = 1
        self.write_uint8(self._seq_num)

    cdef object write_vector(self, value):
        """
        Encodes the given value to VECTOR and then writes that to the buffer.
        """
        cdef VectorEncoder encoder = VectorEncoder.__new__(VectorEncoder)
        encoder.encode(value)
        self.write_qlocator(encoder._pos)
        self._write_raw_bytes_and_length(encoder._data, encoder._pos)
