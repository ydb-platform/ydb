#------------------------------------------------------------------------------
# Copyright (c) 2023, 2024, Oracle and/or its affiliates.
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
# transport.pyx
#
# Cython file defining the transport class used by the client for sending and
# receiving packets (embedded in thin_impl.pyx).
#------------------------------------------------------------------------------

cdef bint DEBUG_PACKETS = ("PYO_DEBUG_PACKETS" in os.environ)

cdef class Transport:

    cdef:
        object _transport
        object _ssl_context
        str _ssl_server_hostname
        uint32_t _transport_num
        ssize_t _max_packet_size
        uint32_t _op_num
        bytes _partial_buf
        bint _full_packet_size
        bint _is_async

    cdef str _get_debugging_header(self, str operation):
        """
        Returns the header line used for debugging packets.
        """
        cdef str header
        self._op_num += 1
        current_date = datetime.datetime.now().isoformat(
            sep=" ",
            timespec="milliseconds"
        )
        return (
            f"{current_date} "
            f"{operation} [op {self._op_num}] on socket {self._transport_num}"
        )

    cdef int _print_output(self, str text) except -1:
        """
        Prints and flushes the text to stdout to ensure that multiple
        threads don't lose output due to buffering.
        """
        print(text + "\n", flush=True)

    cdef int _print_packet(self, str operation, object data) except -1:
        """
        Print the packet content in a format suitable for debugging.
        """
        offset = 0
        hex_data = memoryview(data).hex().upper()
        output_lines = [self._get_debugging_header(operation)]
        while hex_data:
            line_hex_data = hex_data[:16]
            hex_data = hex_data[16:]
            hex_dump_values = []
            printable_values = []
            for i in range(0, len(line_hex_data), 2):
                hex_byte = line_hex_data[i:i + 2]
                hex_dump_values.append(hex_byte)
                byte_val = ord(bytes.fromhex(hex_byte))
                char_byte = chr(byte_val)
                if char_byte.isprintable() and byte_val < 128 \
                        and char_byte != " ":
                    printable_values.append(char_byte)
                else:
                    printable_values.append(".")
            while len(hex_dump_values) < 8:
                hex_dump_values.append("  ")
                printable_values.append(" ")
            output_lines.append(
                f'{offset:04} : {" ".join(hex_dump_values)} '
                f'|{"".join(printable_values)}|'
            )
            offset += 8
        self._print_output("\n".join(output_lines))

    cdef int disconnect(self) except -1:
        """
        Disconnects the transport.
        """
        if self._transport is not None:
            if DEBUG_PACKETS:
                self._print_output(
                    self._get_debugging_header("Disconnecting transport")
                )
            self._transport.close()
            self._transport = None

    cdef int create_ssl_context(self, ConnectParamsImpl params,
                                Description description,
                                Address address) except -1:
        """
        Creates the SSL context used for establishing TLS communications
        between the database and the client.
        """

        # start with a default SSL context, unless a custom one is supplied
        self._ssl_context = params.ssl_context
        if self._ssl_context is None:
            self._ssl_context = ssl.create_default_context()

        # set the minimum and maximum versions of the TLS protocol
        if description.ssl_version is not None:
            self._ssl_context.minimum_version = description.ssl_version
            self._ssl_context.maximum_version = description.ssl_version

        # if the platform is macOS, check if the certifi package is installed;
        # if certifi is not installed, load the certificates from the macOS
        # keychain in PEM format
        if sys.platform == "darwin" and certifi is None:
            global macos_certs
            if macos_certs is None:
                certs = subprocess.run(["security", "find-certificate",
                                        "-a", "-p"],
                                        stdout=subprocess.PIPE).stdout
                macos_certs = certs.decode("utf-8")
            self._ssl_context.load_verify_locations(cadata=macos_certs)

        # if a wallet is specified, either mTLS is being used or a set of
        # certificates is being loaded to validate the server
        if description.wallet_location is not None:
            pem_file_name = os.path.join(description.wallet_location,
                                         PEM_WALLET_FILE_NAME)
            if not os.path.exists(pem_file_name):
                errors._raise_err(errors.ERR_WALLET_FILE_MISSING,
                                  name=pem_file_name)
            self._ssl_context.load_verify_locations(pem_file_name)
            wallet_password = params._get_wallet_password()
            try:
                self._ssl_context.load_cert_chain(pem_file_name,
                                                  password=wallet_password)
            except ssl.SSLError:
                pass

        # determine the SSL server host name to use if desired; otherwise, mark
        # the SSL context to indicate that server host name matching is not
        # desired by the client (should generally be avoided)
        if description.ssl_server_dn_match \
                and description.ssl_server_cert_dn is None:
            self._ssl_server_hostname = address.host
        else:
            self._ssl_context.check_hostname = False

    cdef Packet extract_packet(self, bytes data=None):
        """
        Extracts a packet from the data, if possible (after first appending it
        to the partial buffer, if applicable). Any extra data not needed by the
        packet is stored in the partial buffer for a later call to this
        function.
        """
        cdef:
            ssize_t size, packet_size
            Packet packet
            char_type *ptr

        # update the partial buffer with the new data
        if data is not None:
            if self._partial_buf is None:
                self._partial_buf = data
            else:
                self._partial_buf += data
        size = 0 if self._partial_buf is None else len(self._partial_buf)

        # if enough bytes for the packet header, extract the packet size
        if size >= PACKET_HEADER_SIZE:

            # extract the packet size
            ptr = <char_type*> self._partial_buf
            if self._full_packet_size:
                packet_size = unpack_uint32(ptr, BYTE_ORDER_MSB)
            else:
                packet_size = unpack_uint16(ptr, BYTE_ORDER_MSB)

            # if enough bytes are available for the packet, return it
            if size >= packet_size:
                packet = Packet.__new__(Packet)
                packet.packet_size = packet_size
                packet.packet_type = ptr[4]
                packet.packet_flags = ptr[5]

                # store the packet buffer and adjust the partial buffer, as
                # needed
                if size == packet_size:
                    packet.buf = self._partial_buf
                    self._partial_buf = None
                else:
                    packet.buf = self._partial_buf[:packet_size]
                    self._partial_buf = self._partial_buf[packet_size:]

                # display packet, if requested
                if DEBUG_PACKETS:
                    self._print_packet("Receiving packet", packet.buf)
                return packet

    cdef tuple get_host_info(self):
        """
        Return a 2-tuple supplying the host and port to which the transport is
        connected.
        """
        cdef object sock
        if self._is_async:
            sock = self._transport.get_extra_info('socket')
        else:
            sock = self._transport
        return sock.getpeername()[:2]

    cdef int has_data_ready(self, bint *data_ready) except -1:
        """
        Returns true if data is ready to be read on the transport.
        """
        socket_list = [self._transport]
        read_socks, _, _ = select.select(socket_list, [], [], 0)
        data_ready[0] = bool(read_socks)

    cdef int negotiate_tls(self, object sock,
                           Description description) except -1:
        """
        Negotiate TLS on the socket.
        """
        self._transport = self._ssl_context.wrap_socket(
            sock, server_hostname=self._ssl_server_hostname
        )
        if description.ssl_server_dn_match \
                and description.ssl_server_cert_dn is not None:
            if not get_server_dn_matches(self._transport,
                                         description.ssl_server_cert_dn):
                errors._raise_err(errors.ERR_INVALID_SERVER_CERT_DN)

    cdef int renegotiate_tls(self, Description description) except -1:
        """
        Renegotiate TLS on the socket.
        """
        orig_sock = self._transport
        sock = socket.socket(family=orig_sock.family, type=orig_sock.type,
                             proto=orig_sock.proto, fileno=orig_sock.detach())
        self.negotiate_tls(sock, description)

    async def negotiate_tls_async(self, BaseAsyncProtocol protocol,
                                  Description description):
        """
        Negotiate TLS on the socket asynchronously.
        """
        orig_transport = self._transport
        loop = protocol._read_buf._loop
        self._transport = await loop.start_tls(
            self._transport, protocol,
            self._ssl_context,
            server_hostname=self._ssl_server_hostname
        )
        if description.ssl_server_dn_match \
                and description.ssl_server_cert_dn is not None:
            sock = self._transport.get_extra_info("ssl_object")
            if not get_server_dn_matches(sock, description.ssl_server_cert_dn):
                errors._raise_err(errors.ERR_INVALID_SERVER_CERT_DN)
        return orig_transport

    cdef int send_oob_break(self) except -1:
        """
        Sends an out-of-band break on the transport.
        """
        if DEBUG_PACKETS:
            self._print_output(
                self._get_debugging_header("Sending out of band break")
            )
        self._transport.send(b"!", socket.MSG_OOB)

    cdef int set_from_socket(self, object transport,
                             ConnectParamsImpl params,
                             Description description,
                             Address address) except -1:
        """
        Sets the transport from a socket.
        """
        cdef object sock
        if self._is_async:
            sock = transport.get_extra_info('socket')
        else:
            sock = transport
        if description.expire_time > 0:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            if hasattr(socket, "TCP_KEEPIDLE") \
                    and hasattr(socket, "TCP_KEEPINTVL") \
                    and hasattr(socket, "TCP_KEEPCNT"):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE,
                                description.expire_time * 60)
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 6)
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 10)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        if sock.gettimeout() != 0:
            sock.settimeout(None)
        self._transport = transport
        self._transport_num = sock.fileno()

    cdef Packet read_packet(self):
        """
        Reads a packet from the transport.
        """
        cdef:
            Packet packet
            bytes data
        packet = self.extract_packet()
        while packet is None:
            try:
                data = self._transport.recv(self._max_packet_size)
            except ConnectionResetError as e:
                errors._raise_err(errors.ERR_CONNECTION_CLOSED, str(e),
                                  cause=e)
            if len(data) == 0:
                self.disconnect()
                errors._raise_err(errors.ERR_CONNECTION_CLOSED)
            packet = self.extract_packet(data)
        return packet

    cdef int set_timeout(self, double value) except -1:
        """
        Sets the timeout on the transport.
        """
        self._transport.settimeout(value or None)

    cdef int write_packet(self, WriteBuffer buf) except -1:
        """
        Writes a packet on the transport.
        """
        data = bytes(buf._data_view[:buf._pos])
        if DEBUG_PACKETS:
            self._print_packet("Sending packet", data)
        try:
            if self._is_async:
                self._transport.write(data)
            else:
                self._transport.send(data)
        except OSError as e:
            self.disconnect()
            errors._raise_err(errors.ERR_CONNECTION_CLOSED, cause=e)
