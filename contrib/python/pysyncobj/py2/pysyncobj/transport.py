from .config import FAIL_REASON
from .dns_resolver import globalDnsResolver
from .monotonic import monotonic as monotonicTime
from .node import Node, TCPNode
from .tcp_connection import TcpConnection, CONNECTION_STATE
from .tcp_server import TcpServer
import functools
import os
import threading
import time
import random


class TransportNotReadyError(Exception):
    """Transport failed to get ready for operation."""


class Transport(object):
    """Base class for implementing a transport between PySyncObj nodes"""

    def __init__(self, syncObj, selfNode, otherNodes):
        """
        Initialise the transport

        :param syncObj: SyncObj
        :type syncObj: SyncObj
        :param selfNode: current server node, or None if this is a read-only node
        :type selfNode: Node or None
        :param otherNodes: partner nodes
        :type otherNodes: list of Node
        """

        self._onMessageReceivedCallback = None
        self._onNodeConnectedCallback = None
        self._onNodeDisconnectedCallback = None
        self._onReadonlyNodeConnectedCallback = None
        self._onReadonlyNodeDisconnectedCallback = None
        self._onUtilityMessageCallbacks = {}

    def setOnMessageReceivedCallback(self, callback):
        """
        Set the callback for when a message is received, or disable callback by passing None

        :param callback callback
        :type callback function(node: Node, message: any) or None
        """

        self._onMessageReceivedCallback = callback

    def setOnNodeConnectedCallback(self, callback):
        """
        Set the callback for when the connection to a (non-read-only) node is established, or disable callback by passing None

        :param callback callback
        :type callback function(node: Node) or None
        """

        self._onNodeConnectedCallback = callback

    def setOnNodeDisconnectedCallback(self, callback):
        """
        Set the callback for when the connection to a (non-read-only) node is terminated or is considered dead, or disable callback by passing None

        :param callback callback
        :type callback function(node: Node) or None
        """

        self._onNodeDisconnectedCallback = callback

    def setOnReadonlyNodeConnectedCallback(self, callback):
        """
        Set the callback for when a read-only node connects, or disable callback by passing None

        :param callback callback
        :type callback function(node: Node) or None
        """

        self._onReadonlyNodeConnectedCallback = callback

    def setOnReadonlyNodeDisconnectedCallback(self, callback):
        """
        Set the callback for when a read-only node disconnects (or the connection is lost), or disable callback by passing None

        :param callback callback
        :type callback function(node: Node) or None
        """

        self._onReadonlyNodeDisconnectedCallback = callback

    def setOnUtilityMessageCallback(self, message, callback):
        """
        Set the callback for when an utility message is received, or disable callback by passing None

        :param message: the utility message string (add, remove, set_version, and so on)
        :type message: str
        :param callback: callback
        :type callback: function(message: list, callback: function) or None
        """

        if callback:
            self._onUtilityMessageCallbacks[message] = callback
        elif message in self._onUtilityMessageCallbacks:
            del self._onUtilityMessageCallbacks[message]

    # Helper functions so you don't need to check for the callbacks manually in subclasses
    def _onMessageReceived(self, node, message):
        if self._onMessageReceivedCallback is not None:
            self._onMessageReceivedCallback(node, message)

    def _onNodeConnected(self, node):
        if self._onNodeConnectedCallback is not None:
            self._onNodeConnectedCallback(node)

    def _onNodeDisconnected(self, node):
        if self._onNodeDisconnectedCallback is not None:
            self._onNodeDisconnectedCallback(node)

    def _onReadonlyNodeConnected(self, node):
        if self._onReadonlyNodeConnectedCallback is not None:
            self._onReadonlyNodeConnectedCallback(node)

    def _onReadonlyNodeDisconnected(self, node):
        if self._onReadonlyNodeDisconnectedCallback is not None:
            self._onReadonlyNodeDisconnectedCallback(node)

    def tryGetReady(self):
        """
        Try to get the transport ready for operation. This may for example mean binding a server to a port.

        :raises TransportNotReadyError: if the transport fails to get ready for operation
        """

    @property
    def ready(self):
        """
        Whether the transport is ready for operation.

        :rtype bool
        """

        return True

    def waitReady(self):
        """
        Wait for the transport to be ready.

        :raises TransportNotReadyError: if the transport fails to get ready for operation
        """

    def addNode(self, node):
        """
        Add a node to the network

        :param node node to add
        :type node Node
        """

    def dropNode(self, node):
        """
        Remove a node from the network (meaning connections, buffers, etc. related to this node can be dropped)

        :param node node to drop
        :type node Node
        """

    def send(self, node, message):
        """
        Send a message to a node.
        The message should be picklable.
        The return value signifies whether the message is thought to have been sent successfully. It does not necessarily mean that the message actually arrived at the node.

        :param node target node
        :type node Node
        :param message message
        :type message any
        :returns success
        :rtype bool
        """

        raise NotImplementedError

    def destroy(self):
        """
        Destroy the transport
        """


class TCPTransport(Transport):
    def __init__(self, syncObj, selfNode, otherNodes):
        """
        Initialise the TCP transport. On normal (non-read-only) nodes, this will start a TCP server. On all nodes, it will initiate relevant connections to other nodes.

        :param syncObj: SyncObj
        :type syncObj: SyncObj
        :param selfNode: current node (None if this is a read-only node)
        :type selfNode: TCPNode or None
        :param otherNodes: partner nodes
        :type otherNodes: iterable of TCPNode
        """

        super(TCPTransport, self).__init__(syncObj, selfNode, otherNodes)
        self._syncObj = syncObj
        self._server = None
        self._connections = {} # Node object -> TcpConnection object
        self._unknownConnections = set() # set of TcpConnection objects
        self._selfNode = selfNode
        self._selfIsReadonlyNode = selfNode is None
        self._nodes = set() # set of TCPNode
        self._readonlyNodes = set() # set of Node
        self._nodeAddrToNode = {} # node ID/address -> TCPNode (does not include read-only nodes)
        self._lastConnectAttempt = {} # TPCNode -> float (seconds since epoch)
        self._preventConnectNodes = set() # set of TCPNode to which no (re)connection should be triggered on _connectIfNecessary; used via dropNode and destroy to cleanly remove a node
        self._readonlyNodesCounter = 0
        self._lastBindAttemptTime = 0
        self._bindAttempts = 0
        self._bindOverEvent = threading.Event() # gets triggered either when the server has either been bound correctly or when the number of bind attempts exceeds the config value maxBindRetries
        self._ready = False
        self._send_random_sleep_duration = 0

        self._syncObj.addOnTickCallback(self._onTick)

        for node in otherNodes:
            self.addNode(node)

        if not self._selfIsReadonlyNode:
            self._createServer()
        else:
            self._ready = True

    def _connToNode(self, conn):
        """
        Find the node to which a connection belongs.

        :param conn: connection object
        :type conn: TcpConnection
        :returns corresponding node or None if the node cannot be found
        :rtype Node or None
        """

        for node in self._connections:
            if self._connections[node] is conn:
                return node
        return None

    def tryGetReady(self):
        """
        Try to bind the server if necessary.

        :raises TransportNotReadyError if the server could not be bound
        """

        self._maybeBind()

    @property
    def ready(self):
        return self._ready

    def _createServer(self):
        """
        Create the TCP server (but don't bind yet)
        """

        conf = self._syncObj.conf
        bindAddr = conf.bindAddress
        seflAddr = getattr(self._selfNode, 'address')
        if bindAddr is not None:
            host, port = bindAddr.rsplit(':', 1)
        elif seflAddr is not None:
            host, port = seflAddr.rsplit(':', 1)
            if ':' in host:
                host = '::'
            else:
                host = '0.0.0.0'
        else:
            raise RuntimeError('Unable to determine bind address')
        
        if host != '0.0.0.0':
            host = globalDnsResolver().resolve(host)
        self._server = TcpServer(self._syncObj._poller, host, port, onNewConnection = self._onNewIncomingConnection,
                                 sendBufferSize = conf.sendBufferSize,
                                 recvBufferSize = conf.recvBufferSize,
                                 connectionTimeout = conf.connectionTimeout)

    def _maybeBind(self):
        """
        Bind the server unless it is already bound, this is a read-only node, or the last attempt was too recently.

        :raises TransportNotReadyError if the bind attempt fails
        """

        if self._ready or self._selfIsReadonlyNode or monotonicTime() < self._lastBindAttemptTime + self._syncObj.conf.bindRetryTime:
            return
        self._lastBindAttemptTime = monotonicTime()
        try:
            self._server.bind()
        except Exception as e:
            self._bindAttempts += 1
            if self._syncObj.conf.maxBindRetries and self._bindAttempts >= self._syncObj.conf.maxBindRetries:
                self._bindOverEvent.set()
                raise TransportNotReadyError
        else:
            self._ready = True
            self._bindOverEvent.set()

    def _onTick(self):
        """
        Tick callback. Binds the server and connects to other nodes as necessary.
        """

        try:
            self._maybeBind()
        except TransportNotReadyError:
            pass
        self._connectIfNecessary()

    def _onNewIncomingConnection(self, conn):
        """
        Callback for connections initiated by the other side

        :param conn: connection object
        :type conn: TcpConnection
        """

        self._unknownConnections.add(conn)
        encryptor = self._syncObj.encryptor
        if encryptor:
            conn.encryptor = encryptor
        conn.setOnMessageReceivedCallback(functools.partial(self._onIncomingMessageReceived, conn))
        conn.setOnDisconnectedCallback(functools.partial(self._onDisconnected, conn))

    def _onIncomingMessageReceived(self, conn, message):
        """
        Callback for initial messages on incoming connections. Handles encryption, utility messages, and association of the connection with a Node.
        Once this initial setup is done, the relevant connected callback is executed, and further messages are deferred to the onMessageReceived callback.

        :param conn: connection object
        :type conn: TcpConnection
        :param message: received message
        :type message: any
        """

        if self._syncObj.encryptor and not conn.sendRandKey:
            conn.sendRandKey = message
            conn.recvRandKey = os.urandom(32)
            conn.send(conn.recvRandKey)
            return

        # Utility messages
        if isinstance(message, list) and self._onUtilityMessage(conn, message):
            return

        # At this point, message should be either a node ID (i.e. address) or 'readonly'
        node = self._nodeAddrToNode[message] if message in self._nodeAddrToNode else None

        if node is None and message != 'readonly':
            conn.disconnect()
            self._unknownConnections.discard(conn)
            return

        readonly = node is None
        if readonly:
            nodeId = str(self._readonlyNodesCounter)
            node = Node(nodeId)
            self._readonlyNodes.add(node)
            self._readonlyNodesCounter += 1

        self._unknownConnections.discard(conn)
        self._connections[node] = conn
        conn.setOnMessageReceivedCallback(functools.partial(self._onMessageReceived, node))
        if not readonly:
            self._onNodeConnected(node)
        else:
            self._onReadonlyNodeConnected(node)

    def _onUtilityMessage(self, conn, message):
        command = message[0]
        if command in self._onUtilityMessageCallbacks:
            message[0] = command.upper()
            callback = functools.partial(self._utilityCallback, conn = conn, args = message)
            try:
                self._onUtilityMessageCallbacks[command](message[1:], callback)
            except Exception as e:
                conn.send(str(e))
            return True

    def _utilityCallback(self, res, err, conn, args):
        """
        Callback for the utility messages

        :param res: result of the command
        :param err: error code (one of pysyncobj.config.FAIL_REASON)
        :param conn: utility connection
        :param args: command with arguments
        """

        if not (err is None and res):
            cmdResult = 'SUCCESS' if err == FAIL_REASON.SUCCESS else 'FAIL'
            res = ' '.join(map(str, [cmdResult] + args))
        conn.send(res)

    def _shouldConnect(self, node):
        """
        Check whether this node should initiate a connection to another node

        :param node: the other node
        :type node: Node
        """

        return isinstance(node, TCPNode) and node not in self._preventConnectNodes and (self._selfIsReadonlyNode or self._selfNode.address > node.address)

    def _connectIfNecessarySingle(self, node):
        """
        Connect to a node if necessary.

        :param node: node to connect to
        :type node: Node
        """

        if node in self._connections and self._connections[node].state != CONNECTION_STATE.DISCONNECTED:
            return True
        if not self._shouldConnect(node):
            return False
        assert node in self._connections # Since we "should connect" to this node, there should always be a connection object already in place.
        if node in self._lastConnectAttempt and monotonicTime() - self._lastConnectAttempt[node] < self._syncObj.conf.connectionRetryTime:
            return False
        self._lastConnectAttempt[node] = monotonicTime()
        return self._connections[node].connect(node.ip, node.port)

    def _connectIfNecessary(self):
        """
        Connect to all nodes as necessary.
        """

        for node in self._nodes:
            self._connectIfNecessarySingle(node)

    def _sendSelfAddress(self, conn):
        if self._selfIsReadonlyNode:
            conn.send('readonly')
        else:
            conn.send(self._selfNode.address)

    def _onOutgoingConnected(self, conn):
        """
        Callback for when a new connection from this to another node is established. Handles encryption and informs the other node which node this is.
        If encryption is disabled, this triggers the onNodeConnected callback and messages are deferred to the onMessageReceived callback.
        If encryption is enabled, the first message is handled by _onOutgoingMessageReceived.

        :param conn: connection object
        :type conn: TcpConnection
        """

        if self._syncObj.encryptor:
            conn.setOnMessageReceivedCallback(functools.partial(self._onOutgoingMessageReceived, conn)) # So we can process the sendRandKey
            conn.recvRandKey = os.urandom(32)
            conn.send(conn.recvRandKey)
        else:
            self._sendSelfAddress(conn)
            # The onMessageReceived callback is configured in addNode already.
            self._onNodeConnected(self._connToNode(conn))

    def _onOutgoingMessageReceived(self, conn, message):
        """
        Callback for receiving a message on a new outgoing connection. Used only if encryption is enabled to exchange the random keys.
        Once the key exchange is done, this triggers the onNodeConnected callback, and further messages are deferred to the onMessageReceived callback.

        :param conn: connection object
        :type conn: TcpConnection
        :param message: received message
        :type message: any
        """

        if not conn.sendRandKey:
            conn.sendRandKey = message
            self._sendSelfAddress(conn)

        node = self._connToNode(conn)
        conn.setOnMessageReceivedCallback(functools.partial(self._onMessageReceived, node))
        self._onNodeConnected(node)

    def _onDisconnected(self, conn):
        """
        Callback for when a connection is terminated or considered dead. Initiates a reconnect if necessary.

        :param conn: connection object
        :type conn: TcpConnection
        """

        self._unknownConnections.discard(conn)
        node = self._connToNode(conn)
        if node is not None:
            if node in self._nodes:
                self._onNodeDisconnected(node)
                self._connectIfNecessarySingle(node)
            else:
                self._readonlyNodes.discard(node)
                self._onReadonlyNodeDisconnected(node)

    def waitReady(self):
        """
        Wait for the TCP transport to become ready for operation, i.e. the server to be bound.
        This method should be called from a different thread than used for the SyncObj ticks.

        :raises TransportNotReadyError: if the number of bind tries exceeds the configured limit
        """

        self._bindOverEvent.wait()
        if not self._ready:
            raise TransportNotReadyError

    def addNode(self, node):
        """
        Add a node to the network

        :param node: node to add
        :type node: TCPNode
        """

        self._nodes.add(node)
        self._nodeAddrToNode[node.address] = node
        if self._shouldConnect(node):
            conn = TcpConnection(
                poller = self._syncObj._poller,
                timeout = self._syncObj.conf.connectionTimeout,
                sendBufferSize = self._syncObj.conf.sendBufferSize,
                recvBufferSize = self._syncObj.conf.recvBufferSize,
                keepalive = self._syncObj.conf.tcp_keepalive,
            )
            conn.encryptor = self._syncObj.encryptor
            conn.setOnConnectedCallback(functools.partial(self._onOutgoingConnected, conn))
            conn.setOnMessageReceivedCallback(functools.partial(self._onMessageReceived, node))
            conn.setOnDisconnectedCallback(functools.partial(self._onDisconnected, conn))
            self._connections[node] = conn

    def dropNode(self, node):
        """
        Drop a node from the network

        :param node: node to drop
        :type node: Node
        """

        conn = self._connections.pop(node, None)
        if conn is not None:
            # Calling conn.disconnect() immediately triggers the onDisconnected callback if the connection isn't already disconnected, so this is necessary to prevent the automatic reconnect.
            self._preventConnectNodes.add(node)
            conn.disconnect()
            self._preventConnectNodes.remove(node)
        if isinstance(node, TCPNode):
            self._nodes.discard(node)
            self._nodeAddrToNode.pop(node.address, None)
        else:
            self._readonlyNodes.discard(node)
        self._lastConnectAttempt.pop(node, None)

    def send(self, node, message):
        """
        Send a message to a node. Returns False if the connection appears to be dead either before or after actually trying to send the message.

        :param node: target node
        :type node: Node
        :param message: message
        :param message: any
        :returns success
        :rtype bool
        """

        if node not in self._connections or self._connections[node].state != CONNECTION_STATE.CONNECTED:
            return False
        if self._send_random_sleep_duration:
            time.sleep(random.random() * self._send_random_sleep_duration)
        self._connections[node].send(message)
        if self._connections[node].state != CONNECTION_STATE.CONNECTED:
            return False
        return True

    def destroy(self):
        """
        Destroy this transport
        """

        self.setOnMessageReceivedCallback(None)
        self.setOnNodeConnectedCallback(None)
        self.setOnNodeDisconnectedCallback(None)
        self.setOnReadonlyNodeConnectedCallback(None)
        self.setOnReadonlyNodeDisconnectedCallback(None)
        for node in self._nodes | self._readonlyNodes:
            self.dropNode(node)
        if self._server is not None:
            self._server.unbind()
        for conn in list(self._unknownConnections):
            conn.disconnect()
        self._unknownConnections = set()
