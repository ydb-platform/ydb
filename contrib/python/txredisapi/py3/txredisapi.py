# coding: utf-8
# Copyright 2009 Alexandre Fiori
# https://github.com/fiorix/txredisapi
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#
# Credits:
#   The Protocol class is an improvement of txRedis' protocol,
#   by Dorian Raymer and Ludovico Magnocavallo.
#
#   Sharding and Consistent Hashing implementation by Gleicon Moraes.
#

import six

import bisect
import collections
import functools
import operator
import re
import warnings
import zlib
import string
import hashlib
import random

from typing import Optional, Union
from twisted.internet import defer, ssl
from twisted.internet import protocol
from twisted.internet import reactor
from twisted.internet.tcp import Connector
from twisted.protocols import basic
from twisted.protocols import policies
from twisted.python import log
from twisted.python.failure import Failure

try:
    import hiredis
except ImportError:
    hiredis = None


class RedisError(Exception):
    pass


class ConnectionError(RedisError):
    pass


class ResponseError(RedisError):
    pass


class ScriptDoesNotExist(ResponseError):
    pass


class NoScriptRunning(ResponseError):
    pass


class InvalidResponse(RedisError):
    pass


class InvalidData(RedisError):
    pass


class WatchError(RedisError):
    pass


class TimeoutError(ConnectionError):
    pass


def list_or_args(command, keys, args):
    oldapi = bool(args)
    try:
        iter(keys)
        if isinstance(keys, six.string_types) or \
                isinstance(keys, six.binary_type):
            keys = [keys]
            if not oldapi:
                return keys
            oldapi = True
    except TypeError:
        oldapi = True
        keys = [keys]

    if oldapi:
        warnings.warn(DeprecationWarning(
            "Passing *args to redis.%s is deprecated. "
            "Pass an iterable to ``keys`` instead" % command))
        keys.extend(args)
    return keys

# Possible first characters in a string containing an integer or a float.
_NUM_FIRST_CHARS = frozenset(string.digits + "+-.")


class MultiBulkStorage(object):
    def __init__(self, parent=None):
        self.items = None
        self.pending = None
        self.parent = parent

    def set_pending(self, pending):
        if self.pending is None:
            if pending < 0:
                self.items = None
                self.pending = 0
            else:
                self.items = []
                self.pending = pending
            return self
        else:
            m = MultiBulkStorage(self)
            m.set_pending(pending)
            return m

    def append(self, item):
        self.pending -= 1
        self.items.append(item)


class LineReceiver(protocol.Protocol, basic._PauseableMixin):
    callLater = reactor.callLater
    line_mode = 1
    __buffer = six.b('')
    delimiter = six.b('\r\n')
    MAX_LENGTH = 16384

    def clearLineBuffer(self):
        b = self.__buffer
        self.__buffer = six.b('')
        return b

    def dataReceived(self, data, unpause=False):
        if unpause is True:
            if self.__buffer:
                self.__buffer = data + self.__buffer
            else:
                self.__buffer += data

            self.resumeProducing()
        else:
            self.__buffer = self.__buffer + data

        while self.line_mode and not self.paused:
            try:
                line, self.__buffer = self.__buffer.split(self.delimiter, 1)
            except ValueError:
                if len(self.__buffer) > self.MAX_LENGTH:
                    line, self.__buffer = self.__buffer, six.b('')
                    return self.lineLengthExceeded(line)
                break
            else:
                linelength = len(line)
                if linelength > self.MAX_LENGTH:
                    exceeded = line + self.__buffer
                    self.__buffer = six.b('')
                    return self.lineLengthExceeded(exceeded)
                if hasattr(line, 'decode'):
                    why = self.lineReceived(line.decode())
                else:
                    why = self.lineReceived(line)
                if why or self.transport and self.transport.disconnecting:
                    return why
        else:
            if not self.paused:
                data = self.__buffer
                self.__buffer = six.b('')
                if data:
                    return self.rawDataReceived(data)

    def setLineMode(self, extra=six.b('')):
        self.line_mode = 1
        if extra:
            self.pauseProducing()
            self.callLater(0, self.dataReceived, extra, True)

    def setRawMode(self):
        self.line_mode = 0

    def rawDataReceived(self, data):
        raise NotImplementedError

    def lineReceived(self, line):
        raise NotImplementedError

    def sendLine(self, line):
        if isinstance(line, six.text_type):
            line = line.encode()
        return self.transport.write(line + self.delimiter)

    def lineLengthExceeded(self, line):
        return self.transport.loseConnection()


class ReplyQueue(defer.DeferredQueue):
    """
    Subclass defer.DeferredQueue to maintain consistency of
    producers / consumers in light of defer.cancel
    """
    def _cancelGet(self, d):
        # rather than remove(d), the default twisted behavior
        # we need to maintain an entry in the waiting list
        # because the reply code assumes that every call
        # to transport.write() generates a corresponding
        # reply value in the queue.
        # so we will just replace the cancelled deferred
        # with a noop
        i = self.waiting.index(d)
        self.waiting[i] = defer.Deferred()


def _blocking_command(release_on_callback):
    """
    Decorator used for marking protocol methods as `blocking` (methods that
    block connection from being used for sending another requests)

    release_on_callback means whether connection should be automatically
    released when deferred returned by method is fired
    """
    def decorator(method):
        method._blocking = True
        method._release_on_callback = release_on_callback
        return method
    return decorator


class BaseRedisProtocol(LineReceiver):
    """
    Redis client protocol.
    """

    def __init__(self, charset="utf-8", errors="strict", replyTimeout=None,
                 password=None, dbid=None, convertNumbers=True):
        self.charset = charset
        self.errors = errors

        self.bulk_length = 0
        self.bulk_buffer = bytearray()

        self.post_proc = []
        self.multi_bulk = MultiBulkStorage()

        self.replyQueue = ReplyQueue()

        self.transactions = 0
        self.pendingTransaction = False
        self.inTransaction = False
        self.inMulti = False
        self.unwatch_cc = lambda: ()
        self.commit_cc = lambda: ()

        self.script_hashes = set()

        self.pipelining = False
        self.pipelined_commands = []
        self.pipelined_replies = []

        self.replyTimeout = replyTimeout
        self.password = password
        self.dbid = dbid
        self.convertNumbers = convertNumbers

        self._waiting_for_connect = []
        self._waiting_for_disconnect = []


    def whenConnected(self):
        d = defer.Deferred()
        self._waiting_for_connect.append(d)
        return d


    def whenDisconnected(self):
        d = defer.Deferred()
        self._waiting_for_disconnect.append(d)
        return d


    @defer.inlineCallbacks
    def connectionMade(self):
        if self.password is not None:
            try:
                response = yield self.auth(self.password)
                if isinstance(response, ResponseError):
                    raise response
            except Exception as e:
                self.factory.continueTrying = False
                self.transport.loseConnection()

                msg = "Redis error: could not auth: %s" % (str(e))
                self.factory.connectionError(msg)
                if self.factory.isLazy:
                    log.msg(msg)
                return None

        if self.dbid is not None:
            try:
                response = yield self.select(self.dbid)
                if isinstance(response, ResponseError):
                    raise response
            except Exception as e:
                self.factory.continueTrying = False
                self.transport.loseConnection()

                msg = "Redis error: could not set dbid=%s: %s" % \
                      (self.dbid, str(e))
                self.factory.connectionError(msg)
                if self.factory.isLazy:
                    log.msg(msg)
                return None

        self.connected = 1
        self._waiting_for_connect, dfrs = [], self._waiting_for_connect
        for d in dfrs:
            d.callback(self)

    def connectionLost(self, why):
        self.connected = 0
        self.script_hashes.clear()

        self._waiting_for_disconnect, dfrs = [], self._waiting_for_disconnect
        for d in dfrs:
            d.callback(self)

        LineReceiver.connectionLost(self, why)
        while self.replyQueue.waiting:
            self.replyReceived(ConnectionError("Lost connection"))

    def lineReceived(self, line):
        """
        Reply types:
          "-" error message
          "+" single line status reply
          ":" integer number (protocol level only?)
          "$" bulk data
          "*" multi-bulk data
        """
        if line:
            token, data = line[0], line[1:]
        else:
            return

        if token == "$":  # bulk data
            try:
                self.bulk_length = int(data)
            except ValueError:
                self.replyReceived(InvalidResponse("Cannot convert data "
                                                   "'%s' to integer" % data))
            else:
                if self.bulk_length == -1:
                    self.bulk_length = 0
                    self.bulkDataReceived(None)
                else:
                    self.bulk_length += 2  # 2 == \r\n
                    self.setRawMode()

        elif token == "*":  # multi-bulk data
            try:
                n = int(data)
            except (TypeError, ValueError):
                self.multi_bulk = MultiBulkStorage()
                self.replyReceived(InvalidResponse("Cannot convert "
                                                   "multi-response header "
                                                   "'%s' to integer" % data))
            else:
                self.multi_bulk = self.multi_bulk.set_pending(n)
                if n in (0, -1):
                    self.multiBulkDataReceived()

        elif token == "+":  # single line status
            if data == "QUEUED":
                self.transactions += 1
                self.replyReceived(data)
            else:
                if self.multi_bulk.pending:
                    self.handleMultiBulkElement(data)
                else:
                    self.replyReceived(data)

        elif token == "-":  # error
            reply = ResponseError(data[4:] if data[:4] == "ERR" else data)
            if self.multi_bulk.pending:
                self.handleMultiBulkElement(reply)
            else:
                self.replyReceived(reply)

        elif token == ":":  # integer
            try:
                reply = int(data)
            except ValueError:
                reply = InvalidResponse(
                    "Cannot convert data '%s' to integer" % data)

            if self.multi_bulk.pending:
                self.handleMultiBulkElement(reply)
            else:
                self.replyReceived(reply)

    def rawDataReceived(self, data):
        """
        Process and dispatch to bulkDataReceived.
        """
        if self.bulk_length:
            data, rest = data[:self.bulk_length], data[self.bulk_length:]
            self.bulk_length -= len(data)
        else:
            rest = ""

        self.bulk_buffer.extend(data)
        if self.bulk_length == 0:
            bulk_buffer = self.bulk_buffer[:-2]
            self.bulk_buffer = bytearray()
            self.bulkDataReceived(bytes(bulk_buffer))
            self.setLineMode(extra=rest)

    def bulkDataReceived(self, data):
        """
        Receipt of a bulk data element.
        """
        el = None
        if data is not None:
            el = self.tryConvertData(data)

        if self.multi_bulk.pending or self.multi_bulk.items:
            self.handleMultiBulkElement(el)
        else:
            self.replyReceived(el)

    def tryConvertData(self, data):
        # The hiredis reader implicitly returns integers
        if isinstance(data, six.integer_types):
            return data
        if isinstance(data, list):
            return [self.tryConvertData(x) for x in data]
        el = None
        if self.convertNumbers:
            if data:
                num_data = data
                try:
                    if isinstance(data, six.binary_type):
                        num_data = data.decode()
                except UnicodeError:
                    pass
                else:
                    if num_data[0] in _NUM_FIRST_CHARS:  # Most likely a number
                        try:
                            el = int(num_data) if num_data.find('.') == -1 \
                                else float(num_data)
                        except ValueError:
                            pass

        if el is None:
            el = data
            if self.charset is not None:
                try:
                    el = data.decode(self.charset)
                except UnicodeDecodeError:
                    pass
                except AttributeError:
                    el = data
        return el

    def handleMultiBulkElement(self, element):
        self.multi_bulk.append(element)

        if not self.multi_bulk.pending:
            self.multiBulkDataReceived()

    def multiBulkDataReceived(self):
        """
        Receipt of list or set of bulk data elements.
        """
        while self.multi_bulk.parent and not self.multi_bulk.pending:
            p = self.multi_bulk.parent
            p.append(self.multi_bulk.items)
            self.multi_bulk = p

        if not self.multi_bulk.pending:
            reply = self.multi_bulk.items
            self.multi_bulk = MultiBulkStorage()

            reply = self.handleTransactionData(reply)

            self.replyReceived(reply)

    def handleTransactionData(self, reply):
        if self.inTransaction and isinstance(reply, list):
            # watch or multi has been called
            if self.transactions > 0:
                # multi: this must be an exec [commit] reply
                self.transactions -= len(reply)
            if self.transactions == 0:
                self.commit_cc()
            if not self.inTransaction:  # multi: this must be an exec reply
                tmp = []
                for f, v in zip(self.post_proc[1:], reply):
                    if callable(f):
                        tmp.append(f(v))
                    else:
                        tmp.append(v)
                    reply = tmp
            self.post_proc = []
        return reply

    def replyReceived(self, reply):
        """
        Complete reply received and ready to be pushed to the requesting
        function.
        """
        self.replyQueue.put(reply)

    @staticmethod
    def handle_reply(r):
        if isinstance(r, Exception):
            raise r
        return r

    def _encode_value(self, arg):
        if isinstance(arg, six.binary_type):
            return arg
        elif isinstance(arg, six.text_type):
            if self.charset is None:
                try:
                    return arg.encode()
                except UnicodeError:
                    pass
                raise InvalidData("Encoding charset was not specified")
            try:
                return arg.encode(self.charset, self.errors)
            except UnicodeEncodeError as e:
                raise InvalidData(
                    "Error encoding unicode value '%s': %s" %
                    (repr(arg), e))
        elif isinstance(arg, float):
            return format(arg, "f").encode()
        elif isinstance(arg, bytearray):
            return bytes(arg)
        else:
            return str(arg).format().encode()

    def _build_command(self, *args, **kwargs):
        # Build the redis command.
        cmds = bytearray()
        cmd_count = 0
        for s in args:
            cmd = self._encode_value(s)
            cmds.extend(six.b("$"))
            for token in self._encode_value(len(cmd)), cmd:
                cmds.extend(token)
                cmds.extend(six.b("\r\n"))
            cmd_count += 1

        command = bytes(six.b("").join(
            [six.b("*"), self._encode_value(cmd_count), six.b("\r\n")]) + cmds)
        if not isinstance(command, six.binary_type):
            command = command.encode()
        return command

    def execute_command(self, *args, **kwargs):
        if self.connected == 0:
            raise ConnectionError("Not connected")
        else:
            command = self._build_command(*args, **kwargs)
            # When pipelining, buffer this command into our list of
            # pipelined commands. Otherwise, write the command immediately.
            if self.pipelining:
                self.pipelined_commands.append(command)
            else:
                self.transport.write(command)

            # Return deferred that will contain the result of this command.
            # Note: when using pipelining, this deferred will NOT return
            # until after execute_pipeline is called.

            result = defer.Deferred()

            def fire_result(value):
                if result.called:
                    return
                result.callback(value)

            response = self.replyQueue.get().addCallback(self.handle_reply)
            response.addBoth(fire_result)

            apply_timeout = kwargs.get('apply_timeout', True)
            if self.replyTimeout and apply_timeout:
                delayed_call = None

                def fire_timeout():
                    error_text = 'Not received Redis response in {0} seconds'.format(self.replyTimeout)
                    result.errback(TimeoutError(error_text))
                    while self.replyQueue.waiting:
                        self.replyQueue.put(TimeoutError(error_text))
                    self.transport.abortConnection()

                def cancel_timeout(value):
                    if delayed_call.active():
                        delayed_call.cancel()
                    return value

                delayed_call = self.callLater(self.replyTimeout, fire_timeout)
                result.addBoth(cancel_timeout)

            # When pipelining, we need to keep track of the deferred replies
            # so that we can wait for them in a DeferredList when
            # execute_pipeline is called.
            if self.pipelining:
                self.pipelined_replies.append(result)

            if self.inMulti:
                self.post_proc.append(kwargs.get("post_proc"))
            else:
                if "post_proc" in kwargs:
                    f = kwargs["post_proc"]
                    if callable(f):
                        result.addCallback(f)
            return result

    ##
    # REDIS COMMANDS
    ##

    # Connection handling
    def quit(self):
        """
        Close the connection
        """
        self.factory.continueTrying = False
        return self.execute_command("QUIT")

    def auth(self, password):
        """
        Simple password authentication if enabled
        """
        return self.execute_command("AUTH", password)

    def ping(self):
        """
        Ping the server
        """
        return self.execute_command("PING")

    # Commands operating on all value types
    def exists(self, key):
        """
        Test if a key exists
        """
        return self.execute_command("EXISTS", key)

    def delete(self, keys, *args):
        """
        Delete one or more keys
        """
        keys = list_or_args("delete", keys, args)
        return self.execute_command("DEL", *keys)

    def type(self, key):
        """
        Return the type of the value stored at key
        """
        return self.execute_command("TYPE", key)

    def keys(self, pattern="*"):
        """
        Return all the keys matching a given pattern
        """
        return self.execute_command("KEYS", pattern)

    @staticmethod
    def _build_scan_args(cursor, pattern, count):
        """
        Construct arguments list for SCAN, SSCAN, HSCAN, ZSCAN commands
        """
        args = [cursor]
        if pattern is not None:
            args.extend(("MATCH", pattern))
        if count is not None:
            args.extend(("COUNT", count))

        return args

    def scan(self, cursor=0, pattern=None, count=None):
        """
        Incrementally iterate the keys in database
        """
        args = self._build_scan_args(cursor, pattern, count)
        return self.execute_command("SCAN", *args)

    def randomkey(self):
        """
        Return a random key from the key space
        """
        return self.execute_command("RANDOMKEY")

    def rename(self, oldkey, newkey):
        """
        Rename the old key in the new one,
        destroying the newname key if it already exists
        """
        return self.execute_command("RENAME", oldkey, newkey)

    def renamenx(self, oldkey, newkey):
        """
        Rename the oldname key to newname,
        if the newname key does not already exist
        """
        return self.execute_command("RENAMENX", oldkey, newkey)

    def dbsize(self):
        """
        Return the number of keys in the current db
        """
        return self.execute_command("DBSIZE")

    def expire(self, key, time):
        """
        Set a time to live in seconds on a key
        """
        return self.execute_command("EXPIRE", key, time)

    def persist(self, key):
        """
        Remove the expire from a key
        """
        return self.execute_command("PERSIST", key)

    def ttl(self, key):
        """
        Get the time to live in seconds of a key
        """
        return self.execute_command("TTL", key)

    def select(self, index):
        """
        Select the DB with the specified index
        """
        return self.execute_command("SELECT", index)

    def move(self, key, dbindex):
        """
        Move the key from the currently selected DB to the dbindex DB
        """
        return self.execute_command("MOVE", key, dbindex)

    def flush(self, all_dbs=False):
        warnings.warn(DeprecationWarning(
            "redis.flush() has been deprecated, "
            "use redis.flushdb() or redis.flushall() instead"))
        return all_dbs and self.flushall() or self.flushdb()

    def flushdb(self):
        """
        Remove all the keys from the currently selected DB
        """
        return self.execute_command("FLUSHDB")

    def flushall(self):
        """
        Remove all the keys from all the databases
        """
        return self.execute_command("FLUSHALL")

    def time(self):
        """
        Returns the current server time as a two items lists: a Unix timestamp
        and the amount of microseconds already elapsed in the current second
        """
        return self.execute_command("TIME")

    # Commands operating on string values
    def set(self, key, value, expire=None, pexpire=None,
            only_if_not_exists=False, only_if_exists=False):
        """
        Set a key to a string value
        """
        args = []
        if expire is not None:
            args.extend(("EX", expire))
        if pexpire is not None:
            args.extend(("PX", pexpire))
        if only_if_not_exists and only_if_exists:
            raise RedisError("only_if_not_exists and only_if_exists "
                             "cannot be true simultaneously")
        if only_if_not_exists:
            args.append("NX")
        if only_if_exists:
            args.append("XX")
        return self.execute_command("SET", key, value, *args)

    def get(self, key):
        """
        Return the string value of the key
        """
        return self.execute_command("GET", key)

    def getbit(self, key, offset):
        """
        Return the bit value at offset in the string value stored at key
        """
        return self.execute_command("GETBIT", key, offset)

    def getset(self, key, value):
        """
        Set a key to a string returning the old value of the key
        """
        return self.execute_command("GETSET", key, value)

    def mget(self, keys, *args):
        """
        Multi-get, return the strings values of the keys
        """
        keys = list_or_args("mget", keys, args)
        return self.execute_command("MGET", *keys)

    def setbit(self, key, offset, value):
        """
        Sets or clears the bit at offset in the string value stored at key
        """
        if isinstance(value, bool):
            value = int(value)
        return self.execute_command("SETBIT", key, offset, value)

    def setnx(self, key, value):
        """
        Set a key to a string value if the key does not exist
        """
        return self.execute_command("SETNX", key, value)

    def setex(self, key, time, value):
        """
        Set+Expire combo command
        """
        return self.execute_command("SETEX", key, time, value)

    def mset(self, mapping):
        """
        Set the respective keys to the respective values.
        """
        items = []
        for pair in six.iteritems(mapping):
            items.extend(pair)
        return self.execute_command("MSET", *items)

    def msetnx(self, mapping):
        """
        Set multiple keys to multiple values in a single atomic
        operation if none of the keys already exist
        """
        items = []
        for pair in six.iteritems(mapping):
            items.extend(pair)
        return self.execute_command("MSETNX", *items)

    def bitop(self, operation, destkey, *srckeys):
        """
        Perform a bitwise operation between multiple keys
        and store the result in the destination key.
        """
        srclen = len(srckeys)
        if srclen == 0:
            return defer.fail(RedisError("no ``srckeys`` specified"))
        if isinstance(operation, six.string_types):
            operation = operation.upper()
        elif operation is operator.and_ or operation is operator.__and__:
            operation = 'AND'
        elif operation is operator.or_ or operation is operator.__or__:
            operation = 'OR'
        elif operation is operator.__xor__ or operation is operator.xor:
            operation = 'XOR'
        elif operation is operator.__not__ or operation is operator.not_:
            operation = 'NOT'
        if operation not in ('AND', 'OR', 'XOR', 'NOT'):
            return defer.fail(InvalidData(
                "Invalid operation: %s" % operation))
        if operation == 'NOT' and srclen > 1:
            return defer.fail(RedisError(
                "bitop NOT takes only one ``srckey``"))
        return self.execute_command('BITOP', operation, destkey, *srckeys)

    def bitcount(self, key, start=None, end=None):
        if (end is None and start is not None) or \
                (start is None and end is not None):
            raise RedisError("``start`` and ``end`` must both be specified")
        if start is not None:
            t = (start, end)
        else:
            t = ()
        return self.execute_command("BITCOUNT", key, *t)

    def incr(self, key, amount=1):
        """
        Increment the integer value of key
        """
        return self.execute_command("INCRBY", key, amount)

    def incrby(self, key, amount):
        """
        Increment the integer value of key by integer
        """
        return self.incr(key, amount)

    def decr(self, key, amount=1):
        """
        Decrement the integer value of key
        """
        return self.execute_command("DECRBY", key, amount)

    def decrby(self, key, amount):
        """
        Decrement the integer value of key by integer
        """
        return self.decr(key, amount)

    def append(self, key, value):
        """
        Append the specified string to the string stored at key
        """
        return self.execute_command("APPEND", key, value)

    def substr(self, key, start, end=-1):
        """
        Return a substring of a larger string
        """
        return self.execute_command("SUBSTR", key, start, end)

    # Commands operating on lists
    def push(self, key, value, tail=False):
        warnings.warn(DeprecationWarning(
            "redis.push() has been deprecated, "
            "use redis.lpush() or redis.rpush() instead"))

        return tail and self.rpush(key, value) or self.lpush(key, value)

    def rpush(self, key, value):
        """
        Append an element to the tail of the List value at key
        """
        if isinstance(value, tuple) or isinstance(value, list):
            return self.execute_command("RPUSH", key, *value)
        else:
            return self.execute_command("RPUSH", key, value)

    def lpush(self, key, value):
        """
        Append an element to the head of the List value at key
        """
        if isinstance(value, tuple) or isinstance(value, list):
            return self.execute_command("LPUSH", key, *value)
        else:
            return self.execute_command("LPUSH", key, value)

    def llen(self, key):
        """
        Return the length of the List value at key
        """
        return self.execute_command("LLEN", key)

    def lrange(self, key, start, end):
        """
        Return a range of elements from the List at key
        """
        return self.execute_command("LRANGE", key, start, end)

    def ltrim(self, key, start, end):
        """
        Trim the list at key to the specified range of elements
        """
        return self.execute_command("LTRIM", key, start, end)

    def lindex(self, key, index):
        """
        Return the element at index position from the List at key
        """
        return self.execute_command("LINDEX", key, index)

    def lset(self, key, index, value):
        """
        Set a new value as the element at index position of the List at key
        """
        return self.execute_command("LSET", key, index, value)

    def lrem(self, key, count, value):
        """
        Remove the first-N, last-N, or all the elements matching value
        from the List at key
        """
        return self.execute_command("LREM", key, count, value)

    def pop(self, key, tail=False):
        warnings.warn(DeprecationWarning(
            "redis.pop() has been deprecated, "
            "user redis.lpop() or redis.rpop() instead"))

        return tail and self.rpop(key) or self.lpop(key)

    def lpop(self, key):
        """
        Return and remove (atomically) the first element of the List at key
        """
        return self.execute_command("LPOP", key)

    def rpop(self, key):
        """
        Return and remove (atomically) the last element of the List at key
        """
        return self.execute_command("RPOP", key)

    @_blocking_command(release_on_callback=True)
    def blpop(self, keys, timeout=0):
        """
        Blocking LPOP
        """
        if isinstance(keys, six.string_types):
            keys = [keys]
        else:
            keys = list(keys)

        keys.append(timeout)
        return self.execute_command("BLPOP", *keys, apply_timeout=False)

    @_blocking_command(release_on_callback=True)
    def brpop(self, keys, timeout=0):
        """
        Blocking RPOP
        """
        if isinstance(keys, six.string_types):
            keys = [keys]
        else:
            keys = list(keys)

        keys.append(timeout)
        return self.execute_command("BRPOP", *keys, apply_timeout=False)

    @_blocking_command(release_on_callback=True)
    def brpoplpush(self, source, destination, timeout=0):
        """
        Pop a value from a list, push it to another list and return
        it; or block until one is available.
        """
        return self.execute_command("BRPOPLPUSH", source, destination, timeout, apply_timeout=False)

    def rpoplpush(self, srckey, dstkey):
        """
        Return and remove (atomically) the last element of the source
        List  stored at srckey and push the same element to the
        destination List stored at dstkey
        """
        return self.execute_command("RPOPLPUSH", srckey, dstkey)

    def _make_set(self, result):
        if isinstance(result, list):
            return set(result)
        return result

    # Commands operating on sets
    def sadd(self, key, members, *args):
        """
        Add the specified member to the Set value at key
        """
        members = list_or_args("sadd", members, args)
        return self.execute_command("SADD", key, *members)

    def srem(self, key, members, *args):
        """
        Remove the specified member from the Set value at key
        """
        members = list_or_args("srem", members, args)
        return self.execute_command("SREM", key, *members)

    def spop(self, key):
        """
        Remove and return (pop) a random element from the Set value at key
        """
        return self.execute_command("SPOP", key)

    def smove(self, srckey, dstkey, member):
        """
        Move the specified member from one Set to another atomically
        """
        return self.execute_command(
            "SMOVE", srckey, dstkey, member).addCallback(bool)

    def scard(self, key):
        """
        Return the number of elements (the cardinality) of the Set at key
        """
        return self.execute_command("SCARD", key)

    def sismember(self, key, value):
        """
        Test if the specified value is a member of the Set at key
        """
        return self.execute_command("SISMEMBER", key, value).addCallback(bool)

    def sinter(self, keys, *args):
        """
        Return the intersection between the Sets stored at key1, ..., keyN
        """
        keys = list_or_args("sinter", keys, args)
        return self.execute_command("SINTER", *keys).addCallback(
            self._make_set)

    def sinterstore(self, dstkey, keys, *args):
        """
        Compute the intersection between the Sets stored
        at key1, key2, ..., keyN, and store the resulting Set at dstkey
        """
        keys = list_or_args("sinterstore", keys, args)
        return self.execute_command("SINTERSTORE", dstkey, *keys)

    def sunion(self, keys, *args):
        """
        Return the union between the Sets stored at key1, key2, ..., keyN
        """
        keys = list_or_args("sunion", keys, args)
        return self.execute_command("SUNION", *keys).addCallback(
            self._make_set)

    def sunionstore(self, dstkey, keys, *args):
        """
        Compute the union between the Sets stored
        at key1, key2, ..., keyN, and store the resulting Set at dstkey
        """
        keys = list_or_args("sunionstore", keys, args)
        return self.execute_command("SUNIONSTORE", dstkey, *keys)

    def sdiff(self, keys, *args):
        """
        Return the difference between the Set stored at key1 and
        all the Sets key2, ..., keyN
        """
        keys = list_or_args("sdiff", keys, args)
        return self.execute_command("SDIFF", *keys).addCallback(
            self._make_set)

    def sdiffstore(self, dstkey, keys, *args):
        """
        Compute the difference between the Set key1 and all the
        Sets key2, ..., keyN, and store the resulting Set at dstkey
        """
        keys = list_or_args("sdiffstore", keys, args)
        return self.execute_command("SDIFFSTORE", dstkey, *keys)

    def smembers(self, key):
        """
        Return all the members of the Set value at key
        """
        return self.execute_command("SMEMBERS", key).addCallback(
            self._make_set)

    def srandmember(self, key):
        """
        Return a random member of the Set value at key
        """
        return self.execute_command("SRANDMEMBER", key)

    def sscan(self, key, cursor=0, pattern=None, count=None):
        args = self._build_scan_args(cursor, pattern, count)
        return self.execute_command("SSCAN", key, *args)

    # Commands operating on sorted zsets (sorted sets)
    def zadd(self, key, score, member, *args):
        """
        Add the specified member to the Sorted Set value at key
        or update the score if it already exist
        """
        if args:
            # Args should be pairs (have even number of elements)
            if len(args) % 2:
                return defer.fail(InvalidData(
                    "Invalid number of arguments to ZADD"))
            else:
                l = [score, member]
                l.extend(args)
                args = l
        else:
            args = [score, member]
        return self.execute_command("ZADD", key, *args)

    def zrem(self, key, *args):
        """
        Remove the specified member from the Sorted Set value at key
        """
        return self.execute_command("ZREM", key, *args)

    def zincr(self, key, member):
        return self.zincrby(key, 1, member)

    def zdecr(self, key, member):
        return self.zincrby(key, -1, member)

    @staticmethod
    def _handle_zincrby(data):
        if isinstance(data, (six.binary_type, six.text_type)):
            return float(data)
        return data

    def zincrby(self, key, increment, member):
        """
        If the member already exists increment its score by increment,
        otherwise add the member setting increment as score
        """
        return self.execute_command("ZINCRBY",
                                    key, increment,
                                    member).addCallback(self._handle_zincrby)

    def zrank(self, key, member):
        """
        Return the rank (or index) or member in the sorted set at key,
        with scores being ordered from low to high
        """
        return self.execute_command("ZRANK", key, member)

    def zrevrank(self, key, member):
        """
        Return the rank (or index) or member in the sorted set at key,
        with scores being ordered from high to low
        """
        return self.execute_command("ZREVRANK", key, member)

    def _handle_withscores(self, r):
        if isinstance(r, list):
            # Return a list tuples of form (value, score)
            return list((x[0], float(x[1])) for x in zip(r[::2], r[1::2]))
        return r

    def _zrange(self, key, start, end, withscores, reverse):
        if reverse:
            cmd = "ZREVRANGE"
        else:
            cmd = "ZRANGE"
        if withscores:
            pieces = (cmd, key, start, end, "WITHSCORES")
        else:
            pieces = (cmd, key, start, end)
        r = self.execute_command(*pieces)
        if withscores:
            r.addCallback(self._handle_withscores)
        return r

    def zrange(self, key, start=0, end=-1, withscores=False):
        """
        Return a range of elements from the sorted set at key
        """
        return self._zrange(key, start, end, withscores, False)

    def zrevrange(self, key, start=0, end=-1, withscores=False):
        """
        Return a range of elements from the sorted set at key,
        exactly like ZRANGE, but the sorted set is ordered in
        traversed in reverse order, from the greatest to the smallest score
        """
        return self._zrange(key, start, end, withscores, True)

    def _zrangebyscore(self, key, min, max, withscores, offset, count, rev):
        if rev:
            cmd = "ZREVRANGEBYSCORE"
        else:
            cmd = "ZRANGEBYSCORE"
        if (offset is None) != (count is None):  # XNOR
            return defer.fail(InvalidData(
                "Invalid count and offset arguments to %s" % cmd))
        if withscores:
            pieces = [cmd, key, min, max, "WITHSCORES"]
        else:
            pieces = [cmd, key, min, max]
        if offset is not None and count is not None:
            pieces.extend(("LIMIT", offset, count))
        r = self.execute_command(*pieces)
        if withscores:
            r.addCallback(self._handle_withscores)
        return r

    def zrangebyscore(self, key, min='-inf', max='+inf',
                      withscores=False, offset=None, count=None):
        """
        Return all the elements with score >= min and score <= max
        (a range query) from the sorted set
        """
        return self._zrangebyscore(key, min, max, withscores, offset,
                                   count, False)

    def zrevrangebyscore(self, key, max='+inf', min='-inf',
                         withscores=False, offset=None, count=None):
        """
        ZRANGEBYSCORE in reverse order
        """
        # ZREVRANGEBYSCORE takes max before min
        return self._zrangebyscore(key, max, min, withscores, offset,
                                   count, True)

    def zcount(self, key, min='-inf', max='+inf'):
        """
        Return the number of elements with score >= min and score <= max
        in the sorted set
        """
        if min == '-inf' and max == '+inf':
            return self.zcard(key)
        return self.execute_command("ZCOUNT", key, min, max)

    def zcard(self, key):
        """
        Return the cardinality (number of elements) of the sorted set at key
        """
        return self.execute_command("ZCARD", key)

    @staticmethod
    def _handle_zscore(data):
        if isinstance(data, (six.binary_type, six.text_type)):
            return int(data)
        return data

    def zscore(self, key, element):
        """
        Return the score associated with the specified element of the sorted
        set at key
        """
        return self.execute_command("ZSCORE", key,
                                    element).addCallback(self._handle_zscore)

    def zremrangebyrank(self, key, min=0, max=-1):
        """
        Remove all the elements with rank >= min and rank <= max from
        the sorted set
        """
        return self.execute_command("ZREMRANGEBYRANK", key, min, max)

    def zremrangebyscore(self, key, min='-inf', max='+inf'):
        """
        Remove all the elements with score >= min and score <= max from
        the sorted set
        """
        return self.execute_command("ZREMRANGEBYSCORE", key, min, max)

    def zunionstore(self, dstkey, keys, aggregate=None):
        """
        Perform a union over a number of sorted sets with optional
        weight and aggregate
        """
        return self._zaggregate("ZUNIONSTORE", dstkey, keys, aggregate)

    def zinterstore(self, dstkey, keys, aggregate=None):
        """
        Perform an intersection over a number of sorted sets with optional
        weight and aggregate
        """
        return self._zaggregate("ZINTERSTORE", dstkey, keys, aggregate)

    def _zaggregate(self, command, dstkey, keys, aggregate):
        pieces = [command, dstkey, len(keys)]
        if isinstance(keys, dict):
            keys, weights = list(zip(*keys.items()))
        else:
            weights = None

        pieces.extend(keys)
        if weights:
            pieces.append("WEIGHTS")
            pieces.extend(weights)

        if aggregate:
            if aggregate is min:
                aggregate = 'MIN'
            elif aggregate is max:
                aggregate = 'MAX'
            elif aggregate is sum:
                aggregate = 'SUM'
            else:
                err_flag = True
                if isinstance(aggregate, six.string_types):
                    aggregate_u = aggregate.upper()
                    if aggregate_u in ('MIN', 'MAX', 'SUM'):
                        aggregate = aggregate_u
                        err_flag = False
                if err_flag:
                    return defer.fail(InvalidData(
                        "Invalid aggregate function: %s" % aggregate))
            pieces.extend(("AGGREGATE", aggregate))
        return self.execute_command(*pieces)

    def zscan(self, key, cursor=0, pattern=None, count=None):
        args = self._build_scan_args(cursor, pattern, count)
        return self.execute_command("ZSCAN", key, *args)

    # Commands operating on hashes
    def hset(self, key, field, value):
        """
        Set the hash field to the specified value. Creates the hash if needed
        """
        return self.execute_command("HSET", key, field, value)

    def hsetnx(self, key, field, value):
        """
        Set the hash field to the specified value if the field does not exist.
        Creates the hash if needed
        """
        return self.execute_command("HSETNX", key, field, value)

    def hget(self, key, field):
        """
        Retrieve the value of the specified hash field.
        """
        return self.execute_command("HGET", key, field)

    def hmget(self, key, fields):
        """
        Get the hash values associated to the specified fields.
        """
        return self.execute_command("HMGET", key, *fields)

    def hmset(self, key, mapping):
        """
        Set the hash fields to their respective values.
        """
        items = []
        for pair in six.iteritems(mapping):
            items.extend(pair)
        return self.execute_command("HMSET", key, *items)

    def hincr(self, key, field):
        return self.hincrby(key, field, 1)

    def hdecr(self, key, field):
        return self.hincrby(key, field, -1)

    def hincrby(self, key, field, integer):
        """
        Increment the integer value of the hash at key on field with integer.
        """
        return self.execute_command("HINCRBY", key, field, integer)

    def hexists(self, key, field):
        """
        Test for existence of a specified field in a hash
        """
        return self.execute_command("HEXISTS", key, field).addCallback(bool)

    def hdel(self, key, fields):
        """
        Remove the specified field or fields from a hash
        """
        if isinstance(fields, six.string_types):
            fields = [fields]
        else:
            fields = list(fields)
        return self.execute_command("HDEL", key, *fields)

    def hlen(self, key):
        """
        Return the number of items in a hash.
        """
        return self.execute_command("HLEN", key)

    def hkeys(self, key):
        """
        Return all the fields in a hash.
        """
        return self.execute_command("HKEYS", key)

    def hvals(self, key):
        """
        Return all the values in a hash.
        """
        return self.execute_command("HVALS", key)

    def hgetall(self, key):
        """
        Return all the fields and associated values in a hash.
        """
        f = lambda d: dict(list(zip(d[::2], d[1::2])))
        return self.execute_command("HGETALL", key, post_proc=f)

    def hscan(self, key, cursor=0, pattern=None, count=None):
        args = self._build_scan_args(cursor, pattern, count)
        return self.execute_command("HSCAN", key, *args)

    # Sorting
    def sort(self, key, start=None, end=None, by=None, get=None,
             desc=None, alpha=False, store=None):
        if (start is not None and end is None) or \
           (end is not None and start is None):
            raise RedisError("``start`` and ``end`` must both be specified")

        pieces = [key]
        if by is not None:
            pieces.append("BY")
            pieces.append(by)
        if start is not None and end is not None:
            pieces.append("LIMIT")
            pieces.append(start)
            pieces.append(end)
        if get is not None:
            pieces.append("GET")
            pieces.append(get)
        if desc:
            pieces.append("DESC")
        if alpha:
            pieces.append("ALPHA")
        if store is not None:
            pieces.append("STORE")
            pieces.append(store)

        return self.execute_command("SORT", *pieces)

    def _clear_txstate(self):
        if self.inTransaction:
            self.pendingTransaction = False
            self.inTransaction = False
            self.inMulti = False
            self.factory.connectionQueue.put(self)

    @_blocking_command(release_on_callback=False)
    def watch(self, keys):
        if not self.pendingTransaction:
            self.pendingTransaction = True
            self.inTransaction = False
            self.inMulti = False
            self.unwatch_cc = self._clear_txstate
            self.commit_cc = lambda: ()
        if isinstance(keys, six.string_types):
            keys = [keys]
        d = self.execute_command("WATCH", *keys).addCallback(self._tx_started)
        return d

    def unwatch(self):
        self.unwatch_cc()
        return self.execute_command("UNWATCH")

    # Transactions
    # multi() will return a deferred with a "connection" object
    # That object must be used for further interactions within
    # the transaction. At the end, either exec() or discard()
    # must be executed.
    @_blocking_command(release_on_callback=False)
    def multi(self, keys=None):
        self.pendingTransaction = True
        self.inTransaction = False
        self.inMulti = True
        self.unwatch_cc = lambda: ()
        self.commit_cc = self._clear_txstate
        if keys is not None:
            d = self.watch(keys)
            d.addCallback(lambda _: self.execute_command("MULTI"))
        else:
            d = self.execute_command("MULTI")
        d.addCallback(self._tx_started)
        return d

    def _tx_started(self, response):
        if response != 'OK':
            raise RedisError('Invalid response: %s' % response)
        self.inTransaction = True
        return self

    def _commit_check(self, response):
        if response is None:
            self.transactions = 0
            self._clear_txstate()
            raise WatchError("Transaction failed")
        else:
            return response

    def commit(self):
        if self.inMulti is False:
            raise RedisError("Not in transaction")
        return self.execute_command("EXEC").addCallback(self._commit_check)

    def discard(self):
        if self.inMulti is False:
            raise RedisError("Not in transaction")
        self.post_proc = []
        self.transactions = 0
        self._clear_txstate()
        return self.execute_command("DISCARD")

    # Returns a proxy that works just like .multi() except that commands
    # are simply buffered to be written all at once in a pipeline.
    # http://redis.io/topics/pipelining
    @_blocking_command(release_on_callback=False)
    def pipeline(self):

        # Return a deferred that returns self (rather than simply self) to allow
        # ConnectionHandler to wrap this method with async connection retrieval.
        self.pipelining = True
        self.pipelined_commands = []
        self.pipelined_replies = []
        return defer.succeed(self)

    @defer.inlineCallbacks
    def execute_pipeline(self):
        if not self.pipelining:
            err = "Not currently pipelining commands, " \
                  "please use pipeline() first"
            raise RedisError(err)

        # Flush all the commands at once to redis. Wait for all replies
        # to come back using a deferred list.
        self.transport.write(six.b("").join(self.pipelined_commands))

        d = defer.DeferredList(
            deferredList=self.pipelined_replies,
            fireOnOneErrback=True,
            consumeErrors=True,
            )

        d.addBoth(self._clear_pipeline_state)

        results = yield d

        return [value for success, value in results]

    def _clear_pipeline_state(self, response):
        if self.pipelining:
            self.pipelining = False
            self.pipelined_commands = []
            self.pipelined_replies = []
            self.factory.connectionQueue.put(self)

        return response

    # Publish/Subscribe
    # see the SubscriberProtocol for subscribing to channels
    def publish(self, channel, message):
        """
        Publish message to a channel
        """
        return self.execute_command("PUBLISH", channel, message)

    # Persistence control commands
    def save(self):
        """
        Synchronously save the DB on disk
        """
        return self.execute_command("SAVE")

    def bgsave(self):
        """
        Asynchronously save the DB on disk
        """
        return self.execute_command("BGSAVE")

    def lastsave(self):
        """
        Return the UNIX time stamp of the last successfully saving of the
        dataset on disk
        """
        return self.execute_command("LASTSAVE")

    def shutdown(self):
        """
        Synchronously save the DB on disk, then shutdown the server
        """
        self.factory.continueTrying = False
        return self.execute_command("SHUTDOWN")

    def bgrewriteaof(self):
        """
        Rewrite the append only file in background when it gets too big
        """
        return self.execute_command("BGREWRITEAOF")

    def _process_info(self, r):
        if isinstance(r, six.binary_type):
            r = r.decode()
        keypairs = [x for x in r.split('\r\n') if
                    ':' in x and not x.startswith('#')]
        d = {}
        for kv in keypairs:
            k, v = kv.split(':')
            d[k] = v
        return d

    # Remote server control commands
    def info(self, type=None):
        """
        Provide information and statistics about the server
        """
        if type is None:
            return self.execute_command("INFO")
        else:
            r = self.execute_command("INFO", type)
            return r.addCallback(self._process_info)

    # slaveof is missing

    # Redis 2.6 scripting commands
    def _eval(self, script, script_hash, keys, args):
        n = len(keys)
        keys_and_args = tuple(keys) + tuple(args)
        r = self.execute_command("EVAL", script, n, *keys_and_args)
        if script_hash in self.script_hashes:
            return r
        return r.addCallback(self._eval_success, script_hash)

    def _eval_success(self, r, script_hash):
        self.script_hashes.add(script_hash)
        return r

    def _evalsha_failed(self, err, script, script_hash, keys, args):
        if err.check(ScriptDoesNotExist):
            return self._eval(script, script_hash, keys, args)
        return err

    def eval(self, script, keys=[], args=[]):
        if isinstance(script, six.text_type):
            script = script.encode()
        h = hashlib.sha1(script).hexdigest()
        if h in self.script_hashes:
            return self.evalsha(h, keys, args).addErrback(
                self._evalsha_failed, script, h, keys, args)
        return self._eval(script, h, keys, args)

    def _evalsha_errback(self, err, script_hash):
        if err.check(ResponseError):
            if err.value.args[0].startswith(u'NOSCRIPT'):
                if script_hash in self.script_hashes:
                    self.script_hashes.remove(script_hash)
                raise ScriptDoesNotExist("No script matching hash: %s found" %
                                         script_hash)
        return err

    def evalsha(self, sha1_hash, keys=[], args=[]):
        n = len(keys)
        keys_and_args = tuple(keys) + tuple(args)
        r = self.execute_command("EVALSHA",
                                 sha1_hash, n,
                                 *keys_and_args)
        r.addErrback(self._evalsha_errback, sha1_hash)
        if sha1_hash not in self.script_hashes:
            r.addCallback(self._eval_success, sha1_hash)
        return r

    def _script_exists_success(self, r):
        l = [bool(x) for x in r]
        if len(l) == 1:
            return l[0]
        else:
            return l

    def script_exists(self, *hashes):
        return self.execute_command("SCRIPT", "EXISTS",
                                    post_proc=self._script_exists_success,
                                    *hashes)

    def _script_flush_success(self, r):
        self.script_hashes.clear()
        return r

    def script_flush(self):
        return self.execute_command("SCRIPT", "FLUSH").addCallback(
            self._script_flush_success)

    def _handle_script_kill(self, r):
        if isinstance(r, Failure):
            if r.check(ResponseError):
                if r.value.args[0].startswith(u'NOTBUSY'):
                    raise NoScriptRunning("No script running")
        return r

    def script_kill(self):
        return self.execute_command("SCRIPT",
                                    "KILL").addBoth(self._handle_script_kill)

    def script_load(self, script):
        return self.execute_command("SCRIPT",  "LOAD", script)

    # Redis 2.8.9 HyperLogLog commands
    def pfadd(self, key, elements, *args):
        elements = list_or_args("pfadd", elements, args)
        return self.execute_command("PFADD", key, *elements)

    def pfcount(self, keys, *args):
        keys = list_or_args("pfcount", keys, args)
        return self.execute_command("PFCOUNT", *keys)

    def pfmerge(self, destKey, sourceKeys, *args):
        sourceKeys = list_or_args("pfmerge", sourceKeys, args)
        return self.execute_command("PFMERGE", destKey, *sourceKeys)

    _SENTINEL_NODE_FLAGS = (("is_master", "master"), ("is_slave", "slave"),
                            ("is_sdown", "s_down"), ("is_odown", "o_down"),
                            ("is_sentinel", "sentinel"),
                            ("is_disconnected", "disconnected"),
                            ("is_master_down", "master_down"))

    def _parse_sentinel_state(self, state_array):
        as_dict = dict(
            (self.tryConvertData(key), self.tryConvertData(value))
            for key, value in zip(state_array[::2], state_array[1::2])
        )
        flags = set(as_dict['flags'].split(','))
        for bool_name, flag_name in self._SENTINEL_NODE_FLAGS:
            as_dict[bool_name] = flag_name in flags
        return as_dict

    def sentinel_masters(self):
        def convert(raw):
            result = {}
            for array in raw:
                as_dict = self._parse_sentinel_state(array)
                result[as_dict['name']] = as_dict
            return result
        return self.execute_command("SENTINEL", "MASTERS").addCallback(convert)

    def sentinel_slaves(self, service_name):
        def convert(raw):
            return [
                self._parse_sentinel_state(array)
                for array in raw
            ]
        return self.execute_command("SENTINEL", "SLAVES", service_name)\
            .addCallback(convert)

    def sentinel_get_master_addr_by_name(self, service_name):
        return self.execute_command("SENTINEL", "GET-MASTER-ADDR-BY-NAME", service_name)

    def role(self):
        return self.execute_command("ROLE")


class HiredisProtocol(BaseRedisProtocol):
    def __init__(self, *args, **kwargs):
        BaseRedisProtocol.__init__(self, *args, **kwargs)
        self._reader = hiredis.Reader(protocolError=InvalidData,
                                      replyError=ResponseError)

    def dataReceived(self, data, unpause=False):
        if data:
            self._reader.feed(data)
        res = self._reader.gets()
        while res is not False:
            if isinstance(res, (six.text_type, six.binary_type, list)):
                res = self.tryConvertData(res)
            if res == "QUEUED":
                self.transactions += 1
            else:
                res = self.handleTransactionData(res)

            self.replyReceived(res)
            res = self._reader.gets()

    def _convert_bin_values(self, result):
        if isinstance(result, list):
            return [self._convert_bin_values(x) for x in result]
        elif isinstance(result, dict):
            return dict((self._convert_bin_values(k), self._convert_bin_values(v))
                        for k, v in six.iteritems(result))
        elif isinstance(result, six.binary_type):
            return self.tryConvertData(result)
        return result

    def commit(self):
        r = BaseRedisProtocol.commit(self)
        return r.addCallback(self._convert_bin_values)

    def scan(self, cursor=0, pattern=None, count=None):
        r = BaseRedisProtocol.scan(self, cursor, pattern, count)
        return r.addCallback(self._convert_bin_values)

    def sscan(self, key, cursor=0, pattern=None, count=None):
        r = BaseRedisProtocol.sscan(self, key, cursor, pattern, count)
        return r.addCallback(self._convert_bin_values)

if hiredis is not None:
    RedisProtocol = HiredisProtocol
else:
    RedisProtocol = BaseRedisProtocol


class MonitorProtocol(RedisProtocol):
    """
    monitor has the same behavior as subscribe: hold the connection until
    something happens.

    take care with the performance impact: http://redis.io/commands/monitor
    """

    def messageReceived(self, message):
        pass

    def replyReceived(self, reply):
        self.messageReceived(reply)

    def monitor(self):
        return self.execute_command("MONITOR", apply_timeout=False)

    def stop(self):
        self.transport.loseConnection()


class SubscriberProtocol(RedisProtocol):
    _sub_unsub_reponses = set([u"subscribe", u"unsubscribe", u"psubscribe", u"punsubscribe",
                               b"subscribe", b"unsubscribe", b"psubscribe", b"punsubscribe"])

    def messageReceived(self, pattern, channel, message):
        pass

    def replyReceived(self, reply):
        if isinstance(reply, list):
            reply_len = len(reply)
            if reply_len >= 3 and reply[-3] in (u"message", b"message"):
                self.messageReceived(None, *reply[-2:])
            elif reply_len >= 4 and reply[-4] in (u"pmessage", b"pmessage"):
                self.messageReceived(*reply[-3:])
            elif reply_len >= 3 and reply[-3] in self._sub_unsub_reponses and len(self.replyQueue.waiting) == 0:
                pass
            else:
                self.replyQueue.put(reply[-3:])
        else:
            self.replyQueue.put(reply)

    def subscribe(self, channels):
        if isinstance(channels, six.string_types):
            channels = [channels]
        return self.execute_command("SUBSCRIBE", *channels, apply_timeout=False)

    def unsubscribe(self, channels):
        if isinstance(channels, six.string_types):
            channels = [channels]
        return self.execute_command("UNSUBSCRIBE", *channels)

    def psubscribe(self, patterns):
        if isinstance(patterns, six.string_types):
            patterns = [patterns]
        return self.execute_command("PSUBSCRIBE", *patterns, apply_timeout=False)

    def punsubscribe(self, patterns):
        if isinstance(patterns, six.string_types):
            patterns = [patterns]
        return self.execute_command("PUNSUBSCRIBE", *patterns)


class ConnectionHandler(object):
    def __init__(self, factory):
        self._factory = factory
        self._connected = factory.deferred

    def disconnect(self):
        self._factory.continueTrying = 0
        self._factory.disconnectCalled = True
        for conn in self._factory.pool:
            try:
                conn.transport.loseConnection()
            except:
                pass

        return self._factory.waitForEmptyPool()

    def __getattr__(self, method):
        def wrapper(*args, **kwargs):
            protocol_method = getattr(self._factory.protocol, method)
            blocking = getattr(protocol_method, '_blocking', False)
            release_on_callback = getattr(protocol_method, '_release_on_callback', True)

            d = self._factory.getConnection(peek=not blocking)

            def callback(connection):
                try:
                    d = protocol_method(connection, *args, **kwargs)
                except:
                    if blocking:
                        self._factory.connectionQueue.put(connection)
                    raise

                def put_back(reply):
                    self._factory.connectionQueue.put(connection)
                    return reply

                if blocking and release_on_callback:
                    d.addBoth(put_back)

                return d
            d.addCallback(callback)
            return d
        return wrapper

    def __repr__(self):
        try:
            cli = self._factory.pool[0].transport.getPeer()
        except:
            return "<Redis Connection: Not connected>"
        else:
            return "<Redis Connection: %s:%s - %d connection(s)>" % \
                   (cli.host, cli.port, self._factory.size)


class UnixConnectionHandler(ConnectionHandler):
    def __repr__(self):
        try:
            cli = self._factory.pool[0].transport.getPeer()
        except:
            return "<Redis Connection: Not connected>"
        else:
            return "<Redis Unix Connection: %s - %d connection(s)>" % \
                   (cli.name, self._factory.size)


ShardedMethods = frozenset([
    "decr",
    "delete",
    "exists",
    "expire",
    "get",
    "get_type",
    "getset",
    "hdel",
    "hexists",
    "hget",
    "hgetall",
    "hincrby",
    "hkeys",
    "hlen",
    "hmget",
    "hmset",
    "hset",
    "hvals",
    "incr",
    "lindex",
    "llen",
    "lrange",
    "lrem",
    "lset",
    "ltrim",
    "pop",
    "publish",
    "push",
    "rename",
    "sadd",
    "set",
    "setex",
    "setnx",
    "sismember",
    "smembers",
    "srem",
    "ttl",
    "zadd",
    "zcard",
    "zcount",
    "zdecr",
    "zincr",
    "zincrby",
    "zrange",
    "zrangebyscore",
    "zrevrangebyscore",
    "zrevrank",
    "zrank",
    "zrem",
    "zremrangebyscore",
    "zremrangebyrank",
    "zrevrange",
    "zscore"
])

_findhash = re.compile(r'.+\{(.*)\}.*')


class HashRing(object):
    """Consistent hash for redis API"""
    def __init__(self, nodes=[], replicas=160):
        self.nodes = []
        self.replicas = replicas
        self.ring = {}
        self.sorted_keys = []

        for n in nodes:
            self.add_node(n)

    def add_node(self, node):
        self.nodes.append(node)
        for x in range(self.replicas):
            uuid = node._factory.uuid
            if isinstance(uuid, six.text_type):
                uuid = uuid.encode()
            crckey = zlib.crc32(six.b(":").join(
                [uuid, str(x).format().encode()]))
            self.ring[crckey] = node
            self.sorted_keys.append(crckey)

        self.sorted_keys.sort()

    def remove_node(self, node):
        self.nodes.remove(node)
        for x in range(self.replicas):
            crckey = zlib.crc32(six.b(":").join(
                [node, str(x).format().encode()]))
            self.ring.remove(crckey)
            self.sorted_keys.remove(crckey)

    def get_node(self, key):
        n, i = self.get_node_pos(key)
        return n
    # self.get_node_pos(key)[0]

    def get_node_pos(self, key):
        if len(self.ring) == 0:
            return [None, None]
        crc = zlib.crc32(key)
        idx = bisect.bisect(self.sorted_keys, crc)
        # prevents out of range index
        idx = min(idx, (self.replicas * len(self.nodes)) - 1)
        return [self.ring[self.sorted_keys[idx]], idx]

    def iter_nodes(self, key):
        if len(self.ring) == 0:
            yield None, None
        node, pos = self.get_node_pos(key)
        for k in self.sorted_keys[pos:]:
            yield k, self.ring[k]

    def __call__(self, key):
        return self.get_node(key)


class ShardedConnectionHandler(object):
    def __init__(self, connections):
        if isinstance(connections, defer.DeferredList):
            self._ring = None
            connections.addCallback(self._makeRing)
        else:
            self._ring = HashRing(connections)

    def _makeRing(self, connections):
        connections = list(map(operator.itemgetter(1), connections))
        self._ring = HashRing(connections)
        return self

    @defer.inlineCallbacks
    def disconnect(self):
        if not self._ring:
            raise ConnectionError("Not connected")

        for conn in self._ring.nodes:
            yield conn.disconnect()
        return True

    def _wrap(self, method, *args, **kwargs):
        try:
            key = args[0]
            assert isinstance(key, six.string_types)
        except:
            raise ValueError(
                "Method '%s' requires a key as the first argument" % method)

        m = _findhash.match(key)
        if m is not None and len(m.groups()) >= 1:
            node = self._ring(m.groups()[0])
        else:
            node = self._ring(key)

        return getattr(node, method)(*args, **kwargs)

    def pipeline(self):
        raise NotImplementedError("Pipelining is not supported across shards")

    def __getattr__(self, method):
        if method in ShardedMethods:
            return functools.partial(self._wrap, method)
        else:
            raise NotImplementedError("Method '%s' cannot be sharded" % method)

    @defer.inlineCallbacks
    def mget(self, keys, *args):
        """
        high-level mget, required because of the sharding support
        """

        keys = list_or_args("mget", keys, args)
        group = collections.defaultdict(lambda: [])
        for k in keys:
            node = self._ring(k)
            group[node].append(k)

        deferreds = []
        for node, keys in six.iteritems(group.items):
            nd = node.mget(keys)
            deferreds.append(nd)

        result = []
        response = yield defer.DeferredList(deferreds)
        for (success, values) in response:
            if success:
                result += values

        return result

    def __repr__(self):
        nodes = []
        for conn in self._ring.nodes:
            try:
                cli = conn._factory.pool[0].transport.getPeer()
            except:
                pass
            else:
                nodes.append(six.b("%s:%s/%d") %
                             (cli.host, cli.port, conn._factory.size))
        return "<Redis Sharded Connection: %s>" % ", ".join(nodes)


class ShardedUnixConnectionHandler(ShardedConnectionHandler):
    def __repr__(self):
        nodes = []
        for conn in self._ring.nodes:
            try:
                cli = conn._factory.pool[0].transport.getPeer()
            except:
                pass
            else:
                nodes.append(six.b("%s/%d") %
                             (cli.name, conn._factory.size))
        return "<Redis Sharded Connection: %s>" % ", ".join(nodes)


class PeekableQueue(defer.DeferredQueue):
    """
    A DeferredQueue that supports peeking, accessing random item without
    removing them from the queue.
    """
    def __init__(self, *args, **kwargs):
        defer.DeferredQueue.__init__(self, *args, **kwargs)

        self.peekers = []

    def peek(self):
        if self.pending:
            return defer.succeed(random.choice(self.pending))
        else:
            d = defer.Deferred()
            self.peekers.append(d)
            return d

    def remove(self, item):
        self.pending.remove(item)

    def put(self, obj):
        for d in self.peekers:
            d.callback(obj)
        self.peekers = []

        defer.DeferredQueue.put(self, obj)


class RedisFactory(protocol.ReconnectingClientFactory):
    maxDelay = 10
    protocol = RedisProtocol

    noisy = False

    def __init__(self, uuid, dbid, poolsize, isLazy=False,
                 handler=ConnectionHandler, charset="utf-8", password=None,
                 replyTimeout=None, convertNumbers=True):
        if not isinstance(poolsize, int):
            raise ValueError("Redis poolsize must be an integer, not %s" %
                             repr(poolsize))

        if not isinstance(dbid, (int, type(None))):
            raise ValueError("Redis dbid must be an integer, not %s" %
                             repr(dbid))

        self.uuid = uuid
        self.dbid = dbid
        self.poolsize = poolsize
        self.isLazy = isLazy
        self.charset = charset
        self.password = password
        self.replyTimeout = replyTimeout
        self.convertNumbers = convertNumbers

        self.idx = 0
        self.size = 0
        self.pool = []
        self.deferred = defer.Deferred()
        self.handler = handler(self)
        self.connectionQueue = PeekableQueue()
        self._waitingForEmptyPool = set()
        self.disconnectCalled = False

    def buildProtocol(self, addr):
        p = self.protocol(self.charset, replyTimeout=self.replyTimeout,
                          password=self.password, dbid=self.dbid,
                          convertNumbers=self.convertNumbers)
        p.factory = self
        p.whenConnected().addCallback(self.addConnection)
        return p

    def addConnection(self, conn):
        if self.disconnectCalled:
            conn.transport.loseConnection()
            return

        conn.whenDisconnected().addCallback(self.delConnection)
        self.connectionQueue.put(conn)
        self.pool.append(conn)
        self.size = len(self.pool)
        if self.deferred:
            if self.size == self.poolsize:
                self.deferred.callback(self.handler)
                self.deferred = None

    def delConnection(self, conn):
        try:
            self.pool.remove(conn)
        except Exception as e:
            log.msg("Could not remove connection from pool: %s" % str(e))

        self.size = len(self.pool)
        if not self.size and self._waitingForEmptyPool:
            deferreds = self._waitingForEmptyPool
            self._waitingForEmptyPool = set()
            for d in deferreds:
                d.callback(None)

    def _cancelWaitForEmptyPool(self, deferred):
        self._waitingForEmptyPool.discard(deferred)
        deferred.errback(defer.CancelledError())

    def waitForEmptyPool(self):
        """
        Returns a Deferred which fires when the pool size has reached 0.
        """
        if not self.size:
            return defer.succeed(None)
        d = defer.Deferred(self._cancelWaitForEmptyPool)
        self._waitingForEmptyPool.add(d)
        return d

    def connectionError(self, why):
        if self.deferred:
            self.deferred.errback(ValueError(why))
            self.deferred = None

    @defer.inlineCallbacks
    def getConnection(self, peek=False):
        if not self.continueTrying and not self.size:
            raise ConnectionError("Not connected")

        while True:
            if peek:
                conn = yield self.connectionQueue.peek()
            else:
                conn = yield self.connectionQueue.get()
            if conn.connected == 0:
                log.msg('Discarding dead connection.')
                if peek:
                    self.connectionQueue.remove(conn)
            else:
                return conn


class SubscriberFactory(RedisFactory):
    protocol = SubscriberProtocol

    def __init__(self, isLazy=False, handler=ConnectionHandler):
        RedisFactory.__init__(self, None, None, 1, isLazy=isLazy,
                              handler=handler)


class MonitorFactory(RedisFactory):
    protocol = MonitorProtocol

    def __init__(self, isLazy=False, handler=ConnectionHandler):
        RedisFactory.__init__(self, None, None, 1, isLazy=isLazy,
                              handler=handler)


def makeConnection(host, port, dbid, poolsize, reconnect, isLazy,
                   charset, password, ssl_context_factory, connectTimeout, replyTimeout,
                   convertNumbers):
    uuid = "%s:%d" % (host, port)
    factory = RedisFactory(uuid, dbid, poolsize, isLazy, ConnectionHandler,
                           charset, password, replyTimeout, convertNumbers)
    factory.continueTrying = reconnect
    for x in range(poolsize):
        if ssl_context_factory is True:
            ssl_context_factory = ssl.ClientContextFactory()
        if ssl_context_factory:
            reactor.connectSSL(host, port, factory, ssl_context_factory, connectTimeout)
        else:
            reactor.connectTCP(host, port, factory, connectTimeout)

    if isLazy:
        return factory.handler
    else:
        return factory.deferred


def makeShardedConnection(hosts, dbid, poolsize, reconnect, isLazy,
                          charset, password, ssl_context_factory, connectTimeout, replyTimeout,
                          convertNumbers):
    err = "Please use a list or tuple of host:port for sharded connections"
    if not isinstance(hosts, (list, tuple)):
        raise ValueError(err)

    connections = []
    for item in hosts:
        try:
            host, port = item.split(":")
            port = int(port)
        except:
            raise ValueError(err)

        c = makeConnection(host, port, dbid, poolsize, reconnect, isLazy,
                           charset, password, ssl_context_factory, connectTimeout, replyTimeout,
                           convertNumbers)
        connections.append(c)

    if isLazy:
        return ShardedConnectionHandler(connections)
    else:
        deferred = defer.DeferredList(connections)
        ShardedConnectionHandler(deferred)
        return deferred


def Connection(host="localhost", port=6379, dbid=None, reconnect=True,
               charset="utf-8", password=None, ssl_context_factory: Union[ssl.ClientContextFactory, bool]=False,
               connectTimeout=None, replyTimeout=None, convertNumbers=True):
    return makeConnection(host, port, dbid, 1, reconnect, False,
                          charset, password, ssl_context_factory, connectTimeout, replyTimeout,
                          convertNumbers)


def lazyConnection(host="localhost", port=6379, dbid=None, reconnect=True,
                   charset="utf-8", password=None, ssl_context_factory: Union[ssl.ClientContextFactory, bool]=False,
                   connectTimeout=None, replyTimeout=None, convertNumbers=True):
    return makeConnection(host, port, dbid, 1, reconnect, True,
                          charset, password, ssl_context_factory, connectTimeout, replyTimeout,
                          convertNumbers)


def ConnectionPool(host="localhost", port=6379, dbid=None,
                   poolsize=10, reconnect=True, charset="utf-8", password=None, ssl_context_factory: Union[ssl.ClientContextFactory, bool]=False,
                   connectTimeout=None, replyTimeout=None,
                   convertNumbers=True):
    return makeConnection(host, port, dbid, poolsize, reconnect, False,
                          charset, password, ssl_context_factory, connectTimeout, replyTimeout,
                          convertNumbers)


def lazyConnectionPool(host="localhost", port=6379, dbid=None,
                       poolsize=10, reconnect=True, charset="utf-8",
                       password=None, ssl_context_factory: Union[ssl.ClientContextFactory, bool]=False, connectTimeout=None, replyTimeout=None,
                       convertNumbers=True):
    return makeConnection(host, port, dbid, poolsize, reconnect, True,
                          charset, password, ssl_context_factory, connectTimeout, replyTimeout,
                          convertNumbers)


def ShardedConnection(hosts, dbid=None, reconnect=True, charset="utf-8",
                      password=None, ssl_context_factory: Union[ssl.ClientContextFactory, bool]=False, connectTimeout=None, replyTimeout=None,
                      convertNumbers=True):
    return makeShardedConnection(hosts, dbid, 1, reconnect, False,
                                 charset, password, ssl_context_factory, connectTimeout,
                                 replyTimeout, convertNumbers)


def lazyShardedConnection(hosts, dbid=None, reconnect=True, charset="utf-8",
                          password=None, ssl_context_factory: Union[ssl.ClientContextFactory, bool]=False,
                          connectTimeout=None, replyTimeout=None,
                          convertNumbers=True):
    return makeShardedConnection(hosts, dbid, 1, reconnect, True,
                                 charset, password, ssl_context_factory, connectTimeout,
                                 replyTimeout, convertNumbers)


def ShardedConnectionPool(hosts, dbid=None, poolsize=10, reconnect=True,
                          charset="utf-8", password=None, ssl_context_factory: Union[ssl.ClientContextFactory, bool]=False,
                          connectTimeout=None, replyTimeout=None,
                          convertNumbers=True):
    return makeShardedConnection(hosts, dbid, poolsize, reconnect, False,
                                 charset, password, ssl_context_factory, connectTimeout,
                                 replyTimeout, convertNumbers)


def lazyShardedConnectionPool(hosts, dbid=None, poolsize=10, reconnect=True,
                              charset="utf-8", password=None, ssl_context_factory: Union[ssl.ClientContextFactory, bool]=False,
                              connectTimeout=None, replyTimeout=None,
                              convertNumbers=True):
    return makeShardedConnection(hosts, dbid, poolsize, reconnect, True,
                                 charset, password, ssl_context_factory, connectTimeout,
                                 replyTimeout, convertNumbers)


def makeUnixConnection(path, dbid, poolsize, reconnect, isLazy,
                       charset, password, connectTimeout, replyTimeout,
                       convertNumbers):
    factory = RedisFactory(path, dbid, poolsize, isLazy, UnixConnectionHandler,
                           charset, password, replyTimeout, convertNumbers)
    factory.continueTrying = reconnect
    for x in range(poolsize):
        reactor.connectUNIX(path, factory, connectTimeout)

    if isLazy:
        return factory.handler
    else:
        return factory.deferred


def makeShardedUnixConnection(paths, dbid, poolsize, reconnect, isLazy,
                              charset, password, connectTimeout, replyTimeout,
                              convertNumbers):
    err = "Please use a list or tuple of paths for sharded unix connections"
    if not isinstance(paths, (list, tuple)):
        raise ValueError(err)

    connections = []
    for path in paths:
        c = makeUnixConnection(path, dbid, poolsize, reconnect, isLazy,
                               charset, password, connectTimeout, replyTimeout,
                               convertNumbers)
        connections.append(c)

    if isLazy:
        return ShardedUnixConnectionHandler(connections)
    else:
        deferred = defer.DeferredList(connections)
        ShardedUnixConnectionHandler(deferred)
        return deferred


def UnixConnection(path="/tmp/redis.sock", dbid=None, reconnect=True,
                   charset="utf-8", password=None,
                   connectTimeout=None, replyTimeout=None, convertNumbers=True):
    return makeUnixConnection(path, dbid, 1, reconnect, False,
                              charset, password, connectTimeout, replyTimeout,
                              convertNumbers)


def lazyUnixConnection(path="/tmp/redis.sock", dbid=None, reconnect=True,
                       charset="utf-8", password=None,
                       connectTimeout=None, replyTimeout=None,
                       convertNumbers=True):
    return makeUnixConnection(path, dbid, 1, reconnect, True,
                              charset, password, connectTimeout, replyTimeout,
                              convertNumbers)


def UnixConnectionPool(path="/tmp/redis.sock", dbid=None, poolsize=10,
                       reconnect=True, charset="utf-8", password=None,
                       connectTimeout=None, replyTimeout=None,
                       convertNumbers=True):
    return makeUnixConnection(path, dbid, poolsize, reconnect, False,
                              charset, password, connectTimeout, replyTimeout,
                              convertNumbers)


def lazyUnixConnectionPool(path="/tmp/redis.sock", dbid=None, poolsize=10,
                           reconnect=True, charset="utf-8", password=None,
                           connectTimeout=None, replyTimeout=None,
                           convertNumbers=True):
    return makeUnixConnection(path, dbid, poolsize, reconnect, True,
                              charset, password, connectTimeout, replyTimeout,
                              convertNumbers)


def ShardedUnixConnection(paths, dbid=None, reconnect=True, charset="utf-8",
                          password=None, connectTimeout=None, replyTimeout=None,
                          convertNumbers=True):
    return makeShardedUnixConnection(paths, dbid, 1, reconnect, False,
                                     charset, password, connectTimeout,
                                     replyTimeout, convertNumbers)


def lazyShardedUnixConnection(paths, dbid=None, reconnect=True,
                              charset="utf-8", password=None,
                              connectTimeout=None, replyTimeout=None,
                              convertNumbers=True):
    return makeShardedUnixConnection(paths, dbid, 1, reconnect, True,
                                     charset, password, connectTimeout,
                                     replyTimeout, convertNumbers)


def ShardedUnixConnectionPool(paths, dbid=None, poolsize=10, reconnect=True,
                              charset="utf-8", password=None,
                              connectTimeout=None, replyTimeout=None,
                              convertNumbers=True):
    return makeShardedUnixConnection(paths, dbid, poolsize, reconnect, False,
                                     charset, password, connectTimeout,
                                     replyTimeout, convertNumbers)


def lazyShardedUnixConnectionPool(paths, dbid=None, poolsize=10,
                                  reconnect=True, charset="utf-8",
                                  password=None, connectTimeout=None,
                                  replyTimeout=None, convertNumbers=True):
    return makeShardedUnixConnection(paths, dbid, poolsize, reconnect, True,
                                     charset, password, connectTimeout,
                                     replyTimeout, convertNumbers)


class MasterNotFoundError(ConnectionError):
    pass


class SentinelRedisProtocol(RedisProtocol):

    def connectionMade(self):
        self.factory.resetDelay()

        def check_role(role):
            if self.factory.is_master and role[0] != "master":
                self.transport.loseConnection()
                return defer.succeed(None)
            else:
                self.factory.resetDelay()
                return RedisProtocol.connectionMade(self)

        if self.password is not None:
            self.auth(self.password)

        return self.role().addCallback(check_role)


class SentinelConnectionFactory(RedisFactory):

    initialDelay = 0.1
    protocol = SentinelRedisProtocol

    def __init__(self, sentinel_manager, service_name, is_master, *args, **kwargs):
        RedisFactory.__init__(self, *args, **kwargs)

        self.sentinel_manager = sentinel_manager
        self.service_name = service_name
        self.is_master = is_master

        self._current_master_addr = None
        self._slave_no = 0

    def clientConnectionFailed(self, connector, reason):
        self.try_to_connect(connector)

    def clientConnectionLost(self, connector, unused_reason):
        self.try_to_connect(connector, nodelay=True)

    def try_to_connect(self, connector, force_master=False, nodelay=False):
        if not self.continueTrying:
            return

        def on_discovery_err(failure):
            failure.trap(MasterNotFoundError)
            log.msg("txredisapi: Can't get address from Sentinel: {0}".format(failure.value))
            reactor.callLater(self.delay, self.try_to_connect, connector)
            self.resetDelay()

        def on_master_addr(addr):
            if self._current_master_addr is not None and \
               self._current_master_addr != addr:
                self.resetDelay()
                # master has changed, dropping all alive connections
                for conn in self.pool:
                    conn.transport.loseConnection()

            self._current_master_addr = addr
            connector.host, connector.port = addr
            if nodelay:
                connector.connect()
            else:
                self.retry(connector)

        def on_slave_addrs(addrs):
            if addrs:
                connector.host, connector.port = addrs[self._slave_no % len(addrs)]
                self._slave_no += 1
                if nodelay:
                    connector.connect()
                else:
                    self.retry(connector)
            else:
                log.msg("txredisapi: No slaves discovered, falling back to master")
                self.try_to_connect(connector, force_master=True, nodelay=True)

        if self.is_master or force_master:
            self.sentinel_manager.discover_master(self.service_name) \
                .addCallbacks(on_master_addr, on_discovery_err)
        else:
            self.sentinel_manager.discover_slaves(self.service_name) \
                .addCallback(on_slave_addrs)


class Sentinel(object):

    discovery_timeout = 10

    def __init__(self, sentinel_addresses, min_other_sentinels=0, **connection_kwargs):
        self.sentinels = [
            lazyConnection(host, port, **connection_kwargs)
            for host, port in sentinel_addresses
        ]

        self.min_other_sentinels = min_other_sentinels

    def disconnect(self):
        return defer.gatherResults([sentinel.disconnect() for sentinel in self.sentinels],
                                   consumeErrors = True)

    def check_master_state(self, state):
        if not state["is_master"] or state["is_sdown"] or state["is_odown"]:
            return False

        if int(state["num-other-sentinels"]) < self.min_other_sentinels:
            return False

        return True

    def discover_master(self, service_name):
        result = defer.Deferred()

        def on_response(response):
            if result.called:
                return

            state = response.get(service_name)
            if state and self.check_master_state(state):
                result.callback((state["ip"], int(state["port"])))
                timeout_call.cancel()

        def on_timeout():
            if not result.called:
                result.errback(MasterNotFoundError(
                    "No master found for {0}".format(service_name)))

        # Ignoring errors
        for sentinel in self.sentinels:
            sentinel.sentinel_masters().addCallbacks(on_response, lambda _: None)

        timeout_call = reactor.callLater(self.discovery_timeout, on_timeout)

        return result

    @staticmethod
    def filter_slaves(slaves):
        """Remove slaves that are in ODOWN or SDOWN state"""
        return [
            (slave["ip"], int(slave["port"]))
            for slave in slaves
            if not slave["is_odown"] and not slave["is_sdown"]
        ]

    def discover_slaves(self, service_name):
        result = defer.Deferred()

        def on_response(response):
            if result.called:
                return

            slaves = self.filter_slaves(response)
            if slaves:
                result.callback(slaves)
                timeout_call.cancel()

        def on_timeout():
            if not result.called:
                result.callback([])

        for sentinel in self.sentinels:
            sentinel.sentinel_slaves(service_name).addCallbacks(on_response, lambda _: None)

        timeout_call = reactor.callLater(self.discovery_timeout, on_timeout)

        return result

    @staticmethod
    def _connect_factory_and_return_handler(factory, poolsize):
        for _ in range(poolsize):
            # host and port will be rewritten by try_to_connect
            connector = Connector("0.0.0.0", None, factory, factory.maxDelay, None, reactor)
            factory.try_to_connect(connector, nodelay=True)
        return factory.handler

    def master_for(self, service_name, factory_class=SentinelConnectionFactory,
                   dbid=None, poolsize=1, **connection_kwargs):
        factory = factory_class(sentinel_manager=self, service_name=service_name,
                                is_master=True, uuid=None, dbid=dbid,
                                poolsize=poolsize, **connection_kwargs)
        return self._connect_factory_and_return_handler(factory, poolsize)

    def slave_for(self, service_name, factory_class=SentinelConnectionFactory,
                  dbid=None, poolsize=1, **connection_kwargs):
        factory = factory_class(sentinel_manager=self, service_name=service_name,
                                is_master=False, uuid=None, dbid=dbid,
                                poolsize=poolsize, **connection_kwargs)
        return self._connect_factory_and_return_handler(factory, poolsize)


__all__ = [
    "Connection", "lazyConnection",
    "ConnectionPool", "lazyConnectionPool",
    "ShardedConnection", "lazyShardedConnection",
    "ShardedConnectionPool", "lazyShardedConnectionPool",
    "UnixConnection", "lazyUnixConnection",
    "UnixConnectionPool", "lazyUnixConnectionPool",
    "ShardedUnixConnection", "lazyShardedUnixConnection",
    "ShardedUnixConnectionPool", "lazyShardedUnixConnectionPool",
    "Sentinel", "MasterNotFoundError"
]

__author__ = "Alexandre Fiori"
__version__ = version = "1.4.11"
