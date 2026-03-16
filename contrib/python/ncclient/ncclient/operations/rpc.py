# Copyright 2009 Shikhar Bhushan
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from threading import Event, Lock
from uuid import uuid4

from ncclient.xml_ import *
from ncclient.logging_ import SessionLoggerAdapter
from ncclient.transport import SessionListener
from ncclient.operations import util
from ncclient.operations.errors import OperationError, TimeoutExpiredError, MissingCapabilityError

import logging
logger = logging.getLogger("ncclient.operations.rpc")


class RPCError(OperationError):

    "Represents an `rpc-error`. It is a type of :exc:`OperationError` and can be raised as such."

    tag_to_attr = {
        qualify("error-type"): "_type",
        qualify("error-tag"): "_tag",
        qualify("error-app-tag"): "_app_tag",
        qualify("error-severity"): "_severity",
        qualify("error-info"): "_info",
        qualify("error-path"): "_path",
        qualify("error-message"): "_message"
    }

    def __init__(self, raw, errs=None):
        self._raw = raw
        if errs is None:
            # Single RPCError
            self._errlist = None
            for attr in RPCError.tag_to_attr.values():
                setattr(self, attr, None)
            for subele in raw:
                attr = RPCError.tag_to_attr.get(subele.tag, None)
                if attr is not None:
                    setattr(self, attr, subele.text if attr != "_info" else to_xml(subele) )
            if self.message is not None:
                OperationError.__init__(self, self.message)
            else:
                OperationError.__init__(self, self.to_dict())
        else:
            # Multiple errors returned. Errors is a list of RPCError objs
            self._errlist = errs
            errlist = []
            for err in errs:
                if err.severity:
                    errsev = err.severity
                else:
                    errsev = 'undefined'
                if err.message:
                    errmsg = err.message
                else:
                    errmsg = 'not an error message in the reply. Enable debug'
                errordict = {"severity": errsev, "message":errmsg}
                errlist.append(errordict)
            # We are interested in the severity and the message
            self._severity = 'warning'
            self._message = "\n".join(["%s: %s" %(err['severity'].strip(), err['message'].strip()) for err in errlist])
            self.errors = errs
            has_error = filter(lambda higherr: higherr['severity'] == 'error', errlist)
            if has_error:
                self._severity = 'error'
            OperationError.__init__(self, self.message)

    def to_dict(self):
        return dict([ (attr[1:], getattr(self, attr)) for attr in RPCError.tag_to_attr.values() ])

    @property
    def xml(self):
        "The `rpc-error` element as returned in XML. \
        Multiple errors are returned as list of RPC errors"
        return self._raw

    @property
    def type(self):
        "The contents of the `error-type` element."
        return self._type

    @property
    def tag(self):
        "The contents of the `error-tag` element."
        return self._tag

    @property
    def app_tag(self):
        "The contents of the `error-app-tag` element."
        return self._app_tag

    @property
    def severity(self):
        "The contents of the `error-severity` element."
        return self._severity

    @property
    def path(self):
        "The contents of the `error-path` element if present or `None`."
        return self._path

    @property
    def message(self):
        "The contents of the `error-message` element if present or `None`."
        return self._message

    @property
    def info(self):
        "XML string or `None`; representing the `error-info` element."
        return self._info

    @property
    def errlist(self):
        "List of errors if this represents multiple errors, otherwise None."
        return self._errlist


class RPCReply:

    """Represents an *rpc-reply*. Only concerns itself with whether the operation was successful.

    *raw*: the raw unparsed reply

    *huge_tree*: parse XML with very deep trees and very long text content

    .. note::
        If the reply has not yet been parsed there is an implicit, one-time parsing overhead to
        accessing some of the attributes defined by this class.
    """

    ERROR_CLS = RPCError
    "Subclasses can specify a different error class, but it should be a subclass of `RPCError`."

    def __init__(self, raw, huge_tree=False, parsing_error_transform=None):
        self._raw = raw
        self._parsing_error_transform = parsing_error_transform
        self._parsed = False
        self._root = None
        self._errors = []
        self._huge_tree = huge_tree

    def __repr__(self):
        return self._raw

    def parse(self):
        "Parses the *rpc-reply*."
        if self._parsed: return
        root = self._root = to_ele(self._raw, huge_tree=self._huge_tree) # The <rpc-reply> element
        # Per RFC 4741 an <ok/> tag is sent when there are no errors or warnings
        ok = root.find(qualify("ok"))
        if ok is None:
            # Create RPCError objects from <rpc-error> elements
            error = root.find('.//'+qualify('rpc-error'))
            if error is not None:
                for err in root.getiterator(error.tag):
                    # Process a particular <rpc-error>
                    self._errors.append(self.ERROR_CLS(err))
        try:
            self._parsing_hook(root)
        except Exception as e:
            if self._parsing_error_transform is None:
                # re-raise as we have no workaround
                raise e

            # Apply device specific workaround and try again
            self._parsing_error_transform(root)
            self._parsing_hook(root)

        self._parsed = True

    def _parsing_hook(self, root):
        "No-op by default. Gets passed the *root* element for the reply."
        pass

    def set_parsing_error_transform(self, transform_function):
        self._parsing_error_transform = transform_function

    @property
    def xml(self):
        "*rpc-reply* element as returned."
        return self._raw

    @property
    def ok(self):
        "Boolean value indicating if there were no errors."
        self.parse()
        return not self.errors # empty list => false

    @property
    def error(self):
        "Returns the first :class:`RPCError` and `None` if there were no errors."
        self.parse()
        if self._errors:
            return self._errors[0]
        else:
            return None

    @property
    def errors(self):
        "List of `RPCError` objects. Will be empty if there were no *rpc-error* elements in reply."
        self.parse()
        return self._errors


class RPCReplyListener(SessionListener): # internal use

    creation_lock = Lock()

    # one instance per session -- maybe there is a better way??
    def __new__(cls, session, device_handler):
        with RPCReplyListener.creation_lock:
            instance = session.get_listener_instance(cls)
            if instance is None:
                instance = object.__new__(cls)
                instance._lock = Lock()
                instance._id2rpc = {}
                instance._device_handler = device_handler
                #instance._pipelined = session.can_pipeline
                session.add_listener(instance)
                instance.logger = SessionLoggerAdapter(logger,
                                                       {'session': session})
            return instance

    def register(self, id, rpc):
        with self._lock:
            self._id2rpc[id] = rpc

    def callback(self, root, raw):
        tag, attrs = root
        if self._device_handler.perform_qualify_check():
            if tag != qualify("rpc-reply"):
                return
        if "message-id" not in attrs:
            # required attribute so raise OperationError
            raise OperationError("Could not find 'message-id' attribute in <rpc-reply>")
        else:
            id = attrs["message-id"]  # get the msgid
            with self._lock:
                try:
                    rpc = self._id2rpc[id]  # the corresponding rpc
                    self.logger.debug("Delivering to %r", rpc)
                    rpc.deliver_reply(raw)
                except KeyError:
                    raise OperationError("Unknown 'message-id': %s" % id)
                # no catching other exceptions, fail loudly if must
                else:
                    # if no error delivering, can del the reference to the RPC
                    del self._id2rpc[id]

    def errback(self, err):
        try:
            for rpc in self._id2rpc.values():
                rpc.deliver_error(err)
        finally:
            self._id2rpc.clear()


class RaiseMode:
    """
    Define how errors indicated by RPC should be handled.

    Note that any error_filters defined in the device handler will still be
    applied, even if ERRORS or ALL is defined: If the filter matches, an exception
    will NOT be raised.

    """
    NONE = 0
    "Don't attempt to raise any type of `rpc-error` as :exc:`RPCError`."

    ERRORS = 1
    "Raise only when the `error-type` indicates it is an honest-to-god error."

    ALL = 2
    "Don't look at the `error-type`, always raise."


class RPC:

    """Base class for all operations, directly corresponding to *rpc* requests. Handles making the request, and taking delivery of the reply."""

    DEPENDS = []
    """Subclasses can specify their dependencies on capabilities as a list of URI's or abbreviated names, e.g. ':writable-running'. These are verified at the time of instantiation. If the capability is not available, :exc:`MissingCapabilityError` is raised."""

    REPLY_CLS = RPCReply
    "By default :class:`RPCReply`. Subclasses can specify a :class:`RPCReply` subclass."


    def __init__(self, session, device_handler, async_mode=False, timeout=30, raise_mode=RaiseMode.NONE, huge_tree=False):
        """
        *session* is the :class:`~ncclient.transport.Session` instance

        *device_handler" is the :class:`~ncclient.devices.*.*DeviceHandler` instance

        *async* specifies whether the request is to be made asynchronously, see :attr:`is_async`

        *timeout* is the timeout for a synchronous request, see :attr:`timeout`

        *raise_mode* specifies the exception raising mode, see :attr:`raise_mode`

        *huge_tree* parse xml with huge_tree support (e.g. for large text config retrieval), see :attr:`huge_tree`
        """
        self._session = session
        try:
            for cap in self.DEPENDS:
                self._assert(cap)
        except AttributeError:
            pass
        self._async = async_mode
        self._timeout = timeout
        self._raise_mode = raise_mode
        self._huge_tree = huge_tree
        self._id = uuid4().urn # Keeps things simple instead of having a class attr with running ID that has to be locked
        self._listener = RPCReplyListener(session, device_handler)
        self._listener.register(self._id, self)
        self._reply = None
        self._error = None
        self._event = Event()
        self._device_handler = device_handler
        self.logger = SessionLoggerAdapter(logger, {'session': session})


    def _wrap(self, subele):
        # internal use
        ele = new_ele("rpc", {"message-id": self._id},
                      **self._device_handler.get_xml_extra_prefix_kwargs())
        ele.append(subele)
        return to_xml(ele)

    def _request(self, op):
        """Implementations of :meth:`request` call this method to send the request and process the reply.

        In synchronous mode, blocks until the reply is received and returns :class:`RPCReply`. Depending on the :attr:`raise_mode` a `rpc-error` element in the reply may lead to an :exc:`RPCError` exception.

        In asynchronous mode, returns immediately, returning `self`. The :attr:`event` attribute will be set when the reply has been received (see :attr:`reply`) or an error occured (see :attr:`error`).

        *op* is the operation to be requested as an :class:`~xml.etree.ElementTree.Element`
        """
        self.logger.info('Requesting %r', self.__class__.__name__)
        req = self._wrap(op)
        self._session.send(req)
        if self._async:
            self.logger.debug('Async request, returning %r', self)
            return self
        else:
            self.logger.debug('Sync request, will wait for timeout=%r', self._timeout)
            self._event.wait(self._timeout)
            if self._event.is_set():
                if self._error:
                    # Error that prevented reply delivery
                    raise self._error
                self._reply.parse()
                if self._reply.error is not None and not self._device_handler.is_rpc_error_exempt(self._reply.error.message):
                    # <rpc-error>'s [ RPCError ]

                    if self._raise_mode == RaiseMode.ALL or (self._raise_mode == RaiseMode.ERRORS and self._reply.error.severity == "error"):
                        errlist = []
                        errors = self._reply.errors
                        if len(errors) > 1:
                            raise RPCError(to_ele(self._reply._raw), errs=errors)
                        else:
                            raise self._reply.error
                if self._device_handler.transform_reply():
                    return NCElement(self._reply, self._device_handler.transform_reply(), huge_tree=self._huge_tree)
                else:
                    return self._reply
            else:
                raise TimeoutExpiredError('ncclient timed out while waiting for an rpc reply.')

    def request(self):
        """Subclasses must implement this method. Typically only the request needs to be built as an
        :class:`~xml.etree.ElementTree.Element` and everything else can be handed off to
        :meth:`_request`."""
        pass

    def _assert(self, capability):
        """Subclasses can use this method to verify that a capability is available with the NETCONF
        server, before making a request that requires it. A :exc:`MissingCapabilityError` will be
        raised if the capability is not available."""
        if capability not in self._session.server_capabilities:
            raise MissingCapabilityError('Server does not support [%s]' % capability)

    def deliver_reply(self, raw):
        # internal use
        self._reply = self.REPLY_CLS(raw, huge_tree=self._huge_tree)

        # Set the reply_parsing_error transform outside the constructor, to keep compatibility for
        # third party reply classes outside of ncclient
        self._reply.set_parsing_error_transform(
            self._device_handler.reply_parsing_error_transform(self.REPLY_CLS)
        )

        self._event.set()

    def deliver_error(self, err):
        # internal use
        self._error = err
        self._event.set()

    @property
    def reply(self):
        ":class:`RPCReply` element if reply has been received or `None`"
        return self._reply

    @property
    def error(self):
        """:exc:`Exception` type if an error occured or `None`.

        .. note::
            This represents an error which prevented a reply from being received. An *rpc-error*
            does not fall in that category -- see `RPCReply` for that.
        """
        return self._error

    @property
    def id(self):
        "The *message-id* for this RPC."
        return self._id

    @property
    def session(self):
        "The `~ncclient.transport.Session` object associated with this RPC."
        return self._session

    @property
    def event(self):
        """:class:`~threading.Event` that is set when reply has been received or when an error preventing
        delivery of the reply occurs.
        """
        return self._event

    def __set_async(self, async_mode=True):
        self._async = async_mode
        if async_mode and not self._session.can_pipeline:
            raise UserWarning('Asynchronous mode not supported for this device/session')

    def __set_raise_mode(self, mode):
        assert(mode in (RaiseMode.NONE, RaiseMode.ERRORS, RaiseMode.ALL))
        self._raise_mode = mode

    def __set_timeout(self, timeout):
        self._timeout = timeout

    raise_mode = property(fget=lambda self: self._raise_mode, fset=__set_raise_mode)
    """Depending on this exception raising mode, an `rpc-error` in the reply may be raised as an :exc:`RPCError` exception. Valid values are the constants defined in :class:`RaiseMode`. """

    is_async = property(fget=lambda self: self._async, fset=__set_async)
    """Specifies whether this RPC will be / was requested asynchronously. By default RPC's are synchronous."""

    timeout = property(fget=lambda self: self._timeout, fset=__set_timeout)
    """Timeout in seconds for synchronous waiting defining how long the RPC request will block on a reply before raising :exc:`TimeoutExpiredError`.

    Irrelevant for asynchronous usage.
    """

    @property
    def huge_tree(self):
        """Whether `huge_tree` support for XML parsing of RPC replies is enabled (default=False)"""
        return self._huge_tree

    @huge_tree.setter
    def huge_tree(self, x):
        self._huge_tree = x

class GenericRPC(RPC):
    """Generic rpc commands wrapper"""
    REPLY_CLS = RPCReply
    """See :class:`RPCReply`."""

    def request(self, rpc_command, source=None, filter=None, config=None, target=None, format=None):
        """
        *rpc_command* specifies rpc command to be dispatched either in plain text or in xml element format (depending on command)

        *target* name of the configuration datastore being edited

        *source* name of the configuration datastore being queried

        *config* is the configuration, which must be rooted in the `config` element. It can be specified either as a string or an :class:`~xml.etree.ElementTree.Element`.

        *filter* specifies the portion of the configuration to retrieve (by default entire configuration is retrieved)

        :seealso: :ref:`filter_params`

        Examples of usage::

            m.rpc('rpc_command')

        or dispatch element like ::

            rpc_command = new_ele('get-xnm-information')
            sub_ele(rpc_command, 'type').text = "xml-schema"
            m.rpc(rpc_command)
        """

        if etree.iselement(rpc_command):
            node = rpc_command
        else:
            node = new_ele(rpc_command)
        if target is not None:
            node.append(util.datastore_or_url("target", target, self._assert))
        if source is not None:
            node.append(util.datastore_or_url("source", source, self._assert))
        if filter is not None:
            node.append(util.build_filter(filter))
        if config is not None:
            node.append(validated_element(config, ("config", qualify("config"))))

        return self._request(node)
