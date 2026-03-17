# This program is free software; you can redistribute it and/or modify
# it under the terms of the (LGPL) GNU Lesser General Public License as
# published by the Free Software Foundation; either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Library Lesser General Public License for more details at
# ( http://www.gnu.org/licenses/lgpl.html ).
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
# written by: Jeff Ortel ( jortel@redhat.com )

"""
The I{2nd generation} service proxy provides access to web services.
See I{README.txt}
"""

import suds
from suds import *
import suds.bindings.binding
from suds.builder import Builder
from suds.cache import ObjectCache
import suds.metrics as metrics
from suds.options import Options
from suds.plugin import PluginContainer
from suds.properties import Unskin
from suds.reader import DefinitionsReader
from suds.resolver import PathResolver
from suds.sax.document import Document
from suds.sax.parser import Parser
from suds.servicedefinition import ServiceDefinition
from suds.transport import TransportError, Request
from suds.transport.https import HttpAuthenticated
from suds.umx.basic import Basic as UmxBasic
from suds.wsdl import Definitions
from . import sudsobject

from http.cookiejar import CookieJar
from copy import deepcopy
import http.client
from urllib.parse import urlparse

from logging import getLogger
log = getLogger(__name__)


class Client(UnicodeMixin):
    """
    A lightweight web services client.
    I{(2nd generation)} API.
    @ivar wsdl: The WSDL object.
    @type wsdl:L{Definitions}
    @ivar service: The service proxy used to invoke operations.
    @type service: L{Service}
    @ivar factory: The factory used to create objects.
    @type factory: L{Factory}
    @ivar sd: The service definition
    @type sd: L{ServiceDefinition}
    @ivar messages: The last sent/received messages.
    @type messages: str[2]
    """
    @classmethod
    def items(cls, sobject):
        """
        Extract the I{items} from a suds object much like the
        items() method works on I{dict}.
        @param sobject: A suds object
        @type sobject: L{Object}
        @return: A list of items contained in I{sobject}.
        @rtype: [(key, value),...]
        """
        return sudsobject.items(sobject)

    @classmethod
    def dict(cls, sobject):
        """
        Convert a sudsobject into a dictionary.
        @param sobject: A suds object
        @type sobject: L{Object}
        @return: A python dictionary containing the
            items contained in I{sobject}.
        @rtype: dict
        """
        return sudsobject.asdict(sobject)

    @classmethod
    def metadata(cls, sobject):
        """
        Extract the metadata from a suds object.
        @param sobject: A suds object
        @type sobject: L{Object}
        @return: The object's metadata
        @rtype: L{sudsobject.Metadata}
        """
        return sobject.__metadata__

    def __init__(self, url, **kwargs):
        """
        @param url: The URL for the WSDL.
        @type url: str
        @param kwargs: keyword arguments.
        @see: L{Options}
        """
        options = Options()
        options.transport = HttpAuthenticated()
        self.options = options
        if "cache" not in kwargs:
            kwargs["cache"] = ObjectCache(days=1)
        self.set_options(**kwargs)
        reader = DefinitionsReader(options, Definitions)
        self.wsdl = reader.open(url)
        plugins = PluginContainer(options.plugins)
        plugins.init.initialized(wsdl=self.wsdl)
        self.factory = Factory(self.wsdl)
        self.service = ServiceSelector(self, self.wsdl.services)
        self.sd = []
        for s in self.wsdl.services:
            sd = ServiceDefinition(self.wsdl, s)
            self.sd.append(sd)
        self.messages = dict(tx=None, rx=None)

    def set_options(self, **kwargs):
        """
        Set options.
        @param kwargs: keyword arguments.
        @see: L{Options}
        """
        p = Unskin(self.options)
        p.update(kwargs)

    def add_prefix(self, prefix, uri):
        """
        Add I{static} mapping of an XML namespace prefix to a namespace.
        This is useful for cases when a wsdl and referenced schemas make heavy
        use of namespaces and those namespaces are subject to change.
        @param prefix: An XML namespace prefix.
        @type prefix: str
        @param uri: An XML namespace URI.
        @type uri: str
        @raise Exception: when prefix is already mapped.
        """
        root = self.wsdl.root
        mapped = root.resolvePrefix(prefix, None)
        if mapped is None:
            root.addPrefix(prefix, uri)
            return
        if mapped[1] != uri:
            raise Exception('"%s" already mapped as "%s"' % (prefix, mapped))

    def clone(self):
        """
        Get a shallow clone of this object.
        The clone only shares the WSDL.  All other attributes are
        unique to the cloned object including options.
        @return: A shallow clone.
        @rtype: L{Client}
        """
        class Uninitialized(Client):
            def __init__(self):
                pass
        clone = Uninitialized()
        clone.options = Options()
        cp = Unskin(clone.options)
        mp = Unskin(self.options)
        cp.update(deepcopy(mp))
        clone.wsdl = self.wsdl
        clone.factory = self.factory
        clone.service = ServiceSelector(clone, self.wsdl.services)
        clone.sd = self.sd
        clone.messages = dict(tx=None, rx=None)
        return clone

    def __unicode__(self):
        s = ['\n']
        s.append('Suds ( https://fedorahosted.org/suds/ )')
        s.append('  version: %s' % suds.__version__)
        if ( suds.__build__ ):
            s.append('  build: %s' % suds.__build__)
        for sd in self.sd:
            s.append('\n\n%s' % str(sd))
        return ''.join(s)


class Factory:
    """
    A factory for instantiating types defined in the wsdl
    @ivar resolver: A schema type resolver.
    @type resolver: L{PathResolver}
    @ivar builder: A schema object builder.
    @type builder: L{Builder}
    """

    def __init__(self, wsdl):
        """
        @param wsdl: A schema object.
        @type wsdl: L{wsdl.Definitions}
        """
        self.wsdl = wsdl
        self.resolver = PathResolver(wsdl)
        self.builder = Builder(self.resolver)

    def create(self, name):
        """
        create a WSDL type by name
        @param name: The name of a type defined in the WSDL.
        @type name: str
        @return: The requested object.
        @rtype: L{Object}
        """
        timer = metrics.Timer()
        timer.start()
        type = self.resolver.find(name)
        if type is None:
            raise TypeNotFound(name)
        if type.enum():
            result = sudsobject.Factory.object(name)
            for e, a in type.children():
                setattr(result, e.name, e.name)
        else:
            try:
                result = self.builder.build(type)
            except Exception as e:
                log.error("create '%s' failed", name, exc_info=True)
                raise BuildError(name, e)
        timer.stop()
        metrics.log.debug('%s created: %s', name, timer)
        return result

    def separator(self, ps):
        """
        Set the path separator.
        @param ps: The new path separator.
        @type ps: char
        """
        self.resolver = PathResolver(self.wsdl, ps)


class ServiceSelector:
    """
    The B{service} selector is used to select a web service.
    In most cases, the wsdl only defines (1) service in which access
    by subscript is passed through to a L{PortSelector}.  This is also the
    behavior when a I{default} service has been specified.  In cases
    where multiple services have been defined and no default has been
    specified, the service is found by name (or index) and a L{PortSelector}
    for the service is returned.  In all cases, attribute access is
    forwarded to the L{PortSelector} for either the I{first} service or the
    I{default} service (when specified).
    @ivar __client: A suds client.
    @type __client: L{Client}
    @ivar __services: A list of I{wsdl} services.
    @type __services: list
    """
    def __init__(self, client, services):
        """
        @param client: A suds client.
        @type client: L{Client}
        @param services: A list of I{wsdl} services.
        @type services: list
        """
        self.__client = client
        self.__services = services

    def __getattr__(self, name):
        """
        Request to access an attribute is forwarded to the
        L{PortSelector} for either the I{first} service or the
        I{default} service (when specified).
        @param name: The name of a method.
        @type name: str
        @return: A L{PortSelector}.
        @rtype: L{PortSelector}.
        """
        default = self.__ds()
        if default is None:
            port = self.__find(0)
        else:
            port = default
        return getattr(port, name)

    def __getitem__(self, name):
        """
        Provides selection of the I{service} by name (string) or
        index (integer).  In cases where only (1) service is defined
        or a I{default} has been specified, the request is forwarded
        to the L{PortSelector}.
        @param name: The name (or index) of a service.
        @type name: (int|str)
        @return: A L{PortSelector} for the specified service.
        @rtype: L{PortSelector}.
        """
        if len(self.__services) == 1:
            port = self.__find(0)
            return port[name]
        default = self.__ds()
        if default is not None:
            port = default
            return port[name]
        return self.__find(name)

    def __find(self, name):
        """
        Find a I{service} by name (string) or index (integer).
        @param name: The name (or index) of a service.
        @type name: (int|str)
        @return: A L{PortSelector} for the found service.
        @rtype: L{PortSelector}.
        """
        service = None
        if not len(self.__services):
            raise Exception('No services defined')
        if isinstance(name, int):
            try:
                service = self.__services[name]
                name = service.name
            except IndexError:
                raise ServiceNotFound('at [%d]' % name)
        else:
            for s in self.__services:
                if name == s.name:
                    service = s
                    break
        if service is None:
            raise ServiceNotFound(name)
        return PortSelector(self.__client, service.ports, name)

    def __ds(self):
        """
        Get the I{default} service if defined in the I{options}.
        @return: A L{PortSelector} for the I{default} service.
        @rtype: L{PortSelector}.
        """
        ds = self.__client.options.service
        if ds is None:
            return None
        else:
            return self.__find(ds)


class PortSelector:
    """
    The B{port} selector is used to select a I{web service} B{port}.
    In cases where multiple ports have been defined and no default has been
    specified, the port is found by name (or index) and a L{MethodSelector}
    for the port is returned.  In all cases, attribute access is
    forwarded to the L{MethodSelector} for either the I{first} port or the
    I{default} port (when specified).
    @ivar __client: A suds client.
    @type __client: L{Client}
    @ivar __ports: A list of I{service} ports.
    @type __ports: list
    @ivar __qn: The I{qualified} name of the port (used for logging).
    @type __qn: str
    """
    def __init__(self, client, ports, qn):
        """
        @param client: A suds client.
        @type client: L{Client}
        @param ports: A list of I{service} ports.
        @type ports: list
        @param qn: The name of the service.
        @type qn: str
        """
        self.__client = client
        self.__ports = ports
        self.__qn = qn

    def __getattr__(self, name):
        """
        Request to access an attribute is forwarded to the
        L{MethodSelector} for either the I{first} port or the
        I{default} port (when specified).
        @param name: The name of a method.
        @type name: str
        @return: A L{MethodSelector}.
        @rtype: L{MethodSelector}.
        """
        default = self.__dp()
        if default is None:
            m = self.__find(0)
        else:
            m = default
        return getattr(m, name)

    def __getitem__(self, name):
        """
        Provides selection of the I{port} by name (string) or
        index (integer).  In cases where only (1) port is defined
        or a I{default} has been specified, the request is forwarded
        to the L{MethodSelector}.
        @param name: The name (or index) of a port.
        @type name: (int|str)
        @return: A L{MethodSelector} for the specified port.
        @rtype: L{MethodSelector}.
        """
        default = self.__dp()
        if default is None:
            return self.__find(name)
        else:
            return default

    def __find(self, name):
        """
        Find a I{port} by name (string) or index (integer).
        @param name: The name (or index) of a port.
        @type name: (int|str)
        @return: A L{MethodSelector} for the found port.
        @rtype: L{MethodSelector}.
        """
        port = None
        if not len(self.__ports):
            raise Exception('No ports defined: %s' % self.__qn)
        if isinstance(name, int):
            qn = '%s[%d]' % (self.__qn, name)
            try:
                port = self.__ports[name]
            except IndexError:
                raise PortNotFound(qn)
        else:
            qn = '.'.join((self.__qn, name))
            for p in self.__ports:
                if name == p.name:
                    port = p
                    break
        if port is None:
            raise PortNotFound(qn)
        qn = '.'.join((self.__qn, port.name))
        return MethodSelector(self.__client, port.methods, qn)

    def __dp(self):
        """
        Get the I{default} port if defined in the I{options}.
        @return: A L{MethodSelector} for the I{default} port.
        @rtype: L{MethodSelector}.
        """
        dp = self.__client.options.port
        if dp is None:
            return None
        else:
            return self.__find(dp)


class MethodSelector:
    """
    The B{method} selector is used to select a B{method} by name.
    @ivar __client: A suds client.
    @type __client: L{Client}
    @ivar __methods: A dictionary of methods.
    @type __methods: dict
    @ivar __qn: The I{qualified} name of the method (used for logging).
    @type __qn: str
    """
    def __init__(self, client, methods, qn):
        """
        @param client: A suds client.
        @type client: L{Client}
        @param methods: A dictionary of methods.
        @type methods: dict
        @param qn: The I{qualified} name of the port.
        @type qn: str
        """
        self.__client = client
        self.__methods = methods
        self.__qn = qn

    def __getattr__(self, name):
        """
        Get a method by name and return it in an I{execution wrapper}.
        @param name: The name of a method.
        @type name: str
        @return: An I{execution wrapper} for the specified method name.
        @rtype: L{Method}
        """
        return self[name]

    def __getitem__(self, name):
        """
        Get a method by name and return it in an I{execution wrapper}.
        @param name: The name of a method.
        @type name: str
        @return: An I{execution wrapper} for the specified method name.
        @rtype: L{Method}
        """
        m = self.__methods.get(name)
        if m is None:
            qn = '.'.join((self.__qn, name))
            raise MethodNotFound(qn)
        return Method(self.__client, m)


class Method:
    """
    The I{method} (namespace) object.
    @ivar client: A client object.
    @type client: L{Client}
    @ivar method: A I{wsdl} method.
    @type I{wsdl} Method.
    """

    def __init__(self, client, method):
        """
        @param client: A client object.
        @type client: L{Client}
        @param method: A I{raw} method.
        @type I{raw} Method.
        """
        self.client = client
        self.method = method

    def __call__(self, *args, **kwargs):
        """
        Invoke the method.
        """
        clientclass = self.clientclass(kwargs)
        client = clientclass(self.client, self.method)
        try:
            return client.invoke(args, kwargs)
        except WebFault as e:
            if self.faults():
                raise
            return (http.client.INTERNAL_SERVER_ERROR, e)

    def faults(self):
        """ get faults option """
        return self.client.options.faults

    def clientclass(self, kwargs):
        """ get soap client class """
        if SimClient.simulation(kwargs):
            return SimClient
        return SoapClient


class SoapClient:
    """
    A lightweight soap based web client B{**not intended for external use}
    @ivar service: The target method.
    @type service: L{Service}
    @ivar method: A target method.
    @type method: L{Method}
    @ivar options: A dictonary of options.
    @type options: dict
    @ivar cookiejar: A cookie jar.
    @type cookiejar: libcookie.CookieJar
    """

    def __init__(self, client, method):
        """
        @param client: A suds client.
        @type client: L{Client}
        @param method: A target method.
        @type method: L{Method}
        """
        self.client = client
        self.method = method
        self.options = client.options
        self.cookiejar = CookieJar()

    def invoke(self, args, kwargs):
        """
        Send the required soap message to invoke the specified method
        @param args: A list of args for the method invoked.
        @type args: list
        @param kwargs: Named (keyword) args for the method invoked.
        @type kwargs: dict
        @return: The result of the method invocation.
        @rtype: I{builtin}|I{subclass of} L{Object}
        """
        timer = metrics.Timer()
        timer.start()
        binding = self.method.binding.input
        soapenv = binding.get_message(self.method, args, kwargs)
        timer.stop()
        metrics.log.debug("message for '%s' created: %s", self.method.name,
            timer)
        timer.start()
        result = self.send(soapenv)
        timer.stop()
        metrics.log.debug("method '%s' invoked: %s", self.method.name, timer)
        return result

    def send(self, soapenv):
        """
        Send soap message.
        @param soapenv: A soap envelope to send.
        @type soapenv: L{Document}
        @return: The reply to the sent message.
        @rtype: I{builtin} or I{subclass of} L{Object}
        """
        location = self.location()
        log.debug('sending to (%s)\nmessage:\n%s', location, soapenv)
        original_soapenv = soapenv
        plugins = PluginContainer(self.options.plugins)
        plugins.message.marshalled(envelope=soapenv.root())
        if self.options.prettyxml:
            soapenv = soapenv.str()
        else:
            soapenv = soapenv.plain()
        soapenv = soapenv.encode('utf-8')
        ctx = plugins.message.sending(envelope=soapenv)
        soapenv = ctx.envelope
        if self.options.nosend:
            return RequestContext(self, soapenv, original_soapenv)
        request = Request(location, soapenv)
        request.headers = self.headers()
        try:
            timer = metrics.Timer()
            timer.start()
            reply = self.options.transport.send(request)
            timer.stop()
            metrics.log.debug('waited %s on server reply', timer)
        except TransportError as e:
            content = e.fp and e.fp.read() or ''
            return self.process_reply(reply=content, status=e.httpcode,
                description=tostr(e), original_soapenv=original_soapenv)
        return self.process_reply(reply=reply.message,
            original_soapenv=original_soapenv)

    def process_reply(self, reply, status=None, description=None,
        original_soapenv=None):
        if status is None:
            status = http.client.OK
        if status in (http.client.ACCEPTED, http.client.NO_CONTENT):
            return
        failed = True
        try:
            if status == http.client.OK:
                log.debug('HTTP succeeded:\n%s', reply)
            else:
                log.debug('HTTP failed - %d - %s:\n%s', status, description,
                    reply)

            # (todo)
            #   Consider whether and how to allow plugins to handle error,
            # httplib.ACCEPTED & httplib.NO_CONTENT replies as well as
            # successful ones.
            #                                 (todo) (27.03.2013.) (Jurko)
            plugins = PluginContainer(self.options.plugins)
            ctx = plugins.message.received(reply=reply)
            reply = ctx.reply

            # SOAP standard states that SOAP errors must be accompanied by HTTP
            # status code 500 - internal server error:
            #
            # From SOAP 1.1 Specification:
            #   In case of a SOAP error while processing the request, the SOAP
            # HTTP server MUST issue an HTTP 500 "Internal Server Error"
            # response and include a SOAP message in the response containing a
            # SOAP Fault element (see section 4.4) indicating the SOAP
            # processing error.
            #
            # From WS-I Basic profile:
            #   An INSTANCE MUST use a "500 Internal Server Error" HTTP status
            # code if the response message is a SOAP Fault.
            replyroot = None
            if status in (http.client.OK, http.client.INTERNAL_SERVER_ERROR):
                replyroot = _parse(reply)
                plugins.message.parsed(reply=replyroot)
                fault = self.get_fault(replyroot)
                if fault:
                    if status != http.client.INTERNAL_SERVER_ERROR:
                        log.warn("Web service reported a SOAP processing "
                            "fault using an unexpected HTTP status code %d. "
                            "Reporting as an internal server error.", status)
                    if self.options.faults:
                        raise WebFault(fault, replyroot)
                    return (http.client.INTERNAL_SERVER_ERROR, fault)
            if status != http.client.OK:
                if self.options.faults:
                    # (todo)
                    #   Use a more specific exception class here.
                    #                         (27.03.2013.) (Jurko)
                    raise Exception((status, description))
                return (status, description)

            if self.options.retxml:
                failed = False
                return reply

            result = replyroot and self.method.binding.output.get_reply(
                self.method, replyroot)
            ctx = plugins.message.unmarshalled(reply=result)
            result = ctx.reply
            failed = False
            if self.options.faults:
                return result
            return (http.client.OK, result)
        finally:
            if failed and original_soapenv:
                log.error(original_soapenv)

    def get_fault(self, replyroot):
        """Extract fault information from the specified SOAP reply.

          Returns an I{unmarshalled} fault L{Object} or None in case the given
        XML document does not contain the SOAP <Fault> element.

        @param replyroot: A SOAP reply message root XML element or None.
        @type replyroot: L{Element}
        @return: A fault object.
        @rtype: L{Object}
        """
        envns = suds.bindings.binding.envns
        soapenv = replyroot and replyroot.getChild('Envelope', envns)
        soapbody = soapenv and soapenv.getChild('Body', envns)
        fault = soapbody and soapbody.getChild('Fault', envns)
        return fault is not None and UmxBasic().process(fault)

    def headers(self):
        """
        Get HTTP headers or the HTTP/HTTPS request.
        @return: A dictionary of header/values.
        @rtype: dict
        """
        action = self.method.soap.action
        if isinstance(action, str):
            action = action.encode('utf-8')
        stock = {'Content-Type':'text/xml; charset=utf-8', 'SOAPAction':action}
        result = dict(stock, **self.options.headers)
        log.debug('headers = %s', result)
        return result

    def location(self):
        """
        Returns the SOAP request's target location URL.

        """
        return Unskin(self.options).get('location', self.method.location)


class SimClient(SoapClient):
    """
    Loopback client used for message/reply simulation.
    """

    injkey = '__inject'

    @classmethod
    def simulation(cls, kwargs):
        """ get whether loopback has been specified in the I{kwargs}. """
        return SimClient.injkey in kwargs

    def invoke(self, args, kwargs):
        """
        Send the required soap message to invoke the specified method
        @param args: A list of args for the method invoked.
        @type args: list
        @param kwargs: Named (keyword) args for the method invoked.
        @type kwargs: dict
        @return: The result of the method invocation.
        @rtype: I{builtin} or I{subclass of} L{Object}
        """
        simulation = kwargs[self.injkey]
        msg = simulation.get('msg')
        if msg is not None:
            assert msg.__class__ is suds.byte_str_class
            return self.send(_parse(msg))
        msg = self.method.binding.input.get_message(self.method, args, kwargs)
        log.debug('inject (simulated) send message:\n%s', msg)
        reply = simulation.get('reply')
        if reply is not None:
            assert reply.__class__ is suds.byte_str_class
            status = simulation.get('status')
            description=simulation.get('description')
            if description is None:
                description = 'injected reply'
            return self.process_reply(reply=reply, status=status,
                description=description, original_soapenv=msg)
        raise Exception('reply or msg injection parameter expected');


class RequestContext:
    """
    A request context.
    Returned when the ''nosend'' options is specified. Allows the caller to
    take care of sending the request himself and simply return the reply data
    for further processing.
    @ivar client: The suds client.
    @type client: L{Client}
    @ivar envelope: The request SOAP envelope.
    @type envelope: str
    @ivar original_envelope: The original request SOAP envelope before plugin
                             processing.
    @type original_envelope: str
    """

    def __init__(self, client, envelope, original_envelope):
        """
        @param client: The suds client.
        @type client: L{Client}
        @param envelope: The request SOAP envelope.
        @type envelope: str
        @param original_envelope: The original request SOAP envelope before
                                  plugin processing.
        @type original_envelope: str
        """
        self.client = client
        self.envelope = envelope
        self.original_envelope = original_envelope

    def process_reply(self, reply, status=None, description=None):
        """
        Re-entry for processing a successful reply.
        @param reply: The reply SOAP envelope.
        @type reply: str
        @param status: The HTTP status code
        @type status: int
        @param description: Additional status description.
        @type description: str
        @return: The returned value for the invoked method.
        @return: The result of the method invocation.
        @rtype: I{builtin}|I{subclass of} L{Object}
        """
        return self.client.process_reply(reply=reply, status=status,
            description=description, original_soapenv=self.original_envelope)


def _parse(string):
    """
    Parses the given XML document content and returns the resulting root XML
    element node. Returns None if the given XML content is empty.
    @param string: XML document content to parse.
    @type string: str
    @return: Resulting root XML element node or None.
    @rtype: L{Element}
    """
    if len(string) > 0:
        return Parser().parse(string=string)
