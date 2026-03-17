# This program is free software; you can redistribute it and/or modify it under
# the terms of the (LGPL) GNU Lesser General Public License as published by the
# Free Software Foundation; either version 3 of the License, or (at your
# option) any later version.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU Library Lesser General Public License
# for more details at ( http://www.gnu.org/licenses/lgpl.html ).
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program; if not, write to the Free Software Foundation, Inc.,
# 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
# written by: Jeff Ortel ( jortel@redhat.com )

"""
Provides classes for (WS) SOAP bindings.
"""

from suds import *
from suds.sax import Namespace
from suds.sax.document import Document
from suds.sax.element import Element
from suds.sudsobject import Factory
from suds.mx import Content
from suds.mx.literal import Literal as MxLiteral
from suds.umx.typed import Typed as UmxTyped
from suds.bindings.multiref import MultiRef
from suds.xsd.query import TypeQuery, ElementQuery
from suds.xsd.sxbasic import Element as SchemaElement
from suds.options import Options
from suds.plugin import PluginContainer

from copy import deepcopy


envns = ('SOAP-ENV', 'http://schemas.xmlsoap.org/soap/envelope/')


class Binding:
    """
    The SOAP binding class used to process outgoing and incoming SOAP messages
    per the WSDL port binding.
    @ivar wsdl: The WSDL.
    @type wsdl: L{suds.wsdl.Definitions}
    @ivar schema: The collective schema contained within the WSDL.
    @type schema: L{xsd.schema.Schema}
    @ivar options: A dictionary options.
    @type options: L{Options}
    """

    def __init__(self, wsdl):
        """
        @param wsdl: A WSDL.
        @type wsdl: L{wsdl.Definitions}
        """
        self.wsdl = wsdl
        self.multiref = MultiRef()

    def schema(self):
        return self.wsdl.schema

    def options(self):
        return self.wsdl.options

    def unmarshaller(self):
        """
        Get the appropriate schema based XML decoder.
        @return: Typed unmarshaller.
        @rtype: L{UmxTyped}
        """
        return UmxTyped(self.schema())

    def marshaller(self):
        """
        Get the appropriate XML encoder.
        @return: An L{MxLiteral} marshaller.
        @rtype: L{MxLiteral}
        """
        return MxLiteral(self.schema(), self.options().xstq)

    def param_defs(self, method):
        """
        Get parameter definitions.
        Each I{pdef} is a tuple (I{name}, L{xsd.sxbase.SchemaObject})
        @param method: A service method.
        @type method: I{service.Method}
        @return: A collection of parameter definitions
        @rtype: [I{pdef},..]
        """
        raise Exception('not implemented')

    def get_message(self, method, args, kwargs):
        """
        Get a SOAP message for the specified method, args and SOAP headers.
        This is the entry point for creating an outbound SOAP message.
        @param method: The method being invoked.
        @type method: I{service.Method}
        @param args: A list of args for the method invoked.
        @type args: list
        @param kwargs: Named (keyword) args for the method invoked.
        @type kwargs: dict
        @return: The SOAP envelope.
        @rtype: L{Document}
        """

        content = self.headercontent(method)
        header = self.header(content)
        content = self.bodycontent(method, args, kwargs)
        body = self.body(content)
        env = self.envelope(header, body)
        if self.options().prefixes:
            body.normalizePrefixes()
            env.promotePrefixes()
        else:
            env.refitPrefixes()
        return Document(env)

    def get_reply(self, method, replyroot):
        """
        Process the I{reply} for the specified I{method} by unmarshalling it
        into into Python object(s).
        @param method: The name of the invoked method.
        @type method: str
        @param replyroot: The reply XML root node received after invoking the
            specified method.
        @type reply: L{Element}
        @return: The unmarshalled reply.  The returned value is an L{Object} or
            a I{list} depending on whether the service returns a single object
            or a collection.
        @rtype: L{Object} or I{list}
        """
        soapenv = replyroot.getChild('Envelope', envns)
        soapenv.promotePrefixes()
        soapbody = soapenv.getChild('Body', envns)
        soapbody = self.multiref.process(soapbody)
        nodes = self.replycontent(method, soapbody)
        rtypes = self.returned_types(method)
        if len(rtypes) > 1:
            return self.replycomposite(rtypes, nodes)
        if len(rtypes) == 0:
            return
        if rtypes[0].multi_occurrence():
            return self.replylist(rtypes[0], nodes)
        if len(nodes):
            resolved = rtypes[0].resolve(nobuiltin=True)
            return self.unmarshaller().process(nodes[0], resolved)

    def replylist(self, rt, nodes):
        """
        Construct a I{list} reply. This mehod is called when it has been
        detected that the reply is a list.
        @param rt: The return I{type}.
        @type rt: L{suds.xsd.sxbase.SchemaObject}
        @param nodes: A collection of XML nodes.
        @type nodes: [L{Element},...]
        @return: A list of I{unmarshalled} objects.
        @rtype: [L{Object},...]
        """
        result = []
        resolved = rt.resolve(nobuiltin=True)
        unmarshaller = self.unmarshaller()
        for node in nodes:
            sobject = unmarshaller.process(node, resolved)
            result.append(sobject)
        return result

    def replycomposite(self, rtypes, nodes):
        """
        Construct a I{composite} reply. This method is called when it has been
        detected that the reply has multiple root nodes.
        @param rtypes: A list of known return I{types}.
        @type rtypes: [L{suds.xsd.sxbase.SchemaObject},...]
        @param nodes: A collection of XML nodes.
        @type nodes: [L{Element},...]
        @return: The I{unmarshalled} composite object.
        @rtype: L{Object},...
        """
        dictionary = {}
        for rt in rtypes:
            dictionary[rt.name] = rt
        unmarshaller = self.unmarshaller()
        composite = Factory.object('reply')
        for node in nodes:
            tag = node.name
            rt = dictionary.get(tag, None)
            if rt is None:
                if node.get('id') is None:
                    raise Exception('<%s/> not mapped to message part' % tag)
                else:
                    continue
            resolved = rt.resolve(nobuiltin=True)
            sobject = unmarshaller.process(node, resolved)
            value = getattr(composite, tag, None)
            if value is None:
                if rt.multi_occurrence():
                    value = []
                    setattr(composite, tag, value)
                    value.append(sobject)
                else:
                    setattr(composite, tag, sobject)
            else:
                if not isinstance(value, list):
                    value = [value,]
                    setattr(composite, tag, value)
                value.append(sobject)
        return composite

    def mkparam(self, method, pdef, object):
        """
        Builds a parameter for the specified I{method} using the parameter
        definition (pdef) and the specified value (object).
        @param method: A method name.
        @type method: str
        @param pdef: A parameter definition.
        @type pdef: tuple: (I{name}, L{xsd.sxbase.SchemaObject})
        @param object: The parameter value.
        @type object: any
        @return: The parameter fragment.
        @rtype: L{Element}
        """
        marshaller = self.marshaller()
        content = Content(tag=pdef[0], value=object, type=pdef[1],
            real=pdef[1].resolve())
        return marshaller.process(content)

    def mkheader(self, method, hdef, object):
        """
        Builds a soapheader for the specified I{method} using the header
        definition (hdef) and the specified value (object).
        @param method: A method name.
        @type method: str
        @param hdef: A header definition.
        @type hdef: tuple: (I{name}, L{xsd.sxbase.SchemaObject})
        @param object: The header value.
        @type object: any
        @return: The parameter fragment.
        @rtype: L{Element}
        """
        marshaller = self.marshaller()
        if isinstance(object, (list, tuple)):
            tags = []
            for item in object:
                tags.append(self.mkheader(method, hdef, item))
            return tags
        content = Content(tag=hdef[0], value=object, type=hdef[1])
        return marshaller.process(content)

    def envelope(self, header, body):
        """
        Build the B{<Envelope/>} for a SOAP outbound message.
        @param header: The SOAP message B{header}.
        @type header: L{Element}
        @param body: The SOAP message B{body}.
        @type body: L{Element}
        @return: The SOAP envelope containing the body and header.
        @rtype: L{Element}
        """
        env = Element('Envelope', ns=envns)
        env.addPrefix(Namespace.xsins[0], Namespace.xsins[1])
        env.append(header)
        env.append(body)
        return env

    def header(self, content):
        """
        Build the B{<Body/>} for a SOAP outbound message.
        @param content: The header content.
        @type content: L{Element}
        @return: the SOAP body fragment.
        @rtype: L{Element}
        """
        header = Element('Header', ns=envns)
        header.append(content)
        return header

    def bodycontent(self, method, args, kwargs):
        """
        Get the content for the SOAP I{body} node.
        @param method: A service method.
        @type method: I{service.Method}
        @param args: method parameter values
        @type args: list
        @param kwargs: Named (keyword) args for the method invoked.
        @type kwargs: dict
        @return: The XML content for the <body/>
        @rtype: [L{Element},..]
        """
        raise Exception('not implemented')

    def headercontent(self, method):
        """
        Get the content for the SOAP I{Header} node.
        @param method: A service method.
        @type method: I{service.Method}
        @return: The XML content for the <body/>
        @rtype: [L{Element},..]
        """
        n = 0
        content = []
        wsse = self.options().wsse
        if wsse is not None:
            content.append(wsse.xml())
        headers = self.options().soapheaders
        if not isinstance(headers, (tuple,list,dict)):
            headers = (headers,)
        if len(headers) == 0:
            return content
        pts = self.headpart_types(method)
        if isinstance(headers, (tuple,list)):
            for header in headers:
                if isinstance(header, Element):
                    content.append(deepcopy(header))
                    continue
                if len(pts) == n: break
                h = self.mkheader(method, pts[n], header)
                ns = pts[n][1].namespace('ns0')
                h.setPrefix(ns[0], ns[1])
                content.append(h)
                n += 1
        else:
            for pt in pts:
                header = headers.get(pt[0])
                if header is None:
                    continue
                h = self.mkheader(method, pt, header)
                ns = pt[1].namespace('ns0')
                h.setPrefix(ns[0], ns[1])
                content.append(h)
        return content

    def replycontent(self, method, body):
        """
        Get the reply body content.
        @param method: A service method.
        @type method: I{service.Method}
        @param body: The SOAP body.
        @type body: L{Element}
        @return: The body content.
        @rtype: [L{Element},...]
        """
        raise Exception('not implemented')

    def body(self, content):
        """
        Build the B{<Body/>} for a SOAP outbound message.
        @param content: The body content.
        @type content: L{Element}
        @return: The SOAP body fragment.
        @rtype: L{Element}
        """
        body = Element('Body', ns=envns)
        body.append(content)
        return body

    def bodypart_types(self, method, input=True):
        """
        Get a list of I{parameter definitions} (pdef) defined for the specified
        method. Each I{pdef} is a tuple (I{name}, L{xsd.sxbase.SchemaObject}).
        @param method: A service method.
        @type method: I{service.Method}
        @param input: Defines input/output message.
        @type input: boolean
        @return:  A list of parameter definitions
        @rtype: [I{pdef},]
        """
        result = []
        if input:
            parts = method.soap.input.body.parts
        else:
            parts = method.soap.output.body.parts
        for p in parts:
            if p.element is not None:
                query = ElementQuery(p.element)
            else:
                query = TypeQuery(p.type)
            pt = query.execute(self.schema())
            if pt is None:
                raise TypeNotFound(query.ref)
            if p.type is not None:
                pt = PartElement(p.name, pt)
            if input:
                if pt.name is None:
                    result.append((p.name, pt))
                else:
                    result.append((pt.name, pt))
            else:
                result.append(pt)
        return result

    def headpart_types(self, method, input=True):
        """
        Get a list of I{parameter definitions} (pdef) defined for the specified
        method. Each I{pdef} is a tuple (I{name}, L{xsd.sxbase.SchemaObject}).
        @param method: A service method.
        @type method: I{service.Method}
        @param input: Defines input/output message.
        @type input: boolean
        @return:  A list of parameter definitions
        @rtype: [I{pdef},]
        """
        result = []
        if input:
            headers = method.soap.input.headers
        else:
            headers = method.soap.output.headers
        for header in headers:
            part = header.part
            if part.element is not None:
                query = ElementQuery(part.element)
            else:
                query = TypeQuery(part.type)
            pt = query.execute(self.schema())
            if pt is None:
                raise TypeNotFound(query.ref)
            if part.type is not None:
                pt = PartElement(part.name, pt)
            if input:
                if pt.name is None:
                    result.append((part.name, pt))
                else:
                    result.append((pt.name, pt))
            else:
                result.append(pt)
        return result

    def returned_types(self, method):
        """
        Get the L{xsd.sxbase.SchemaObject} returned by the I{method}.
        @param method: A service method.
        @type method: I{service.Method}
        @return: The name of the type return by the method.
        @rtype: [I{rtype},..]
        """
        result = []
        for rt in self.bodypart_types(method, input=False):
            result.append(rt)
        return result


class PartElement(SchemaElement):
    """
    A part used to represent a message part when the part
    references a schema type and thus assumes to be an element.
    @ivar resolved: The part type.
    @type resolved: L{suds.xsd.sxbase.SchemaObject}
    """

    def __init__(self, name, resolved):
        """
        @param name: The part name.
        @type name: str
        @param resolved: The part type.
        @type resolved: L{suds.xsd.sxbase.SchemaObject}
        """
        root = Element('element', ns=Namespace.xsdns)
        SchemaElement.__init__(self, resolved.schema, root)
        self.__resolved = resolved
        self.name = name
        self.form_qualified = False

    def implany(self):
        return self

    def optional(self):
        return True

    def namespace(self, prefix=None):
        return Namespace.default

    def resolve(self, nobuiltin=False):
        if nobuiltin and self.__resolved.builtin():
            return self
        return self.__resolved
