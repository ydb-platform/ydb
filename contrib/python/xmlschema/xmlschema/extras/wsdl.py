#
# Copyright (c), 2016-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
# mypy: ignore-errors

from xmlschema.exceptions import XMLSchemaException, XMLSchemaValueError
from xmlschema.names import XSD_NAMESPACE, WSDL_NAMESPACE, SOAP_NAMESPACE, \
    XSD_ANY_TYPE, XSD_SCHEMA
from xmlschema.utils.qnames import get_qname, local_name, get_extended_qname, \
    get_prefixed_qname
from xmlschema.utils.urls import normalize_url
from xmlschema.locations import SCHEMAS_DIR, get_locations
from xmlschema.documents import SCHEMA_KWARGS, XmlDocument
from xmlschema.validators import XMLSchemaBase, XMLSchema10

# WSDL 1.1 global declarations
WSDL_IMPORT = '{%s}import' % WSDL_NAMESPACE
WSDL_TYPES = '{%s}types' % WSDL_NAMESPACE
WSDL_MESSAGE = '{%s}message' % WSDL_NAMESPACE
WSDL_PORT_TYPE = '{%s}portType' % WSDL_NAMESPACE
WSDL_BINDING = '{%s}binding' % WSDL_NAMESPACE
WSDL_SERVICE = '{%s}service' % WSDL_NAMESPACE

# Other WSDL tags
WSDL_PART = '{%s}part' % WSDL_NAMESPACE
WSDL_PORT = '{%s}port' % WSDL_NAMESPACE
WSDL_INPUT = '{%s}input' % WSDL_NAMESPACE
WSDL_OUTPUT = '{%s}output' % WSDL_NAMESPACE
WSDL_FAULT = '{%s}fault' % WSDL_NAMESPACE
WSDL_OPERATION = '{%s}operation' % WSDL_NAMESPACE

# WSDL SOAP Extensions
SOAP_BINDING = '{%s}binding' % SOAP_NAMESPACE
SOAP_OPERATION = '{%s}operation' % SOAP_NAMESPACE
SOAP_BODY = '{%s}body' % SOAP_NAMESPACE
SOAP_FAULT = '{%s}fault' % SOAP_NAMESPACE
SOAP_HEADER = '{%s}header' % SOAP_NAMESPACE
SOAP_HEADERFAULT = '{%s}headerfault' % SOAP_NAMESPACE
SOAP_ADDRESS = '{%s}address' % SOAP_NAMESPACE


class WsdlParseError(XMLSchemaException, SyntaxError):
    """An error during parsing of a WSDL document."""


class Wsdl11Maps:

    def __init__(self, wsdl_document):
        self.wsdl_document = wsdl_document
        self.imports = {}
        self.messages = {}
        self.port_types = {}
        self.bindings = {}
        self.services = {}

    def clear(self):
        self.imports.clear()
        self.messages.clear()
        self.port_types.clear()
        self.bindings.clear()
        self.services.clear()


class WsdlComponent:

    def __init__(self, elem, wsdl_document):
        self.elem = elem
        self.wsdl_document = wsdl_document

        try:
            self.name = get_qname(wsdl_document.target_namespace, elem.attrib['name'])
        except KeyError:
            self.name = None

    def __repr__(self):
        return '%s(name=%r)' % (self.__class__.__name__, self.prefixed_name)

    def get(self, name):
        return self.elem.get(name)

    @property
    def attrib(self):
        return self.elem.attrib

    @property
    def local_name(self):
        if self.name:
            return local_name(self.name)

    @property
    def prefixed_name(self):
        if self.name:
            return get_prefixed_qname(self.name, self.wsdl_document.namespaces)

    def map_qname(self, qname):
        return get_prefixed_qname(qname, self.wsdl_document.namespaces)

    def unmap_qname(self, qname):
        return get_extended_qname(qname, self.wsdl_document.namespaces)

    def _parse_reference(self, elem, attribute_name):
        try:
            return self.unmap_qname(elem.attrib[attribute_name])
        except KeyError:
            return None  # a missing attribute is already caught by XSD validator


class WsdlMessage(WsdlComponent):

    def __init__(self, elem, wsdl_document):
        super().__init__(elem, wsdl_document)
        self.parts = {}
        xsd_elements = wsdl_document.schema.maps.elements
        xsd_types = wsdl_document.schema.maps.types

        for child in elem.iterfind(WSDL_PART):
            part_name = child.get('name')
            if part_name is None:
                continue  # Ignore, missing name is already caught by XSD validator
            elif part_name in self.parts:
                msg = "duplicated part {!r} for {!r}"
                wsdl_document.parse_error(msg.format(part_name, self))

            try:
                element_attr = child.attrib['element']
            except KeyError:
                pass
            else:
                if 'type' in child.attrib:
                    msg = "ambiguous binding with both 'type' and 'element' attributes"
                    wsdl_document.parse_error(msg)

                element_name = get_extended_qname(element_attr, wsdl_document.namespaces)
                try:
                    self.parts[part_name] = xsd_elements[element_name]
                except KeyError:
                    self.parts[part_name] = xsd_types[XSD_ANY_TYPE]
                    msg = f"missing schema element {element_name!r}"
                    wsdl_document.parse_error(msg)

                continue  # pragma: no cover

            try:
                type_attr = child.attrib['type']
            except KeyError:
                msg = "missing both 'type' and 'element' attributes"
                wsdl_document.parse_error(msg)
            else:
                type_name = get_extended_qname(type_attr, wsdl_document.namespaces)
                try:
                    self.parts[part_name] = xsd_types[type_name]
                except KeyError:
                    self.parts[part_name] = xsd_types[XSD_ANY_TYPE]
                    msg = f"missing schema type {type_name!r}"
                    wsdl_document.parse_error(msg)


class WsdlPortType(WsdlComponent):

    def __init__(self, elem, wsdl_document):
        super().__init__(elem, wsdl_document)
        self.operations = {}

        for child in elem.iterfind(WSDL_OPERATION):
            operation_name = child.get('name')
            if operation_name is None:
                continue  # Ignore, missing name is already caught by XSD validator

            operation = WsdlOperation(child, wsdl_document)
            key = operation.key
            if key in self.operations:
                msg = "duplicated operation {!r} for {!r}"
                wsdl_document.parse_error(msg.format(operation_name, self))

            self.operations[key] = operation


class WsdlOperation(WsdlComponent):

    input = output = None
    soap_operation = None

    def __init__(self, elem, wsdl_document):
        super().__init__(elem, wsdl_document)
        self.faults = {}

        input_child = elem.find(WSDL_INPUT)
        if input_child is not None:
            self.input = WsdlInput(input_child, wsdl_document)

        output_child = elem.find(WSDL_OUTPUT)
        if output_child is not None:
            self.output = WsdlOutput(output_child, wsdl_document)

        for fault_child in elem.iterfind(WSDL_FAULT):
            fault = WsdlFault(fault_child, wsdl_document)
            if fault.name is None:
                continue
            elif fault.local_name in self.faults:
                msg = "duplicated fault {!r} for {!r}"
                wsdl_document.parse_error(msg.format(fault.local_name, self))
            self.faults[fault.local_name] = fault

        if input_child is not None and output_child is not None:
            children = self.elem[:]
            input_pos = children.index(input_child)
            output_pos = children.index(output_child)
            if input_pos < output_pos:
                self.transmission = 'request-response'
            else:
                self.transmission = 'solicit-response'

        elif input_child is not None:
            self.transmission = 'one-way'
        elif output_child is not None:
            self.transmission = 'notification'
        else:
            self.transmission = None

    @property
    def key(self):
        return self.local_name, \
            getattr(self.input, 'local_name', None), \
            getattr(self.output, 'local_name', None)

    @property
    def soap_action(self):
        """The SOAP operation's action URI if any, `None` otherwise."""
        if self.soap_operation is not None:
            return self.soap_operation.get('soapAction')

    @property
    def soap_style(self):
        """The SOAP operation's style if any, `None` otherwise."""
        if self.soap_operation is not None:
            style = self.soap_operation.get('style')
            return style if style in ('rpc', 'document') else 'document'


class WsdlMessageReference(WsdlComponent):
    message = None

    def __init__(self, elem, wsdl_document):
        super().__init__(elem, wsdl_document)
        message_name = self._parse_reference(elem, 'message')
        try:
            self.message = wsdl_document.maps.messages[message_name]
        except KeyError:
            if message_name:
                msg = "unknown message {!r} for {!r}"
                wsdl_document.parse_error(msg.format(message_name, self))


class WsdlInput(WsdlMessageReference):
    soap_headers = ()
    soap_body = None


class WsdlOutput(WsdlMessageReference):
    soap_headers = ()
    soap_body = None


class WsdlFault(WsdlMessageReference):
    soap_fault = None


class SoapParameter(WsdlComponent):

    @property
    def use(self):
        use = self.elem.get('use')
        return use if use in ('literal', 'encoded') else None

    @property
    def encoding_style(self):
        return self.elem.get('encodingStyle')

    @property
    def namespace(self):
        return self.elem.get('namespace', '')


class SoapBody(SoapParameter):
    """Class for soap:body bindings."""

    def __init__(self, elem, wsdl_document):
        super().__init__(elem, wsdl_document)
        self.parts = elem.get('parts', '').split()


class SoapFault(SoapParameter):
    """Class for soap:fault bindings."""


class SoapHeader(WsdlMessageReference, SoapParameter):
    """Class for soap:header bindings."""
    part = None

    def __init__(self, elem, wsdl_document):
        super().__init__(elem, wsdl_document)
        if self.message is not None and 'part' in elem.attrib:
            try:
                self.part = self.message.parts[elem.attrib['part']]
            except KeyError:
                msg = "missing message part {!r}"
                wsdl_document.parse_error(msg.format(elem.attrib['part']))

        if elem.tag == SOAP_HEADER:
            self.faults = [SoapHeaderFault(e, wsdl_document)
                           for e in elem.iterfind(SOAP_HEADERFAULT)]


class SoapHeaderFault(SoapHeader):
    """Class for soap:headerfault bindings."""


class WsdlBinding(WsdlComponent):

    port_type = None
    """The wsdl:portType definition related to the binding instance."""

    soap_binding = None
    """The SOAP binding element if any, `None` otherwise."""

    def __init__(self, elem, wsdl_document):
        super().__init__(elem, wsdl_document)
        self.operations = {}

        if wsdl_document.soap_binding:
            self.soap_binding = elem.find(SOAP_BINDING)
            if self.soap_binding is None:
                msg = "missing soap:binding element for {!r}"
                wsdl_document.parse_error(msg.format(self))

        port_type_name = self._parse_reference(elem, 'type')
        try:
            self.port_type = wsdl_document.maps.port_types[port_type_name]
        except KeyError:
            msg = "missing port type {!r} for {!r}"
            wsdl_document.parse_error(msg.format(port_type_name, self))
            return  # pragma: no cover

        for op_child in elem.iterfind(WSDL_OPERATION):
            op_name = op_child.get('name')
            if op_name is None:
                continue  # Ignore, missing name is already caught by XSD validator

            input_child = op_child.find(WSDL_INPUT)
            input_name = None if input_child is None else input_child.get('name')
            output_child = op_child.find(WSDL_OUTPUT)
            output_name = None if output_child is None else output_child.get('name')

            key = op_name, input_name, output_name
            if key in self.operations:
                msg = "duplicated operation {!r} for {!r}"
                wsdl_document.parse_error(msg.format(op_name, self))

            try:
                operation = self.port_type.operations[key]
            except KeyError:
                msg = "operation {!r} not found for {!r}"
                wsdl_document.parse_error(msg.format(op_name, self))
                continue  # pragma: no cover
            else:
                self.operations[key] = operation

            if wsdl_document.soap_binding:
                operation.soap_operation = op_child.find(SOAP_OPERATION)

            if input_child is not None:
                for body_child in input_child.iterfind(SOAP_BODY):
                    operation.input.soap_body = SoapBody(body_child, wsdl_document)
                    break
                operation.input.soap_headers = [
                    SoapHeader(e, wsdl_document) for e in input_child.iterfind(SOAP_HEADER)
                ]

            if output_child is not None:
                for body_child in output_child.iterfind(SOAP_BODY):
                    operation.output.soap_body = SoapBody(body_child, wsdl_document)
                    break
                operation.output.soap_headers = [
                    SoapHeader(e, wsdl_document) for e in output_child.iterfind(SOAP_HEADER)
                ]

            for fault_child in op_child.iterfind(WSDL_FAULT):
                fault = WsdlFault(fault_child, wsdl_document)
                if fault.name and fault.local_name not in operation.faults:
                    msg = "missing fault {!r} in {!r}"
                    wsdl_document.parse_error(msg.format(fault.local_name, operation))

                for soap_fault_child in fault_child.iterfind(SOAP_FAULT):
                    fault = SoapFault(soap_fault_child, wsdl_document)
                    if fault.name:
                        try:
                            operation.faults[fault.local_name].soap_fault = fault
                        except KeyError:
                            msg = "missing fault {!r} in {!r}"
                            wsdl_document.parse_error(msg.format(fault.local_name, operation))

    @property
    def soap_transport(self):
        """The SOAP binding's transport URI if any, `None` otherwise."""
        if self.soap_binding is not None:
            return self.soap_binding.get('transport')

    @property
    def soap_style(self):
        """The SOAP binding's style if any, `None` otherwise."""
        if self.soap_binding is not None:
            style = self.soap_binding.get('style')
            return style if style in ('rpc', 'document') else 'document'


class WsdlPort(WsdlComponent):

    binding = None
    soap_location = None

    def __init__(self, elem, wsdl_document):
        super().__init__(elem, wsdl_document)

        binding_name = self._parse_reference(elem, 'binding')
        try:
            self.binding = wsdl_document.maps.bindings[binding_name]
        except KeyError:
            if binding_name:
                msg = "unknown binding {!r} for {!r} output"
                wsdl_document.parse_error(msg.format(binding_name, self))

        if wsdl_document.soap_binding:
            for child in elem.iterfind(SOAP_ADDRESS):
                self.soap_location = child.get('location')
                break


class WsdlService(WsdlComponent):

    def __init__(self, elem, wsdl_document):
        super().__init__(elem, wsdl_document)
        self.ports = {}

        for port_child in elem.iterfind(WSDL_PORT):
            port = WsdlPort(port_child, wsdl_document)
            port_name = port.local_name

            if port_name is None:
                continue  # Ignore, missing name is already caught by XSD validator
            elif port_name in self.ports:
                msg = "duplicated port {!r} for {!r}"
                wsdl_document.parse_error(msg.format(port.prefixed_name, self))
            else:
                self.ports[port_name] = port


class Wsdl11Document(XmlDocument):
    """
    Class for WSDL 1.1 documents.

    :param source: a string containing XML data or a file path or a URL or a \
    file like object or an ElementTree or an Element.
    :param schema: additional schema for providing XSD types and elements to the \
    WSDL document. Can be a :class:`xmlschema.XMLSchema` instance or a file-like \
    object or a file path or a URL of a resource or a string containing the XSD schema.
    :param cls: class to use for building the schema instance (for default \
    :class:`xmlschema.XMLSchema10` is used).
    :param validation: the XSD validation mode to use for validating the XML document, \
    that can be 'strict' (default), 'lax' or 'skip'.
    :param maps: WSDL definitions shared maps.
    :param namespaces: is an optional mapping from namespace prefix to URI.
    :param locations: resource location hints, that can be a dictionary or a \
    sequence of couples (namespace URI, resource URL).
    :param kwargs: other optional arguments for initializing :class:`xmlschema.XMLResource` \
    base class or building :class:`xmlschema.XMLSchema` instances provided as keyword arguments.
    """
    target_namespace = ''
    soap_binding = False

    def __init__(self, source, schema=None, cls=None, validation='strict',
                 namespaces=None, maps=None, locations=None, base_url=None, **kwargs):

        if kwargs.get('lazy'):
            raise WsdlParseError(f"{self.__class__!r} instance cannot be lazy")

        if maps is not None:
            self.maps = maps
            self.schema = maps.wsdl_document.schema
        else:
            if cls is None:
                cls = XMLSchema10

            xsd_filepath = SCHEMAS_DIR.joinpath('WSDL', 'wsdl.xsd')
            if isinstance(schema, XMLSchemaBase):
                self.schema = schema
            else:
                self.schema = cls(
                    source=schema or xsd_filepath,
                    base_url=base_url,
                    locations=locations,
                    **{k: v for k, v in kwargs.items() if k in SCHEMA_KWARGS}
                )

            self.schema.add_schema(xsd_filepath, WSDL_NAMESPACE, base_url, True)
            self.maps = Wsdl11Maps(self)

        super().__init__(
            source=source,
            schema=self.schema,
            validation=validation,
            namespaces=namespaces,
            locations=locations,
            **kwargs,
        )
        self.target_namespace = self.root.get('targetNamespace', '')
        self.soap_binding = SOAP_NAMESPACE in self.namespaces.values()
        self.locations = get_locations(locations, base_url)

        if self.namespace == XSD_NAMESPACE and \
                self.schema.maps.get_schema(source=self.url) is None:
            self.schema.__class__(
                source=self,
                global_maps=self.schema.maps,
                locations=self.locations,
                **{k: v for k, v in kwargs.items() if k in SCHEMA_KWARGS}
            )
            return

        if self is self.maps.wsdl_document:
            self.maps.clear()

        self._parse_imports()
        self._parse_types()
        self._parse_messages()
        self._parse_port_types()
        self._parse_bindings()
        self._parse_services()

    def get_arguments(self):
        """Returns keyword arguments for rebuilding the WSDL document."""
        kwargs = super().get_arguments()
        kwargs['locations'] = self.locations
        return kwargs

    @property
    def imports(self):
        """WSDL 1.1 imports of XSD or WSDL additional resources."""
        return self.maps.imports

    @property
    def messages(self):
        """WSDL 1.1 messages."""
        return self.maps.messages

    @property
    def port_types(self):
        """WSDL 1.1 port types."""
        return self.maps.port_types

    @property
    def bindings(self):
        """WSDL 1.1 bindings."""
        return self.maps.bindings

    @property
    def services(self):
        """WSDL 1.1 services."""
        return self.maps.services

    def parse_error(self, message):
        if self._validation == 'strict':
            raise WsdlParseError(message)
        elif self._validation == 'lax':
            self.errors.append(WsdlParseError(message))

    def _parse_types(self):
        path = f'{WSDL_TYPES}/{XSD_SCHEMA}'

        for child in self.root.iterfind(path):
            source = self.subresource(child)
            self.schema.__class__(source, global_maps=self.schema.maps)

    def _parse_messages(self):
        for child in self.iterfind(WSDL_MESSAGE):
            message = WsdlMessage(child, self)
            if message.name in self.maps.messages:
                self.parse_error(f"duplicated message {message.prefixed_name!r}")
            else:
                self.maps.messages[message.name] = message

    def _parse_port_types(self):
        for child in self.iterfind(WSDL_PORT_TYPE):
            port_type = WsdlPortType(child, self)
            if port_type.name in self.maps.port_types:
                self.parse_error(f"duplicated port type {port_type.prefixed_name!r}")
            else:
                self.maps.port_types[port_type.name] = port_type

    def _parse_bindings(self):
        for child in self.iterfind(WSDL_BINDING):
            binding = WsdlBinding(child, self)
            if binding.name in self.maps.bindings:
                self.parse_error(f"duplicated binding {binding.prefixed_name!r}")
            else:
                self.maps.bindings[binding.name] = binding

    def _parse_services(self):
        for child in self.iterfind(WSDL_SERVICE):
            service = WsdlService(child, self)
            if service.name in self.maps.services:
                self.parse_error(f"duplicated service {service.prefixed_name!r}")
            else:
                self.maps.services[service.name] = service

    def _parse_imports(self):
        for child in self.root:
            if child.tag != WSDL_IMPORT:
                continue

            namespace = child.get('namespace', '').strip()
            location = child.get('location', '').strip()
            locations = [location] if location else []
            if namespace in self.locations:
                locations.extend(self.locations[namespace])

            import_error = None
            for url in locations:
                try:
                    self.import_namespace(namespace, url, self.base_url)
                except OSError as err:
                    if import_error is None:
                        import_error = err
                except SyntaxError as err:
                    msg = f"can't import namespace {namespace!r}: {err}."
                    self.parse_error(msg)
                except XMLSchemaValueError as err:
                    self.parse_error(err)
                else:
                    break
            else:
                if import_error is not None:
                    msg = "import of namespace {!r} from {!r} failed: {}."
                    self.parse_error(msg.format(namespace, locations, str(import_error)))
                self.maps.imports[namespace] = None

    def import_namespace(self, namespace, location, base_url=None):
        if namespace == self.target_namespace:
            msg = "namespace to import must be different from the " \
                  "'targetNamespace' of the WSDL document"
            raise XMLSchemaValueError(msg)

        elif namespace in self.maps.imports:
            return self.maps.imports[namespace]

        url = normalize_url(location, base_url or self.base_url)
        wsdl_document = self.__class__(
            source=url,
            maps=self.maps,
            namespaces=self._init_namespaces,
            validation=self._validation,
            base_url=self.base_url,
            allow=self.allow,
            defuse=self.defuse,
            timeout=self.timeout,
        )

        if wsdl_document.target_namespace != namespace:
            msg = 'imported {!r} has an unmatched namespace {!r}'
            self.parse_error(msg.format(wsdl_document, namespace))

        self.maps.imports[namespace] = wsdl_document
        return wsdl_document
