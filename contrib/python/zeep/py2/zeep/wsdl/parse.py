"""
    zeep.wsdl.parse
    ~~~~~~~~~~~~~~~

"""
from lxml import etree

from zeep.exceptions import IncompleteMessage, LookupError, NamespaceError
from zeep.utils import qname_attr
from zeep.wsdl import definitions

NSMAP = {
    "wsdl": "http://schemas.xmlsoap.org/wsdl/",
    "wsaw": "http://www.w3.org/2006/05/addressing/wsdl",
    "wsam": "http://www.w3.org/2007/05/addressing/metadata",
}


def parse_abstract_message(wsdl, xmlelement):
    """Create an AbstractMessage object from a xml element.

    Definition::

        <definitions .... >
            <message name="nmtoken"> *
                <part name="nmtoken" element="qname"? type="qname"?/> *
            </message>
        </definitions>

    :param wsdl: The parent definition instance
    :type wsdl: zeep.wsdl.wsdl.Definition
    :param xmlelement: The XML node
    :type xmlelement: lxml.etree._Element
    :rtype: zeep.wsdl.definitions.AbstractMessage

    """
    tns = wsdl.target_namespace
    message_name = qname_attr(xmlelement, "name", tns)
    parts = []

    for part in xmlelement.findall("wsdl:part", namespaces=NSMAP):
        part_name = part.get("name")
        part_element = qname_attr(part, "element")
        part_type = qname_attr(part, "type")

        try:
            if part_element is not None:
                part_element = wsdl.types.get_element(part_element)
            if part_type is not None:
                part_type = wsdl.types.get_type(part_type)

        except (NamespaceError, LookupError):
            raise IncompleteMessage(
                (
                    "The wsdl:message for %r contains an invalid part (%r): "
                    "invalid xsd type or elements"
                )
                % (message_name.text, part_name)
            )

        part = definitions.MessagePart(part_element, part_type)
        parts.append((part_name, part))

    # Create the object, add the parts and return it
    msg = definitions.AbstractMessage(message_name)
    for part_name, part in parts:
        msg.add_part(part_name, part)
    return msg


def parse_abstract_operation(wsdl, xmlelement):
    """Create an AbstractOperation object from a xml element.

    This is called from the parse_port_type function since the abstract
    operations are part of the port type element.

    Definition::

        <wsdl:operation name="nmtoken">*
           <wsdl:documentation .... /> ?
           <wsdl:input name="nmtoken"? message="qname">?
               <wsdl:documentation .... /> ?
           </wsdl:input>
           <wsdl:output name="nmtoken"? message="qname">?
               <wsdl:documentation .... /> ?
           </wsdl:output>
           <wsdl:fault name="nmtoken" message="qname"> *
               <wsdl:documentation .... /> ?
           </wsdl:fault>
        </wsdl:operation>

    :param wsdl: The parent definition instance
    :type wsdl: zeep.wsdl.wsdl.Definition
    :param xmlelement: The XML node
    :type xmlelement: lxml.etree._Element
    :rtype: zeep.wsdl.definitions.AbstractOperation

    """
    name = xmlelement.get("name")
    kwargs = {"fault_messages": {}}

    for msg_node in xmlelement:
        tag_name = etree.QName(msg_node.tag).localname
        if tag_name not in ("input", "output", "fault"):
            continue

        param_msg = qname_attr(msg_node, "message", wsdl.target_namespace)
        param_name = msg_node.get("name")

        try:
            param_value = wsdl.get("messages", param_msg.text)
        except IndexError:
            return

        if tag_name == "input":
            kwargs["input_message"] = param_value
        elif tag_name == "output":
            kwargs["output_message"] = param_value
        else:
            kwargs["fault_messages"][param_name] = param_value

        wsa_action = msg_node.get(etree.QName(NSMAP["wsam"], "Action"))
        if not wsa_action:
            wsa_action = msg_node.get(etree.QName(NSMAP["wsaw"], "Action"))
        param_value.wsa_action = wsa_action

    kwargs["name"] = name
    kwargs["parameter_order"] = xmlelement.get("parameterOrder")
    return definitions.AbstractOperation(**kwargs)


def parse_port_type(wsdl, xmlelement):
    """Create a PortType object from a xml element.

    Definition::

        <wsdl:definitions .... >
            <wsdl:portType name="nmtoken">
                <wsdl:operation name="nmtoken" .... /> *
            </wsdl:portType>
        </wsdl:definitions>

    :param wsdl: The parent definition instance
    :type wsdl: zeep.wsdl.wsdl.Definition
    :param xmlelement: The XML node
    :type xmlelement: lxml.etree._Element
    :rtype: zeep.wsdl.definitions.PortType

    """
    name = qname_attr(xmlelement, "name", wsdl.target_namespace)
    operations = {}
    for elm in xmlelement.findall("wsdl:operation", namespaces=NSMAP):
        operation = parse_abstract_operation(wsdl, elm)
        if operation:
            operations[operation.name] = operation
    return definitions.PortType(name, operations)


def parse_port(wsdl, xmlelement):
    """Create a Port object from a xml element.

    This is called via the parse_service function since ports are part of the
    service xml elements.

    Definition::

        <wsdl:port name="nmtoken" binding="qname"> *
           <wsdl:documentation .... /> ?
           <-- extensibility element -->
        </wsdl:port>

    :param wsdl: The parent definition instance
    :type wsdl: zeep.wsdl.wsdl.Definition
    :param xmlelement: The XML node
    :type xmlelement: lxml.etree._Element
    :rtype: zeep.wsdl.definitions.Port

    """
    name = xmlelement.get("name")
    binding_name = qname_attr(xmlelement, "binding", wsdl.target_namespace)
    return definitions.Port(name, binding_name=binding_name, xmlelement=xmlelement)


def parse_service(wsdl, xmlelement):
    """

    Definition::

        <wsdl:service name="nmtoken"> *
            <wsdl:documentation .... />?
            <wsdl:port name="nmtoken" binding="qname"> *
               <wsdl:documentation .... /> ?
               <-- extensibility element -->
            </wsdl:port>
            <-- extensibility element -->
        </wsdl:service>

    Example::

          <service name="StockQuoteService">
            <documentation>My first service</documentation>
            <port name="StockQuotePort" binding="tns:StockQuoteBinding">
              <soap:address location="http://example.com/stockquote"/>
            </port>
          </service>

    :param wsdl: The parent definition instance
    :type wsdl: zeep.wsdl.wsdl.Definition
    :param xmlelement: The XML node
    :type xmlelement: lxml.etree._Element
    :rtype: zeep.wsdl.definitions.Service

    """
    name = xmlelement.get("name")
    ports = []
    for port_node in xmlelement.findall("wsdl:port", namespaces=NSMAP):
        port = parse_port(wsdl, port_node)
        if port:
            ports.append(port)

    obj = definitions.Service(name)
    for port in ports:
        obj.add_port(port)
    return obj
