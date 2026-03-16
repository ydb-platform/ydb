# Load and dump a CAN database in KCD format.

import logging
from collections import defaultdict
from xml.etree import ElementTree
from xml.etree.ElementTree import Element, SubElement

from ...conversion import BaseConversion
from ...namedsignalvalue import NamedSignalValue
from ...utils import (
    SORT_SIGNALS_DEFAULT,
    sort_signals_by_start_bit,
    start_bit,
    type_sort_signals,
)
from ..bus import Bus
from ..internal_database import InternalDatabase
from ..message import Message
from ..node import Node
from ..signal import Signal
from .utils import num

LOGGER = logging.getLogger(__name__)

# The KCD XML namespace.
NAMESPACE = 'http://kayak.2codeornot2code.org/1.0'
NAMESPACES = {'ns': NAMESPACE}

ROOT_TAG = f'{{{NAMESPACE}}}NetworkDefinition'


def _start_bit(offset, byte_order):
    if byte_order == 'big_endian':
        return (8 * (offset // 8) + (7 - (offset % 8)))
    else:
        return offset


def _get_node_name_by_id(nodes, node_id):
    for node in nodes:
        if node['id'] == node_id:
            return node['name']


def _load_signal_element(signal, nodes):
    """Load given signal element and return a signal object.

    """

    # Default values.
    name = None
    offset = None
    length = 1
    byte_order = 'little_endian'
    is_signed = False
    is_float = False
    minimum = None
    maximum = None
    slope = 1
    intercept = 0
    unit = None
    labels = None
    notes = None
    receivers = []

    # Signal XML attributes.
    for key, value in signal.attrib.items():
        if key == 'name':
            name = value
        elif key == 'offset':
            offset = int(value)
        elif key == 'length':
            length = int(value)
        elif key == 'endianess':
            byte_order = f'{value}_endian'
        else:
            LOGGER.debug("Ignoring unsupported signal attribute '%s'.", key)

    # Value XML element.
    value = signal.find('ns:Value', NAMESPACES)

    if value is not None:
        for key, _value in value.attrib.items():
            if key == 'min':
                minimum = num(_value)
            elif key == 'max':
                maximum = num(_value)
            elif key == 'slope':
                slope = num(_value)
            elif key == 'intercept':
                intercept = num(_value)
            elif key == 'unit':
                unit = _value
            elif key == 'type':
                is_signed = (_value == 'signed')
                is_float = (_value in ['single', 'double'])
            else:
                LOGGER.debug("Ignoring unsupported signal value attribute '%s'.",
                             key)

    # Notes.
    try:
        notes = signal.find('ns:Notes', NAMESPACES).text
    except AttributeError:
        pass

    # Label set XML element.
    label_set = signal.find('ns:LabelSet', NAMESPACES)

    if label_set is not None:
        labels = {}

        for label in label_set.iterfind('ns:Label', NAMESPACES):
            label_value = int(label.attrib['value'])
            label_name = label.attrib['name']
            labels[label_value] = NamedSignalValue(label_value, label_name)

        # TODO: Label groups.

    # Receivers.
    consumer = signal.find('ns:Consumer', NAMESPACES)

    if consumer is not None:
        for receiver in consumer.iterfind('ns:NodeRef', NAMESPACES):
            receivers.append(_get_node_name_by_id(nodes,
                                                  receiver.attrib['id']))

    conversion = BaseConversion.factory(
        scale=slope,
        offset=intercept,
        choices=labels,
        is_float=is_float,
    )

    return Signal(name=name,
                  start=_start_bit(offset, byte_order),
                  length=length,
                  receivers=receivers,
                  byte_order=byte_order,
                  is_signed=is_signed,
                  conversion=conversion,
                  minimum=minimum,
                  maximum=maximum,
                  unit=unit,
                  comment=notes,
                  )


def _load_multiplex_element(mux, nodes):
    """Load given multiplex elements and its signals and return list of signals.

    """

    mux_signal = _load_signal_element(mux, nodes)
    mux_signal.is_multiplexer = True
    signals = [mux_signal]

    for mux_group in mux.iterfind('ns:MuxGroup', NAMESPACES):
        multiplexer_id = mux_group.attrib['count']

        for signal_element in mux_group.iterfind('ns:Signal', NAMESPACES):
            signal = _load_signal_element(signal_element, nodes)
            signal.multiplexer_ids = [int(multiplexer_id)]
            signal.multiplexer_signal = mux_signal.name
            signals.append(signal)

    return signals


def _load_message_element(message, bus_name, nodes, strict, sort_signals):
    """Load given message element and return a message object.

    """

    # Default values.
    name = None
    frame_id = None
    is_extended_frame = False
    notes = None
    length = 'auto'
    interval = None
    senders = []

    # Message XML attributes.
    for key, value in message.attrib.items():
        if key == 'name':
            name = value
        elif key == 'id':
            frame_id = int(value, 0)
        elif key == 'format':
            is_extended_frame = (value == 'extended')
        elif key == 'length':
            length = value  # 'auto' needs additional processing after knowing all signals
        elif key == 'interval':
            interval = int(value)
        else:
            LOGGER.debug("Ignoring unsupported message attribute '%s'.", key)
            # TODO: triggered, count, remote

    # Comment.
    try:
        notes = message.find('ns:Notes', NAMESPACES).text
    except AttributeError:
        pass

    # Senders.
    producer = message.find('ns:Producer', NAMESPACES)

    if producer is not None:
        for sender in producer.iterfind('ns:NodeRef', NAMESPACES):
            senders.append(_get_node_name_by_id(nodes,
                                                sender.attrib['id']))

    # Find all signals in this message.
    signals = []

    for mux in message.iterfind('ns:Multiplex', NAMESPACES):
        signals += _load_multiplex_element(mux, nodes)

    for signal in message.iterfind('ns:Signal', NAMESPACES):
        signals.append(_load_signal_element(signal, nodes))

    if length == 'auto':
        if signals:
            last_signal = sorted(signals, key=start_bit)[-1]
            length = (start_bit(last_signal) + last_signal.length + 7) // 8
        else:
            length = 0
    else:
        length = int(length)

    return Message(frame_id=frame_id,
                   is_extended_frame=is_extended_frame,
                   name=name,
                   length=length,
                   unused_bit_pattern=0xff,
                   senders=senders,
                   send_type=None,
                   cycle_time=interval,
                   signals=signals,
                   comment=notes,
                   bus_name=bus_name,
                   strict=strict,
                   sort_signals=sort_signals)


def _indent_xml(element, indent, level=0):
    i = "\n" + level * indent

    if len(element):
        if not element.text or not element.text.strip():
            element.text = i + indent

        if not element.tail or not element.tail.strip():
            element.tail = i

        for child in element:
            _indent_xml(child, indent, level + 1)

        if not child.tail or not child.tail.strip():
            child.tail = i
    elif level and (not element.tail or not element.tail.strip()):
        element.tail = i


def _dump_notes(parent, comment):
    notes = SubElement(parent, 'Notes')
    notes.text = comment


def _dump_signal(signal, node_refs, signal_element):
    signal_element.set('name', signal.name)

    offset = _start_bit(signal.start, signal.byte_order)
    signal_element.set('offset', str(offset))

    # Length.
    if signal.length != 1:
        signal_element.set('length', str(signal.length))

    # Byte order.
    if signal.byte_order != 'little_endian':
        signal_element.set('endianess', signal.byte_order[:-7])

    # Comment.
    if signal.comment is not None:
        _dump_notes(signal_element, signal.comment)

    # Receivers.
    if signal.receivers:
        consumer = SubElement(signal_element, 'Consumer')

        for receiver in signal.receivers:
            SubElement(consumer,
                       'NodeRef',
                       id=str(node_refs[receiver]))

    # Value.
    value = Element('Value')

    if signal.minimum is not None:
        value.set('min', str(signal.minimum))

    if signal.maximum is not None:
        value.set('max', str(signal.maximum))

    if signal.scale != 1:
        value.set('slope', str(signal.scale))

    if signal.offset != 0:
        value.set('intercept', str(signal.offset))

    if signal.unit is not None:
        value.set('unit', signal.unit)

    if signal.is_float:
        if signal.length == 32:
            type_name = 'single'
        else:
            type_name = 'double'
    elif signal.is_signed:
        type_name = 'signed'
    else:
        type_name = None

    if type_name is not None:
        value.set('type', type_name)

    if value.attrib:
        signal_element.append(value)

    # Label set.
    if signal.choices:
        label_set = SubElement(signal_element, 'LabelSet')

        for value, name in signal.choices.items():
            SubElement(label_set, 'Label', name=str(name), value=str(value))


def _dump_mux_group(multiplexer_id,
                    multiplexed_signals,
                    node_refs,
                    parent):
    mux_group = SubElement(parent,
                           'MuxGroup',
                           count=str(multiplexer_id))

    for signal in multiplexed_signals:
        _dump_signal(signal,
                     node_refs,
                     SubElement(mux_group, 'Signal'))

def _dump_mux_groups(multiplexer_name, signals, node_refs, parent):
    signals_per_count = defaultdict(list)

    for signal in signals:
        if signal.multiplexer_signal != multiplexer_name:
            continue

        multiplexer_id = signal.multiplexer_ids[0]
        signals_per_count[multiplexer_id].append(signal)

    for multiplexer_id, multiplexed_signals in signals_per_count.items():
        _dump_mux_group(multiplexer_id,
                        multiplexed_signals,
                        node_refs,
                        parent)


def _dump_message(message, bus, node_refs, sort_signals):
    frame_id = f'0x{message.frame_id:03X}'
    message_element = SubElement(bus,
                                 'Message',
                                 id=frame_id,
                                 name=message.name,
                                 length=str(message.length))

    if message.cycle_time is not None:
        message_element.set('interval', str(message.cycle_time))

    if message.is_extended_frame:
        message_element.set('format', 'extended')

    # Comment.
    if message.comment is not None:
        _dump_notes(message_element, message.comment)

    # Senders.
    if message.senders:
        producer = SubElement(message_element, 'Producer')

        for sender in message.senders:
            SubElement(producer,
                       'NodeRef',
                       id=str(node_refs[sender]))

    # Signals.
    if sort_signals:
        signals = sort_signals(message.signals)
    else:
        signals = message.signals

    for signal in signals:
        if signal.is_multiplexer:
            signal_element = SubElement(message_element, 'Multiplex')
            _dump_signal(signal,
                         node_refs,
                         signal_element)
            _dump_mux_groups(signal.name,
                             signals,
                             node_refs,
                             signal_element)
        elif signal.multiplexer_ids is None:
            _dump_signal(signal,
                         node_refs,
                         SubElement(message_element, 'Signal'))


def _dump_version(version, parent):
    if version is not None:
        SubElement(parent, 'Document', version=version)


def _dump_nodes(nodes, node_refs, parent):
    for node_id, node in enumerate(nodes, 1):
        SubElement(parent, 'Node', id=str(node_id), name=node.name)
        node_refs[node.name] = node_id


def _dump_messages(messages, node_refs, parent, sort_signals):
    bus = SubElement(parent, 'Bus', name='Bus')

    for message in messages:
        _dump_message(message, bus, node_refs, sort_signals)


def dump_string(database: InternalDatabase, *, sort_signals:type_sort_signals=SORT_SIGNALS_DEFAULT) -> str:
    """Format given database in KCD file format.

    """
    if sort_signals == SORT_SIGNALS_DEFAULT:
        sort_signals = None

    node_refs: dict[str, int] = {}

    attrib = {
        'xmlns:xsi': 'http://www.w3.org/2001/XMLSchema-instance',
        'xmlns': 'http://kayak.2codeornot2code.org/1.0',
        'xsi:noNamespaceSchemaLocation': 'Definition.xsd'
    }
    network_definition = Element('NetworkDefinition', attrib)

    if database.version is None:
        database.version = ''

    _dump_version(database.version, network_definition)
    _dump_nodes(database.nodes, node_refs, network_definition)
    _dump_messages(database.messages, node_refs, network_definition, sort_signals)

    _indent_xml(network_definition, '  ')

    return ElementTree.tostring(network_definition, encoding='unicode')


def load_string(string:str, strict:bool=True, sort_signals:type_sort_signals=sort_signals_by_start_bit) -> InternalDatabase:
    """Parse given KCD format string.

    """

    root = ElementTree.fromstring(string)

    # Should be replaced with a validation using the XSD file.
    if root.tag != ROOT_TAG:
        raise ValueError(f'Expected root element tag {ROOT_TAG}, but got {root.tag}.')

    nodes = [node.attrib for node in root.iterfind('./ns:Node', NAMESPACES)]
    buses = []
    messages = []

    try:
        document = root.find('ns:Document', NAMESPACES)
        version = document.attrib.get('version', None)  # type: ignore  # avoid mypy error: Item "None" of "Optional[Element]" has no attribute "attrib"
    except AttributeError:
        version = None

    for bus in root.iterfind('ns:Bus', NAMESPACES):
        bus_name = bus.attrib['name']
        bus_baudrate = int(bus.get('baudrate', 500000))
        buses.append(Bus(bus_name, baudrate=bus_baudrate))

        for message in bus.iterfind('ns:Message', NAMESPACES):
            messages.append(_load_message_element(message,
                                                  bus_name,
                                                  nodes,
                                                  strict,
                                                  sort_signals))

    return InternalDatabase(messages,
                            [
                                Node(name=node['name'], comment=None)
                                for node in nodes
                            ],
                            buses,
                            version)
