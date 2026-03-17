# Load and dump a CAN database in DBC format.

import re
from collections import OrderedDict, defaultdict
from copy import deepcopy
from decimal import Decimal

import textparser  # type: ignore
from textparser import (
    Any,
    AnyUntil,
    DelimitedList,
    OneOrMore,
    OneOrMoreDict,
    Optional,
    Sequence,
    Token,
    TokenizeError,
    ZeroOrMore,
    choice,
    tokenize_init,
)

from ...conversion import BaseConversion
from ...namedsignalvalue import NamedSignalValue
from ...utils import (
    SORT_SIGNALS_DEFAULT,
    sort_signals_by_start_bit,
    sort_signals_by_start_bit_reversed,
    type_sort_attributes,
    type_sort_choices,
    type_sort_signals,
)
from ..attribute import Attribute
from ..attribute_definition import AttributeDefinition
from ..bus import Bus
from ..environment_variable import EnvironmentVariable
from ..internal_database import InternalDatabase
from ..message import Message
from ..node import Node
from ..signal import Signal
from ..signal_group import SignalGroup
from .dbc_specifics import DbcSpecifics
from .utils import num

DBC_FMT = (
    'VERSION "{version}"\r\n'
    '\r\n'
    '\r\n'
    'NS_ : \r\n'
    '\tNS_DESC_\r\n'
    '\tCM_\r\n'
    '\tBA_DEF_\r\n'
    '\tBA_\r\n'
    '\tVAL_\r\n'
    '\tCAT_DEF_\r\n'
    '\tCAT_\r\n'
    '\tFILTER\r\n'
    '\tBA_DEF_DEF_\r\n'
    '\tEV_DATA_\r\n'
    '\tENVVAR_DATA_\r\n'
    '\tSGTYPE_\r\n'
    '\tSGTYPE_VAL_\r\n'
    '\tBA_DEF_SGTYPE_\r\n'
    '\tBA_SGTYPE_\r\n'
    '\tSIG_TYPE_REF_\r\n'
    '\tVAL_TABLE_\r\n'
    '\tSIG_GROUP_\r\n'
    '\tSIG_VALTYPE_\r\n'
    '\tSIGTYPE_VALTYPE_\r\n'
    '\tBO_TX_BU_\r\n'
    '\tBA_DEF_REL_\r\n'
    '\tBA_REL_\r\n'
    '\tBA_DEF_DEF_REL_\r\n'
    '\tBU_SG_REL_\r\n'
    '\tBU_EV_REL_\r\n'
    '\tBU_BO_REL_\r\n'
    '\tSG_MUL_VAL_\r\n'
    '\r\n'
    'BS_:\r\n'
    '\r\n'
    'BU_: {bu}\r\n'
    '{val_table}'
    '\r\n'
    '\r\n'
    '{bo}\r\n'
    '\r\n'
    '{bo_tx_bu}\r\n'
    '{ev}\r\n'
    '\r\n'
    '{cm}\r\n'
    '{ba_def}\r\n'
    '{ba_def_rel}'
    '{ba_def_def}\r\n'
    '{ba_def_def_rel}'
    '{ba}\r\n'
    '{ba_rel}'
    '{val}\r\n'
    '{signal_types}\r\n'
    '{sig_group}\r\n'
    '{sig_mux_values}\r\n'
)


# Signal types.
SIGNAL_TYPE_FLOAT = 1
SIGNAL_TYPE_DOUBLE = 2

FLOAT_SIGNAL_TYPES = [
    SIGNAL_TYPE_FLOAT,
    SIGNAL_TYPE_DOUBLE
]

FLOAT_LENGTH_TO_SIGNAL_TYPE = {
    32: SIGNAL_TYPE_FLOAT,
    64: SIGNAL_TYPE_DOUBLE
}

ATTRIBUTE_DEFINITION_LONG_ENVVAR_NAME = AttributeDefinition(
    'SystemEnvVarLongSymbol',
    default_value='',
    kind='EV_',
    type_name='STRING')
ATTRIBUTE_DEFINITION_LONG_NODE_NAME = AttributeDefinition(
    'SystemNodeLongSymbol',
    default_value='',
    kind='BU_',
    type_name='STRING')

ATTRIBUTE_DEFINITION_LONG_MESSAGE_NAME = AttributeDefinition(
    'SystemMessageLongSymbol',
    default_value='',
    kind='BO_',
    type_name='STRING')

ATTRIBUTE_DEFINITION_LONG_SIGNAL_NAME = AttributeDefinition(
    'SystemSignalLongSymbol',
    default_value='',
    kind='SG_',
    type_name='STRING')

ATTRIBUTE_DEFINITION_VFRAMEFORMAT = AttributeDefinition(
    name='VFrameFormat',
    default_value='StandardCAN',
    kind='BO_',
    type_name='ENUM',
    choices=['StandardCAN', 'ExtendedCAN',
             'reserved', 'J1939PG',
             'reserved', 'reserved',
             'reserved', 'reserved',
             'reserved', 'reserved',
             'reserved', 'reserved',
             'reserved', 'reserved',
             'StandardCAN_FD', 'ExtendedCAN_FD'])

ATTRIBUTE_DEFINITION_CANFD_BRS = AttributeDefinition(
    name='CANFD_BRS',
    default_value='1',
    kind='BO_',
    type_name='ENUM',
    choices=['0', '1'])

ATTRIBUTE_DEFINITION_BUS_TYPE = AttributeDefinition(
    name='BusType',
    default_value='CAN',
    type_name='STRING')

ATTRIBUTE_DEFINITION_GENMSGCYCLETIME = AttributeDefinition(
    name='GenMsgCycleTime',
    default_value=0,
    kind='BO_',
    type_name='INT',
    minimum=0,
    maximum=2**16-1)

ATTRIBUTE_DEFINITION_GENSIGSTARTVALUE = AttributeDefinition(
    name='GenSigStartValue',
    default_value=0,
    kind='SG_',
    type_name='FLOAT',
    minimum=0,
    maximum=100000000000)


def to_int(value):
    return int(Decimal(value))

def to_float(value):
    return float(Decimal(value))

class Parser(textparser.Parser):

    def tokenize(self, string):
        keywords = {
            'BA_',
            'BA_DEF_',
            'BA_DEF_DEF_',
            'BA_DEF_DEF_REL_',
            'BA_DEF_REL_',
            'BA_DEF_SGTYPE_',
            'BA_REL_',
            'BA_SGTYPE_',
            'BO_',
            'BO_TX_BU_',
            'BS_',
            'BU_',
            'BU_BO_REL_',
            'BU_EV_REL_',
            'BU_SG_REL_',
            'CAT_',
            'CAT_DEF_',
            'CM_',
            'ENVVAR_DATA_',
            'EV_',
            'EV_DATA_',
            'FILTER',
            'NS_',
            'NS_DESC_',
            'SG_',
            'SG_MUL_VAL_',
            'SGTYPE_',
            'SGTYPE_VAL_',
            'SIG_GROUP_',
            'SIG_TYPE_REF_',
            'SIG_VALTYPE_',
            'SIGTYPE_VALTYPE_',
            'VAL_',
            'VAL_TABLE_',
            'VERSION'
        }

        names = {
            'LPAREN': '(',
            'RPAREN': ')',
            'LBRACE': '[',
            'RBRACE': ']',
            'COMMA':  ',',
            'AT':     '@',
            'SCOLON': ';',
            'COLON':  ':',
            'PIPE':   '|',
            'SIGN':   '+/-'
        }

        token_specs = [
            ('SKIP',     r'[ \r\n\t]+|//.*?\n'),
            ('NUMBER',   r'[-+]?\d+\.?\d*([eE][+-]?\d+)?'),
            ('WORD',     r'[A-Za-z0-9_]+'),
            ('STRING',   r'"(\\"|[^"])*?"'),
            ('LPAREN',   r'\('),
            ('RPAREN',   r'\)'),
            ('LBRACE',   r'\['),
            ('RBRACE',   r'\]'),
            ('COMMA',    r','),
            ('PIPE',     r'\|'),
            ('AT',       r'@'),
            ('SIGN',     r'[+-]'),
            ('SCOLON',   r';'),
            ('COLON',    r':'),
            ('MISMATCH', r'.')
        ]

        tokens, token_regex = tokenize_init(token_specs)

        for mo in re.finditer(token_regex, string, re.DOTALL):
            kind = mo.lastgroup

            if kind == 'SKIP':
                pass
            elif kind == 'STRING':
                value = mo.group(kind)[1:-1].replace('\\"', '"')
                tokens.append(Token(kind, value, mo.start()))
            elif kind != 'MISMATCH':
                value = mo.group(kind)

                if value in keywords:
                    kind = value

                if kind in names:
                    kind = names[kind]

                tokens.append(Token(kind, value, mo.start()))
            else:
                raise TokenizeError(string, mo.start())

        return tokens

    def grammar(self):
        version = Sequence('VERSION', 'STRING')

        ns = Sequence('NS_', ':', AnyUntil(Sequence(Any(), ':')))

        bs = Sequence('BS_', ':')

        nodes = Sequence('BU_', ':', ZeroOrMore('WORD'))

        signal = Sequence(
            'SG_', choice(Sequence('WORD', 'WORD'), Sequence('WORD')), ':',
            'NUMBER', '|', 'NUMBER', '@', 'NUMBER', '+/-',
            '(', 'NUMBER', ',', 'NUMBER', ')',
            '[', 'NUMBER', '|', 'NUMBER', ']',
            'STRING',
            DelimitedList('WORD'))

        message = Sequence(
            'BO_', 'NUMBER', 'WORD', ':', 'NUMBER', 'WORD', ZeroOrMore(signal))

        environment_variable = Sequence(
            'EV_', 'WORD', ':', 'NUMBER',
            '[', 'NUMBER', '|', 'NUMBER', ']',
            'STRING', 'NUMBER', 'NUMBER', 'WORD', 'WORD', ';')

        comment = Sequence(
            'CM_',
            choice(
                Sequence('SG_', 'NUMBER', 'WORD', 'STRING'),
                Sequence('BO_', 'NUMBER', 'STRING'),
                Sequence('EV_', 'WORD', 'STRING'),
                Sequence('BU_', 'WORD', 'STRING'),
                'STRING'),
            ';')

        attribute_definition = Sequence(
            'BA_DEF_',
            Optional(choice('SG_', 'BO_', 'EV_', 'BU_')),
            'STRING',
            'WORD',
            Optional(choice(DelimitedList('STRING'), ZeroOrMore('NUMBER'))),
            ';')

        attribute_definition_default = Sequence(
            'BA_DEF_DEF_', 'STRING', choice('NUMBER', 'STRING'), ';')

        attribute = Sequence(
            'BA_', 'STRING',
            ZeroOrMore(choice(Sequence('BO_', 'NUMBER'),
                              Sequence('SG_', 'NUMBER', 'WORD'),
                              Sequence('BU_', 'WORD'),
                              Sequence('EV_', 'WORD'))),
            choice('NUMBER', 'STRING'),
            ';')

        attribute_definition_rel = Sequence(
            'BA_DEF_REL_',
            Optional(choice('BU_SG_REL_', 'BU_BO_REL_')),
            'STRING',
            'WORD',
            Optional(choice(DelimitedList('STRING'), OneOrMore('NUMBER'))),
            ';')

        attribute_definition_default_rel = Sequence(
            'BA_DEF_DEF_REL_', 'STRING', choice('NUMBER', 'STRING'), ';')

        attribute_rel_sg = Sequence(
            'BA_REL_', 'STRING', 'BU_SG_REL_', 'WORD', 'SG_', 'NUMBER',
            'WORD', choice('NUMBER', 'STRING'), ';')

        attribute_rel_bo = Sequence(
            'BA_REL_', 'STRING', 'BU_BO_REL_', 'WORD', 'NUMBER',
            choice('NUMBER', 'STRING'), ';')

        choice_ = Sequence(
            'VAL_',
            Optional('NUMBER'),
            'WORD',
            ZeroOrMore(Sequence('NUMBER', 'STRING')),
            ';')

        value_table = Sequence(
            'VAL_TABLE_', 'WORD', ZeroOrMore(Sequence('NUMBER', 'STRING')), ';')

        signal_type = Sequence(
            'SIG_VALTYPE_', 'NUMBER', 'WORD', ':', 'NUMBER', ';')

        signal_multiplexer_values = Sequence(
            'SG_MUL_VAL_',
            'NUMBER',
            'WORD',
            'WORD',
            DelimitedList(Sequence('NUMBER', 'NUMBER')),
            ';')

        message_add_sender = Sequence(
            'BO_TX_BU_', 'NUMBER', ':', DelimitedList('WORD'), ';')

        signal_group = Sequence(
            'SIG_GROUP_', 'NUMBER', 'WORD', 'NUMBER', ':', ZeroOrMore('WORD'), ';')

        return OneOrMoreDict(
            choice(
                message,
                comment,
                attribute_definition,
                value_table,
                choice_,
                attribute,
                attribute_rel_sg,
                attribute_rel_bo,
                attribute_definition_rel,
                attribute_definition_default,
                attribute_definition_default_rel,
                signal_group,
                signal_type,
                signal_multiplexer_values,
                message_add_sender,
                environment_variable,
                nodes,
                ns,
                bs,
                version))


class LongNamesConverter:
    def __init__(self, long_names: list[str]) -> None:

        self.long_to_short: dict[str, str] = {}
        self.short_to_long: dict[str, str] = {}

        for long_name in sorted(long_names, key=lambda s: (len(s), s)):
            short_name = long_name[:32]
            index = -1
            while short_name in self.short_to_long:
                index += 1
                short_name = f'{short_name[:27]}_{index:04d}'

            self.long_to_short[long_name] = short_name
            self.short_to_long[short_name] = long_name


def get_dbc_frame_id(message):
    frame_id = message.frame_id

    if message.is_extended_frame:
        frame_id |= 0x80000000

    return frame_id

def get_dbc_name(name):
    #replace special chars with '_'
    name = re.sub(r'\W', '_', name)
    #append '_' if it starts with a number
    if name[0].isdigit():
        name = '_' + name

    return name


def _get_node_name(attributes, name):
    try:
        return attributes['node'][name]['SystemNodeLongSymbol'].value
    except (KeyError, TypeError):
        return name


def _get_environment_variable_name(attributes, name):
    try:
        return attributes['envvar'][name]['SystemEnvVarLongSymbol'].value
    except (KeyError, TypeError):
        return name


def _dump_version(database):
    return '' if database.version is None else database.version


def _dump_nodes(database):
    bu = []

    for node in database.nodes:
        bu.append(node.name)

    return bu


def _dump_value_tables(database):
    if database.dbc is None:
        return []

    val_table = []

    for name, choices in database.dbc.value_tables.items():
        choices = [
            f'{number} "{text}"'
            for number, text in sorted(choices.items(), reverse=True)
        ]
        val_table.append('VAL_TABLE_ {} {} ;'.format(name, ' '.join(choices)))

    return [*val_table, '']


def _dump_messages(database, sort_signals):
    bo = []

    def format_mux(signal):
        if signal.is_multiplexer:
            return ' M'
        elif signal.multiplexer_ids is not None:
            return f' m{signal.multiplexer_ids[0]}'
        else:
            return ''

    def format_receivers(signal):
        if signal.receivers:
            return ' ' + ','.join(signal.receivers)
        else:
            return 'Vector__XXX'

    def format_senders(message):
        if message.senders:
            return message.senders[0]
        else:
            return 'Vector__XXX'

    for message in database.messages:
        msg = []
        msg.append(
            f'BO_ {get_dbc_frame_id(message)} {message.name}: {message.length} {format_senders(message)}')

        if sort_signals:
            signals = sort_signals(message.signals)
        else:
            signals = message.signals
        for signal in signals:
            fmt = (' SG_ {name}{mux} : {start}|{length}@{byte_order}{sign}'
                   ' ({scale},{offset})'
                   ' [{minimum}|{maximum}] "{unit}" {receivers}')
            msg.append(fmt.format(
                name=signal.name,
                mux=format_mux(signal),
                start=signal.start,
                length=signal.length,
                receivers=format_receivers(signal),
                byte_order=(0 if signal.byte_order == 'big_endian' else 1),
                sign=('-' if signal.is_signed else '+'),
                scale=signal.scale,
                offset=signal.offset,
                minimum=(0 if signal.minimum is None else signal.minimum),
                maximum=(0 if signal.maximum is None else signal.maximum),
                unit='' if signal.unit is None else signal.unit))

        bo.append('\r\n'.join(msg))

    return bo


def _dump_senders(database):
    bo_tx_bu = []

    for message in database.messages:
        if len(message.senders) > 1:
            bo_tx_bu.append(
                'BO_TX_BU_ {frame_id} : {senders};'.format(
                    frame_id=get_dbc_frame_id(message),
                    senders=','.join(message.senders)))

    return bo_tx_bu


def _dump_comments(database, sort_signals):
    cm = []

    for bus in database.buses:
        if bus.comment is not None:
            cm.append(f'CM_ "{bus.comment}";')

    for node in database.nodes:
        if node.comment is not None:
            cm.append(
                'CM_ BU_ {name} "{comment}";'.format(
                    name=node.name,
                    comment=node.comment.replace('"', '\\"')))

    for message in database.messages:
        if message.comment is not None:
            cm.append(
                'CM_ BO_ {frame_id} "{comment}";'.format(
                    frame_id=get_dbc_frame_id(message),
                    comment=message.comment.replace('"', '\\"')))

        if sort_signals:
            signals = sort_signals(message.signals)
        else:
            signals = message.signals
        for signal in signals:
            if signal.comment is not None:
                cm.append(
                    'CM_ SG_ {frame_id} {name} "{comment}";'.format(
                        frame_id=get_dbc_frame_id(message),
                        name=signal.name,
                        comment=signal.comment.replace('"', '\\"')))

    if database.dbc is not None:
        # Dump environment variable comments (CM_ EV_ <name> "comment";)
        for env in database.dbc.environment_variables.values():
            if env.comment is not None:
                escaped_comment = env.comment.replace('"', '\\"')
                cm.append(f'CM_ EV_ {env.name} "{escaped_comment}";')

    return cm


def _dump_signal_types(database):
    valtype = []

    for message in database.messages:
        for signal in message.signals:
            if not signal.is_float:
                continue

            valtype.append(
                f'SIG_VALTYPE_ {get_dbc_frame_id(message)} {signal.name} : {FLOAT_LENGTH_TO_SIGNAL_TYPE[signal.length]};')

    return valtype


def _need_startval_def(database):
    return any(s.raw_initial is not None
               for m in database.messages
               for s in m.signals)

def _need_cycletime_def(database):
    # If the user has added cycle times to a database which didn't start with them,
    # we need to add the global attribute definition so the output DBC is valid
    return any(m.cycle_time is not None
               for m in database.messages)

def _bus_is_canfd(database: InternalDatabase) -> bool:
    if database.dbc is None or database.dbc.attributes is None:
        return False
    bus_type = database.dbc.attributes.get('BusType', None)
    if bus_type is None:
        return False
    return bus_type.value == 'CAN FD'  # type: ignore[no-any-return]

def _dump_attribute_definitions(database: InternalDatabase) -> list[str]:
    ba_def = []

    if database.dbc is None:
        definitions = OrderedDict()
    else:
        definitions = database.dbc.attribute_definitions

    # define "GenMsgCycleTime" attribute for specifying the cycle
    # times of messages if it has not been explicitly defined
    if 'GenMsgCycleTime' not in definitions and _need_cycletime_def(database):
        definitions['GenMsgCycleTime'] = ATTRIBUTE_DEFINITION_GENMSGCYCLETIME
    if 'GenSigStartValue' not in definitions and _need_startval_def(database):
        definitions['GenSigStartValue'] = ATTRIBUTE_DEFINITION_GENSIGSTARTVALUE

    # create 'VFrameFormat' and 'CANFD_BRS' attribute definitions if bus is CAN FD
    if _bus_is_canfd(database):
        if 'VFrameFormat' not in definitions:
            definitions['VFrameFormat'] = ATTRIBUTE_DEFINITION_VFRAMEFORMAT
        if 'CANFD_BRS' not in definitions:
            definitions['CANFD_BRS'] = ATTRIBUTE_DEFINITION_CANFD_BRS

    def get_value(definition, value):
        if definition.minimum is None:
            value = ''
        else:
            value = f' {value}'

        return value

    def get_minimum(definition):
        return get_value(definition, definition.minimum)

    def get_maximum(definition):
        return get_value(definition, definition.maximum)

    def get_kind(definition):
        return '' if definition.kind is None else definition.kind + ' '

    for definition in definitions.values():
        if definition.type_name == 'ENUM':
            choices = ','.join([f'"{choice}"'
                                for choice in definition.choices])
            ba_def.append(
                f'BA_DEF_ {get_kind(definition)} "{definition.name}" {definition.type_name}  {choices};')
        elif definition.type_name in ['INT', 'FLOAT', 'HEX']:
            ba_def.append(
                f'BA_DEF_ {get_kind(definition)} "{definition.name}" {definition.type_name}{get_minimum(definition)}{get_maximum(definition)};')
        elif definition.type_name == 'STRING':
            ba_def.append(
                f'BA_DEF_ {get_kind(definition)} "{definition.name}" {definition.type_name} ;')

    return ba_def


def _dump_attribute_definitions_rel(database):
    ba_def_rel = []

    if database.dbc is None:
        definitions = OrderedDict()
    else:
        definitions = database.dbc.attribute_definitions_rel

    def get_value(definition, value):
        if definition.minimum is None:
            value = ''
        else:
            value = f' {value}'

        return value

    def get_minimum(definition):
        return get_value(definition, definition.minimum)

    def get_maximum(definition):
        return get_value(definition, definition.maximum)

    for definition in definitions.values():
        if definition.type_name == 'ENUM':
            choices = ','.join([f'"{choice}"'
                                for choice in definition.choices])
            ba_def_rel.append(
                f'BA_DEF_REL_ {definition.kind}  "{definition.name}" {definition.type_name}  {choices};')
        elif definition.type_name in ['INT', 'FLOAT', 'HEX']:
            ba_def_rel.append(
                f'BA_DEF_REL_ {definition.kind}  "{definition.name}" {definition.type_name}{get_minimum(definition)}{get_maximum(definition)};')
        elif definition.type_name == 'STRING':
            ba_def_rel.append(
                f'BA_DEF_REL_ {definition.kind}  "{definition.name}" {definition.type_name} ;')

    return ba_def_rel


def _dump_attribute_definition_defaults(database):
    ba_def_def = []

    if database.dbc is None:
        definitions = OrderedDict()
    else:
        definitions = database.dbc.attribute_definitions

    for definition in definitions.values():
        if definition.default_value is not None:
            if definition.type_name in ["STRING", "ENUM"]:
                fmt = 'BA_DEF_DEF_  "{name}" "{value}";'
            else:
                fmt = 'BA_DEF_DEF_  "{name}" {value};'

            ba_def_def.append(fmt.format(name=definition.name,
                                         value=definition.default_value))

    return ba_def_def


def _dump_attribute_definition_defaults_rel(database):
    ba_def_def_rel = []

    if database.dbc is None:
        definitions = OrderedDict()
    else:
        definitions = database.dbc.attribute_definitions_rel

    for definition in definitions.values():
        if definition.default_value is not None:
            if definition.type_name in ["STRING", "ENUM"]:
                fmt = 'BA_DEF_DEF_REL_ "{name}" "{value}";'
            else:
                fmt = 'BA_DEF_DEF_REL_ "{name}" {value};'

            ba_def_def_rel.append(fmt.format(name=definition.name,
                                             value=definition.default_value))

    return ba_def_def_rel


def _dump_attributes(database, sort_signals, sort_attributes):
    attributes = []

    def get_value(attribute):
        result = attribute.value

        if attribute.definition.type_name == "STRING":
            result = f'"{attribute.value}"'

        return result

    if database.dbc is not None:
        if database.dbc.attributes is not None:
            for attribute in database.dbc.attributes.values():
                attributes.append(('dbc', attribute, None, None, None, None))

        for envvar in database.dbc.environment_variables.values():
            if envvar.dbc is not None:
                if envvar.dbc.attributes is not None:
                    for attribute in envvar.dbc.attributes.values():
                        attributes.append(('envvar', attribute, None, None, None, envvar))

    for node in database.nodes:
        if node.dbc is not None:
            if node.dbc.attributes is not None:
                for attribute in node.dbc.attributes.values():
                    attributes.append(('node', attribute, node, None, None, None))

    for message in database.messages:
        # retrieve the ordered dictionary of message attributes
        msg_attributes = OrderedDict()
        if message.dbc is not None and message.dbc.attributes is not None:
            msg_attributes.update(message.dbc.attributes)

        # synchronize the attribute for the message cycle time with
        # the cycle time specified by the message object
        gen_msg_cycle_time_def: AttributeDefinition  # type: ignore[annotation-unchecked]
        msg_cycle_time = message.cycle_time or 0
        if gen_msg_cycle_time_def := database.dbc.attribute_definitions.get("GenMsgCycleTime"):
            if msg_cycle_time != gen_msg_cycle_time_def.default_value:
                msg_attributes['GenMsgCycleTime'] = Attribute(
                    value=msg_cycle_time,
                    definition=gen_msg_cycle_time_def,
                )
            elif 'GenMsgCycleTime' in msg_attributes:
                del msg_attributes['GenMsgCycleTime']
        elif 'GenMsgCycleTime' in msg_attributes:
            del msg_attributes['GenMsgCycleTime']

        # if bus is CAN FD, set VFrameFormat
        v_frame_format_def: AttributeDefinition  # type: ignore[annotation-unchecked]
        if v_frame_format_def := database.dbc.attribute_definitions.get("VFrameFormat"):
            if message.protocol == 'j1939':
                v_frame_format_str = 'J1939PG'
            elif message.is_fd and message.is_extended_frame:
                v_frame_format_str = 'ExtendedCAN_FD'
            elif message.is_fd:
                v_frame_format_str = 'StandardCAN_FD'
            elif message.is_extended_frame:
                v_frame_format_str = 'ExtendedCAN'
            else:
                v_frame_format_str = 'StandardCAN'
            v_frame_format_def = _get_enum_vframeformat_attribute(v_frame_format_def)
            # only set the VFrameFormat if it valid according to the attribute definition
            if (
                v_frame_format_str in v_frame_format_def.choices
                and v_frame_format_str != v_frame_format_def.default_value
            ):
                msg_attributes['VFrameFormat'] = Attribute(
                    value=v_frame_format_def.choices.index(v_frame_format_str),
                    definition=v_frame_format_def,
                )


        # output all message attributes
        for attribute in msg_attributes.values():
            attributes.append(('message', attribute, None, message, None, None))

        # handle the signals contained in the message
        if sort_signals:
            signals = sort_signals(message.signals)
        else:
            signals = message.signals
        for signal in signals:
            # retrieve the ordered dictionary of signal attributes
            sig_attributes = OrderedDict()
            if signal.dbc is not None and signal.dbc.attributes is not None:
                sig_attributes = signal.dbc.attributes

            # synchronize the attribute for the signal start value with
            # the start value specified by the message object
            if signal.raw_initial is None and 'GenSigStartValue' in sig_attributes:
                del sig_attributes['GenSigStartValue']
            elif signal.raw_initial is not None:
                sig_attributes['GenSigStartValue'] = Attribute(
                    value=signal.raw_initial,
                    definition=ATTRIBUTE_DEFINITION_GENSIGSTARTVALUE)

            # output all signal attributes
            for attribute in sig_attributes.values():
                attributes.append(('signal', attribute, None, message, signal, None))

    if sort_attributes:
        attributes = sort_attributes(attributes)

    ba = []
    for typ, attribute, node, message, signal, envvar in attributes:
        if typ == 'dbc':
            ba.append(f'BA_ "{attribute.definition.name}" '
                      f'{get_value(attribute)};')
        elif typ == 'envvar':
            ba.append(f'BA_ "{attribute.definition.name}" '
                      f'{attribute.definition.kind} '
                      f'{envvar.name} '
                      f'{get_value(attribute)};')
        elif typ == 'node':
            ba.append(f'BA_ "{attribute.definition.name}" '
                      f'{attribute.definition.kind} '
                      f'{node.name} '
                      f'{get_value(attribute)};')
        elif typ == 'message':
            ba.append(f'BA_ "{attribute.definition.name}" '
                      f'{attribute.definition.kind} '
                      f'{get_dbc_frame_id(message)} '
                      f'{get_value(attribute)};')
        elif typ == 'signal':
            ba.append(f'BA_ "{attribute.definition.name}" '
                      f'{attribute.definition.kind} '
                      f'{get_dbc_frame_id(message)} '
                      f'{signal.name} '
                      f'{get_value(attribute)};')

    return ba


def _dump_attributes_rel(database, sort_signals):
    ba_rel = []

    def get_value(attribute):
        result = attribute.value

        if attribute.definition.type_name == "STRING":
            result = '"' + attribute.value + '"'

        return result

    if database.dbc is not None and database.dbc.attributes_rel is not None:
        attributes_rel = database.dbc.attributes_rel
        for frame_id, element in attributes_rel.items():
            if "signal" in element:
                for signal_name, signal_lst in element['signal'].items():
                    for node_name, node_dict in signal_lst['node'].items():
                        for attribute in node_dict.values():
                            ba_rel.append(f'BA_REL_ "{attribute.definition.name}" '
                                          f'BU_SG_REL_ '
                                          f'{node_name} '
                                          f'SG_ '
                                          f'{frame_id} '
                                          f'{signal_name} '
                                          f'{get_value(attribute)};')
            elif "node" in element:
                for node_name, node_dict in element['node'].items():
                    for attribute in node_dict.values():
                        ba_rel.append(f'BA_REL_ "{attribute.definition.name}" '
                                      f'BU_BO_REL_ '
                                      f'{node_name} '
                                      f'{frame_id} '
                                      f'{get_value(attribute)};')

    return ba_rel


def _dump_choices(database, sort_signals, sort_choices):
    val = []

    for message in database.messages:
        if sort_signals:
            signals = sort_signals(message.signals)
        else:
            signals = message.signals
        for signal in signals:
            if signal.choices is None:
                continue

            if sort_choices:
                choices = sort_choices(signal.choices)
            else:
                choices = signal.choices

            val.append(
                'VAL_ {frame_id} {name} {choices} ;'.format(
                    frame_id=get_dbc_frame_id(message),
                    name=signal.name,
                    choices=' '.join([f'{value} "{text}"' for value, text in choices.items()])))

    return val


def _dump_signal_groups(database):
    sig_group = []

    for message in database.messages:
        if message.signal_groups is None:
            continue

        for signal_group in message.signal_groups:
            all_sig_names = [sig.name for sig in message.signals]
            signal_group.signal_names = list(filter(lambda sig_name: sig_name in all_sig_names, signal_group.signal_names))
            sig_group.append(
                'SIG_GROUP_ {frame_id} {signal_group_name} {repetitions} : {signal_names};'.format(
                    frame_id=get_dbc_frame_id(message),
                    signal_group_name=signal_group.name,
                    repetitions=signal_group.repetitions,
                    signal_names=' '.join(signal_group.signal_names)
                ))

    return sig_group


def _is_extended_mux_needed(messages):
    """Check for messages with more than one mux signal or signals with
    more than one multiplexer value.

    """

    for message in messages:
        multiplexers = [
            signal.name
            for signal in message.signals
            if signal.is_multiplexer
        ]

        if len(multiplexers) > 1:
            return True

        for signal in message.signals:
            if signal.multiplexer_ids:
                if len(signal.multiplexer_ids) > 1:
                    return True

    return False


def _create_mux_ranges(multiplexer_ids):
    """Create a list of ranges based on a list of single values.

    Example:
        Input:  [1, 2, 3, 5,      7, 8, 9]
        Output: [[1, 3], [5, 5], [7, 9]]

    """

    ordered = sorted(multiplexer_ids)
    # Anything but ordered[0] - 1
    prev_value = ordered[0]
    ranges = []

    for value in ordered:
        if value == prev_value + 1:
            ranges[-1][1] = value
        else:
            ranges.append([value, value])

        prev_value = value

    return ranges


def _dump_signal_mux_values(database):
    """Create multiplex entries ("SG_MUL_VAL_") if extended multiplexing
    is used.

    """

    if not _is_extended_mux_needed(database.messages):
        return []

    sig_mux_values = []

    for message in database.messages:
        for signal in message.signals:
            if not signal.multiplexer_ids:
                continue

            ranges = ', '.join([
                f'{minimum}-{maximum}'
                for minimum, maximum in _create_mux_ranges(signal.multiplexer_ids)
            ])

            sig_mux_values.append(
                f'SG_MUL_VAL_ {get_dbc_frame_id(message)} {signal.name} {signal.multiplexer_signal} {ranges};')

    return sig_mux_values


def _dump_environment_variables(database: InternalDatabase) -> list[str]:
    """Dump environment variables (EV_ entries)."""
    ev_lines: list[str] = []

    if database.dbc is None:
        return ev_lines

    for env in database.dbc.environment_variables.values():
        # Prepare values, using empty strings for None where appropriate
        env_type = env.env_type if env.env_type is not None else ''
        minimum = '' if env.minimum is None else env.minimum
        maximum = '' if env.maximum is None else env.maximum
        # escape unit quotes
        unit = '' if env.unit is None else env.unit.replace('"', '\\"')
        initial = '' if env.initial_value is None else env.initial_value
        env_id = '' if env.env_id is None else env.env_id
        access_type = '' if env.access_type is None else env.access_type
        access_node = '' if env.access_node is None else env.access_node

        escaped_unit = unit
        ev_lines.append(
            f'EV_ {env.name}: {env_type} [{minimum}|{maximum}] "{escaped_unit}" {initial} {env_id} {access_type} {access_node};'
        )

    return ev_lines


def _load_comments(tokens):
    comments = defaultdict(dict)

    for comment in tokens.get('CM_', []):
        if not isinstance(comment[1], list):
            # CANdb++ behaviour: all bus comments are concatenated
            existing_comment = comments['database'].get('bus', '')
            comments['database']['bus'] = existing_comment + comment[1]
            continue

        item = comment[1]
        kind = item[0]

        if kind == 'SG_':
            frame_id = int(item[1])

            if 'signal' not in comments[frame_id]:
                comments[frame_id]['signal'] = {}

            comments[frame_id]['signal'][item[2]] = item[3]
        elif kind == 'BO_':
            frame_id = int(item[1])
            comments[frame_id]['message'] = item[2]
        elif kind == 'BU_':
            node_name = item[1]
            comments[node_name] = item[2]
        elif kind == 'EV_':
            environment_variable_name = item[1]
            comments[environment_variable_name] = item[2]

    return comments


def _load_attribute_definitions(tokens):
    return tokens.get('BA_DEF_', [])


def _load_attribute_definition_defaults(tokens):
    defaults = OrderedDict()

    for default_attr in tokens.get('BA_DEF_DEF_', []):
        defaults[default_attr[1]] = default_attr[2]

    return defaults


def _load_attribute_definitions_relation(tokens):
    return tokens.get('BA_DEF_REL_', [])


def _load_attribute_definition_relation_defaults(tokens):
    defaults = OrderedDict()

    for default_attr in tokens.get('BA_DEF_DEF_REL_', []):
        defaults[default_attr[1]] = default_attr[2]

    return defaults


def _load_attributes(tokens, definitions):
    attributes = OrderedDict()
    attributes['node'] = OrderedDict()
    attributes['envvar'] = OrderedDict()

    def to_object(attribute):
        value = attribute[3]

        definition = definitions[attribute[1]]

        if definition.type_name in ['INT', 'HEX', 'ENUM']:
            value = to_int(value)
        elif definition.type_name == 'FLOAT':
            value = to_float(value)

        return Attribute(value=value,
                         definition=definition)

    for attribute in tokens.get('BA_', []):
        name = attribute[1]

        if len(attribute[2]) > 0:
            item = attribute[2][0]
            kind = item[0]

            if kind == 'SG_':
                frame_id_dbc = int(item[1])
                signal = item[2]

                if frame_id_dbc not in attributes:
                    attributes[frame_id_dbc] = {}
                    attributes[frame_id_dbc]['message'] = OrderedDict()

                if 'signal' not in attributes[frame_id_dbc]:
                    attributes[frame_id_dbc]['signal'] = OrderedDict()

                if signal not in attributes[frame_id_dbc]['signal']:
                    attributes[frame_id_dbc]['signal'][signal] = OrderedDict()

                attributes[frame_id_dbc]['signal'][signal][name] = to_object(attribute)
            elif kind == 'BO_':
                frame_id_dbc = int(item[1])

                if frame_id_dbc not in attributes:
                    attributes[frame_id_dbc] = {}
                    attributes[frame_id_dbc]['message'] = OrderedDict()

                attributes[frame_id_dbc]['message'][name] = to_object(attribute)
            elif kind == 'BU_':
                node = item[1]

                if node not in attributes['node']:
                    attributes['node'][node] = OrderedDict()

                attributes['node'][node][name] = to_object(attribute)
            elif kind == 'EV_':
                envvar = item[1]

                if envvar not in attributes['envvar']:
                    attributes['envvar'][envvar] = OrderedDict()

                attributes['envvar'][envvar][name] = to_object(attribute)
        else:
            if 'database' not in attributes:
                attributes['database'] = OrderedDict()

            attributes['database'][name] = to_object(attribute)

    return attributes


def _load_attributes_rel(tokens, definitions):
    attributes_rel = OrderedDict()

    def to_object(attribute, value):

        definition = definitions[attribute[1]]

        if definition.type_name in ['INT', 'HEX', 'ENUM']:
            value = to_int(value)
        elif definition.type_name == 'FLOAT':
            value = to_float(value)

        return Attribute(value=value,
                         definition=definition)

    for attribute in tokens.get('BA_REL_', []):
        name = attribute[1]
        rel_type = attribute[2]
        node = attribute[3]

        if rel_type == "BU_SG_REL_":

            frame_id_dbc = int(attribute[5])
            signal = attribute[6]

            if frame_id_dbc not in attributes_rel:
                attributes_rel[frame_id_dbc] = {}

            if 'signal' not in attributes_rel[frame_id_dbc]:
                attributes_rel[frame_id_dbc]['signal'] = OrderedDict()

            if signal not in attributes_rel[frame_id_dbc]['signal']:
                attributes_rel[frame_id_dbc]['signal'][signal] = OrderedDict()

            if 'node' not in attributes_rel[frame_id_dbc]['signal'][signal]:
                attributes_rel[frame_id_dbc]['signal'][signal]['node'] = OrderedDict()

            if node not in attributes_rel[frame_id_dbc]['signal'][signal]['node']:
                attributes_rel[frame_id_dbc]['signal'][signal]['node'][node] = OrderedDict()

            attributes_rel[frame_id_dbc]['signal'][signal]['node'][node][name] = to_object(attribute, attribute[7])

        elif rel_type == "BU_BO_REL_":
            frame_id_dbc = int(attribute[4])

            if frame_id_dbc not in attributes_rel:
                attributes_rel[frame_id_dbc] = {}

            if 'node' not in attributes_rel[frame_id_dbc]:
                attributes_rel[frame_id_dbc]['node'] = OrderedDict()

            if node not in attributes_rel[frame_id_dbc]['node']:
                attributes_rel[frame_id_dbc]['node'][node] = OrderedDict()

            attributes_rel[frame_id_dbc]['node'][node][name] = to_object(attribute, attribute[5])

        else:
            pass

    return attributes_rel


def _load_value_tables(tokens):
    """Load value tables, that is, choice definitions.

    """

    value_tables = OrderedDict()

    for value_table in tokens.get('VAL_TABLE_', []):
        name = value_table[1]
        choices = {int(number): NamedSignalValue(int(number), text) for number, text in value_table[2]}
        #choices = {int(number): text for number, text in value_table[2]}
        value_tables[name] = choices

    return value_tables


def _load_environment_variables(tokens, comments, attributes, attribute_definitions):
    environment_variables = OrderedDict()

    for env_var in tokens.get('EV_', []):
        short_name = env_var[1]
        long_name = _get_environment_variable_name(attributes, short_name)
        environment_variables[long_name] = EnvironmentVariable(
            name=long_name,
            env_type=int(env_var[3]),
            minimum=num(env_var[5]),
            maximum=num(env_var[7]),
            unit=env_var[9],
            initial_value=num(env_var[10]),
            env_id=int(env_var[11]),
            access_type=env_var[12],
            access_node=env_var[13],
            comment=comments.get(env_var[1], None),
            dbc_specifics=DbcSpecifics(attributes['envvar'].get(short_name, None),
                                       attribute_definitions))

    return environment_variables

def _load_choices(tokens):
    choices = defaultdict(dict)

    for _choice in tokens.get('VAL_', []):
        if len(_choice[1]) == 0:
            continue

        od = OrderedDict((int(v[0]), NamedSignalValue(int(v[0]), v[1])) for v in _choice[3])

        if len(od) == 0:
            continue

        frame_id = int(_choice[1][0])
        choices[frame_id][_choice[2]] = od

    return choices

def _load_message_senders(tokens, attributes):
    """Load additional message senders.

    """

    message_senders = defaultdict(list)

    for senders in tokens.get('BO_TX_BU_', []):
        frame_id = int(senders[1])
        message_senders[frame_id] += [
            _get_node_name(attributes, sender) for sender in senders[3]
        ]

    return message_senders


def _load_signal_types(tokens):
    """Load signal types.

    """

    signal_types = defaultdict(dict)

    for signal_type in tokens.get('SIG_VALTYPE_', []):
        frame_id = int(signal_type[1])
        signal_name = signal_type[2]
        signal_types[frame_id][signal_name] = int(signal_type[4])

    return signal_types


def _load_signal_multiplexer_values(tokens):
    """Load additional signal multiplexer values.

    """

    signal_multiplexer_values = defaultdict(dict)

    for signal_multiplexer_value in tokens.get('SG_MUL_VAL_', []):
        frame_id = int(signal_multiplexer_value[1])
        signal_name = signal_multiplexer_value[2]
        multiplexer_signal = signal_multiplexer_value[3]
        multiplexer_ids = []

        for lower, upper in signal_multiplexer_value[4]:
            lower = int(lower)
            upper = int(upper[1:])
            # ToDo: Probably store ranges as tuples to not run out of
            #       memory on huge ranges.
            multiplexer_ids.extend(range(lower, upper + 1))

        if multiplexer_signal not in signal_multiplexer_values[frame_id]:
            signal_multiplexer_values[frame_id][multiplexer_signal] = {}

        multiplexer_signal = signal_multiplexer_values[frame_id][multiplexer_signal]
        multiplexer_signal[signal_name] = multiplexer_ids

    return signal_multiplexer_values


def _load_signal_groups(tokens, attributes):
    """Load signal groups.

    """

    signal_groups = defaultdict(list)


    def get_attributes(frame_id_dbc, signal):
        """Get attributes for given signal.

        """

        try:
            return attributes[frame_id_dbc]['signal'][signal]
        except KeyError:
            return None

    def get_signal_name(frame_id_dbc, name):
        signal_attributes = get_attributes(frame_id_dbc, name)

        try:
            return signal_attributes['SystemSignalLongSymbol'].value
        except (KeyError, TypeError):
            return name

    for signal_group in tokens.get('SIG_GROUP_',[]):
        frame_id = int(signal_group[1])
        signal_names = [get_signal_name(frame_id, signal_name) for signal_name in signal_group[5]]
        signal_groups[frame_id].append(SignalGroup(name=signal_group[2],
                                                   repetitions=int(signal_group[3]),
                                                   signal_names=signal_names))

    return signal_groups


def _load_signals(tokens,
                  comments,
                  attributes,
                  definitions,
                  choices,
                  signal_types,
                  signal_multiplexer_values,
                  frame_id_dbc,
                  multiplexer_signal):
    signal_to_multiplexer = {}

    try:
        signal_multiplexer_values = signal_multiplexer_values[frame_id_dbc]

        for multiplexer_name, items in signal_multiplexer_values.items():
            for name in items:
                signal_to_multiplexer[name] = multiplexer_name
    except KeyError:
        pass

    def get_attributes(frame_id_dbc, signal):
        """Get attributes for given signal.

        """

        try:
            return attributes[frame_id_dbc]['signal'][signal]
        except KeyError:
            return None

    def get_comment(frame_id_dbc, signal):
        """Get comment for given signal.

        """

        try:
            return comments[frame_id_dbc]['signal'][signal]
        except KeyError:
            return None

    def get_choices(frame_id_dbc, signal):
        """Get choices for given signal.

        """

        try:
            return choices[frame_id_dbc][signal]
        except KeyError:
            return None

    def get_is_multiplexer(signal):
        if len(signal[1]) == 2:
            return signal[1][1].endswith('M')
        else:
            return False

    def get_multiplexer_ids(signal, multiplexer_signal):
        ids = []

        if multiplexer_signal is not None:
            if len(signal) == 2 and not signal[1].endswith('M'):
                value = signal[1][1:].rstrip('M')
                ids.append(int(value))
        else:
            multiplexer_signal = get_multiplexer_signal(signal,
                                                        multiplexer_signal)

        try:
            ids.extend(
                signal_multiplexer_values[multiplexer_signal][signal[0]])
        except KeyError:
            pass

        if ids:
            return list(set(ids))

    def get_multiplexer_signal(signal, multiplexer_signal):
        if len(signal) != 2:
            return

        if multiplexer_signal is None:
            try:
                return signal_to_multiplexer[signal[0]]
            except KeyError:
                pass
        elif signal[0] != multiplexer_signal:
            return multiplexer_signal

    def get_receivers(receivers):
        if receivers == ['Vector__XXX']:
            receivers = []

        return [_get_node_name(attributes, receiver) for receiver in receivers]

    def get_minimum(minimum, maximum):
        if minimum == maximum == '0':
            return None
        else:
            return num(minimum)

    def get_maximum(minimum, maximum):
        if minimum == maximum == '0':
            return None
        else:
            return num(maximum)

    def get_is_float(frame_id_dbc, signal):
        """Get is_float for given signal.

        """

        try:
            return signal_types[frame_id_dbc][signal] in FLOAT_SIGNAL_TYPES
        except KeyError:
            return False

    def get_signal_name(frame_id_dbc, name):
        signal_attributes = get_attributes(frame_id_dbc, name)

        try:
            return signal_attributes['SystemSignalLongSymbol'].value
        except (KeyError, TypeError):
            return name

    def get_signal_initial_value(frame_id_dbc, name):
        signal_attributes = get_attributes(frame_id_dbc, name)

        try:
            return signal_attributes['GenSigStartValue'].value
        except (KeyError, TypeError):
            return None

    def get_signal_spn(frame_id_dbc, name):
        signal_attributes = get_attributes(frame_id_dbc, name)
        if signal_attributes is not None and 'SPN' in signal_attributes:
            if (value := signal_attributes['SPN'].value) is not None:
                return value

        if definitions is not None and 'SPN' in definitions:
            return definitions['SPN'].default_value

        return None

    signals = []

    for signal in tokens:
        signals.append(
            Signal(name=get_signal_name(frame_id_dbc, signal[1][0]),
                   start=int(signal[3]),
                   length=int(signal[5]),
                   receivers=get_receivers(signal[20]),
                   byte_order=('big_endian'
                               if signal[7] == '0'
                               else 'little_endian'),
                   is_signed=(signal[8] == '-'),
                   raw_initial=get_signal_initial_value(frame_id_dbc, signal[1][0]),
                   conversion=BaseConversion.factory(
                       scale=num(signal[10]),
                       offset=num(signal[12]),
                       is_float=get_is_float(frame_id_dbc, signal[1][0]),
                       choices=get_choices(frame_id_dbc, signal[1][0]),
                   ),
                   minimum=get_minimum(signal[15], signal[17]),
                   maximum=get_maximum(signal[15], signal[17]),
                   unit=(None if signal[19] == '' else signal[19]),
                   spn=get_signal_spn(frame_id_dbc, signal[1][0]),
                   dbc_specifics=DbcSpecifics(get_attributes(frame_id_dbc, signal[1][0]),
                                              definitions),
                   comment=get_comment(frame_id_dbc,
                                       signal[1][0]),
                   is_multiplexer=get_is_multiplexer(signal),
                   multiplexer_ids=get_multiplexer_ids(signal[1],
                                                       multiplexer_signal),
                   multiplexer_signal=get_multiplexer_signal(signal[1],
                                                             multiplexer_signal)))

    return signals


def _get_enum_vframeformat_attribute(int_attribute):
    """Get VFrameFormat attribute definition as ENUM.

    """

    if int_attribute.type_name != 'INT':
        return int_attribute

    default_value = int_attribute.default_value
    enum_attribute = deepcopy(ATTRIBUTE_DEFINITION_VFRAMEFORMAT)
    enum_attribute.default_value = enum_attribute.choices[default_value]

    return enum_attribute

def _load_messages(tokens,
                   comments,
                   attributes,
                   definitions,
                   choices,
                   message_senders,
                   signal_types,
                   signal_multiplexer_values,
                   strict,
                   bus_name,
                   signal_groups,
                   sort_signals):
    """Load messages.

    """

    def get_attributes(frame_id_dbc):
        """Get attributes for given message.

        """

        try:
            return attributes[frame_id_dbc]['message']
        except KeyError:
            return None

    def get_comment(frame_id_dbc):
        """Get comment for given message.

        """

        try:
            return comments[frame_id_dbc]['message']
        except KeyError:
            return None

    def get_send_type(frame_id_dbc):
        """Get send type for a given message.

        """

        result = None
        message_attributes = get_attributes(frame_id_dbc)

        try:
            result = message_attributes['GenMsgSendType'].value

            # if definitions is enum (otherwise above value is maintained) -> Prevents ValueError
            if definitions['GenMsgSendType'].choices is not None:
                # Resolve ENUM index to ENUM text
                result = definitions['GenMsgSendType'].choices[int(result)]
        except (KeyError, TypeError):
            try:
                result = definitions['GenMsgSendType'].default_value
            except (KeyError, TypeError):
                result = None

        return result

    def get_cycle_time(frame_id_dbc):
        """Get cycle time for a given message.

        """
        message_attributes = get_attributes(frame_id_dbc)

        gen_msg_cycle_time_def = definitions.get('GenMsgCycleTime')
        if gen_msg_cycle_time_def is None:
            return None

        if message_attributes:
            gen_msg_cycle_time_attr = message_attributes.get('GenMsgCycleTime')
            if gen_msg_cycle_time_attr:
                return gen_msg_cycle_time_attr.value or None

        return gen_msg_cycle_time_def.default_value or None


    def get_frame_format(frame_id_dbc):
        """Get frame format for a given message"""

        message_attributes = get_attributes(frame_id_dbc)
        ref_definitions = definitions.get('VFrameFormat', None)
        if ref_definitions is None:
            return None

        ref_definitions = _get_enum_vframeformat_attribute(ref_definitions)

        try:
            frame_format = message_attributes['VFrameFormat'].value
            frame_format = ref_definitions.choices[frame_format]
        except (KeyError, TypeError):
            frame_format = ref_definitions.default_value

        return frame_format

    def get_protocol(frame_id_dbc):
        """Get protocol for a given message.

        """

        frame_format = get_frame_format(frame_id_dbc)

        if frame_format == 'J1939PG':
            return 'j1939'
        else:
            return None

    def get_message_name(frame_id_dbc, name):
        message_attributes = get_attributes(frame_id_dbc)

        try:
            return message_attributes['SystemMessageLongSymbol'].value
        except (KeyError, TypeError):
            return name

    def get_signal_groups(frame_id_dbc):
        try:
            return signal_groups[frame_id_dbc]
        except KeyError:
            return None

    messages = []

    for message in tokens.get('BO_', []):
        # Any message named VECTOR__INDEPENDENT_SIG_MSG contains
        # signals not assigned to any message. Cantools does not yet
        # support unassigned signals. Discard them for now.
        if message[2] == 'VECTOR__INDEPENDENT_SIG_MSG':
            continue

        # Frame id.
        frame_id_dbc = int(message[1])
        frame_id = frame_id_dbc & 0x7fffffff
        is_extended_frame = bool(frame_id_dbc & 0x80000000)
        frame_format = get_frame_format(frame_id_dbc)
        if frame_format is not None:
            is_fd = frame_format.endswith("CAN_FD")
        else:
            is_fd = False

        # Senders.
        senders = [_get_node_name(attributes, message[5])]

        for node in message_senders.get(frame_id_dbc, []):
            if node not in senders:
                senders.append(node)

        if senders == ['Vector__XXX']:
            senders = []

        # Signal multiplexing.
        multiplexer_signal = None

        for signal in message[6]:
            if len(signal[1]) == 2:
                if signal[1][1].endswith('M'):
                    if multiplexer_signal is None:
                        multiplexer_signal = signal[1][0]
                    else:
                        multiplexer_signal = None
                        break

        signals = _load_signals(message[6],
                                comments,
                                attributes,
                                definitions,
                                choices,
                                signal_types,
                                signal_multiplexer_values,
                                frame_id_dbc,
                                multiplexer_signal)

        messages.append(
            Message(frame_id=frame_id,
                    is_extended_frame=is_extended_frame,
                    name=get_message_name(frame_id_dbc, message[2]),
                    length=int(message[4], 0),
                    senders=senders,
                    send_type=get_send_type(frame_id_dbc),
                    cycle_time=get_cycle_time(frame_id_dbc),
                    dbc_specifics=DbcSpecifics(get_attributes(frame_id_dbc),
                                               definitions),
                    signals=signals,
                    comment=get_comment(frame_id_dbc),
                    strict=strict,
                    unused_bit_pattern=0xff,
                    protocol=get_protocol(frame_id_dbc),
                    bus_name=bus_name,
                    signal_groups=get_signal_groups(frame_id_dbc),
                    sort_signals=sort_signals,
                    is_fd=is_fd))

    return messages


def _load_version(tokens):
    return tokens.get('VERSION', [[None, None]])[0][1]


def _load_bus(attributes, comments):
    try:
        bus_name = attributes['database']['DBName'].value
    except KeyError:
        bus_name = ''

    try:
        bus_baudrate = attributes['database']['Baudrate'].value
    except KeyError:
        bus_baudrate = None

    try:
        bus_comment = comments['database']['bus']
    except KeyError:
        bus_comment = None

    if not any([bus_name, bus_baudrate, bus_comment]):
        return None

    return Bus(bus_name, baudrate=bus_baudrate, comment=bus_comment)


def _load_nodes(tokens, comments, attributes, definitions):
    nodes = None

    for token in tokens.get('BU_', []):
        nodes = [Node(name=_get_node_name(attributes, node),
                      comment=comments.get(node, None),
                      dbc_specifics=DbcSpecifics(attributes['node'].get(node, None),
                                                 definitions))
                 for node in token[2]]

    return nodes


def get_attribute_definition(database, name, default):
    if database.dbc is None:
        database.dbc = DbcSpecifics()

    if name not in database.dbc.attribute_definitions:
        database.dbc.attribute_definitions[name] = default

    return database.dbc.attribute_definitions[name]


def get_long_envvar_name_attribute_definition(database):
    return get_attribute_definition(database,
                                    'SystemEnvVarLongSymbol',
                                    ATTRIBUTE_DEFINITION_LONG_ENVVAR_NAME)


def get_long_node_name_attribute_definition(database):
    return get_attribute_definition(database,
                                    'SystemNodeLongSymbol',
                                    ATTRIBUTE_DEFINITION_LONG_NODE_NAME)


def get_long_message_name_attribute_definition(database):
    return get_attribute_definition(database,
                                    'SystemMessageLongSymbol',
                                    ATTRIBUTE_DEFINITION_LONG_MESSAGE_NAME)


def get_long_signal_name_attribute_definition(database):
    return get_attribute_definition(database,
                                    'SystemSignalLongSymbol',
                                    ATTRIBUTE_DEFINITION_LONG_SIGNAL_NAME)


def try_remove_attribute(dbc, name):
    try:
        dbc.attributes.pop(name)
    except (KeyError, AttributeError):
        pass


def remove_special_chars(database):
    for node in database.nodes:
        new_node_name = get_dbc_name(node.name)

        for message in database.messages:
            for index, sender in enumerate(message.senders):
                if sender == node.name:
                    message.senders[index] = new_node_name

            for signal in message.signals:
                for index, receiver in enumerate(signal.receivers):
                    if receiver == node.name:
                        signal.receivers[index] = new_node_name

        node.name = new_node_name

    for message in database.messages:
        message.name = get_dbc_name(message.name)

        for signal in message.signals:
            signal.name = get_dbc_name(signal.name)

        if message.signal_groups is not None:
            for signal_group in message.signal_groups:
                signal_group.name = get_dbc_name(signal_group.name)
                signal_group.signal_names = [get_dbc_name(sig_name) for sig_name in signal_group.signal_names]

    return database


def make_node_names_unique(database: InternalDatabase, shorten_long_names: bool) -> None:
    converter = LongNamesConverter([node.name for node in database.nodes])

    for node in database.nodes:
        long_name = node.name
        short_name = converter.long_to_short[long_name]
        try_remove_attribute(node.dbc, 'SystemNodeLongSymbol')

        if (long_name == short_name) or not shorten_long_names:
            continue

        for message in database.messages:
            for index, sender in enumerate(message.senders):
                if sender == node.name:
                    message.senders[index] = short_name

            for signal in message.signals:
                for index, receiver in enumerate(signal.receivers):
                    if receiver == node.name:
                        signal.receivers[index] = short_name

        if node.dbc is None:
            node.dbc = DbcSpecifics()

        node.dbc.attributes['SystemNodeLongSymbol'] = Attribute(
            long_name,
            get_long_node_name_attribute_definition(database))
        node.name = short_name


def make_message_names_unique(database: InternalDatabase, shorten_long_names: bool) -> None:
    converter = LongNamesConverter([message.name for message in database.messages])

    for message in database.messages:
        long_name = message.name
        short_name = converter.long_to_short[long_name]
        try_remove_attribute(message.dbc, 'SystemMessageLongSymbol')

        if (long_name == short_name) or not shorten_long_names:
            continue

        if message.dbc is None:
            message.dbc = DbcSpecifics()

        message.dbc.attributes['SystemMessageLongSymbol'] = Attribute(
            long_name,
            get_long_message_name_attribute_definition(database))
        message.name = short_name


def make_signal_names_unique(database: InternalDatabase, shorten_long_names: bool) -> None:
    for message in database.messages:
        converter = LongNamesConverter([signal.name for signal in message.signals])
        for signal in message.signals:
            long_name = signal.name
            short_name = converter.long_to_short[long_name]
            try_remove_attribute(signal.dbc, 'SystemSignalLongSymbol')

            if (long_name == short_name) or not shorten_long_names:
                continue

            if signal.dbc is None:
                signal.dbc = DbcSpecifics()

            signal.dbc.attributes['SystemSignalLongSymbol'] = Attribute(
                long_name,
                get_long_signal_name_attribute_definition(database))
            signal.name = short_name

        if shorten_long_names and message.signal_groups:
            for signal_group in message.signal_groups:
                signal_group.signal_names = [converter.long_to_short[long_name]
                                             for long_name in signal_group.signal_names]


def make_envvar_names_unique(database: InternalDatabase, shorten_long_names: bool) -> None:
    if database.dbc is None:
        return

    envvars = database.dbc.environment_variables.values()
    converter = LongNamesConverter([envvar.name for envvar in envvars])

    for envvar in envvars:
        long_name = envvar.name
        short_name = converter.long_to_short[long_name]
        try_remove_attribute(envvar.dbc, 'SystemEnvVarLongSymbol')

        if (long_name == short_name) or not shorten_long_names:
            continue

        envvar.dbc.attributes['SystemEnvVarLongSymbol'] = Attribute(
            long_name,
            get_long_envvar_name_attribute_definition(database))
        envvar.name = short_name


def make_names_unique(database: InternalDatabase, shorten_long_names: bool) -> InternalDatabase:
    """Make message, signal and node names unique and add attributes for
    their long names.

    """

    make_node_names_unique(database, shorten_long_names)
    make_message_names_unique(database, shorten_long_names)
    make_signal_names_unique(database, shorten_long_names)
    make_envvar_names_unique(database, shorten_long_names)

    return database


def dump_string(database: InternalDatabase,
                sort_signals:type_sort_signals=SORT_SIGNALS_DEFAULT,
                sort_attribute_signals:type_sort_signals=SORT_SIGNALS_DEFAULT,
                sort_attributes:type_sort_attributes=None,
                sort_choices:type_sort_choices=None,
                shorten_long_names:bool=True) -> str:
    """Format database in DBC file format.
       sort_signals defines how to sort signals in message definitions
       sort_attribute_signals defines how to sort signals in metadata -
          comments, value table definitions and attributes

    """

    if sort_signals == SORT_SIGNALS_DEFAULT:
        sort_signals = sort_signals_by_start_bit_reversed
    if sort_attribute_signals == SORT_SIGNALS_DEFAULT:
        sort_attribute_signals = sort_signals_by_start_bit_reversed

    # Make a deep copy of the database as names and attributes will be
    # modified for items with long names.
    database = deepcopy(database)

    if database.dbc is None:
        database.dbc = DbcSpecifics()

    database = remove_special_chars(database)
    database = make_names_unique(database, shorten_long_names)
    bu = _dump_nodes(database)
    val_table = _dump_value_tables(database)
    bo = _dump_messages(database, sort_signals)
    bo_tx_bu = _dump_senders(database)
    cm = _dump_comments(database, sort_attribute_signals)
    signal_types = _dump_signal_types(database)
    ba_def = _dump_attribute_definitions(database)
    ba_def_rel = _dump_attribute_definitions_rel(database)
    ba_def_def = _dump_attribute_definition_defaults(database)
    ba_def_def_rel = _dump_attribute_definition_defaults_rel(database)
    ba = _dump_attributes(database, sort_attribute_signals, sort_attributes)
    ba_rel = _dump_attributes_rel(database, sort_attribute_signals)
    val = _dump_choices(database, sort_attribute_signals, sort_choices)
    sig_group = _dump_signal_groups(database)
    sig_mux_values = _dump_signal_mux_values(database)
    ev = _dump_environment_variables(database)

    return DBC_FMT.format(version=_dump_version(database),
                          bu=' '.join(bu),
                          val_table='\r\n'.join(val_table),
                          bo='\r\n\r\n'.join(bo),
                          bo_tx_bu='\r\n'.join(bo_tx_bu),
                          ev='\r\n\r\n'.join(ev),
                          cm='\r\n'.join(cm),
                          signal_types='\r\n'.join(signal_types),
                          ba_def='\r\n'.join(ba_def),
                          ba_def_rel="".join([elem+"\r\n" for elem in ba_def_rel]),
                          ba_def_def='\r\n'.join(ba_def_def),
                          ba_def_def_rel="".join([elem+"\r\n" for elem in ba_def_def_rel]),
                          ba='\r\n'.join(ba),
                          ba_rel="".join([elem+"\r\n" for elem in ba_rel]),
                          val='\r\n'.join(val),
                          sig_group='\r\n'.join(sig_group),
                          sig_mux_values='\r\n'.join(sig_mux_values))


def get_definitions_dict(definitions, defaults):
    result = OrderedDict()

    def convert_value(definition, value):
        if definition.type_name in ['INT', 'HEX']:
            value = to_int(value)
        elif definition.type_name == 'FLOAT':
            value = to_float(value)

        return value

    for item in definitions:
        if len(item[1]) > 0:
            kind = item[1][0]
        else:
            kind = None

        definition = AttributeDefinition(name=item[2],
                                         kind=kind,
                                         type_name=item[3])
        values = item[4][0]

        if len(values) > 0:
            if definition.type_name == "ENUM":
                definition.choices = values
            elif definition.type_name in ['INT', 'FLOAT', 'HEX']:
                definition.minimum = convert_value(definition, values[0])
                definition.maximum = convert_value(definition, values[1])

        try:
            value = defaults[definition.name]
            definition.default_value = convert_value(definition, value)
        except KeyError:
            definition.default_value = None

        result[definition.name] = definition

    return result


def get_definitions_rel_dict(definitions, defaults):
    result = OrderedDict()

    def convert_value(definition, value):
        if definition.type_name in ['INT', 'HEX']:
            value = to_int(value)
        elif definition.type_name == 'FLOAT':
            value = to_float(value)

        return value

    for item in definitions:
        if len(item[1]) > 0:
            kind = item[1][0]
        else:
            kind = None

        definition = AttributeDefinition(name=item[2],
                                         kind=kind,
                                         type_name=item[3])
        values = item[4]

        if len(values) > 0:
            if definition.type_name == "ENUM":
                definition.choices = values[0]
            elif definition.type_name in ['INT', 'FLOAT', 'HEX']:
                definition.minimum = convert_value(definition, values[0][0])
                definition.maximum = convert_value(definition, values[0][1])

        try:
            value = defaults[definition.name]
            definition.default_value = convert_value(definition, value)
        except KeyError:
            definition.default_value = None

        result[definition.name] = definition

    return result


def load_string(string: str, strict: bool = True,
                sort_signals: type_sort_signals = sort_signals_by_start_bit) -> InternalDatabase:
    """Parse given string.

    """

    tokens = Parser().parse(string)

    comments = _load_comments(tokens)
    definitions = _load_attribute_definitions(tokens)
    defaults = _load_attribute_definition_defaults(tokens)
    definitions_relation = _load_attribute_definitions_relation(tokens)
    defaults_relation = _load_attribute_definition_relation_defaults(tokens)
    attribute_definitions = get_definitions_dict(definitions, defaults)
    attributes = _load_attributes(tokens, attribute_definitions)
    attribute_rel_definitions = get_definitions_rel_dict(definitions_relation, defaults_relation)
    attributes_rel = _load_attributes_rel(tokens, attribute_rel_definitions)
    bus = _load_bus(attributes, comments)
    value_tables = _load_value_tables(tokens)
    choices = _load_choices(tokens)
    message_senders = _load_message_senders(tokens, attributes)
    signal_types = _load_signal_types(tokens)
    signal_multiplexer_values = _load_signal_multiplexer_values(tokens)
    signal_groups = _load_signal_groups(tokens, attributes)
    messages = _load_messages(tokens,
                              comments,
                              attributes,
                              attribute_definitions,
                              choices,
                              message_senders,
                              signal_types,
                              signal_multiplexer_values,
                              strict,
                              bus.name if bus else None,
                              signal_groups,
                              sort_signals)
    nodes = _load_nodes(tokens, comments, attributes, attribute_definitions)
    version = _load_version(tokens)
    environment_variables = _load_environment_variables(tokens, comments, attributes, attribute_definitions)
    dbc_specifics = DbcSpecifics(attributes.get('database', None),
                                 attribute_definitions,
                                 environment_variables,
                                 value_tables,
                                 attributes_rel,
                                 attribute_rel_definitions)

    return InternalDatabase(messages,
                            nodes,
                            [bus] if bus else [],
                            version,
                            dbc_specifics)
