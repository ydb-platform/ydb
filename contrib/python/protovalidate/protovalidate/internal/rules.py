# Copyright 2023-2025 Buf Technologies, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import dataclasses
import datetime
import typing
from collections.abc import Callable

import celpy
from celpy import celtypes
from google.protobuf import any_pb2, descriptor, message, message_factory

from buf.validate import validate_pb2  # type: ignore
from protovalidate.config import Config
from protovalidate.internal.cel_field_presence import InterpretedRunner, in_has


class CompilationError(Exception):
    pass


def make_duration(msg: message.Message) -> celtypes.DurationType:
    return celtypes.DurationType(
        seconds=msg.seconds,  # type: ignore
        nanos=msg.nanos,  # type: ignore
    )


def make_timestamp(msg: message.Message) -> celtypes.TimestampType:
    return celtypes.TimestampType(1970, 1, 1) + make_duration(msg)


def unwrap(msg: message.Message) -> celtypes.Value:
    return field_to_cel(msg, msg.DESCRIPTOR.fields_by_name["value"])


_MSG_TYPE_URL_TO_CTOR: dict[str, Callable[..., celtypes.Value]] = {
    "google.protobuf.Duration": make_duration,
    "google.protobuf.Timestamp": make_timestamp,
    "google.protobuf.StringValue": unwrap,
    "google.protobuf.BytesValue": unwrap,
    "google.protobuf.Int32Value": unwrap,
    "google.protobuf.Int64Value": unwrap,
    "google.protobuf.UInt32Value": unwrap,
    "google.protobuf.UInt64Value": unwrap,
    "google.protobuf.FloatValue": unwrap,
    "google.protobuf.DoubleValue": unwrap,
    "google.protobuf.BoolValue": unwrap,
}


class MessageType(celtypes.MapType):
    msg: message.Message
    desc: descriptor.Descriptor

    def __init__(self, msg: message.Message):
        super().__init__()
        self.msg = msg
        self.desc = msg.DESCRIPTOR
        field: descriptor.FieldDescriptor
        for field in self.desc.fields:
            if field.containing_oneof is not None and not self.msg.HasField(field.name):
                continue
            self[field.name] = field_to_cel(self.msg, field)

    def __getitem__(self, name):
        field = self.desc.fields_by_name[name]
        if field.has_presence and not self.msg.HasField(name):
            if in_has():
                raise KeyError()
            else:
                return _zero_value(field)
        return super().__getitem__(name)


def _msg_to_cel(msg: message.Message) -> celtypes.Value:
    ctor = _MSG_TYPE_URL_TO_CTOR.get(msg.DESCRIPTOR.full_name)
    if ctor is not None:
        return ctor(msg)
    return MessageType(msg)


class FieldDescMetadata(typing.TypedDict):
    name: str
    ctor: typing.Callable[..., celtypes.Value]


_FIELD_DESC_METADATA_MAP: dict[typing.Any, FieldDescMetadata] = {
    descriptor.FieldDescriptor.TYPE_MESSAGE: {"name": "message", "ctor": _msg_to_cel},
    descriptor.FieldDescriptor.TYPE_GROUP: {"name": "group", "ctor": _msg_to_cel},
    descriptor.FieldDescriptor.TYPE_ENUM: {"name": "enum", "ctor": celtypes.IntType},
    descriptor.FieldDescriptor.TYPE_BOOL: {"name": "bool", "ctor": celtypes.BoolType},
    descriptor.FieldDescriptor.TYPE_BYTES: {"name": "bytes", "ctor": celtypes.BytesType},
    descriptor.FieldDescriptor.TYPE_STRING: {"name": "string", "ctor": celtypes.StringType},
    descriptor.FieldDescriptor.TYPE_FLOAT: {"name": "float", "ctor": celtypes.DoubleType},
    descriptor.FieldDescriptor.TYPE_DOUBLE: {"name": "double", "ctor": celtypes.DoubleType},
    descriptor.FieldDescriptor.TYPE_INT32: {"name": "int32", "ctor": celtypes.IntType},
    descriptor.FieldDescriptor.TYPE_INT64: {"name": "int64", "ctor": celtypes.IntType},
    descriptor.FieldDescriptor.TYPE_SINT32: {"name": "sint32", "ctor": celtypes.IntType},
    descriptor.FieldDescriptor.TYPE_SINT64: {"name": "sint64", "ctor": celtypes.IntType},
    descriptor.FieldDescriptor.TYPE_SFIXED32: {"name": "sfixed32", "ctor": celtypes.IntType},
    descriptor.FieldDescriptor.TYPE_SFIXED64: {"name": "sfixed64", "ctor": celtypes.IntType},
    descriptor.FieldDescriptor.TYPE_UINT32: {"name": "uint32", "ctor": celtypes.UintType},
    descriptor.FieldDescriptor.TYPE_UINT64: {"name": "uint64", "ctor": celtypes.UintType},
    descriptor.FieldDescriptor.TYPE_FIXED32: {"name": "fixed32", "ctor": celtypes.UintType},
    descriptor.FieldDescriptor.TYPE_FIXED64: {"name": "fixed64", "ctor": celtypes.UintType},
}


def _get_type_name(fd: typing.Any) -> str:
    md = _FIELD_DESC_METADATA_MAP.get(fd)
    if md is None:
        return "unknown"
    return md["name"]


def _get_type_ctor(fd: typing.Any) -> typing.Optional[typing.Callable[..., celtypes.Value]]:
    md = _FIELD_DESC_METADATA_MAP.get(fd)
    if md is None:
        return None
    return md["ctor"]


def _proto_message_has_field(msg: message.Message, field: descriptor.FieldDescriptor) -> typing.Any:
    if field.is_extension:
        return msg.HasExtension(field)  # type: ignore
    else:
        return msg.HasField(field.name)


def _proto_message_get_field(msg: message.Message, field: descriptor.FieldDescriptor) -> typing.Any:
    if field.is_extension:
        return msg.Extensions[field]  # type: ignore
    else:
        return getattr(msg, field.name)


def _scalar_field_value_to_cel(val: typing.Any, field: descriptor.FieldDescriptor) -> celtypes.Value:
    ctor = _get_type_ctor(field.type)
    if ctor is None:
        msg = "unknown field type"
        raise CompilationError(msg)
    return ctor(val)


def _field_value_to_cel(val: typing.Any, field: descriptor.FieldDescriptor) -> celtypes.Value:
    if field.label == descriptor.FieldDescriptor.LABEL_REPEATED:
        if field.message_type is not None and field.message_type.GetOptions().map_entry:
            return _map_field_value_to_cel(val, field)
        return _repeated_field_value_to_cel(val, field)
    return _scalar_field_value_to_cel(val, field)


def _is_empty_field(msg: message.Message, field: descriptor.FieldDescriptor) -> bool:
    if field.has_presence:  # type: ignore[attr-defined]
        return not _proto_message_has_field(msg, field)
    if field.label == descriptor.FieldDescriptor.LABEL_REPEATED:
        return len(_proto_message_get_field(msg, field)) == 0
    return _proto_message_get_field(msg, field) == field.default_value


def _repeated_field_to_cel(msg: message.Message, field: descriptor.FieldDescriptor) -> celtypes.Value:
    if field.message_type is not None and field.message_type.GetOptions().map_entry:
        return _map_field_to_cel(msg, field)
    return _repeated_field_value_to_cel(_proto_message_get_field(msg, field), field)


def _repeated_field_value_to_cel(val: typing.Any, field: descriptor.FieldDescriptor) -> celtypes.Value:
    result = celtypes.ListType()
    for item in val:
        result.append(_scalar_field_value_to_cel(item, field))
    return result


def _map_field_value_to_cel(mapping: typing.Any, field: descriptor.FieldDescriptor) -> celtypes.Value:
    result = celtypes.MapType()
    key_field = field.message_type.fields[0]
    val_field = field.message_type.fields[1]
    for key, val in mapping.items():
        result[_field_value_to_cel(key, key_field)] = _field_value_to_cel(val, val_field)
    return result


def _map_field_to_cel(msg: message.Message, field: descriptor.FieldDescriptor) -> celtypes.Value:
    return _map_field_value_to_cel(_proto_message_get_field(msg, field), field)


def field_to_cel(msg: message.Message, field: descriptor.FieldDescriptor) -> celtypes.Value:
    if field.label == descriptor.FieldDescriptor.LABEL_REPEATED:
        return _repeated_field_to_cel(msg, field)
    elif field.message_type is not None and not _proto_message_has_field(msg, field):
        return None
    else:
        return _scalar_field_value_to_cel(_proto_message_get_field(msg, field), field)


def _field_to_element(field: descriptor.FieldDescriptor) -> validate_pb2.FieldPathElement:
    return validate_pb2.FieldPathElement(
        field_number=field.number,
        field_name=field.name if not field.is_extension else f"[{field.full_name}]",
        field_type=field.type,
    )


def _oneof_to_element(oneof: descriptor.OneofDescriptor) -> validate_pb2.FieldPathElement:
    return validate_pb2.FieldPathElement(
        field_name=oneof.name,
    )


def _set_path_element_map_key(
    element: validate_pb2.FieldPathElement,
    key: typing.Any,
    key_field: descriptor.FieldDescriptor,
    value_field: descriptor.FieldDescriptor,
):
    element.key_type = key_field.type
    element.value_type = value_field.type
    if key_field.type == descriptor.FieldDescriptor.TYPE_BOOL:
        element.bool_key = key
    elif key_field.type in (
        descriptor.FieldDescriptor.TYPE_INT32,
        descriptor.FieldDescriptor.TYPE_SFIXED32,
        descriptor.FieldDescriptor.TYPE_INT64,
        descriptor.FieldDescriptor.TYPE_SFIXED64,
        descriptor.FieldDescriptor.TYPE_SINT32,
        descriptor.FieldDescriptor.TYPE_SINT64,
    ):
        element.int_key = key
    elif key_field.type in (
        descriptor.FieldDescriptor.TYPE_UINT32,
        descriptor.FieldDescriptor.TYPE_FIXED32,
        descriptor.FieldDescriptor.TYPE_UINT64,
        descriptor.FieldDescriptor.TYPE_FIXED64,
    ):
        element.uint_key = key
    elif key_field.type == descriptor.FieldDescriptor.TYPE_STRING:
        element.string_key = key
    else:
        msg = "unexpected map type"
        raise CompilationError(msg)


class Violation:
    """A singular rule violation."""

    proto: validate_pb2.Violation
    field_value: typing.Any
    rule_value: typing.Any

    def __init__(self, *, field_value: typing.Any = None, rule_value: typing.Any = None, **kwargs):
        self.proto = validate_pb2.Violation(**kwargs)
        self.field_value = field_value
        self.rule_value = rule_value


class RuleContext:
    """The state associated with a single rule evaluation."""

    _cfg: Config

    def __init__(self, *, config: Config, violations: typing.Optional[list[Violation]] = None):
        self._cfg = config
        if violations is None:
            violations = []
        self._violations = violations

    @property
    def fail_fast(self) -> bool:
        return self._cfg.fail_fast

    @property
    def violations(self) -> list[Violation]:
        return self._violations

    def add(self, violation: Violation):
        self._violations.append(violation)

    def add_errors(self, other_ctx):
        self._violations.extend(other_ctx.violations)

    def add_field_path_element(self, element: validate_pb2.FieldPathElement):
        for violation in self._violations:
            violation.proto.field.elements.append(element)

    def add_rule_path_elements(self, elements: typing.Iterable[validate_pb2.FieldPathElement]):
        for violation in self._violations:
            violation.proto.rule.elements.extend(elements)

    @property
    def done(self) -> bool:
        return self.fail_fast and self.has_errors()

    def has_errors(self) -> bool:
        return len(self._violations) > 0

    def sub_context(self):
        return RuleContext(config=self._cfg)


class Rules:
    """The rules associated with a single 'rules' message."""

    def validate(self, ctx: RuleContext, _: message.Message):
        """Validate the message against the rules in this rule."""
        ctx.add(Violation(rule_id="unimplemented", message="Unimplemented"))


@dataclasses.dataclass
class CelRunner:
    runner: celpy.Runner
    rule: validate_pb2.Rule
    rule_value: typing.Optional[typing.Any] = None
    rule_cel: typing.Optional[celtypes.Value] = None
    rule_path: typing.Optional[validate_pb2.FieldPath] = None


class CelRules(Rules):
    """A rule that has rules written in CEL."""

    _cel: list[CelRunner]
    _rules: typing.Optional[message.Message] = None
    _rules_cel: typing.Optional[celtypes.Value] = None

    def __init__(self, rules: typing.Optional[message.Message]):
        self._cel = []
        if rules is not None:
            self._rules = rules
            self._rules_cel = _msg_to_cel(rules)

    def _validate_cel(
        self,
        ctx: RuleContext,
        *,
        this_value: typing.Optional[typing.Any] = None,
        this_cel: typing.Optional[celtypes.Value] = None,
        for_key: bool = False,
    ):
        activation: dict[str, celtypes.Value] = {}
        if this_cel is not None:
            activation["this"] = this_cel
        activation["rules"] = self._rules_cel
        activation["now"] = celtypes.TimestampType(datetime.datetime.now(tz=datetime.timezone.utc))
        for cel in self._cel:
            activation["rule"] = cel.rule_cel
            result = cel.runner.evaluate(activation)
            if isinstance(result, celtypes.BoolType):
                if not result:
                    ctx.add(
                        Violation(
                            field_value=this_value,
                            rule=cel.rule_path,
                            rule_value=cel.rule_value,
                            rule_id=cel.rule.id,
                            message=cel.rule.message,
                            for_key=for_key,
                        ),
                    )
            elif isinstance(result, celtypes.StringType):
                if result:
                    ctx.add(
                        Violation(
                            field_value=this_value,
                            rule=cel.rule_path,
                            rule_value=cel.rule_value,
                            rule_id=cel.rule.id,
                            message=result,
                            for_key=for_key,
                        ),
                    )
            elif isinstance(result, Exception):
                raise result

    def add_rule(
        self,
        env: celpy.Environment,
        funcs: dict[str, celpy.CELFunction],
        rules: validate_pb2.Rule,
        *,
        rule_field: typing.Optional[descriptor.FieldDescriptor] = None,
        rule_path: typing.Optional[validate_pb2.FieldPath] = None,
    ):
        ast = env.compile(rules.expression)
        prog = env.program(ast, functions=funcs)
        rule_value = None
        rule_cel = None
        if rule_field is not None and self._rules is not None:
            rule_value = _proto_message_get_field(self._rules, rule_field)
            rule_cel = field_to_cel(self._rules, rule_field)
        self._cel.append(
            CelRunner(
                runner=prog,
                rule=rules,
                rule_value=rule_value,
                rule_cel=rule_cel,
                rule_path=rule_path,
            )
        )


class MessageOneofRule(Rules):
    """Validates a single buf.validate.MessageOneofRule given via the message option (buf.validate.message).oneof"""

    def __init__(self, fields: list[descriptor.FieldDescriptor], *, required: bool):
        self._fields = fields
        self._required = required

    def validate(self, ctx: RuleContext, msg: message.Message):
        num_set_fields = sum(1 for field in self._fields if not _is_empty_field(msg, field))
        if num_set_fields > 1:
            ctx.add(
                Violation(
                    rule_id="message.oneof",
                    message=f"only one of {', '.join([field.name for field in self._fields])} can be set",
                )
            )
        if self._required and num_set_fields == 0:
            ctx.add(
                Violation(
                    rule_id="message.oneof",
                    message=f"one of {', '.join([field.name for field in self._fields])} must be set",
                )
            )


class MessageRules(CelRules):
    """Message-level rules."""

    _oneofs: list[MessageOneofRule]

    def __init__(self, rules: typing.Optional[message.Message], desc: descriptor.Descriptor):
        super().__init__(rules)
        self._oneofs = []
        self._desc = desc

    def validate(self, ctx: RuleContext, message: message.Message):
        self._validate_cel(ctx, this_cel=_msg_to_cel(message))
        if ctx.done:
            return
        for oneof in self._oneofs:
            oneof.validate(ctx, message)
            if ctx.done:
                return

    def add_oneof(
        self,
        rule: validate_pb2.MessageOneofRule,
    ):
        fields = []
        seen = set()
        if len(rule.fields) == 0:
            msg = f"at least one field must be specified in oneof rule for the message {self._desc.full_name}"
            raise CompilationError(msg)

        for name in rule.fields:
            if name in self._desc.fields_by_name:
                if name in seen:
                    msg = f"duplicate {name} in oneof rule for the message {self._desc.full_name}"
                    raise CompilationError(msg)
                fields.append(self._desc.fields_by_name[name])
                seen.add(name)
            else:
                msg = f'field "{name}" not found in message {self._desc.full_name}'
                raise CompilationError(msg)
        self._oneofs.append(MessageOneofRule(fields, required=rule.required))


def check_field_type(field: descriptor.FieldDescriptor, expected: int, wrapper_name: typing.Optional[str] = None):
    if field.type != expected and (
        field.type != descriptor.FieldDescriptor.TYPE_MESSAGE or field.message_type.full_name != wrapper_name
    ):
        field_type_str = _get_type_name(field.type)
        if expected == 0:
            if wrapper_name is not None:
                expected_type_str = wrapper_name
            else:
                expected_type_str = _get_type_name(descriptor.FieldDescriptor.TYPE_MESSAGE)
        else:
            expected_type_str = _get_type_name(expected)
        msg = f"field {field.name} has type {field_type_str} but expected {expected_type_str}"
        raise CompilationError(msg)


def _is_map(field: descriptor.FieldDescriptor):
    return (
        field.label == descriptor.FieldDescriptor.LABEL_REPEATED
        and field.message_type is not None
        and field.message_type.GetOptions().map_entry
    )


def _is_list(field: descriptor.FieldDescriptor):
    return field.label == descriptor.FieldDescriptor.LABEL_REPEATED and not _is_map(field)


def _zero_value(field: descriptor.FieldDescriptor):
    if field.message_type is not None and field.label != descriptor.FieldDescriptor.LABEL_REPEATED:
        return _field_value_to_cel(message_factory.GetMessageClass(field.message_type)(), field)
    else:
        return _field_value_to_cel(field.default_value, field)


class FieldRules(CelRules):
    """Field-level rules."""

    _ignore_empty = False
    _ignore_default = False
    _required = False
    _zero = None

    _required_rule_path: typing.ClassVar[validate_pb2.FieldPath] = validate_pb2.FieldPath(
        elements=[
            _field_to_element(
                validate_pb2.FieldRules.DESCRIPTOR.fields_by_number[validate_pb2.FieldRules.REQUIRED_FIELD_NUMBER]
            )
        ]
    )

    _cel_rule_path: typing.ClassVar[validate_pb2.FieldPath] = validate_pb2.FieldPath(
        elements=[
            _field_to_element(
                validate_pb2.FieldRules.DESCRIPTOR.fields_by_number[validate_pb2.FieldRules.CEL_FIELD_NUMBER]
            )
        ]
    )

    def __init__(
        self,
        env: celpy.Environment,
        funcs: dict[str, celpy.CELFunction],
        field: descriptor.FieldDescriptor,
        field_level: validate_pb2.FieldRules,
        *,
        for_items: bool = False,
    ):
        type_case = field_level.WhichOneof("type")
        super().__init__(None if type_case is None else getattr(field_level, type_case))
        self._field = field
        self._ignore_empty = field_level.ignore in (
            validate_pb2.IGNORE_IF_UNPOPULATED,
            validate_pb2.IGNORE_IF_DEFAULT_VALUE,
        ) or (
            field.has_presence  # type: ignore[attr-defined]
            and not for_items
        )
        self._ignore_default = field.has_presence and field_level.ignore == validate_pb2.IGNORE_IF_DEFAULT_VALUE  # type: ignore[attr-defined]
        self._required = field_level.required
        if self._ignore_default:
            self._zero = _zero_value(field)
        type_case = field_level.WhichOneof("type")
        if type_case is not None:
            rules: message.Message = getattr(field_level, type_case)
            # For each set field in the message, look for the private rule
            # extension.
            for list_field, _ in rules.ListFields():
                if validate_pb2.predefined in list_field.GetOptions().Extensions:
                    for cel in list_field.GetOptions().Extensions[validate_pb2.predefined].cel:
                        self.add_rule(
                            env,
                            funcs,
                            cel,
                            rule_field=list_field,
                            rule_path=validate_pb2.FieldPath(
                                elements=[
                                    _field_to_element(list_field),
                                    _field_to_element(
                                        field_level.DESCRIPTOR.fields_by_name[type_case],
                                    ),
                                ]
                            ),
                        )
        for i, cel in enumerate(field_level.cel):
            rule_path = validate_pb2.FieldPath()
            rule_path.CopyFrom(self._cel_rule_path)
            rule_path.elements[0].index = i
            self.add_rule(env, funcs, cel, rule_path=rule_path)

    def validate(self, ctx: RuleContext, message: message.Message):
        if _is_empty_field(message, self._field):
            if self._required:
                ctx.add(
                    Violation(
                        field=validate_pb2.FieldPath(
                            elements=[
                                _field_to_element(self._field),
                            ],
                        ),
                        rule=FieldRules._required_rule_path,
                        rule_value=self._required,
                        rule_id="required",
                        message="value is required",
                    ),
                )
                return
            if self._ignore_empty:
                return
        val = getattr(message, self._field.name)
        cel_val = _field_value_to_cel(val, self._field)
        if self._ignore_default and cel_val == self._zero:
            return
        sub_ctx = ctx.sub_context()
        self._validate_value(sub_ctx, val)
        self._validate_cel(sub_ctx, this_value=_proto_message_get_field(message, self._field), this_cel=cel_val)
        if sub_ctx.has_errors():
            element = _field_to_element(self._field)
            sub_ctx.add_field_path_element(element)
            ctx.add_errors(sub_ctx)

    def validate_item(self, ctx: RuleContext, val: typing.Any, *, for_key: bool = False):
        self._validate_value(ctx, val, for_key=for_key)
        self._validate_cel(ctx, this_value=val, this_cel=_scalar_field_value_to_cel(val, self._field), for_key=for_key)

    def _validate_value(self, ctx: RuleContext, val: typing.Any, *, for_key: bool = False):
        pass


class AnyRules(FieldRules):
    """Rules for an Any field."""

    _in_rule_path: typing.ClassVar[validate_pb2.FieldPath] = validate_pb2.FieldPath(
        elements=[
            _field_to_element(validate_pb2.AnyRules.DESCRIPTOR.fields_by_number[validate_pb2.AnyRules.IN_FIELD_NUMBER]),
            _field_to_element(
                validate_pb2.FieldRules.DESCRIPTOR.fields_by_number[validate_pb2.FieldRules.ANY_FIELD_NUMBER]
            ),
        ],
    )

    _not_in_rule_path: typing.ClassVar[validate_pb2.FieldPath] = validate_pb2.FieldPath(
        elements=[
            _field_to_element(
                validate_pb2.AnyRules.DESCRIPTOR.fields_by_number[validate_pb2.AnyRules.NOT_IN_FIELD_NUMBER]
            ),
            _field_to_element(
                validate_pb2.FieldRules.DESCRIPTOR.fields_by_number[validate_pb2.FieldRules.ANY_FIELD_NUMBER]
            ),
        ],
    )

    def __init__(
        self,
        env: celpy.Environment,
        funcs: dict[str, celpy.CELFunction],
        field: descriptor.FieldDescriptor,
        field_level: validate_pb2.FieldRules,
    ):
        super().__init__(env, funcs, field, field_level)
        self._in = []
        if getattr(field_level.any, "in"):
            self._in = getattr(field_level.any, "in")
        self._not_in = []
        if field_level.any.not_in:
            self._not_in = field_level.any.not_in

    def _validate_value(self, ctx: RuleContext, value: any_pb2.Any, *, for_key: bool = False):
        if len(self._in) > 0:
            if value.type_url not in self._in:
                ctx.add(
                    Violation(
                        rule=AnyRules._in_rule_path,
                        rule_value=self._in,
                        rule_id="any.in",
                        message="type URL must be in the allow list",
                        for_key=for_key,
                    )
                )
        if value.type_url in self._not_in:
            ctx.add(
                Violation(
                    rule=AnyRules._not_in_rule_path,
                    rule_value=self._not_in,
                    rule_id="any.not_in",
                    message="type URL must not be in the block list",
                    for_key=for_key,
                )
            )


class EnumRules(FieldRules):
    """Rules for an enum field."""

    _defined_only = False

    _defined_only_rule_path: typing.ClassVar[validate_pb2.FieldPath] = validate_pb2.FieldPath(
        elements=[
            _field_to_element(
                validate_pb2.EnumRules.DESCRIPTOR.fields_by_number[validate_pb2.EnumRules.DEFINED_ONLY_FIELD_NUMBER]
            ),
            _field_to_element(
                validate_pb2.FieldRules.DESCRIPTOR.fields_by_number[validate_pb2.FieldRules.ENUM_FIELD_NUMBER]
            ),
        ],
    )

    def __init__(
        self,
        env: celpy.Environment,
        funcs: dict[str, celpy.CELFunction],
        field: descriptor.FieldDescriptor,
        field_level: validate_pb2.FieldRules,
        *,
        for_items: bool = False,
    ):
        super().__init__(env, funcs, field, field_level, for_items=for_items)
        if field_level.enum.defined_only:
            self._defined_only = True

    def validate(self, ctx: RuleContext, message: message.Message):
        super().validate(ctx, message)
        if ctx.done:
            return
        if self._defined_only:
            value = getattr(message, self._field.name)
            if value not in self._field.enum_type.values_by_number:
                ctx.add(
                    Violation(
                        field=validate_pb2.FieldPath(
                            elements=[
                                _field_to_element(self._field),
                            ],
                        ),
                        rule=EnumRules._defined_only_rule_path,
                        rule_value=self._defined_only,
                        rule_id="enum.defined_only",
                        message="value must be one of the defined enum values",
                    ),
                )


class RepeatedRules(FieldRules):
    """Rules for a repeated field."""

    _item_rules: typing.Optional[FieldRules] = None

    _items_rules_suffix: typing.ClassVar[list[validate_pb2.FieldPathElement]] = [
        _field_to_element(
            validate_pb2.RepeatedRules.DESCRIPTOR.fields_by_number[validate_pb2.RepeatedRules.ITEMS_FIELD_NUMBER]
        ),
        _field_to_element(
            validate_pb2.FieldRules.DESCRIPTOR.fields_by_number[validate_pb2.FieldRules.REPEATED_FIELD_NUMBER]
        ),
    ]

    def __init__(
        self,
        env: celpy.Environment,
        funcs: dict[str, celpy.CELFunction],
        field: descriptor.FieldDescriptor,
        field_level: validate_pb2.FieldRules,
        item_rules: typing.Optional[FieldRules],
    ):
        super().__init__(env, funcs, field, field_level)
        if item_rules is not None:
            self._item_rules = item_rules

    def validate(self, ctx: RuleContext, message: message.Message):
        super().validate(ctx, message)
        if ctx.done:
            return
        value = getattr(message, self._field.name)
        if self._item_rules is not None:
            for i, item in enumerate(value):
                if self._item_rules._ignore_empty and not item:
                    continue
                sub_ctx = ctx.sub_context()
                self._item_rules.validate_item(sub_ctx, item)
                if sub_ctx.has_errors():
                    element = _field_to_element(self._field)
                    element.index = i
                    sub_ctx.add_field_path_element(element)
                    sub_ctx.add_rule_path_elements(RepeatedRules._items_rules_suffix)
                    ctx.add_errors(sub_ctx)
                if ctx.done:
                    return


class MapRules(FieldRules):
    """Rules for a map field."""

    _key_rules: typing.Optional[FieldRules] = None
    _value_rules: typing.Optional[FieldRules] = None

    _key_rules_suffix: typing.ClassVar[list[validate_pb2.FieldPathElement]] = [
        _field_to_element(validate_pb2.MapRules.DESCRIPTOR.fields_by_number[validate_pb2.MapRules.KEYS_FIELD_NUMBER]),
        _field_to_element(
            validate_pb2.FieldRules.DESCRIPTOR.fields_by_number[validate_pb2.FieldRules.MAP_FIELD_NUMBER]
        ),
    ]

    _value_rules_suffix: typing.ClassVar[list[validate_pb2.FieldPathElement]] = [
        _field_to_element(validate_pb2.MapRules.DESCRIPTOR.fields_by_number[validate_pb2.MapRules.VALUES_FIELD_NUMBER]),
        _field_to_element(
            validate_pb2.FieldRules.DESCRIPTOR.fields_by_number[validate_pb2.FieldRules.MAP_FIELD_NUMBER]
        ),
    ]

    def __init__(
        self,
        env: celpy.Environment,
        funcs: dict[str, celpy.CELFunction],
        field: descriptor.FieldDescriptor,
        field_level: validate_pb2.FieldRules,
        key_rules: typing.Optional[FieldRules],
        value_rules: typing.Optional[FieldRules],
    ):
        super().__init__(env, funcs, field, field_level)
        if key_rules is not None:
            self._key_rules = key_rules
        if value_rules is not None:
            self._value_rules = value_rules

    def validate(self, ctx: RuleContext, message: message.Message):
        super().validate(ctx, message)
        if ctx.done:
            return
        value = getattr(message, self._field.name)
        for k, v in value.items():
            key_ctx = ctx.sub_context()
            if self._key_rules is not None:
                if not self._key_rules._ignore_empty or k:
                    self._key_rules.validate_item(key_ctx, k, for_key=True)
                    if key_ctx.has_errors():
                        key_ctx.add_rule_path_elements(MapRules._key_rules_suffix)
            map_ctx = ctx.sub_context()
            if self._value_rules is not None:
                if not self._value_rules._ignore_empty or v:
                    self._value_rules.validate_item(map_ctx, v)
                    if map_ctx.has_errors():
                        map_ctx.add_rule_path_elements(MapRules._value_rules_suffix)
            map_ctx.add_errors(key_ctx)
            if map_ctx.has_errors():
                element = _field_to_element(self._field)
                key_field = self._field.message_type.fields_by_name["key"]
                value_field = self._field.message_type.fields_by_name["value"]
                _set_path_element_map_key(element, k, key_field, value_field)
                map_ctx.add_field_path_element(element)
                ctx.add_errors(map_ctx)


class OneofRules(Rules):
    """Rules for a oneof definition."""

    required = True

    def __init__(self, oneof: descriptor.OneofDescriptor, rules: validate_pb2.OneofRules):
        self._oneof = oneof
        if not rules.required:
            self.required = False

    def validate(self, ctx: RuleContext, message: message.Message):
        if not message.WhichOneof(self._oneof.name):
            if self.required:
                ctx.add(
                    Violation(
                        field=validate_pb2.FieldPath(
                            elements=[_oneof_to_element(self._oneof)],
                        ),
                        rule_id="required",
                        message="exactly one field is required in oneof",
                    )
                )
            return


class RuleFactory:
    """Factory for creating and caching rules."""

    _env: celpy.Environment
    _funcs: dict[str, celpy.CELFunction]
    _cache: dict[descriptor.Descriptor, typing.Union[list[Rules], Exception]]

    def __init__(self, funcs: dict[str, celpy.CELFunction]):
        self._env = celpy.Environment(runner_class=InterpretedRunner)
        self._funcs = funcs
        self._cache = {}

    def get(self, descriptor: descriptor.Descriptor) -> list[Rules]:
        if descriptor not in self._cache:
            try:
                self._cache[descriptor] = self._new_rules(descriptor)
            except Exception as e:
                self._cache[descriptor] = e
        result = self._cache[descriptor]
        if isinstance(result, Exception):
            raise result
        return result

    def _new_message_rule(self, rules: validate_pb2.MessageRules, desc: descriptor.Descriptor) -> MessageRules:
        result = MessageRules(rules, desc)
        for oneof in rules.oneof:
            result.add_oneof(oneof)
        for cel in rules.cel:
            result.add_rule(self._env, self._funcs, cel)
        return result

    def _new_scalar_field_rule(
        self,
        field: descriptor.FieldDescriptor,
        field_level: validate_pb2.FieldRules,
        *,
        for_items: bool = False,
    ):
        if field_level.ignore == validate_pb2.IGNORE_ALWAYS:
            return None
        type_case = field_level.WhichOneof("type")
        if type_case is None:
            result = FieldRules(self._env, self._funcs, field, field_level, for_items=for_items)
            return result
        elif type_case == "duration":
            check_field_type(field, 0, "google.protobuf.Duration")
            result = FieldRules(self._env, self._funcs, field, field_level, for_items=for_items)
            return result
        elif type_case == "timestamp":
            check_field_type(field, 0, "google.protobuf.Timestamp")
            result = FieldRules(self._env, self._funcs, field, field_level, for_items=for_items)
            return result
        elif type_case == "enum":
            check_field_type(field, descriptor.FieldDescriptor.TYPE_ENUM)
            result = EnumRules(self._env, self._funcs, field, field_level, for_items=for_items)
            return result
        elif type_case == "bool":
            check_field_type(field, descriptor.FieldDescriptor.TYPE_BOOL, "google.protobuf.BoolValue")
            result = FieldRules(self._env, self._funcs, field, field_level, for_items=for_items)
            return result
        elif type_case == "bytes":
            check_field_type(
                field,
                descriptor.FieldDescriptor.TYPE_BYTES,
                "google.protobuf.BytesValue",
            )
            result = FieldRules(self._env, self._funcs, field, field_level, for_items=for_items)
            return result
        elif type_case == "fixed32":
            check_field_type(field, descriptor.FieldDescriptor.TYPE_FIXED32)
            result = FieldRules(self._env, self._funcs, field, field_level, for_items=for_items)
            return result
        elif type_case == "fixed64":
            check_field_type(field, descriptor.FieldDescriptor.TYPE_FIXED64)
            result = FieldRules(self._env, self._funcs, field, field_level, for_items=for_items)
            return result
        elif type_case == "float":
            check_field_type(
                field,
                descriptor.FieldDescriptor.TYPE_FLOAT,
                "google.protobuf.FloatValue",
            )
            result = FieldRules(self._env, self._funcs, field, field_level, for_items=for_items)
            return result
        elif type_case == "double":
            check_field_type(
                field,
                descriptor.FieldDescriptor.TYPE_DOUBLE,
                "google.protobuf.DoubleValue",
            )
            result = FieldRules(self._env, self._funcs, field, field_level, for_items=for_items)
            return result
        elif type_case == "int32":
            check_field_type(
                field,
                descriptor.FieldDescriptor.TYPE_INT32,
                "google.protobuf.Int32Value",
            )
            result = FieldRules(self._env, self._funcs, field, field_level, for_items=for_items)
            return result
        elif type_case == "int64":
            check_field_type(
                field,
                descriptor.FieldDescriptor.TYPE_INT64,
                "google.protobuf.Int64Value",
            )
            result = FieldRules(self._env, self._funcs, field, field_level, for_items=for_items)
            return result
        elif type_case == "sfixed32":
            check_field_type(field, descriptor.FieldDescriptor.TYPE_SFIXED32)
            result = FieldRules(self._env, self._funcs, field, field_level, for_items=for_items)
            return result
        elif type_case == "sfixed64":
            check_field_type(field, descriptor.FieldDescriptor.TYPE_SFIXED64)
            result = FieldRules(self._env, self._funcs, field, field_level, for_items=for_items)
            return result
        elif type_case == "sint32":
            check_field_type(field, descriptor.FieldDescriptor.TYPE_SINT32)
            result = FieldRules(self._env, self._funcs, field, field_level, for_items=for_items)
            return result
        elif type_case == "sint64":
            check_field_type(field, descriptor.FieldDescriptor.TYPE_SINT64)
            result = FieldRules(self._env, self._funcs, field, field_level, for_items=for_items)
            return result
        elif type_case == "uint32":
            check_field_type(
                field,
                descriptor.FieldDescriptor.TYPE_UINT32,
                "google.protobuf.UInt32Value",
            )
            result = FieldRules(self._env, self._funcs, field, field_level, for_items=for_items)
            return result
        elif type_case == "uint64":
            check_field_type(
                field,
                descriptor.FieldDescriptor.TYPE_UINT64,
                "google.protobuf.UInt64Value",
            )
            result = FieldRules(self._env, self._funcs, field, field_level, for_items=for_items)
            return result
        elif type_case == "string":
            check_field_type(
                field,
                descriptor.FieldDescriptor.TYPE_STRING,
                "google.protobuf.StringValue",
            )
            result = FieldRules(self._env, self._funcs, field, field_level, for_items=for_items)
            return result
        elif type_case == "any":
            check_field_type(field, 0, "google.protobuf.Any")
            result = AnyRules(self._env, self._funcs, field, field_level)
            return result

    def _new_field_rule(
        self,
        field: descriptor.FieldDescriptor,
        rules: validate_pb2.FieldRules,
    ) -> FieldRules:
        if field.label != descriptor.FieldDescriptor.LABEL_REPEATED:
            return self._new_scalar_field_rule(field, rules)
        if field.message_type is not None and field.message_type.GetOptions().map_entry:
            key_rules = None
            if rules.map.HasField("keys"):
                key_field = field.message_type.fields_by_name["key"]
                key_rules = self._new_scalar_field_rule(key_field, rules.map.keys, for_items=True)
            value_rules = None
            if rules.map.HasField("values"):
                value_field = field.message_type.fields_by_name["value"]
                value_rules = self._new_scalar_field_rule(value_field, rules.map.values, for_items=True)
            return MapRules(self._env, self._funcs, field, rules, key_rules, value_rules)
        item_rule = None
        if rules.repeated.HasField("items"):
            item_rule = self._new_scalar_field_rule(field, rules.repeated.items)
        return RepeatedRules(self._env, self._funcs, field, rules, item_rule)

    def _new_rules(self, desc: descriptor.Descriptor) -> list[Rules]:
        result: list[Rules] = []
        rule: typing.Optional[Rules] = None
        all_msg_oneof_fields = set()
        if validate_pb2.message in desc.GetOptions().Extensions:
            message_level = desc.GetOptions().Extensions[validate_pb2.message]
            if message_level.disabled:
                return []
            for oneof in message_level.oneof:
                all_msg_oneof_fields.update(oneof.fields)
            if rule := self._new_message_rule(message_level, desc):
                result.append(rule)

        for oneof in desc.oneofs:
            if validate_pb2.oneof in oneof.GetOptions().Extensions:
                if rule := OneofRules(oneof, oneof.GetOptions().Extensions[validate_pb2.oneof]):
                    result.append(rule)

        for field in desc.fields:
            if validate_pb2.field in field.GetOptions().Extensions:
                field_level = field.GetOptions().Extensions[validate_pb2.field]
                if not field_level.HasField("ignore") and field.name in all_msg_oneof_fields:
                    field_level_override = validate_pb2.FieldRules()
                    field_level_override.CopyFrom(field_level)
                    field_level_override.ignore = validate_pb2.IGNORE_IF_UNPOPULATED
                    field_level = field_level_override
                if field_level.ignore == validate_pb2.IGNORE_ALWAYS:
                    continue
                result.append(self._new_field_rule(field, field_level))
                if field_level.repeated.items.ignore == validate_pb2.IGNORE_ALWAYS:
                    continue
            if field.message_type is None:
                continue
            if field.message_type.GetOptions().map_entry:
                key_field = field.message_type.fields_by_name["key"]
                value_field = field.message_type.fields_by_name["value"]
                if value_field.type != descriptor.FieldDescriptor.TYPE_MESSAGE:
                    continue
                result.append(MapValMsgRule(self, field, key_field, value_field))
            elif field.label == descriptor.FieldDescriptor.LABEL_REPEATED:
                result.append(RepeatedMsgRule(self, field))
            else:
                result.append(SubMsgRule(self, field))
        return result


class SubMsgRule(Rules):
    def __init__(
        self,
        factory: RuleFactory,
        field: descriptor.FieldDescriptor,
    ):
        self._factory = factory
        self._field = field

    def validate(self, ctx: RuleContext, message: message.Message):
        if not message.HasField(self._field.name):
            return
        rules = self._factory.get(self._field.message_type)
        if rules is None:
            return
        val = getattr(message, self._field.name)
        sub_ctx = ctx.sub_context()
        for rule in rules:
            rule.validate(sub_ctx, val)
        if sub_ctx.has_errors():
            element = _field_to_element(self._field)
            sub_ctx.add_field_path_element(element)
            ctx.add_errors(sub_ctx)


class MapValMsgRule(Rules):
    def __init__(
        self,
        factory: RuleFactory,
        field: descriptor.FieldDescriptor,
        key_field: descriptor.FieldDescriptor,
        value_field: descriptor.FieldDescriptor,
    ):
        self._factory = factory
        self._field = field
        self._key_field = key_field
        self._value_field = value_field

    def validate(self, ctx: RuleContext, message: message.Message):
        val = getattr(message, self._field.name)
        if not val:
            return
        rules = self._factory.get(self._value_field.message_type)
        if rules is None:
            return
        for k, v in val.items():
            sub_ctx = ctx.sub_context()
            for rule in rules:
                rule.validate(sub_ctx, v)
            if sub_ctx.has_errors():
                element = _field_to_element(self._field)
                _set_path_element_map_key(element, k, self._key_field, self._value_field)
                sub_ctx.add_field_path_element(element)
                ctx.add_errors(sub_ctx)


class RepeatedMsgRule(Rules):
    def __init__(
        self,
        factory: RuleFactory,
        field: descriptor.FieldDescriptor,
    ):
        self._factory = factory
        self._field = field

    def validate(self, ctx: RuleContext, message: message.Message):
        val = getattr(message, self._field.name)
        if not val:
            return
        rules = self._factory.get(self._field.message_type)
        if rules is None:
            return
        for idx, item in enumerate(val):
            sub_ctx = ctx.sub_context()
            for rule in rules:
                rule.validate(sub_ctx, item)
            if sub_ctx.has_errors():
                element = _field_to_element(self._field)
                element.index = idx
                sub_ctx.add_field_path_element(element)
                ctx.add_errors(sub_ctx)
