import builtins
from typing import Union, Optional, List, Any

import astroid
from pylint.checkers import BaseChecker, utils
from pylint.exceptions import UnknownMessageError

from .transform import transform_module, is_some_protobuf_module, to_pytype, is_composite, is_repeated, is_oneof
from .transform import SimpleDescriptor, PROTOBUF_IMPLICIT_ATTRS, PROTOBUF_ENUM_IMPLICIT_ATTRS

try:
    from pylint.interfaces import IAstroidChecker
except ImportError:
    # compat shim for astroid 2.x
    # TODO: remove in pylint-protobuf 1.x after a period of deprecation
    class IAstroidChecker:
        pass

try:
    from astroid import Index as IndexNode
except ImportError:
    # Compatibility shim for astroid >= 2.6
    class IndexNode(object):
        pass

try:
    from pylint.checkers.utils import (
        only_required_for_messages as check_messages
    )
except (ImportError, ModuleNotFoundError):
    from pylint.checkers.utils import check_messages


_MISSING_IMPORT_IS_ERROR = False
BASE_ID = 59
MESSAGES = {
    'E%02d01' % BASE_ID: (
        'Field %r does not appear in the declared fields of protobuf-'
        'generated class %r',
        'protobuf-undefined-attribute',
        'Used when an undefined field of a protobuf generated class is '
        'accessed, assigned, or passed to HasField/ClearField'
    ),
    'E%02d02' % BASE_ID: (
        'Value %r does not appear in the declared values of protobuf-'
        'generated enum %r and will raise ValueError at runtime',
        'protobuf-enum-value',
        'Used when an undefined enum value of a protobuf generated enum '
        'is built'
    ),
    'E%02d03' % BASE_ID: (
        'Field "%s.%s" is of type %r and value %r will raise TypeError '
        'at runtime',
        'protobuf-type-error',
        'Used when a scalar field is written to by a bad value'
    ),
    'E%02d04' % BASE_ID: (
        'Positional arguments are not allowed in message constructors and '
        'will raise TypeError',
        'protobuf-no-posargs',
        'Used when a message class is initialised with positional '
        'arguments instead of keyword arguments'
    ),
    'E%02d05' % BASE_ID: (
        'Field "%s.%s" does not support assignment',
        'protobuf-no-assignment',
        'Used when a value is written to a read-only field such as a '
        'composite or repeated field'
    ),
    'E%02d06' % BASE_ID: (
        'Repeated fields cannot be checked for membership',
        'protobuf-no-repeated-membership',
        'Used when calling HasField/ClearField on a repeated field'
    ),
    'E%02d07' % BASE_ID: (
        'Non-optional, non-submessage field %r cannot be checked for '
        'membership in proto3 messages',
        'protobuf-no-proto3-membership',
        'Used when calling HasField/ClearField on a non-composite, '
        'non-oneof field in a proto3 syntax file'
    ),
    'E%02d08' % BASE_ID: (
        'Extension field %r does not belong to message %r',
        'protobuf-wrong-extension-scope',
        'Used when attempting to set an extension on a message when '
        'it is not the containing type for that field'
    ),
}
Node = astroid.node_classes.NodeNG


def _get_inferred_values(node):
    # type: (Node) -> List[Node]
    try:
        vals = list(node.infer())  # not the same as node.inferred() for astroid.Instance
    except astroid.InferenceError:
        return []
    return [v for v in vals if v is not astroid.Uninferable]


def _get_protobuf_descriptor(node):
    # type: (Node) -> Optional[SimpleDescriptor]
    # Look for any version of the inferred type to be a Protobuf class
    for val in _get_inferred_values(node):
        cls_def = None
        if hasattr(val, '_proxied'):
            # if wellknowntype(val):  # where to put this side-effect?
            #     self._disable('no-member', node.lineno)
            #     return
            cls_def = val._proxied  # type: astroid.ClassDef
        # elif isinstance(val, astroid.Module):
        #     return self._check_module(val, node)  # FIXME: move
        elif isinstance(val, astroid.ClassDef):
            cls_def = val
        if cls_def and getattr(cls_def, '_is_protobuf_class', False):
            break  # getattr guards against Uninferable (always returns self)
    else:
        return # couldn't find cls_def
    return cls_def._protobuf_descriptor  # type: SimpleDescriptor


def _scalar_typecheck(val, val_type):
    # type: (Union[type, Any], type) -> bool
    # NOTE: transform.to_pytype returns {bool, int, float, str, <other>}
    if type(val) is type:
        check = issubclass
    else:
        check = isinstance
    if val_type is float:
        return check(val, (float, int, bool))
    elif val_type is int:
        return check(val, (int, bool))
    elif val_type is bool:
        return check(val, (float, int, bool))
    elif val_type is str:
        return check(val, (str, bytes))
    elif val_type is bytes:
        return check(val, bytes)
    else:
        return True  # Are there any other scalar protobuf types?


def _resolve_builtin(inst):
    # type: (astroid.Instance) -> Optional[type]
    typename = inst.pytype()
    if not typename.startswith('builtins.'):
        return None
    typename = typename[len('builtins.'):]
    return getattr(builtins, typename, None)  # eh...


class ProtobufDescriptorChecker(BaseChecker):
    __implements__ = IAstroidChecker
    msgs = MESSAGES
    name = 'protobuf-descriptor-checker'
    priority = 0  # need to be higher than builtin typecheck lint

    def __init__(self, linter):
        self.linter = linter

    def visit_import(self, node):
        # type: (astroid.Import) -> None
        for modname, _ in node.names:
            self._check_import(node, modname)

    def visit_importfrom(self, node):
        # type: (astroid.ImportFrom) -> None
        self._check_import(node, node.modname)

    def _check_import(self, node, modname):
        # type: (Union[astroid.Import, astroid.ImportFrom], str) -> None
        if not _MISSING_IMPORT_IS_ERROR or not modname.endswith('_pb2'):
            return  # only relevant when testing
        try:
            node.do_import_module(modname)
        except astroid.AstroidBuildingError:
            assert not _MISSING_IMPORT_IS_ERROR, 'expected to import module "{}"'.format(modname)

    def visit_call(self, node):
        self._check_enum_values(node)
        self._check_init_posargs(node)
        self._check_init_kwargs(node)
        self._check_repeated_scalar(node)
        self._check_repeated_composite(node)
        self._check_hasfield(node)

    @check_messages('protobuf-enum-value')
    def _check_enum_values(self, node):
        if len(node.args) != 1:
            return  # protobuf enum .Value() is only called with one argument
        value_node = node.args[0]
        func = node.func
        if not isinstance(func, astroid.Attribute):
            # NOTE: inference fails on this case
            #   f = Enum.Value  # <-
            #   f('some_value')
            return
        if func.attrname not in ('Value', 'Name'):
            return
        desc = _get_protobuf_descriptor(func.expr)
        if desc is None or not desc.is_enum:
            return
        expected = desc.values if func.attrname == 'Value' else desc.names
        for val_const in _get_inferred_values(value_node):
            if not hasattr(val_const, 'value'):
                continue
            val = val_const.value
            if val not in expected:
                self.add_message('protobuf-enum-value', args=(val, desc.name), node=node)
                break  # should we continue to check?

    def _check_init_posargs(self, node):
        # type: (astroid.Call) -> None
        desc = _get_protobuf_descriptor(node.func)
        self._check_posargs(desc, node)

    def _check_init_kwargs(self, node):
        # type: (astroid.Call) -> None
        desc = _get_protobuf_descriptor(node.func)
        self._check_kwargs(desc, node)

    def _check_repeated_composite(self, node):
        # type: (astroid.Call) -> None
        func = node.func
        if not isinstance(func, astroid.Attribute) or func.attrname != 'add':
            return # Call should look like "message.inner.add(**kwargs)"
        desc = None  # type: Optional[SimpleDescriptor]
        for val in _get_inferred_values(node):
            desc = _get_protobuf_descriptor(val)
            if desc is not None:
                break  # I don't think there's a reasonable solution for ambiguous cases
        self._check_posargs(desc, node)
        self._check_kwargs(desc, node)

    @check_messages('protobuf-no-posargs')
    def _check_posargs(self, desc, node):
        # type: (Optional[SimpleDescriptor], astroid.Call) -> None
        if desc is not None and len(node.args) > 0:
            self.add_message('protobuf-no-posargs', node=node)

    @check_messages('protobuf-type-error', 'unexpected-keyword-arg')
    def _check_kwargs(self, desc, node):
        # type: (Optional[SimpleDescriptor], astroid.Call) -> None
        if desc is None:
            return
        desc_name = desc.name
        keywords = node.keywords or []
        for kw in keywords:
            arg_name, val_node = kw.arg, kw.value
            if arg_name is None:
                # If arg_name is None then we are dealing with **kwargs, in which case we don't
                # really care what arguments are being passed, just ignore them to be safe.
                continue
            elif arg_name not in desc.fields_by_name:
                # This is probably too fragile (should it be replaced by a separate message?)
                # Also, it's probably not fair to always refer to this as a constructor call, though arguably
                # .add() on repeated composite fields is "constructing" the sub-message
                self.add_message('unexpected-keyword-arg', node=node, args=(arg_name, 'constructor'))
                continue
            fd = desc.fields_by_name[arg_name]
            arg_type = to_pytype(fd)
            if isinstance(val_node, astroid.Call):
                val_node = _get_inferred_values(val_node)
                if len(val_node) != 1:
                    continue  # don't reason about ambiguous cases or uninferable
                val_node = val_node[0]
            if is_repeated(fd):
                # What to do about iterators? At a certain point, inference becomes too complicated
                elements = getattr(val_node, 'elts', [])
                for element in elements:
                    if is_composite(fd):
                        self._check_composite_keyword(arg_name, arg_type, fd, node, element, desc_name)
                    else:
                        self._check_noncomposite_keyword(arg_name, arg_type, node, element, desc_name)
            else:
                if is_composite(fd):
                    self._check_composite_keyword(arg_name, arg_type, fd, node, val_node, desc_name)
                else:
                    self._check_noncomposite_keyword(arg_name, arg_type, node, val_node, desc_name)

    def _check_composite_keyword(self, arg_name, arg_type, fd, node, val_node, desc_name):
        for val in _get_inferred_values(val_node):
            if isinstance(val, astroid.Const):
                if val.value is not None:
                    # Special-case None as default of keyword args
                    self.add_message('protobuf-type-error', node=node,
                                     args=(desc_name, arg_name, arg_type.__name__, val.value))
                break
            val_desc = _get_protobuf_descriptor(val)
            if val_desc is None:
                continue  # XXX: ignore?
            if not val_desc.is_typeof_field(fd):
                val = '{}()'.format(val_desc.name)
                self.add_message('protobuf-type-error', node=node,
                                 args=(desc_name, arg_name, arg_type.__name__, val))

    def _check_noncomposite_keyword(self, arg_name, arg_type, node, val_node, desc_name):
        for val_const in _get_inferred_values(val_node):
            if hasattr(val_const, 'value'):
                val = val_const.value
            elif hasattr(val_const, '_proxied'):
                # check for <Instance of builtins.int> etc.
                val = _resolve_builtin(val_const)
                if val is None:  # check for composite assigned to scalar
                    try:
                        val = '{}()'.format(val_const.name)
                    except AttributeError:
                        continue
                    self.add_message('protobuf-type-error', node=node,
                                     args=(desc_name, arg_name, arg_type.__name__, val))
                    break
            else:
                try:
                    val = '{}()'.format(val_const.name)
                except AttributeError:
                    continue
                self.add_message('protobuf-type-error', node=node,
                                 args=(desc_name, arg_name, arg_type.__name__, val))
                break
            if not _scalar_typecheck(val, arg_type):
                if val is not None:
                    # Special-case None as default of keyword args
                    self.add_message('protobuf-type-error', node=node,
                                     args=(desc_name, arg_name, arg_type.__name__, val))
                break

    @check_messages('protobuf-type-error')
    def _check_repeated_scalar(self, node):
        # type: (astroid.Call) -> None
        if len(node.args) != 1:
            return  # append and extend take one argument
        arg_node = node.args[0]
        func = node.func
        if not isinstance(func, astroid.Attribute):
            # NOTE: inference fails on this case
            #   f = Enum.Value  # <-
            #   f('some_value')
            return  # FIXME: check
        if func.attrname not in ('append', 'extend'):
            return

        arg_infer = _get_inferred_values(arg_node)
        if len(arg_infer) != 1:
            return  # no point warning on ambiguous types
        arg = arg_infer[0]

        if func.attrname == 'append':
            if not hasattr(arg, 'value'):
                return
            vals = [arg.value]
        else:  # 'extend'
            if not hasattr(arg, 'elts'):
                return  # FIXME: check how to deal with arbitrary iterables
            vals = []
            for elem in arg.elts:
                c = _get_inferred_values(elem)
                if not c:
                    continue
                c = c[0]
                if not hasattr(c, 'value'):
                    continue
                vals.append(c.value)

        expr = func.expr
        try:
            desc = _get_protobuf_descriptor(expr.expr)
            arg_name = expr.attrname
        except AttributeError:
            return  # only checking <...>.repeated_field.append()
        if desc is None:
            return
        try:
            arg_type = to_pytype(desc.fields_by_name[arg_name])
        except KeyError:
            return  # warn?

        def check_arg(val):
            if not _scalar_typecheck(val, arg_type):
                self.add_message('protobuf-type-error', node=node,
                                 args=(desc.name, arg_name, arg_type.__name__, val))
        for val in vals:
            check_arg(val)

    @check_messages(
        'protobuf-undefined-attribute',
        'protobuf-no-repeated-membership',
        'protobuf-no-proto3-membership',
    )
    def _check_hasfield(self, node):
        # type: (astroid.Call) -> None
        attr = node.func
        if not isinstance(attr, astroid.Attribute) or attr.attrname not in ('HasField', 'ClearField'):
            return
        desc = _get_protobuf_descriptor(attr.expr)
        if desc is None:
            return
        for arg in node.args:
            for val in _get_inferred_values(arg):
                if not hasattr(val, 'value'):
                    continue
                if val.value not in desc.field_names:
                    self.add_message('protobuf-undefined-attribute', node=node, args=(val.value, desc.name))
                    continue
                fd = desc.fields_by_name[val.value]
                if is_repeated(fd):
                    self.add_message('protobuf-no-repeated-membership', node=node)
                    continue
                if desc.proto3:
                    if not is_composite(fd) and not is_oneof(fd):
                        # all fields in proto3 are labelled optional
                        # "optional" fields are members of a singular oneof
                        self.add_message('protobuf-no-proto3-membership', node=node, args=(val.value,))
                        continue

    @check_messages('protobuf-undefined-attribute')
    def visit_assignattr(self, node):
        # type: (astroid.AssignAttr) -> None
        self._assignattr(node)

    @check_messages('protobuf-undefined-attribute')
    def visit_attribute(self, node):
        # type: (astroid.Attribute) -> None
        self._assignattr(node)

    def _assignattr(self, node):
        # type: (Union[astroid.Attribute, astroid.AssignAttr]) -> None
        try:
            vals = node.expr.inferred()
        except astroid.InferenceError:
            return  # TODO: warn or redo
        descriptors = []  # type: List[SimpleDescriptor]
        # Look for any version of the inferred type to be a Protobuf class
        for val in vals:
            cls_def = None
            if val is astroid.Uninferable:
                return  # break early (ref #44 and astroid 03d15b0)
            if hasattr(val, '_proxied'):
                cls_def = val._proxied  # type: astroid.ClassDef
            elif isinstance(val, astroid.ClassDef):
                cls_def = val
            if cls_def and getattr(cls_def, '_is_protobuf_class', False):
                # getattr guards against Uninferable (always returns self so can't use hasattr)
                descriptors.append(cls_def._protobuf_descriptor)

        found = None  # type: Optional[SimpleDescriptor]
        missing = []  # type: List[SimpleDescriptor]
        for desc in descriptors:
            if node.attrname in desc.field_names or node.attrname in desc.extensions_by_name:
                found = desc
                break
            missing.append(desc)
        else:
            if len(missing) != 1:
                return  # don't attempt to warn on multiple options
            desc = missing[0]
            self.add_message('protobuf-undefined-attribute', args=(node.attrname, desc.name), node=node)
        if found is not None:
            self._check_type_error(node, found)
            self._check_no_assign(node, found)

    @check_messages('protobuf-type-error')
    def _check_type_error(self, node, desc):
        # type: (Node, SimpleDescriptor) -> None
        if not isinstance(node, astroid.AssignAttr):
            return
        attr = node.attrname
        value_node = node.assign_type().value  # type: Node
        fd = desc.fields_by_name[attr]  # this should always pass given the check in _assignattr
        if is_composite(fd) or is_repeated(fd):
            return  # skip this check and resolve in _check_no_assign
        type_ = to_pytype(fd)
        for val_const in _get_inferred_values(value_node):
            if not hasattr(val_const, 'value'):
                continue
            val = val_const.value
            if not _scalar_typecheck(val, type_):
                self.add_message('protobuf-type-error', node=node, args=(desc.name, attr, type_.__name__, val))
                break

    @check_messages('protobuf-no-assignment')
    def _check_no_assign(self, node, desc):
        # type: (Node, SimpleDescriptor) -> None
        if not isinstance(node, astroid.AssignAttr):
            return
        attr = node.attrname
        fd = desc.fields_by_name[attr]
        if is_composite(fd) or is_repeated(fd):
            self.add_message('protobuf-no-assignment', node=node, args=(desc.name, attr))

    def visit_subscript(self, node):
        self._check_extension_getitem(node)

    @check_messages('protobuf-wrong-extension-scope')
    def _check_extension_getitem(self, node):
        # type: (astroid.Subscript) -> None
        attr, slice = node.value, node.slice
        if not isinstance(attr, astroid.Attribute) or attr.attrname != 'Extensions':
            return
        if isinstance(slice, IndexNode):  # astroid < 2.6
            value = slice.value
        else:
            value = slice
        if not isinstance(value, astroid.Attribute):
            return
        target_desc = _get_protobuf_descriptor(attr.expr)
        ext_desc = _get_protobuf_descriptor(value.expr)
        if target_desc is None:
            return
        ext_name = value.attrname
        if ext_desc is not None:
            if ext_name not in ext_desc.extensions_by_name:
                return  # should raise protobuf-undefined-attribute elsewhere
            fd = ext_desc.extensions_by_name[ext_name]
        else:  # ext_desc is None
            return  # TODO for now
        if not target_desc.is_extended_by(fd):
            self.add_message('protobuf-wrong-extension-scope', node=node, args=(ext_name, target_desc.name))


def register(linter):
    linter.register_checker(ProtobufDescriptorChecker(linter))


astroid.MANAGER.register_transform(astroid.Module, transform_module, is_some_protobuf_module)
