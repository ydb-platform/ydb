from mypy.mro import calculate_mro, MroError
from mypy.plugin import (
    Plugin, FunctionContext, ClassDefContext, DynamicClassDefContext,
    SemanticAnalyzerPluginInterface
)
from mypy.plugins.common import add_method
from mypy.nodes import (
    NameExpr, Expression, StrExpr, TypeInfo, ClassDef, Block, SymbolTable, SymbolTableNode, GDEF,
    Argument, Var, ARG_STAR2, MDEF, TupleExpr, RefExpr, FuncBase, SymbolNode
)
from mypy.types import (
    UnionType, NoneTyp, Instance, Type, AnyType, TypeOfAny, UninhabitedType, CallableType
)
from mypy.typevars import fill_typevars_with_any

try:
    from mypy.types import get_proper_type
except ImportError:
    get_proper_type = lambda x: x

from typing import Optional, Callable, Dict, List, TypeVar, Union

MYPY = False  # we should support Python 3.5.1 and cases where typing_extensions is not available.
if MYPY:
    from typing_extensions import Final, Type as TypingType

T = TypeVar('T')
CB = Optional[Callable[[T], None]]

COLUMN_NAME = 'sqlalchemy.sql.schema.Column'  # type: Final
CLAUSE_ELEMENT_NAME = 'sqlalchemy.sql.elements.ClauseElement'  # type: Final
COLUMN_ELEMENT_NAME = 'sqlalchemy.sql.elements.ColumnElement'  # type: Final
GROUPING_NAME = 'sqlalchemy.sql.elements.Grouping'  # type: Final
RELATIONSHIP_NAME = 'sqlalchemy.orm.relationships.RelationshipProperty'  # type: Final


# See https://github.com/python/mypy/issues/6617 for plugin API updates.

def fullname(x: Union[FuncBase, SymbolNode]) -> str:
    """Compatibility helper for mypy 0.750 vs older."""
    fn = x.fullname
    if callable(fn):
        return fn()
    return fn


def shortname(x: Union[FuncBase, SymbolNode]) -> str:
    """Compatibility helper for mypy 0.750 vs older."""
    fn = x.name
    if callable(fn):
        return fn()
    return fn


def is_declarative(info: TypeInfo) -> bool:
    """Check if this is a subclass of a declarative base."""
    if info.mro:
        for base in info.mro:
            metadata = base.metadata.get('sqlalchemy')
            if metadata and metadata.get('declarative_base'):
                return True
    return False


def set_declarative(info: TypeInfo) -> None:
    """Record given class as a declarative base."""
    info.metadata.setdefault('sqlalchemy', {})['declarative_base'] = True


class BasicSQLAlchemyPlugin(Plugin):
    """Basic plugin to support simple operations with models.

    Currently supported functionality:
      * Recognize dynamically defined declarative bases.
      * Add an __init__() method to models.
      * Provide better types for 'Column's and 'RelationshipProperty's
        using flags 'primary_key', 'nullable', 'uselist', etc.
    """
    def get_function_hook(self, fullname: str) -> Optional[Callable[[FunctionContext], Type]]:
        if fullname == COLUMN_NAME:
            return column_hook
        if fullname == GROUPING_NAME:
            return grouping_hook
        if fullname == RELATIONSHIP_NAME:
            return relationship_hook
        sym = self.lookup_fully_qualified(fullname)
        if sym and isinstance(sym.node, TypeInfo):
            # May be a model instantiation
            if is_declarative(sym.node):
                return model_hook
        return None

    def get_dynamic_class_hook(self, fullname: str) -> 'CB[DynamicClassDefContext]':
        if fullname == 'sqlalchemy.ext.declarative.api.declarative_base':
            return decl_info_hook
        return None

    def get_class_decorator_hook(self, fullname: str) -> 'CB[ClassDefContext]':
        if fullname == 'sqlalchemy.ext.declarative.api.as_declarative':
            return decl_deco_hook
        return None

    def get_base_class_hook(self, fullname: str) -> 'CB[ClassDefContext]':
        sym = self.lookup_fully_qualified(fullname)
        if sym and isinstance(sym.node, TypeInfo):
            if is_declarative(sym.node):
                return add_model_init_hook
        return None


def add_var_to_class(name: str, typ: Type, info: TypeInfo) -> None:
    """Add a variable with given name and type to the symbol table of a class.

    This also takes care about setting necessary attributes on the variable node.
    """
    var = Var(name)
    var.info = info
    var._fullname = fullname(info) + '.' + name
    var.type = typ
    info.names[name] = SymbolTableNode(MDEF, var)


def add_model_init_hook(ctx: ClassDefContext) -> None:
    """Add a dummy __init__() to a model and record it is generated.

    Instantiation will be checked more precisely when we inferred types
    (using get_function_hook and model_hook).
    """
    if '__init__' in ctx.cls.info.names:
        # Don't override existing definition.
        return
    any = AnyType(TypeOfAny.special_form)
    var = Var('kwargs', any)
    kw_arg = Argument(variable=var, type_annotation=any, initializer=None, kind=ARG_STAR2)
    add_method(ctx, '__init__', [kw_arg], NoneTyp())
    ctx.cls.info.metadata.setdefault('sqlalchemy', {})['generated_init'] = True

    # Also add a selection of auto-generated attributes.
    sym = ctx.api.lookup_fully_qualified_or_none('sqlalchemy.sql.schema.Table')
    if sym:
        assert isinstance(sym.node, TypeInfo)
        typ = Instance(sym.node, [])  # type: Type
    else:
        typ = AnyType(TypeOfAny.special_form)
    add_var_to_class('__table__', typ, ctx.cls.info)


def add_metadata_var(api: SemanticAnalyzerPluginInterface, info: TypeInfo) -> None:
    """Add .metadata attribute to a declarative base."""
    sym = api.lookup_fully_qualified_or_none('sqlalchemy.sql.schema.MetaData')
    if sym:
        assert isinstance(sym.node, TypeInfo)
        typ = Instance(sym.node, [])  # type: Type
    else:
        typ = AnyType(TypeOfAny.special_form)
    add_var_to_class('metadata', typ, info)


def decl_deco_hook(ctx: ClassDefContext) -> None:
    """Support declaring base class as declarative with a decorator.

    For example:
        from from sqlalchemy.ext.declarative import as_declarative

        @as_declarative
        class Base:
            ...
    """
    set_declarative(ctx.cls.info)
    add_metadata_var(ctx.api, ctx.cls.info)


def decl_info_hook(ctx: DynamicClassDefContext) -> None:
    """Support dynamically defining declarative bases.

    For example:
        from sqlalchemy.ext.declarative import declarative_base

        Base = declarative_base()
    """
    cls_bases = []  # type: List[Instance]

    # Passing base classes as positional arguments is currently not handled.
    if 'cls' in ctx.call.arg_names:
        declarative_base_cls_arg = ctx.call.args[ctx.call.arg_names.index("cls")]
        if isinstance(declarative_base_cls_arg, TupleExpr):
            items = [item for item in declarative_base_cls_arg.items]
        else:
            items = [declarative_base_cls_arg]

        for item in items:
            if isinstance(item, RefExpr) and isinstance(item.node, TypeInfo):
                base = fill_typevars_with_any(item.node)
                # TODO: Support tuple types?
                if isinstance(base, Instance):
                    cls_bases.append(base)

    class_def = ClassDef(ctx.name, Block([]))
    class_def.fullname = ctx.api.qualified_name(ctx.name)

    info = TypeInfo(SymbolTable(), class_def, ctx.api.cur_mod_id)
    class_def.info = info
    obj = ctx.api.builtin_type('builtins.object')
    info.bases = cls_bases or [obj]
    try:
        calculate_mro(info)
    except MroError:
        ctx.api.fail("Not able to calculate MRO for declarative base", ctx.call)
        info.bases = [obj]
        info.fallback_to_any = True

    ctx.api.add_symbol_table_node(ctx.name, SymbolTableNode(GDEF, info))
    set_declarative(info)

    # TODO: check what else is added.
    add_metadata_var(ctx.api, info)


def model_hook(ctx: FunctionContext) -> Type:
    """More precise model instantiation check.

    Note: sub-models are not supported.
    Note: this is still not perfect, since the context for inference of
          argument types is 'Any'.
    """
    assert isinstance(ctx.default_return_type, Instance)  # type: ignore[misc]
    model = ctx.default_return_type.type
    metadata = model.metadata.get('sqlalchemy')
    if not metadata or not metadata.get('generated_init'):
        return ctx.default_return_type

    # Collect column names and types defined in the model
    # TODO: cache this?
    expected_types = {}  # type: Dict[str, Type]
    for cls in model.mro[::-1]:
        for name, sym in cls.names.items():
            if isinstance(sym.node, Var):
                tp = get_proper_type(sym.node.type)
                if isinstance(tp, Instance):
                    if fullname(tp.type) in (COLUMN_NAME, RELATIONSHIP_NAME):
                        assert len(tp.args) == 1
                        expected_types[name] = tp.args[0]

    assert len(ctx.arg_names) == 1  # only **kwargs in generated __init__
    assert len(ctx.arg_types) == 1
    for actual_name, actual_type in zip(ctx.arg_names[0], ctx.arg_types[0]):
        if actual_name is None:
            # We can't check kwargs reliably.
            # TODO: support TypedDict?
            continue
        if actual_name not in expected_types:
            ctx.api.fail('Unexpected column "{}" for model "{}"'.format(actual_name,
                                                                        shortname(model)),
                         ctx.context)
            continue
        # Using private API to simplify life.
        ctx.api.check_subtype(actual_type, expected_types[actual_name],  # type: ignore
                              ctx.context,
                              'Incompatible type for "{}" of "{}"'.format(actual_name,
                                                                          shortname(model)),
                              'got', 'expected')
    return ctx.default_return_type


def get_argument_by_name(ctx: FunctionContext, name: str) -> Optional[Expression]:
    """Return the expression for the specific argument.

    This helper should only be used with non-star arguments.
    """
    if name not in ctx.callee_arg_names:
        return None
    idx = ctx.callee_arg_names.index(name)
    args = ctx.args[idx]
    if len(args) != 1:
        # Either an error or no value passed.
        return None
    return args[0]


def get_argtype_by_name(ctx: FunctionContext, name: str) -> Optional[Type]:
    """Same as above but for argument type."""
    if name not in ctx.callee_arg_names:
        return None
    idx = ctx.callee_arg_names.index(name)
    arg_types = ctx.arg_types[idx]
    if len(arg_types) != 1:
        # Either an error or no value passed.
        return None
    return arg_types[0]


def column_hook(ctx: FunctionContext) -> Type:
    """Infer better types for Column calls.

    Examples:
        Column(String) -> Column[Optional[str]]
        Column(String, primary_key=True) -> Column[str]
        Column(String, nullable=False) -> Column[str]
        Column(String, default=...) -> Column[str]
        Column(String, default=..., nullable=True) -> Column[Optional[str]]

    TODO: check the type of 'default'.
    """
    assert isinstance(ctx.default_return_type, Instance)  # type: ignore[misc]

    nullable_arg = get_argument_by_name(ctx, 'nullable')
    primary_arg = get_argument_by_name(ctx, 'primary_key')
    default_arg = get_argument_by_name(ctx, 'default')

    if nullable_arg:
        nullable = parse_bool(nullable_arg)
    else:
        if primary_arg:
            nullable = not parse_bool(primary_arg)
        else:
            nullable = default_arg is None
    # TODO: Add support for literal types.

    if not nullable:
        return ctx.default_return_type
    assert len(ctx.default_return_type.args) == 1
    arg_type = ctx.default_return_type.args[0]
    return Instance(ctx.default_return_type.type, [UnionType([arg_type, NoneTyp()])],
                    line=ctx.default_return_type.line,
                    column=ctx.default_return_type.column)


def grouping_hook(ctx: FunctionContext) -> Type:
    """Infer better types for Grouping calls.

    Examples:
        Grouping(text('asdf')) -> Grouping[None]
        Grouping(Column(String), nullable=False) -> Grouping[str]
        Grouping(Column(String)) -> Grouping[Optional[str]]
    """
    assert isinstance(ctx.default_return_type, Instance)  # type: ignore[misc]

    element_arg_type = get_proper_type(get_argtype_by_name(ctx, 'element'))

    if element_arg_type is not None and isinstance(element_arg_type, Instance):
        if (element_arg_type.type.has_base(CLAUSE_ELEMENT_NAME) and not
                element_arg_type.type.has_base(COLUMN_ELEMENT_NAME)):
            return ctx.default_return_type.copy_modified(args=[NoneTyp()])
    return ctx.default_return_type


def relationship_hook(ctx: FunctionContext) -> Type:
    """Support basic use cases for relationships.

    Examples:
        from sqlalchemy.orm import relationship

        from one import OneModel
        if TYPE_CHECKING:
            from other import OtherModel

        class User(Base):
            __tablename__ = 'users'
            id = Column(Integer(), primary_key=True)
            one = relationship(OneModel)
            other = relationship("OtherModel")

    This also tries to infer the type argument for 'RelationshipProperty'
    using the 'uselist' flag.
    """
    assert isinstance(ctx.default_return_type, Instance)  # type: ignore[misc]
    original_type_arg = ctx.default_return_type.args[0]
    has_annotation = not isinstance(get_proper_type(original_type_arg), UninhabitedType)

    arg = get_argument_by_name(ctx, 'argument')
    arg_type = get_proper_type(get_argtype_by_name(ctx, 'argument'))

    uselist_arg = get_argument_by_name(ctx, 'uselist')

    if isinstance(arg, StrExpr):
        name = arg.value
        sym = None  # type: Optional[SymbolTableNode]
        try:
            # Private API for local lookup, but probably needs to be public.
            sym = ctx.api.lookup_qualified(name)  # type: ignore
        except (KeyError, AssertionError):
            pass
        if sym and isinstance(sym.node, TypeInfo):
            new_arg = fill_typevars_with_any(sym.node)  # type: Type
        else:
            ctx.api.fail('Cannot find model "{}"'.format(name), ctx.context)
            # TODO: Add note() to public API.
            ctx.api.note('Only imported models can be found;'  # type: ignore
                         ' use "if TYPE_CHECKING: ..." to avoid import cycles',
                         ctx.context)
            new_arg = AnyType(TypeOfAny.from_error)
    else:
        if isinstance(arg_type, CallableType) and arg_type.is_type_obj():
            new_arg = fill_typevars_with_any(arg_type.type_object())
        else:
            # Something complex, stay silent for now.
            new_arg = AnyType(TypeOfAny.special_form)

    # We figured out, the model type. Now check if we need to wrap it in List
    if uselist_arg:
        if parse_bool(uselist_arg):
            new_arg = ctx.api.named_generic_type('builtins.list', [new_arg])
    else:
        if has_annotation:
            # If there is an annotation we use it as a source of truth.
            # This will cause false negatives, but it is better than lots of false positives.
            new_arg = original_type_arg

    return Instance(ctx.default_return_type.type, [new_arg],
                    line=ctx.default_return_type.line,
                    column=ctx.default_return_type.column)


# We really need to add this to TypeChecker API
def parse_bool(expr: Expression) -> Optional[bool]:
    if isinstance(expr, NameExpr):
         if expr.fullname == 'builtins.True':
             return True
         if expr.fullname == 'builtins.False':
             return False
    return None


def plugin(version: str) -> 'TypingType[Plugin]':
    return BasicSQLAlchemyPlugin
