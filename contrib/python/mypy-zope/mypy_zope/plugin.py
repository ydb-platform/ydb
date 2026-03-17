import sys
from typing import List, Dict, Any, Callable, Optional, Tuple, Sequence, Union
from typing import Type as PyType
from typing import cast

from mypy.types import (
    Type,
    Instance,
    CallableType,
    UnionType,
    NoneTyp,
    AnyType,
    TypeOfAny,
    PartialType,
    FunctionLike,
)
from mypy.checker import TypeChecker, is_false_literal
from mypy.options import Options
from mypy.nodes import TypeInfo
from mypy.plugin import (
    CheckerPluginInterface,
    SemanticAnalyzerPluginInterface,
    MethodSigContext,
    Plugin,
    AnalyzeTypeContext,
    FunctionContext,
    MethodContext,
    AttributeContext,
    ClassDefContext,
)
from mypy.subtypes import find_member
from mypy.plugins.default import DefaultPlugin

from mypy.nodes import (
    Context,
    Var,
    Argument,
    FuncDef,
    OverloadedFuncDef,
    Decorator,
    CallExpr,
    RefExpr,
    Expression,
    ClassDef,
    Statement,
    Block,
    IndexExpr,
    MemberExpr,
    SymbolTable,
    SymbolTableNode,
    MDEF,
    ARG_POS,
    ARG_OPT,
    FUNC_NO_INFO,
    NameExpr,
    MypyFile,
    SymbolNode,
)

from collections import defaultdict


def make_simple_type(
    fieldtype: str,
    arg_names: List[List[Optional[str]]],
    args: List[List[Expression]],
    api: CheckerPluginInterface,
) -> Optional[Type]:
    typename = SIMPLE_FIELD_TO_TYPE.get(fieldtype)
    if not typename:
        return None
    stdtype = api.named_generic_type(typename, [])
    for nameset, argset in zip(arg_names, args):
        for name, arg in zip(nameset, argset):
            if name == "required" and is_false_literal(arg):
                nonetype = NoneTyp()
                optionaltype = UnionType([stdtype, nonetype])
                return optionaltype
    return stdtype


FIELD_TO_TYPE_MAKER = {
    "zope.schema._bootstrapfields.Text": make_simple_type,
    "zope.schema._bootstrapfields.Bool": make_simple_type,
    "zope.schema._bootstrapfields.Complex": make_simple_type,
    "zope.schema._bootstrapfields.Real": make_simple_type,
    "zope.schema._bootstrapfields.Int": make_simple_type,
}

SIMPLE_FIELD_TO_TYPE = {
    "zope.schema._bootstrapfields.Text": "str",
    "zope.schema._bootstrapfields.Bool": "bool",
    "zope.schema._bootstrapfields.Complex": "complex",
    "zope.schema._bootstrapfields.Real": "float",
    "zope.schema._bootstrapfields.Int": "int",
}


HACK_IS_ABSTRACT_NON_PROPAGATING = -12345678


class ZopeInterfacePlugin(Plugin):
    def __init__(self, options: Options):
        super().__init__(options)
        self.fallback = DefaultPlugin(options)

    def log(self, msg: str) -> None:
        if self.options.verbosity >= 1:
            print("ZOPE:", msg, file=sys.stderr)

    def get_type_analyze_hook(
        self, fullname: str
    ) -> Optional[Callable[[AnalyzeTypeContext], Type]]:
        # print(f"get_type_analyze_hook: {fullname}")
        return None

    def get_function_hook(
        self, fullname: str
    ) -> Optional[Callable[[FunctionContext], Type]]:
        # print(f"get_function_hook: {fullname}")

        def analyze(function_ctx: FunctionContext) -> Type:
            # strtype = function_ctx.api.named_generic_type('builtins.str', [])
            # optstr = function_ctx.api.named_generic_type('typing.Optional', [strtype])
            api = function_ctx.api
            deftype = function_ctx.default_return_type

            if self._is_subclass(deftype, "zope.interface.interface.Attribute"):
                return self._get_schema_field_type(
                    deftype, function_ctx.arg_names, function_ctx.args, api
                )
            if self._is_subclass(deftype, "zope.schema.fieldproperty.FieldProperty"):
                # We cannot accurately determine the type, fallback to Any
                return AnyType(TypeOfAny.implementation_artifact)

            return deftype

        def class_implements_hook(function_ctx: FunctionContext) -> Type:
            assert len(function_ctx.arg_types) == 2
            arg_impl = function_ctx.arg_types[0][0]
            expr = function_ctx.args[0][0]
            implname = expr.fullname if isinstance(expr, NameExpr) else "expression"
            if not isinstance(arg_impl, CallableType):
                function_ctx.api.fail(
                    f"{implname} is not a class, "
                    "cannot mark it as a interface implementation",
                    function_ctx.context,
                )
                return function_ctx.default_return_type
            assert isinstance(arg_impl, CallableType)
            impl_type = arg_impl.ret_type
            assert isinstance(impl_type, Instance)

            for expr, arg_iface in zip(function_ctx.args[1], function_ctx.arg_types[1]):
                exprname = expr.fullname if isinstance(expr, NameExpr) else "expression"
                if not isinstance(arg_iface, CallableType):
                    function_ctx.api.fail(
                        f"{exprname} is not a class, "
                        f"cannot mark {implname} as an implementation of "
                        f"{exprname}",
                        function_ctx.context,
                    )
                    continue

                iface_type = arg_iface.ret_type
                assert isinstance(iface_type, Instance)
                if not self._is_interface(iface_type.type):
                    function_ctx.api.fail(
                        f"{exprname} is not an interface", function_ctx.context,
                    )
                    function_ctx.api.fail(
                        f"Make sure you have stubs for all packages that "
                        f"provide interfaces for {exprname} class hierarchy.",
                        function_ctx.context,
                    )
                    continue

                self._apply_interface(impl_type.type, iface_type.type)
                self._report_implementation_problems(
                    impl_type, iface_type, function_ctx.api, function_ctx.context
                )

            return function_ctx.default_return_type

        if fullname == "zope.interface.declarations.classImplements":
            return class_implements_hook

        # Give preference to deault plugin
        hook = self.fallback.get_function_hook(fullname)
        if hook is not None:
            return hook
        return analyze

    def get_method_signature_hook(
        self, fullname: str
    ) -> Optional[Callable[[MethodSigContext], CallableType]]:
        # print(f"get_method_signature_hook: {fullname}")
        return None

    def get_method_hook(
        self, fullname: str
    ) -> Optional[Callable[[MethodContext], Type]]:
        # print(f"get_method_hook: {fullname}")

        methodname = fullname.split(".")[-1]
        if methodname in ("providedBy", "implementedBy"):

            def analyze(method_ctx: MethodContext) -> Type:
                assert isinstance(method_ctx.context, CallExpr)
                assert isinstance(method_ctx.context.callee, MemberExpr)
                if method_ctx.context.callee.name == "providedBy":
                    method_ctx.context.callee.fullname = "builtins.isinstance"
                else:
                    method_ctx.context.callee.fullname = "builtins.issubclass"
                method_ctx.context.args = [
                    method_ctx.args[0][0],
                    method_ctx.context.callee.expr,
                ]

                return method_ctx.default_return_type

            return analyze

        def analyze_implementation(method_ctx: MethodContext) -> Type:
            deftype = method_ctx.default_return_type
            if not isinstance(method_ctx.context, ClassDef):
                return deftype

            impl_info = method_ctx.context.info
            if impl_info is FUNC_NO_INFO:
                return deftype

            impl_type = Instance(impl_info, [])
            md = self._get_metadata(impl_info)
            ifaces = cast(List[str], md.get("implements", []))
            for ifacename in ifaces:
                # iface_type = method_ctx.api.named_generic_type(ifacename, [])
                assert isinstance(method_ctx.api, TypeChecker)
                iface_type = self._lookup_type(ifacename, method_ctx.api)
                self._report_implementation_problems(
                    impl_type, iface_type, method_ctx.api, method_ctx.context
                )
            return deftype

        if fullname == "zope.interface.declarations.implementer.__call__":
            return analyze_implementation

        return None

    def get_attribute_hook(
        self, fullname: str
    ) -> Optional[Callable[[AttributeContext], Type]]:
        # print(f"get_attribute_hook: {fullname}")
        return None

    def get_class_decorator_hook(
        self, fullname: str
    ) -> Optional[Callable[[ClassDefContext], None]]:
        # print(f"get_class_decorator_hook: {fullname}")

        def apply_implementer(
            iface_arg: Expression,
            class_info: TypeInfo,
            api: SemanticAnalyzerPluginInterface,
        ) -> None:
            if not isinstance(iface_arg, RefExpr):
                api.fail(
                    "Argument to implementer should be a ref expression", iface_arg
                )
                return
            iface_name = iface_arg.fullname
            if iface_name is None:
                # unknown interface, probably from stubless package
                return

            iface_type = iface_arg.node
            if iface_type is None:
                return
            if not isinstance(iface_type, TypeInfo):
                # Possibly an interface from unimported package, ignore
                return

            if not self._is_interface(iface_type):
                api.fail(
                    f"zope.interface.implementer accepts interface, "
                    f"not {iface_name}.",
                    iface_arg,
                )
                api.fail(
                    f"Make sure you have stubs for all packages that "
                    f"provide interfaces for {iface_name} class hierarchy.",
                    iface_arg,
                )
                return

            self._apply_interface(class_info, iface_type)

        def analyze(classdef_ctx: ClassDefContext) -> None:
            api = classdef_ctx.api

            decor = cast(CallExpr, classdef_ctx.reason)

            for iface_arg in decor.args:
                apply_implementer(iface_arg, classdef_ctx.cls.info, api)

        if fullname == "zope.interface.declarations.implementer":
            return analyze
        return None

    def get_metaclass_hook(
        self, fullname: str
    ) -> Optional[Callable[[ClassDefContext], None]]:
        # print(f"get_metaclass_hook: {fullname}")
        def analyze_metaclass(ctx: ClassDefContext) -> None:
            metaclass = ctx.cls.metaclass
            if not isinstance(metaclass, RefExpr):
                return
            info = metaclass.node
            if not isinstance(info, TypeInfo):
                return

            expected = "zope.interface.interface.InterfaceClass"
            if any(node.fullname == expected for node in info.mro):
                self.log(f"Found zope interface: {ctx.cls.fullname}")
                md = self._get_metadata(ctx.cls.info)
                md["is_interface"] = True

        return analyze_metaclass

    def get_base_class_hook(
        self, fullname: str
    ) -> Optional[Callable[[ClassDefContext], None]]:
        # print(f"get_base_class_hook: {fullname}")
        def analyze_subinterface(classdef_ctx: ClassDefContext) -> None:
            # If one of the bases is an interface, this is also an interface
            if isinstance(classdef_ctx.reason, IndexExpr):
                # Generic parameterised interface
                reason = classdef_ctx.reason.base
            else:
                reason = classdef_ctx.reason
            if not isinstance(reason, RefExpr):
                return
            cls_info = classdef_ctx.cls.info

            api = classdef_ctx.api
            base_name = reason.fullname
            if not base_name:
                return
            if "." not in base_name:
                # When class is inherited from a dynamic parameter, it will not
                # have a fully-qualified name, so we cannot use api to look it
                # up. Just give up in this case.
                return
            base_node = api.lookup_fully_qualified_or_none(base_name)
            if not base_node:
                return
            if not isinstance(base_node.node, TypeInfo):
                return

            if self._is_interface(base_node.node):
                self.log(f"Found zope subinterface: {cls_info.fullname}")
                cls_md = self._get_metadata(cls_info)
                cls_md["is_interface"] = True

        return analyze_subinterface

    def get_customize_class_mro_hook(
        self, fullname: str
    ) -> Optional[Callable[[ClassDefContext], None]]:
        # print(f"get_customize_class_mro_hook: {fullname}")

        def analyze_interface_base(classdef_ctx: ClassDefContext) -> None:
            # Create fake constructor to mimic adaptation signature
            info = classdef_ctx.cls.info
            api = classdef_ctx.api
            if "__init__" in info.names:
                # already patched
                return

            # Create a method:
            #
            # def __init__(self, obj, alternate=None) -> None
            #
            # This will make interfaces
            selftp = Instance(info, [])
            anytp = AnyType(TypeOfAny.implementation_artifact)
            init_fn = CallableType(
                arg_types=[selftp, anytp, anytp],
                arg_kinds=[ARG_POS, ARG_POS, ARG_OPT],
                arg_names=["self", "obj", "alternate"],
                ret_type=NoneTyp(),
                fallback=api.named_type("builtins.function"),
            )
            newinit = FuncDef("__init__", [], Block([]), init_fn)
            newinit.info = info
            info.names["__init__"] = SymbolTableNode(
                MDEF, newinit, plugin_generated=True
            )

        def analyze(classdef_ctx: ClassDefContext) -> None:
            info = classdef_ctx.cls.info

            # If we are dealing with an interface, massage it a bit, e.g.
            # inject `self` argument to all methods
            directiface = "zope.interface.interface.Interface" in [
                b.type.fullname for b in info.bases
            ]
            subinterface = any(self._is_interface(b.type) for b in info.bases)
            if directiface or subinterface:
                self._analyze_zope_interface(classdef_ctx.api, classdef_ctx.cls)

        if fullname == "zope.interface.interface.Interface":
            return analyze_interface_base

        return analyze

    def _is_subclass(self, typ: Type, classname: str) -> bool:
        if not isinstance(typ, Instance):
            return False

        parent_names = [t.fullname for t in typ.type.mro]
        return classname in parent_names

    def _get_schema_field_type(
        self,
        typ: Type,
        arg_names: List[List[Optional[str]]],
        args: List[List[Expression]],
        api: CheckerPluginInterface,
    ) -> Type:
        """Given subclass of zope.interface.Attribute, determine python
        type that would correspond to it.
        """
        # If we are not processing an interface, leave the type as is
        assert isinstance(api, TypeChecker)
        scopecls = api.scope.active_class()
        if scopecls is None:
            return typ

        if not self._is_interface(scopecls):
            return typ

        # If default type is a zope.schema.Field, we should convert it to a
        # python type
        if not isinstance(typ, Instance):
            return typ

        parent_names = [t.fullname for t in typ.type.mro]

        # If it is a konwn field, build a python type out of it
        for clsname in parent_names:
            maker = FIELD_TO_TYPE_MAKER.get(clsname)
            if maker is None:
                continue

            convtype = maker(clsname, arg_names, args, api)
            if convtype:
                self.log(
                    f"Converting a field {typ} into type {convtype} "
                    f"for {scopecls.fullname}"
                )
                return convtype

        # For unknown fields, just return ANY
        self.log(f"Unknown field {typ} in interface {scopecls.fullname}")
        return AnyType(
            TypeOfAny.implementation_artifact, line=typ.line, column=typ.column
        )

    def _analyze_zope_interface(
        self, api: SemanticAnalyzerPluginInterface, cls: ClassDef
    ) -> None:
        self.log(f"Adjusting zope interface: {cls.info.fullname}")
        md = self._get_metadata(cls.info)
        # Even though interface is abstract, we mark it as non-abstract to
        # allow adaptation pattern: IInterface(context)
        if md.get("interface_analyzed", False):
            return

        for idx, item in enumerate(cls.defs.body):
            if isinstance(item, FuncDef):
                replacement = self._adjust_interface_function(api, cls.info, item)
            elif isinstance(item, OverloadedFuncDef):
                replacement = self._adjust_interface_overload(api, cls.info, item)
            else:
                continue

            cls.defs.body[idx] = replacement

        md["interface_analyzed"] = True

    def _get_metadata(self, typeinfo: TypeInfo) -> Dict[str, Any]:
        if "zope" not in typeinfo.metadata:
            typeinfo.metadata["zope"] = {}
        return typeinfo.metadata["zope"]

    def _is_interface(self, typeinfo: TypeInfo) -> bool:
        md = self._get_metadata(typeinfo)
        return cast(bool, md.get("is_interface", False))

    def _adjust_interface_function(
        self,
        api: SemanticAnalyzerPluginInterface,
        class_info: TypeInfo,
        func_def: FuncDef,
    ) -> Statement:

        if func_def.arg_names and func_def.arg_names[0] == "self":
            # reveal the common mistake of leaving "self" arguments in the
            # interface
            api.fail("Interface methods should not have 'self' argument", func_def)
        else:
            selftype = Instance(
                class_info, [], line=class_info.line, column=class_info.column
            )
            selfarg = Argument(Var("self", None), selftype, None, ARG_POS)

            if isinstance(func_def.type, CallableType):
                func_def.type.arg_names.insert(0, "self")
                func_def.type.arg_kinds.insert(0, ARG_POS)
                func_def.type.arg_types.insert(0, selftype)
            func_def.arg_names.insert(0, "self")
            func_def.arg_kinds.insert(0, ARG_POS)
            func_def.arguments.insert(0, selfarg)

        # 1: We want mypy to consider this method abstract, so that it allows the
        #    method to have an empty body without causing a warning.
        # 2: We want mypy to consider the interface class NOT abstract, so we can use the
        #   "adaption" pattern.
        # Unfortunately whenever mypy sees (1) it will mark the class as abstract,
        # forcing (2) to be false. This seems to be a change in mypy 0.990, namely
        #    https://github.com/python/mypy/pull/13729
        #
        # Mypy 1.0.0:
        # - allows empty bodies for abstract methods in mypy/checker.py:1240
        #   by testing if
        #       abstract_status != NOT_ABSTRACT.
        # - marks classes as abstract based on their methods in
        #   mypy/semanal_classprop.py:79 by testing if
        #       abstract_status in (IS_ABSTRACT, IMPLICITLY_ABSTRACT)
        #
        # Thus we can make (1) and (2) true by setting abstract_status to some value
        # distinct from these three NOT_ABSTRACT, IS_ABSTRACT and IMPLICITLY_ABSTRACT.
        # These are presently the integers 0, 1, and 2 defined in mypy/nodes.py:738-743.
        func_def.abstract_status = HACK_IS_ABSTRACT_NON_PROPAGATING

        return func_def

    def _adjust_interface_overload(
        self,
        api: SemanticAnalyzerPluginInterface,
        class_info: TypeInfo,
        overload_def: OverloadedFuncDef,
    ) -> Statement:

        for overload_item in overload_def.items:
            if isinstance(overload_item, Decorator):
                func_def = overload_item.func
            else:
                assert isinstance(overload_item, FuncDef)
                func_def = overload_item

            self._adjust_interface_function(api, class_info, func_def)
        return overload_def

    def _report_implementation_problems(
        self,
        impl_type: Instance,
        iface_type: Instance,
        api: CheckerPluginInterface,
        context: Context,
    ) -> None:
        # This mimicks mypy's MessageBuilder.report_protocol_problems with
        # simplifications for zope interfaces.

        # Blatantly assume we are dealing with this particular implementation
        # of CheckerPluginInterface, because it has functionality we want to
        # reuse.
        assert isinstance(api, TypeChecker)

        impl_info = impl_type.type
        iface_info = iface_type.type
        iface_members = self._index_members(iface_info)
        impl_members = self._index_members(impl_info)

        # Report missing members
        missing: Dict[str, List[str]] = defaultdict(list)
        for member, member_iface in iface_members.items():
            if find_member(member, impl_type, impl_type) is None:
                iface_name = member_iface.fullname
                if member_iface == iface_info:
                    # Since interface is directly implemented by this class, we
                    # can use shorter name.
                    iface_name = member_iface.name
                missing[iface_name].append(member)

        if missing:
            for iface_name, members in missing.items():
                missing_fmt = ", ".join(sorted(members))
                api.fail(
                    f"'{impl_info.name}' is missing following "
                    f"'{iface_name}' interface members: {missing_fmt}.",
                    context,
                )

        # Report member type conflicts
        for member, member_iface in iface_members.items():
            iface_mtype = find_member(member, iface_type, iface_type)
            assert iface_mtype is not None
            impl_mtype = find_member(member, impl_type, impl_type)
            if impl_mtype is None:
                continue

            # Find out context for error reporting. If the member is not
            # defined inside the class that declares interface implementation,
            # show the error near the class definition. Otherwise, show it near
            # the member definition.
            ctx: Context = impl_info
            impl_member_def_info = impl_members.get(member)
            impl_name = impl_info.name
            if impl_member_def_info is not None:
                if impl_member_def_info == impl_info:
                    ctx = impl_mtype
                else:
                    impl_name = impl_member_def_info.fullname

            iface_name = iface_info.name
            if member_iface != iface_info:
                iface_name = member_iface.fullname

            if isinstance(impl_mtype, PartialType):
                # We don't know how to deal with partial type here. Partial
                # types will be resolved later when the implementation class is
                # fully type-checked. We are doing our job before that, so all
                # we can do is to skip checking of such members.
                continue

            if isinstance(iface_mtype, FunctionLike) and isinstance(
                impl_mtype, FunctionLike
            ):
                api.check_override(
                    override=impl_mtype,
                    original=iface_mtype,
                    name=impl_name,
                    name_in_super=member,
                    supertype=iface_name,
                    original_class_or_static=False,
                    override_class_or_static=False,
                    node=ctx,
                )

            else:
                # We could check the field type for compatibility with the code
                # below, however this yields too many false-positives in
                # real-life projects. The problem is that we can only check
                # types defined on a class, instead of instance types, so all
                # "descriptor" properties will be falsly reported as type
                # mismatches. Instead we opt-out from property type checking
                # until we figure out the workaround.

                # api.check_subtype(
                #     impl_mtype,
                #     iface_mtype,
                #     context=ctx,
                #     subtype_label=f'"{impl_name}" has type',
                #     supertype_label=f'interface "{iface_name}" defines "{member}" as',
                # )
                pass

    def _lookup_type(self, fullname: str, api: TypeChecker) -> Instance:
        module, names = self._find_module(fullname, api)
        container: Union[MypyFile, TypeInfo] = module
        for name in names:
            sym = container.names[name]
            assert isinstance(sym.node, TypeInfo)
            container = sym.node
        typeinfo = container
        assert isinstance(typeinfo, TypeInfo)

        any_type = AnyType(TypeOfAny.from_omitted_generics)
        return Instance(typeinfo, [any_type] * len(typeinfo.defn.type_vars))

    def _find_module(
        self, fullname: str, api: TypeChecker
    ) -> Tuple[MypyFile, Sequence[str]]:
        parts = fullname.split(".")
        for pn in range(len(parts) - 1, 0, -1):
            moduleparts = parts[:pn]
            names = parts[pn:]
            modulename = ".".join(moduleparts)
            if modulename in api.modules:
                return api.modules[modulename], names

        raise LookupError(fullname)

    def _index_members(self, typeinfo: TypeInfo) -> Dict[str, TypeInfo]:
        # we skip "object" and "Interface" since everyone implements it
        members = {}
        for base in reversed(typeinfo.mro):
            if base.fullname == "builtins.object":
                continue
            if base.fullname == "zope.interface.interface.Interface":
                continue
            for name in base.names:
                # Members of subtypes override members of supertype
                members[name] = base
        return members

    def _apply_interface(self, impl: TypeInfo, iface: TypeInfo) -> None:

        md = self._get_metadata(impl)
        if "implements" not in md:
            md["implements"] = []

        md["implements"].append(iface.fullname)
        self.log(f"Found implementation of " f"{iface.fullname}: {impl.fullname}")

        # Make sure implementation is treated as a subtype of an interface. Pretend
        # there is a decorator for the class that will create a "type promotion",
        # but ensure this only gets applied a single time per interface.
        promote = Instance(iface, [])
        if promote not in impl._promote:
            impl._promote.append(promote)


def plugin(version: str) -> PyType[Plugin]:
    return ZopeInterfacePlugin
