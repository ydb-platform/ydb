import contextlib
from collections.abc import Iterable, Mapping
from dataclasses import replace
from typing import Any, Callable, NamedTuple, Optional

from ...code_tools.cascade_namespace import BuiltinCascadeNamespace, CascadeNamespace
from ...code_tools.code_block_tree import (
    CodeBlock,
    CodeExpr,
    DictItem,
    DictKeyValue,
    DictLiteral,
    Expression,
    LinesWriter,
    ListLiteral,
    RawExpr,
    RawStatement,
    Statement,
    StringLiteral,
    TextSliceWriter,
    statements,
)
from ...code_tools.utils import get_literal_expr, get_literal_from_factory, is_singleton
from ...common import Dumper
from ...compat import CompatExceptionGroup
from ...definitions import DebugTrail
from ...model_tools.definitions import (
    DefaultFactory,
    DefaultFactoryWithSelf,
    DefaultValue,
    DescriptorAccessor,
    ItemAccessor,
    OutputField,
    OutputShape,
)
from ...special_cases_optimization import as_is_stub, get_default_clause
from ...struct_trail import TrailElement, append_trail, extend_trail, render_trail_as_note
from ...utils import Omittable, Omitted
from ..json_schema.definitions import JSONSchema
from ..json_schema.schema_model import JSONSchemaType, JSONValue
from .basic_gen import ModelDumperGen
from .crown_definitions import (
    CrownPath,
    CrownPathElem,
    ExtraExtract,
    ExtraTargets,
    OutCrown,
    OutDictCrown,
    OutExtraMove,
    OutFieldCrown,
    OutListCrown,
    OutNoneCrown,
    OutputNameLayout,
    Placeholder,
    Sieve,
)


class GenState:
    def __init__(self, namespace: CascadeNamespace, debug_trail: DebugTrail, error_handler_name: str):
        self.namespace = namespace
        self.debug_trail = debug_trail

        self.field_id_to_path: dict[str, CrownPath] = {}
        self.path_to_suffix: dict[CrownPath, str] = {}

        self._last_path_idx = 0
        self._path: CrownPath = ()

        self.trail_element_to_name_idx: dict[TrailElement, int] = {}

        self.error_handler_name = error_handler_name
        self.error_handlers: dict[Optional[OutputField], Callable[[Statement], Statement]] = {}
        self.field_to_idx: dict[Optional[OutputField], int] = {}

    def register_field_idx(self, field: Optional[OutputField]) -> int:
        if field in self.field_to_idx:
            return self.field_to_idx[field]
        idx = len(self.field_to_idx)
        self.field_to_idx[field] = idx
        return idx

    def _ensure_path_idx(self, path: CrownPath) -> str:
        try:
            return self.path_to_suffix[path]
        except KeyError:
            self._last_path_idx += 1
            suffix = str(self._last_path_idx)
            self.path_to_suffix[path] = suffix
            return suffix

    @property
    def path(self):
        return self._path

    @contextlib.contextmanager
    def add_key(self, key: CrownPathElem):
        past = self._path

        self._path += (key,)
        self._ensure_path_idx(self._path)
        yield
        self._path = past

    def suffix(self, basis: str, key: Optional[CrownPathElem] = None) -> str:
        path = self._path if key is None else (*self._path, key)
        if not path:
            return basis
        return basis + "_" + self._ensure_path_idx(path)

    @property
    def v_crown(self) -> str:
        return self.suffix("result")

    def get_trail_element_expr(self, trail_element: TrailElement) -> str:
        literal_expr = get_literal_expr(trail_element)
        if literal_expr is not None:
            return literal_expr

        if trail_element in self.trail_element_to_name_idx:
            idx = self.trail_element_to_name_idx[trail_element]
            v_trail_element = f"trail_element_{idx}"
        else:
            idx = len(self.trail_element_to_name_idx)
            self.trail_element_to_name_idx[trail_element] = idx
            v_trail_element = f"trail_element_{idx}"
            self.namespace.add_constant(v_trail_element, trail_element)
        return v_trail_element


class VarExpr(Expression):
    def __init__(self, name: str):
        self.name = name

    def write_lines(self, writer: TextSliceWriter) -> None:
        writer.write(self.name)


class AssignmentStatement(Statement):
    def __init__(self, var: VarExpr, value: Expression):
        self.var = var
        self.value = value

    def write_lines(self, writer: TextSliceWriter) -> None:
        CodeBlock(
            "<var> = <value>",
            var=self.var,
            value=self.value,
        ).write_lines(
            writer,
        )


class ErrorHandling(Statement):
    def __init__(self, state: GenState, field: Optional[OutputField]):
        self._state = state
        self._field = field

    def _get_append_trail(self) -> str:
        if self._field is None:
            if self._state.debug_trail == DebugTrail.ALL:
                return "e"
            raise ValueError
        trail_element = self._state.get_trail_element_expr(self._field.accessor.trail_element)
        return f"append_trail(e, {trail_element})"

    def write_lines(self, writer: TextSliceWriter) -> None:
        if self._state.debug_trail == DebugTrail.DISABLE:
            writer.write("raise")
        elif self._state.debug_trail == DebugTrail.FIRST:
            writer.write(self._get_append_trail())
            writer.write("raise")
        elif self._state.debug_trail == DebugTrail.ALL:
            error_handler = self._state.error_handler_name
            idx = self._state.register_field_idx(self._field)
            writer.write(f"raise {error_handler}({idx}, data, {self._get_append_trail()}) from None")
        else:
            raise ValueError


class FieldErrorCatching(Statement):
    def __init__(self, state: GenState, field: OutputField, stmt: Statement):
        self._state = state
        self._field = field
        self._stmt = stmt

    def write_lines(self, writer: TextSliceWriter) -> None:
        if self._state.debug_trail != DebugTrail.DISABLE:
            CodeBlock(
                """
                try:
                    <stmt>
                except Exception as e:
                    <error_handling>
                """,
                stmt=self._stmt,
                error_handling=ErrorHandling(self._state, self._field),
            ).write_lines(
                writer,
            )
        else:
            self._stmt.write_lines(writer)


class OutVarStatement(NamedTuple):
    stmt: Statement
    var: VarExpr


class DictFragment(NamedTuple):
    before: Optional[Statement] = None
    item: Optional[DictItem] = None
    after: Optional[Statement] = None


class BuiltinModelDumperGen(ModelDumperGen):
    def __init__(
        self,
        shape: OutputShape,
        name_layout: OutputNameLayout,
        debug_trail: DebugTrail,
        fields_dumpers: Mapping[str, Dumper],
        model_identity: str,
    ):
        self._shape = shape
        self._name_layout = name_layout
        self._debug_trail = debug_trail
        self._fields_dumpers = fields_dumpers
        self._id_to_field: dict[str, OutputField] = {field.id: field for field in self._shape.fields}
        self._model_identity = model_identity

    def _v_dumper(self, field: OutputField) -> str:
        return f"dumper_{field.id}"

    def _create_state(self, namespace: CascadeNamespace) -> GenState:
        return GenState(namespace, self._debug_trail, "error_handler")

    def _alloc_var(self, state: GenState, name: str) -> VarExpr:
        state.namespace.register_var(name)
        return VarExpr(name)

    def _get_header(self, state: GenState) -> Statement:
        writer = LinesWriter()
        if state.path_to_suffix:
            writer.write("# suffix to path")
            for path, suffix in state.path_to_suffix.items():
                writer.write(f"# {suffix} -> {list(path)}")

            writer.write("")

        if state.field_id_to_path:
            writer.write("# field to path")
            for f_name, path in state.field_id_to_path.items():
                writer.write(f"# {f_name} -> {list(path)}")

            writer.write("")

        return RawStatement(writer.make_string())

    def produce_code(self, closure_name: str) -> tuple[str, Mapping[str, object]]:
        namespace = BuiltinCascadeNamespace()
        namespace.add_constant("CompatExceptionGroup", CompatExceptionGroup)
        namespace.add_constant("append_trail", append_trail)
        namespace.add_constant("extend_trail", extend_trail)
        namespace.add_constant("render_trail_as_note", render_trail_as_note)
        for field_id, dumper in self._fields_dumpers.items():
            namespace.add_constant(self._v_dumper(self._id_to_field[field_id]), dumper)

        state = self._create_state(namespace)
        body = self._get_body_statement(state)
        header = self._get_header(state)

        writer = LinesWriter()
        closure = CodeBlock(
            """
            def <closure_name>(data):
                <header>
                <body>
            """,
            closure_name=RawExpr(closure_name),
            header=header,
            body=body,
        )
        closure.write_lines(writer)

        result = writer.make_string()
        if state.debug_trail == DebugTrail.ALL:
            error_handler_writer = LinesWriter()
            self._get_error_handler(state).write_lines(error_handler_writer)
            result += "\n\n\n" + error_handler_writer.make_string()
        return result, namespace.all_constants

    def _get_body_statement(self, state: GenState) -> Statement:
        if isinstance(self._name_layout.crown, OutDictCrown):
            out_stmt = self._get_dict_crown_out_stmt(state, self._name_layout.crown)
        elif isinstance(self._name_layout.crown, OutListCrown):
            out_stmt = self._get_list_crown_out_stmt(state, self._name_layout.crown)
        else:
            raise TypeError

        extra_extraction = self._get_extra_extraction(state)
        if extra_extraction is not None:
            out_stmt = OutVarStatement(
                var=out_stmt.var,
                stmt=statements(
                    out_stmt.stmt,
                    extra_extraction.stmt,
                    CodeBlock(
                        "<result>.update(<extra>)",
                        result=out_stmt.var,
                        extra=extra_extraction.var,
                    ),
                ),
            )

        return statements(
            out_stmt.stmt,
            CodeBlock(
                "return <var>",
                var=out_stmt.var,
            ),
        )

    def _get_crown_out_stmt(self, state: GenState, key: CrownPathElem, crown: OutCrown) -> OutVarStatement:
        with state.add_key(key):
            if isinstance(crown, OutDictCrown):
                return self._get_dict_crown_out_stmt(state, crown)
            if isinstance(crown, OutListCrown):
                return self._get_list_crown_out_stmt(state, crown)
            if isinstance(crown, OutFieldCrown):
                raise TypeError
            if isinstance(crown, OutNoneCrown):
                return self._get_none_crown_out_stmt(state, crown)
        raise TypeError

    def _get_dict_crown_out_stmt(self, state: GenState, crown: OutDictCrown) -> OutVarStatement:
        fragments = []
        for key, sub_crown in crown.map.items():
            if isinstance(sub_crown, OutFieldCrown):
                field = self._id_to_field[sub_crown.id]
                state.field_id_to_path[field.id] = state.path
                fragments.append(
                    self._process_dict_field(state, key, field)
                    if key not in crown.sieves else
                    self._process_dict_sieved_field(state, key, crown.sieves[key], field),
                )
            else:
                fragments.append(
                    self._process_dict_non_field(state, key, crown.sieves.get(key), sub_crown),
                )

        var = self._alloc_var(state, state.v_crown)
        return self._get_dict_out_stmt_from_fragments(var, fragments)

    def _process_dict_field(self, state: GenState, key: str, field: OutputField) -> DictFragment:
        if field.is_required:
            out_stmt = self._get_required_field_out_stmt(state, field)
            return DictFragment(
                before=out_stmt.stmt,
                item=DictKeyValue(StringLiteral(key), out_stmt.var),
            )
        return DictFragment(
            after=self._get_optional_field_out_stmt(
                state=state,
                field=field,
                on_access_ok=lambda dumped_var: CodeBlock(
                    """
                    <crown>[<key>] = <dumped_var>
                    """,
                    key=StringLiteral(key),
                    crown=VarExpr(state.v_crown),
                    dumped_var=dumped_var,
                ),
            ),
        )

    def _process_dict_sieved_field(self, state: GenState, key: str, sieve: Sieve, field: OutputField) -> DictFragment:
        access_expr = self._get_access_expr(state.namespace, field)
        raw_var = self._alloc_var(state, f"r_{field.id}")
        dumped_var = self._alloc_var(state, f"dumped_{field.id}")
        condition = self._get_sieve_condition_expr(state, sieve, key, raw_var)

        if field.is_required:
            state.error_handlers[field] = lambda collector: CodeBlock(
                """
                    try:
                        <raw_var> = <access_expr>
                    except Exception as e:
                        <collector>
                    else:
                        if <condition>:
                            try:
                                <dumper>
                            except Exception as e:
                                <collector>
                """,
                raw_var=raw_var,
                access_expr=access_expr,
                condition=condition,
                dumper=self._wrap_with_dumper(field, access_expr),
                collector=collector,
            )
            return DictFragment(
                after=CodeBlock(
                    """
                    <access_stmt>
                    if <condition>:
                        <dumped_stmt>
                        <crown>[<key>] = <dumped_var>
                    """,
                    access_stmt=FieldErrorCatching(
                        state=state,
                        field=field,
                        stmt=AssignmentStatement(raw_var, access_expr),
                    ),
                    condition=condition,
                    dumped_stmt=FieldErrorCatching(
                        state=state,
                        field=field,
                        stmt=AssignmentStatement(dumped_var, self._wrap_with_dumper(field, raw_var)),
                    ),
                    key=StringLiteral(key),
                    crown=VarExpr(state.v_crown),
                    dumped_var=dumped_var,
                ),
            )

        access_error_expr = self._get_access_error_expr(state.namespace, field)
        state.error_handlers[field] = lambda collector: CodeBlock(
            """
                try:
                    <raw_var> = <access_expr>
                except <access_error_expr>:
                    pass
                except Exception as e:
                    <collector>
                else:
                    if <condition>:
                        try:
                            <dumper>
                        except Exception as e:
                            <collector>
            """,
            raw_var=raw_var,
            access_expr=access_expr,
            access_error_expr=access_error_expr,
            condition=condition,
            dumper=self._wrap_with_dumper(field, raw_var),
            collector=collector,
        )
        return DictFragment(
            after=CodeBlock(
                """
                try:
                    <raw_var> = <access_expr>
                except <access_error_expr>:
                    pass
                except Exception as e:
                    <on_unexpected_error>
                else:
                    if <condition>:
                        <dumped_stmt>
                        <crown>[<key>] = <dumped_var>
                """,
                raw_var=raw_var,
                access_expr=access_expr,
                access_error_expr=access_error_expr,
                condition=condition,
                dumped_stmt=FieldErrorCatching(
                    state=state,
                    field=field,
                    stmt=AssignmentStatement(dumped_var, self._wrap_with_dumper(field, raw_var)),
                ),
                key=StringLiteral(key),
                crown=VarExpr(state.v_crown),
                dumped_var=dumped_var,
                on_unexpected_error=ErrorHandling(state, field),
            ),
        )

    def _process_dict_non_field(
        self,
        state: GenState,
        key: str,
        sieve: Optional[Sieve],
        sub_crown: OutCrown,
    ) -> DictFragment:
        out_stmt = self._get_crown_out_stmt(state, key, sub_crown)
        if sieve is None:
            return DictFragment(
                before=out_stmt.stmt,
                item=DictKeyValue(StringLiteral(key), out_stmt.var),
            )

        condition = self._get_sieve_condition_expr(state, sieve, key, out_stmt.var)
        return DictFragment(
            before=out_stmt.stmt,
            after=CodeBlock(
                """
                if <condition>:
                    <crown>[<key>] = <sub_crown_var>
                """,
                condition=condition,
                key=StringLiteral(key),
                crown=VarExpr(state.v_crown),
                sub_crown_var=out_stmt.var,
            ),
        )

    def _get_list_crown_out_stmt(self, state: GenState, crown: OutListCrown) -> OutVarStatement:
        out_stmts = []
        for idx, sub_crown in enumerate(crown.map):
            if isinstance(sub_crown, OutFieldCrown):
                field = self._id_to_field[sub_crown.id]
                state.field_id_to_path[field.id] = state.path
                if field.is_optional:
                    raise ValueError
                out_stmts.append(
                    self._get_required_field_out_stmt(state, field),
                )
            else:
                out_stmts.append(
                    self._get_crown_out_stmt(state, idx, sub_crown),
                )

        var = self._alloc_var(state, state.v_crown)
        return OutVarStatement(
            stmt=statements(
                *(out_stmt.stmt for out_stmt in out_stmts),
                AssignmentStatement(
                    var=var,
                    value=ListLiteral([out_stmt.var for out_stmt in out_stmts]),
                ),
            ),
            var=var,
        )

    def _get_none_crown_out_stmt(self, state: GenState, crown: OutNoneCrown) -> OutVarStatement:
        var = self._alloc_var(state, state.v_crown)
        return OutVarStatement(
            var=var,
            stmt=AssignmentStatement(
                var=var,
                value=self._get_placeholder_expr(state, crown.placeholder),
            ),
        )

    def _get_extra_extraction(self, state: GenState) -> Optional[OutVarStatement]:
        if isinstance(self._name_layout.extra_move, ExtraTargets):
            return self._get_extra_target_extraction(state, self._name_layout.extra_move)
        if isinstance(self._name_layout.extra_move, ExtraExtract):
            return self._get_extra_extract_extraction(state, self._name_layout.extra_move)
        if self._name_layout.extra_move is None:
            return None
        raise ValueError

    def _get_extra_target_extraction(self, state: GenState, extra_targets: ExtraTargets) -> OutVarStatement:
        if len(extra_targets.fields) == 1:
            field = self._id_to_field[extra_targets.fields[0]]
            if field.is_required:
                return self._get_required_field_out_stmt(state, field)

        extra_var = self._alloc_var(state, "extra")
        fragments = []
        for field_id in extra_targets.fields:
            field = self._id_to_field[field_id]
            if field.is_required:
                out_stmt = self._get_required_field_out_stmt(state, field)
                fragments.append(
                    DictFragment(
                        before=out_stmt.stmt,
                        after=CodeBlock(
                            """
                            <extra_var>.update(<dumped_var>)
                            """,
                            extra_var=extra_var,
                            dumped_var=out_stmt.var,
                        ),
                    ),
                )
            else:
                fragments.append(
                    DictFragment(
                        after=self._get_optional_field_out_stmt(
                            state=state,
                            field=field,
                            on_access_ok=lambda dumped_var: CodeBlock(
                                """
                                <extra_var>.update(<dumped_var>)
                                """,
                                extra_var=extra_var,
                                dumped_var=dumped_var,
                            ),
                        ),
                    ),
                )
        return self._get_dict_out_stmt_from_fragments(extra_var, fragments)

    def _get_extra_extract_extraction(self, state: GenState, extra_move: ExtraExtract) -> OutVarStatement:
        state.namespace.add_constant("extractor", extra_move.func)
        var = self._alloc_var(state, "extra")
        if self._debug_trail == DebugTrail.ALL:
            state.error_handlers[None] = (
                lambda collector: CodeBlock(
                    """
                    try:
                        extractor(data)
                    except Exception as e:
                        <collector>
                    """,
                    collector=collector,
                )
            )
            return OutVarStatement(
                stmt=CodeBlock(
                    """
                    try:
                        <var> = extractor(data)
                    except Exception as e:
                        <error_handling>
                    """,
                    var=var,
                    error_handling=ErrorHandling(state, field=None),
                ),
                var=var,
            )
        return OutVarStatement(
            stmt=CodeBlock(
                "<var> = extractor(data)",
                var=var,
            ),
            var=var,
        )

    def _get_error_collector(self, state: GenState, field: Optional[OutputField]) -> Statement:
        if field is None:
            return RawStatement("errors.append(e)")

        trail_element = state.get_trail_element_expr(field.accessor.trail_element)
        return RawStatement(f"errors.append(append_trail(e, {trail_element}))")

    def _get_error_handler(self, state: GenState) -> Statement:
        error_testers = [
            CodeBlock(
                """
                if idx < <idx>:
                    <stmt>
                """,
                idx=RawExpr(repr(idx)),
                stmt=state.error_handlers[field](self._get_error_collector(state, field)),
            )
            for field, idx in state.field_to_idx.items()
        ]
        return CodeBlock(
            """
            def <error_handler>(idx, data, first_exc):
                errors = [first_exc]
                <error_testers>
                return CompatExceptionGroup(<error_msg>, [render_trail_as_note(e) for e in errors])
            """,
            error_testers=statements(*error_testers),
            error_msg=StringLiteral(f"while dumping model {self._model_identity}"),
            error_handler=RawExpr(state.error_handler_name),
        )

    def _get_dict_out_stmt_from_fragments(self, var: VarExpr, fragments: Iterable[DictFragment]) -> OutVarStatement:
        return OutVarStatement(
            stmt=statements(
                *(fragment.before for fragment in fragments if fragment.before is not None),
                AssignmentStatement(
                    var=var,
                    value=DictLiteral(
                        [fragment.item for fragment in fragments if fragment.item is not None],
                    ),
                ),
                *(fragment.after for fragment in fragments if fragment.after is not None),
            ),
            var=var,
        )

    def _get_required_field_out_stmt(self, state: GenState, field: OutputField) -> OutVarStatement:
        access_expr = self._get_access_expr(state.namespace, field)
        dumped = self._wrap_with_dumper(field, access_expr)
        var = self._alloc_var(state, f"dumped_{field.id}")
        state.error_handlers[field] = lambda collector: CodeBlock(
            """
                try:
                    <dumper>
                except Exception as e:
                    <collector>
            """,
            dumper=dumped,
            collector=collector,
        )
        return OutVarStatement(
            var=var,
            stmt=FieldErrorCatching(
                state=state,
                field=field,
                stmt=AssignmentStatement(var, dumped),
            ),
        )

    def _get_optional_field_out_stmt(
        self,
        state: GenState,
        field: OutputField,
        on_access_ok: Callable[[VarExpr], Statement],
    ) -> Statement:
        access_expr = self._get_access_expr(state.namespace, field)
        access_error_expr = self._get_access_error_expr(state.namespace, field)
        raw_var = self._alloc_var(state, f"r_{field.id}")
        dumped_var = self._alloc_var(state, f"dumped_{field.id}")
        state.error_handlers[field] = lambda collector: CodeBlock(
            """
                try:
                    <raw_var> = <access_expr>
                except <access_error_expr>:
                    pass
                except Exception as e:
                    <collector>
                else:
                    try:
                        <dumper>
                    except Exception as e:
                        <collector>
            """,
            raw_var=raw_var,
            access_expr=access_expr,
            access_error_expr=access_error_expr,
            dumper=self._wrap_with_dumper(field, raw_var),
            collector=collector,
        )
        return CodeBlock(
            """
            try:
                <raw_var> = <access_expr>
            except <access_error_expr>:
                pass
            except Exception as e:
                <on_unexpected_error>
            else:
                <dumped_stmt>
                <on_access_ok>
            """,
            raw_var=raw_var,
            access_expr=access_expr,
            access_error_expr=access_error_expr,
            dumped_stmt=FieldErrorCatching(
                state=state,
                field=field,
                stmt=AssignmentStatement(dumped_var, self._wrap_with_dumper(field, raw_var)),
            ),
            on_unexpected_error=ErrorHandling(state, field),
            on_access_ok=on_access_ok(dumped_var),
        )

    def _wrap_with_dumper(self, field: OutputField, expr: Expression) -> Expression:
        return (
            expr
            if self._fields_dumpers[field.id] == as_is_stub else
            CodeExpr(
                "<dumper>(<expr>)",
                dumper=RawExpr(self._v_dumper(field)),
                expr=expr,
            )
        )

    def _get_access_expr(self, namespace: CascadeNamespace, field: OutputField) -> Expression:
        accessor = field.accessor
        if isinstance(accessor, DescriptorAccessor):
            if accessor.attr_name.isidentifier():
                return RawExpr(f"data.{accessor.attr_name}")
            return RawExpr(f"getattr(data, {accessor.attr_name!r})")
        if isinstance(accessor, ItemAccessor):
            return RawExpr(f"data[{accessor.key!r}]")

        accessor_getter = f"accessor_getter_{field.id}"
        namespace.add_constant(accessor_getter, field.accessor.getter)
        return RawExpr(f"{accessor_getter}(data)")

    def _get_access_error_expr(self, namespace: CascadeNamespace, field: OutputField) -> Expression:
        access_error = field.accessor.access_error
        literal_expr = get_literal_expr(access_error)
        if literal_expr is not None:
            return RawExpr(literal_expr)

        access_error_getter = f"access_error_{field.id}"
        namespace.add_constant(access_error_getter, access_error)
        return RawExpr(access_error_getter)

    def _get_sieve_condition_expr(self, state: GenState, sieve: Sieve, key: str, test_var: VarExpr) -> Expression:
        default_clause = get_default_clause(sieve)
        if default_clause is None:
            v_sieve = state.suffix("sieve", key)
            state.namespace.add_constant(v_sieve, sieve)
            return RawExpr(f"{v_sieve}({test_var.name})")

        if isinstance(default_clause, DefaultValue):
            literal_expr = get_literal_expr(default_clause.value)
            if literal_expr is not None:
                return RawExpr(
                    f"{test_var.name} is not {literal_expr}"
                    if is_singleton(default_clause.value) else
                    f"{test_var.name} != {literal_expr}",
                )
            v_default = state.suffix("default", key)
            state.namespace.add_constant(v_default, default_clause.value)
            return RawExpr(f"{test_var.name} != {v_default}")

        if isinstance(default_clause, DefaultFactory):
            literal_expr = get_literal_from_factory(default_clause.factory)
            if literal_expr is not None:
                return RawExpr(f"{test_var.name} != {literal_expr}")
            v_default = state.suffix("default", key)
            state.namespace.add_constant(v_default, default_clause.factory)
            return RawExpr(f"{test_var.name} != {v_default}()")

        if isinstance(default_clause, DefaultFactoryWithSelf):
            v_default = state.suffix("default", key)
            state.namespace.add_constant(v_default, default_clause.factory)
            return RawExpr(f"{test_var.name} != {v_default}(data)")

        raise TypeError

    def _get_placeholder_expr(self, state: GenState, placeholder: Placeholder) -> Expression:
        if isinstance(placeholder, DefaultFactory):
            literal_expr = get_literal_from_factory(placeholder.factory)
            if literal_expr is not None:
                return RawExpr(literal_expr)

            v_placeholder = state.suffix("placeholder")
            state.namespace.add_constant(v_placeholder, placeholder.factory)
            return RawExpr(v_placeholder + "()")

        if isinstance(placeholder, DefaultValue):
            literal_expr = get_literal_expr(placeholder.value)
            if literal_expr is not None:
                return RawExpr(literal_expr)

            v_placeholder = state.suffix("placeholder")
            state.namespace.add_constant(v_placeholder, placeholder.value)
            return RawExpr(v_placeholder)

        raise TypeError


class ModelOutputJSONSchemaGen:
    def __init__(
        self,
        shape: OutputShape,
        extra_move: OutExtraMove,
        field_json_schema_getter: Callable[[OutputField], JSONSchema],
        field_default_dumper: Callable[[OutputField], Omittable[JSONValue]],
        placeholder_dumper: Callable[[Any], JSONValue],
    ):
        self._shape = shape
        self._extra_move = extra_move
        self._field_json_schema_getter = field_json_schema_getter
        self._field_default_dumper = field_default_dumper
        self._placeholder_dumper = placeholder_dumper

    def _convert_dict_crown(self, crown: OutDictCrown) -> JSONSchema:
        return JSONSchema(
            type=JSONSchemaType.OBJECT,
            required=[
                key
                for key, value in crown.map.items()
                if self._is_required_crown(value)
            ],
            properties={
                key: self.convert_crown(value)
                for key, value in crown.map.items()
            },
            additional_properties=self._extra_move is not None,
        )

    def _convert_list_crown(self, crown: OutListCrown) -> JSONSchema:
        items = [
            self.convert_crown(sub_crown)
            for sub_crown in crown.map
        ]
        return JSONSchema(
            type=JSONSchemaType.ARRAY,
            prefix_items=items,
            max_items=len(items),
            min_items=len(items),
        )

    def _convert_field_crown(self, crown: OutFieldCrown) -> JSONSchema:
        field = self._shape.fields_dict[crown.id]
        json_schema = self._field_json_schema_getter(field)
        default = self._field_default_dumper(field)
        if default != Omitted():
            return replace(json_schema, default=default)
        return json_schema

    def _convert_none_crown(self, crown: OutNoneCrown) -> JSONSchema:
        value = (
            crown.placeholder.factory()
            if isinstance(crown.placeholder, DefaultFactory) else
            crown.placeholder.value
        )
        return JSONSchema(const=self._placeholder_dumper(value))

    def _is_required_crown(self, crown: OutCrown) -> bool:
        if isinstance(crown, OutFieldCrown):
            return self._shape.fields_dict[crown.id].is_required
        return True

    def convert_crown(self, crown: OutCrown) -> JSONSchema:
        if isinstance(crown, OutDictCrown):
            return self._convert_dict_crown(crown)
        if isinstance(crown, OutListCrown):
            return self._convert_list_crown(crown)
        if isinstance(crown, OutFieldCrown):
            return self._convert_field_crown(crown)
        if isinstance(crown, OutNoneCrown):
            return self._convert_none_crown(crown)
        raise TypeError
