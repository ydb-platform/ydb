#!/usr/bin/env python3

import sys
import argparse
import json

NEVER_INLINE = {
    'DqPhyStage',
}

COMPLEX_ARGS = {
    'DqCnHashShuffle',
    'DqCnMerge',
    'DqCnMap',
    'DqReplicate',
    'DqSink',
    'DqPhyStage',
    'KqpPhysicalQuery',
    'KqpBlockReadOlapTableRanges',
    'KqpPhysicalTx',
    'KqpTxResultBinding',
    'KqpTableSinkSettings',
    'DqPhyHashCombine',
    'WideCombiner',
    'MapJoinCore',
    'BlockHashJoinCore',
    'BlockAsStruct',
    'BlockMergeFinalizeHashed',
    'BlockCombineHashed',
    'TopSort',
    'Map',
    'NarrowMap',
    'WideMap',
    'WideFilter',
    'ExpandMap',
    'FlatMap',
    'NarrowSqueezeToDict',
    'Condense',
    'WideCondense',
    'Condense1',
    'WideCondense1',
    'KqpOlapFilter',
    'KqpOlapAnd',
    'StructType',
    'AsStruct',
    'Udf',
    'Apply',
    'List',
    'AsList',
    'RangeCreate',
    'RangeFinalize',
    'RangeMultiply',
    'RangeIntersect',
    'RangeUnion',
    'If',
    'IfPresent',
    'TupleType',
}

SIMPLE_OPERATORS = {
    'OptionalType',
    'StructType',
    'DataType',
    'MultiType',
    'FlowType',
    'ResourceType',
    'TupleType',
    'ListType',
    'CallableType',
    'VoidType',
    'Void',
    'BlockType',
    'Nothing',
    'SafeCast',
    'String',
    '-',
    '+',
    '*',
    '/',
    'Int32',  # TODO: generate a collection of types
    'Int64',
}

COLOR_COMMENT = 'comment'
COLOR_FUNC_NAME = 'func'
COLOR_SPECIAL_EXPR = 'spec'
COLOR_STRING_LITERAL = 'str'
COLOR_LITERAL = 'literal'
COLOR_ARG = 'arg'
COLOR_LAMBDA = COLOR_ARG
COLOR_REF = None
COLOR_TABLINE = 'tabline'
COLOR_COLUMNS = 'columns'

COLORS = {
    COLOR_COMMENT: '2;128;128;128',
    COLOR_FUNC_NAME: '2;0;128;128',
    COLOR_SPECIAL_EXPR: '2;128;0;128',
    COLOR_STRING_LITERAL: '2;64;192;192',
    COLOR_LITERAL: '2;64;192;192',
    COLOR_ARG: '2;192;156;0',
    COLOR_TABLINE: '2;64;64;64',
    COLOR_COLUMNS: '2;96;168;96',
}

NOTE_OPEN = '⟨'
NOTE_CLOSE = '⟩'


def ansi_truecolor_spec_to_css_color(spec: str) -> str:
    parts = spec.split(';')
    if len(parts) >= 4 and parts[0] == '2':
        r, g, b = int(parts[1]), int(parts[2]), int(parts[3])
        return 'rgb(%d,%d,%d)' % (r, g, b)
    return 'inherit'


def ast_html_syntax_stylesheet() -> str:
    return '\n'.join(
        '.syntax_%s { color: %s; }' % (name, ansi_truecolor_spec_to_css_color(spec))
        for name, spec in COLORS.items()
    )


def html_syntax_span_open(color_name: str) -> str:
    return '<span class="syntax_%s">' % color_name


class TerminalPrinter:
    class TermColorWrapper:
        def __init__(self, color_name):
            if color_name and (color_name in COLORS) and sys.stdout.isatty():
                self.color = COLORS[color_name]
            else:
                self.color = None

        def __enter__(self):
            if self.color:
                sys.stdout.write('\033[38;%sm' % self.color)
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            if self.color:
                sys.stdout.write('\033[39m')

    def __init__(self):
        pass

    def color(self, color_name):
        return TerminalPrinter.TermColorWrapper(color_name)

    def out(self, s):
        sys.stdout.write(s)

    def endl(self):
        sys.stdout.write('\n')

    def finalize(self):
        self.endl()


class HtmlPrinter:
    lines: list[str]
    curr_line: str
    style_stack: list[str]
    prev_style: str | None

    class HtmlColorWrapper:
        color_name: str | None
        style_stack: list[str]

        def __init__(self, style_stack: list[str], color_name: str | None):
            self.color_name = color_name if (color_name and color_name in COLORS) else None
            self.style_stack = style_stack

        def __enter__(self):
            if self.color_name:
                self.style_stack.append(self.color_name)
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            if self.color_name:
                self.style_stack.pop()

    def __init__(self):
        self.prev_style = None
        self.style_stack = []
        self.curr_line = ''
        self.lines = []

    def current_style(self):
        return self.style_stack[-1] if self.style_stack else None

    def check_change_color(self):
        style = self.current_style()
        if style == self.prev_style:
            return
        if self.prev_style is not None:
            self.curr_line += '</span>'
        if style is not None:
            self.curr_line += html_syntax_span_open(style)
        self.prev_style = style

    def color(self, color_name):
        return HtmlPrinter.HtmlColorWrapper(self.style_stack, color_name)

    def out(self, s):
        self.check_change_color()
        self.curr_line += s

    def endl(self):
        self.check_change_color()
        if self.prev_style is not None:
            self.curr_line += '</span>'
        self.lines.append(self.curr_line)
        self.curr_line = ''
        style = self.current_style()
        if style is not None:
            self.curr_line += html_syntax_span_open(style)
        self.prev_style = style

    def finalize(self):
        if self.curr_line:
            self.endl()
        sys.stdout.write('<style>\n')
        sys.stdout.write(ast_html_syntax_stylesheet())
        sys.stdout.write('\n</style>\n<pre>\n')
        sys.stdout.write('\n'.join(self.lines))
        sys.stdout.write('\n</pre>\n')


class List:
    def __init__(self, is_quote):
        self.list = []
        self.is_quote = is_quote


class Element:
    def __init__(self, is_quote, value, is_quoted_str=False):
        self.value = value
        self.is_quote = is_quote
        self.is_quoted_str = is_quoted_str


class Reference:
    def __init__(self, alias):
        self.alias = alias


def get_oper_from_raw_list(the_list):
    oper = None
    if len(the_list) >= 1 and isinstance(the_list[0], Element):
        item = the_list[0]
        if not item.is_quote and not item.is_quoted_str and isinstance(item.value, str):
            oper = item.value
    return oper


def get_oper(the_list):
    return get_oper_from_raw_list(the_list.list)


def get_oper_color(oper):
    if not oper:
        return None
    elif oper == 'lambda':
        return COLOR_LAMBDA
    elif oper in ('block', 'let', 'return', 'declare'):
        return COLOR_SPECIAL_EXPR
    else:
        return COLOR_FUNC_NAME


class Context:
    printer: TerminalPrinter | HtmlPrinter

    def __init__(
        self, parent: 'Context' = None, shift: int = None, is_lambda_args: bool = False, tabstops: bool = None, printer=None
    ):
        self.shift = 0
        self.lambda_args = set()
        if parent is not None:
            self.printer = parent.printer
            self.tabstops = parent.tabstops
            self.shift = parent.shift
            if not is_lambda_args:
                self.lambda_args.update(parent.lambda_args)
        if printer is not None:
            self.printer = printer
        if shift is not None:
            self.shift = shift
        if tabstops is not None:
            self.tabstops = tabstops
        self.is_lambda_args = is_lambda_args


def get_is_long_oper(the_list: List):
    if len(the_list.list) <= 2:
        return False
    oper = get_oper(the_list)
    if (oper == 'lambda' or oper == 'return') and len(the_list.list) >= 6:  # TODO: this is arbitrary
        body_start_idx = 2 if (oper == 'lambda') else 1
        has_non_refs_in_body = False
        for arg in the_list.list[body_start_idx:]:
            if not isinstance(arg, Reference):
                has_non_refs_in_body = True
                break
        return has_non_refs_in_body
    return oper is not None and (oper in COMPLEX_ARGS)


def has_long_or_block_oper_inside(item):
    if isinstance(item, List):
        if get_is_long_oper(item) or get_oper(item) == 'block':
            return True
        for sub_item in item.list:
            if has_long_or_block_oper_inside(sub_item):
                return True
    else:
        return False


def print_note(context: Context, note):
    if not note:
        return
    with context.printer.color(COLOR_COLUMNS):
        context.printer.out(' ' + NOTE_OPEN + note + NOTE_CLOSE)


def print_list(out, the_list: List, callables, context: Context):
    def print_shift(sh):
        for _ in range(sh):
            if context.tabstops:
                with context.printer.color(COLOR_TABLINE):
                    context.printer.out('\u2506   ')
            else:
                context.printer.out('    ')

    oper = get_oper(the_list)
    is_long_oper = get_is_long_oper(the_list)
    # TODO: very wide multi-output lambdas should also be multi-line, like lambdas with ('block ...) bodies
    is_block_oper = oper is not None and (oper in ('block'))

    if is_long_oper:
        context.shift += 1

    child_list = {}
    if oper and oper in callables:
        child_list = callables[oper].children_names

    notes = getattr(the_list, 'child_notes', None)

    for pos, item in enumerate(the_list.list):
        is_last = pos == (len(the_list.list) - 1)
        is_first = pos == 0
        note = notes.get(pos) if notes else None

        if not is_first and is_long_oper:
            context.printer.endl()
            print_shift(context.shift)

        if pos > 0:
            param_name = child_list.get(pos - 1, None)
            # TODO: looks cool but sometimes confusing
            # if pos == 1 and (param_name == 'Input' or param_name == 'Stream'):
            #     param_name = '⇐'
            # elif param_name == 'Lambda':
            #     param_name = 'λ'
            if param_name:
                with context.printer.color(COLOR_COMMENT):
                    context.printer.out('⦗' + param_name + '⦘')
                if note:
                    print_note(context, note)
                    note = None
                if not is_first and is_long_oper and isinstance(item, List) and has_long_or_block_oper_inside(item):
                    context.printer.endl()
                    print_shift(context.shift)

        if isinstance(item, List):
            is_lambda_args = (oper == 'lambda') and (pos == 1)
            sub_oper = get_oper(item)
            sub_oper_color = (
                get_oper_color('block')
                if oper == 'block'
                else get_oper_color(sub_oper) if not is_lambda_args else COLOR_ARG
            )

            arg_shift = context.shift
            with context.printer.color(sub_oper_color):
                if item.is_quote:
                    context.printer.out('\'')
                context.printer.out('(')
            if is_block_oper:
                arg_shift += 1
                context.printer.endl()
                print_shift(arg_shift)

            sub_ctx = Context(parent=context, shift=arg_shift, is_lambda_args=is_lambda_args)
            print_list(out, item, callables, sub_ctx)
            if is_lambda_args:
                context.lambda_args.update(sub_ctx.lambda_args)
            with context.printer.color(sub_oper_color):
                context.printer.out(')')
            print_note(context, note)
            if sub_oper in ('return', 'let', 'declare'):
                context.printer.endl()
                if is_last:
                    print_shift(context.shift - 1)
                else:
                    print_shift(context.shift)
            elif not is_last:
                context.printer.out(' ')
        elif isinstance(item, Element):
            if item.is_quote:
                with context.printer.color(COLOR_LITERAL):
                    context.printer.out('\'')
            if item.is_quoted_str:
                with context.printer.color(COLOR_STRING_LITERAL):
                    context.printer.out('"')
                    context.printer.out(item.value.encode('unicode_escape').decode('utf-8'))
                    context.printer.out('"')
            else:
                color = get_oper_color(oper) if (oper and pos == 0) else COLOR_LITERAL
                with context.printer.color(color):
                    context.printer.out(str(item.value))
            print_note(context, note)
            if not is_last:
                context.printer.out(' ')
        elif isinstance(item, Reference):
            if context.is_lambda_args:
                color = COLOR_ARG
                context.lambda_args.add(item.alias)
            else:
                color = COLOR_ARG if (item.alias in context.lambda_args) else COLOR_REF
            with context.printer.color(color):
                context.printer.out('$')
                context.printer.out(str(item.alias))
            print_note(context, note)

            if not is_last:
                context.printer.out(' ')
        else:
            raise Exception("Unknown list element type:", item.__class__.__name__)

    if is_long_oper:
        context.shift -= 1
        context.printer.endl()
        print_shift(context.shift)


class Macro:
    def __init__(self, definition, is_leaf):
        self.definition = definition
        self.is_leaf = is_leaf


def collect_refs(the_list):
    table = {}
    ref_counts = {}
    tail = None
    is_leaf = True
    scanning_ref_id = None

    if len(the_list.list) > 2:
        oper = None
        item = the_list.list[0]
        if isinstance(item, Element) and not item.is_quote and not item.is_quoted_str and isinstance(item.value, str):
            oper = item.value
        ref = the_list.list[1]
        if oper == 'let' and isinstance(ref, Reference):
            scanning_ref_id = ref.alias
            tail = the_list.list[2:]

    if tail is None:
        tail = the_list.list

    for item in tail:
        if isinstance(item, List):
            sub_table, sub_counts, sub_is_leaf = collect_refs(item)
            if not sub_is_leaf:
                is_leaf = False
            table.update(sub_table)
            for ref, cnt in sub_counts.items():
                ref_counts[ref] = ref_counts.get(ref, 0) + cnt
        elif isinstance(item, Reference):
            is_leaf = False
            ref = item.alias
            ref_counts[ref] = ref_counts.get(ref, 0) + 1

    if scanning_ref_id is not None:
        table[scanning_ref_id] = Macro(tail, is_leaf)

    return table, ref_counts, is_leaf


def simple_enough_macro(the_list):
    simple = True
    for item in the_list:
        if isinstance(item, List):
            oper = get_oper(item)
            if oper in NEVER_INLINE:
                return False
            if oper == 'lambda' and len(item.list) > 1 and isinstance(item.list[1], List):
                lambda_args = set()
                for sub_item in item.list[1].list:
                    if isinstance(sub_item, Reference):
                        lambda_args.add(sub_item.alias)
                is_simple_lambda = False
                for def_item in item.list[2:]:
                    if not isinstance(def_item, Reference):
                        break
                    if def_item.alias not in lambda_args:
                        break
                else:
                    is_simple_lambda = True
                simple = simple and is_simple_lambda
                continue
            if oper is None:
                if not simple_enough_macro(item.list):
                    simple = False
                    break
                continue
            simple = simple and (oper in SIMPLE_OPERATORS)
    # if not simple:
    #     print('Simplicity broken on: ' << item.list, file=sys.stderr)
    return simple


def prevent_replacement(table, ref_id):
    definition = table[ref_id].definition
    if len(definition) == 1 and isinstance(definition[0], List):
        oper = get_oper(definition[0])
        if oper in NEVER_INLINE:
            return True
    return False


def should_replace_immediately(ref_id, table, ref_counts):
    return (ref_counts.get(ref_id, 0) == 1 or table[ref_id].is_leaf) and not prevent_replacement(table, ref_id)


class ReplaceRefsOptions:
    def __init__(self, max_uses_for_inlining=3):
        self.max_uses_for_inlining = max_uses_for_inlining


def replace_refs(the_list, table, ref_counts, options: ReplaceRefsOptions, current_let_ref_id=None):
    rebuilt = []
    did_replace = set()

    lets = []

    for pos, item in enumerate(the_list):
        if isinstance(item, List):
            sub_list = item.list
            ref_id = None
            if len(sub_list) > 2:
                if (
                    isinstance(sub_list[0], Element)
                    and not sub_list[0].is_quote
                    and not sub_list[0].is_quoted_str
                    and sub_list[0].value == 'let'
                    and isinstance(sub_list[1], Reference)
                ):
                    ref_id = sub_list[1].alias
                    # Remove let definitions that are guaranteed to be replaced
                    if not should_replace_immediately(ref_id, table, ref_counts):
                        l = List(item.is_quote)
                        l.list, sub_did_replace = replace_refs(
                            sub_list, table, ref_counts, options, current_let_ref_id=ref_id
                        )
                        lets.append((ref_id, l, sub_did_replace))
                    continue
            l = List(item.is_quote)
            l.list, sub_did_replace = replace_refs(sub_list, table, ref_counts, options, current_let_ref_id=None)
            did_replace |= sub_did_replace
            rebuilt.append(l)
            continue

        if isinstance(item, Reference):
            ref_id = item.alias
            if ref_id == current_let_ref_id:
                rebuilt.append(item)
                continue

            if ref_id in table:
                should_replace = False
                if should_replace_immediately(ref_id, table, ref_counts):
                    should_replace = True

                if not prevent_replacement(table, ref_id):
                    # this will copy referenced list before mutating
                    replaced, sub_did_replace = replace_refs(table[ref_id].definition, table, ref_counts, options)

                    # if not should_replace:
                    #     oper = get_oper_from_raw_list(the_list)
                    #     if oper == 'DqPhyStage' and pos == 2:
                    #         should_replace = True

                    if not should_replace and ref_counts.get(ref_id) <= options.max_uses_for_inlining:
                        # Maybe we still can decide to replace if the content is simple enough
                        should_replace = simple_enough_macro(replaced)

                if should_replace:
                    rebuilt += replaced
                    did_replace.add(ref_id)
                    did_replace |= sub_did_replace
                else:
                    rebuilt.append(item)
                continue

        rebuilt.append(item)

    filtered_lets = []
    lets.reverse()
    for let_id, let_content, let_replace_set in lets:
        if let_id not in did_replace:
            filtered_lets.append(let_content)
            did_replace |= let_replace_set
    filtered_lets.reverse()

    return filtered_lets + rebuilt, did_replace


def simplify_blocks(the_list):
    """
    Replace (block '( (return a b c) ) with a b c.
    Returns a copy of the program, does not mutate anything in-place
    """
    result = []

    for item in the_list:
        if isinstance(item, List):
            if get_oper(item) == 'block' and not item.is_quote and len(item.list) == 2:
                block_content = item.list[1]
                if isinstance(block_content, List) and len(block_content.list) == 1:
                    maybe_return = block_content.list[0]
                    if get_oper(maybe_return) == 'return':
                        result += simplify_blocks(maybe_return.list[1:])
                        continue
            new_list = List(item.is_quote)
            new_list.list = simplify_blocks(item.list)
            result.append(new_list)
        else:
            result.append(item)

    return result


def read_string(line, pos):
    esc = False
    res = ''
    while pos < len(line):
        if esc:
            res += line[pos]
            pos += 1
            esc = False
            continue
        if line[pos] == '\\':
            esc = True
            pos += 1
            continue
        if line[pos] == '"':
            return res, pos + 1
        res += line[pos]
        pos += 1
    raise Exception("unterminated quoted string")


def read_num(line, pos):
    start = pos
    while pos < len(line):
        if not line[pos].isdigit():
            return int(line[start:pos]), pos
        pos += 1
    return int(line[start:]), pos


def read_keyword(line, pos):
    start = pos
    while pos < len(line):
        if line[pos] == ')' or line[pos].isspace():
            return line[start:pos], pos
        pos += 1
    return line[start:]


def parse(lines):
    curr_stack = [List(False)]
    is_quote = False

    def push(item):
        curr_stack[-1].list.append(item)

    for line in lines:
        line = line.strip()
        if not line:
            continue
        pos = 0
        while pos < len(line):
            if line[pos] == '\'':
                is_quote = True
                pos += 1
                continue

            if line[pos] == '(':
                l = List(is_quote)
                push(l)
                curr_stack.append(l)
                pos += 1
            elif line[pos] == '"':
                tok, pos = read_string(line, pos + 1)
                push(Element(is_quote, tok, is_quoted_str=True))
            elif line[pos].isdigit():
                tok, pos = read_num(line, pos)
                push(Element(is_quote, tok))
            elif line[pos] == ')':
                curr_stack.pop()
                pos += 1
            elif line[pos] == '$':
                tok, pos = read_num(line, pos + 1)
                push(Reference(tok))
            elif line[pos].isspace():
                pos += 1
            else:
                tok, pos = read_keyword(line, pos)
                push(Element(is_quote, tok))
            is_quote = False

    return curr_stack[0]


class NodeDescr:
    def __init__(self, name, base, match_callable, children_names):
        self.name = name
        self.base = base
        self.children_names = children_names
        self.match_callable = match_callable


def parse_node_file(node_file):
    result = {}
    js = json.loads(node_file.read().strip())
    for node in js.get('Nodes', []):
        name = node.get('Name', None)
        if not name:
            continue
        base = node.get('Base', None)
        match = node.get('Match', {})
        match_type = match.get('Type', None)
        match_callable = None
        if match_type == 'Callable':
            match_callable = match.get('Name', None)
        child_names = {}
        for child in node.get('Children', []):
            child_index = int(child.get('Index', -1))
            child_name = child.get('Name', None)
            if not child_name:
                child_name = None
            if child_name:
                child_names[child_index] = child_name
        result[name] = NodeDescr(name, base, match_callable, child_names)

    return result


def inherit_children(node_descriptions):
    for node in node_descriptions.values():
        children_names = dict(node.children_names)
        parent_node = node
        while parent_node.base:
            parent_node = node_descriptions.get(parent_node.base, None)
            if parent_node is None:
                break
            new_children_names = dict(parent_node.children_names)
            new_children_names.update(children_names)
            children_names = new_children_names
        node.children_names = children_names


def add_hardcoded(node_descriptions):
    def try_add(callable, children):
        if callable in node_descriptions:
            return
        node_descriptions[callable] = NodeDescr(callable, None, callable, children)

    try_add('WideTakeBlocks', {0: 'Input', 1: 'Count'})


def build_callable_index(node_descriptions):
    result = {}
    for node in node_descriptions.values():
        if not node.match_callable:
            continue
        if len(node.children_names) == 1 and node.children_names.get(0, None) in (
            'Literal',
            'Type',
            'ItemType',
            'OptionalType',
            'Input',
            'Apply',
            'Callable',
        ):
            continue
        if len(node.children_names) == 2 and node.children_names.get(0, None) == 'Left':
            continue
        result[node.match_callable] = node

    def add_alias(alias, original):
        if alias not in result and original in result:
            result[alias] = result[original]

    add_alias('WideCondense1', 'Condense1')
    add_alias('NarrowSqueezeToDict', 'SqueezeToDict')

    return result


STAGE_OPERS = {'DqPhyStage', 'DqStage'}

WIDE_MAP_OPERS = {'WideMap', 'ExpandMap'}

FLOW_PASSTHROUGH_OPERS = {
    'ToFlow',
    'FromFlow',
    'ToStream',
    'FromStream',
    'AsFlow',
    'WideToBlocks',
    'WideFilter',
    'WideSort',
    'WideTop',
    'WideTopSort',
    'WideTakeBlocks',
    'WideSkipBlocks',
    'Take',
    'Skip',
}

BLOCK_LENGTH_NAME = '_block_length'
UNKNOWN_NAME = '?'


def as_index(item):
    if not isinstance(item, Element):
        return None
    try:
        return int(item.value)
    except (TypeError, ValueError):
        return None


def format_names(names):
    return ', '.join(name if name else UNKNOWN_NAME for name in names)


class ColumnResolver:
    def __init__(self, program: List):
        self.ref_table, _, _ = collect_refs(program)

    def deref(self, item):
        seen = set()
        while isinstance(item, Reference) and item.alias not in seen:
            seen.add(item.alias)
            macro = self.ref_table.get(item.alias)
            if macro is None or len(macro.definition) != 1:
                return None
            item = macro.definition[0]
        return item if isinstance(item, List) else None

    @staticmethod
    def literal(item):
        return str(item.value) if isinstance(item, Element) else None

    @staticmethod
    def child(the_list: List, pos):
        return the_list.list[pos] if pos < len(the_list.list) else None

    def struct_member_names(self, node):
        struct = self.deref(node)
        if struct is None or get_oper(struct) != 'StructType':
            return None
        names = []
        for item in struct.list[1:]:
            member = self.deref(item)
            names.append(self.literal(member.list[0]) if member and member.list else None)
        return names

    def stage_wide_names(self, stage: List):
        settings = self.deref(self.child(stage, 3))
        if settings is None:
            return None
        for item in settings.list:
            setting = self.deref(item)
            if setting is None or len(setting.list) < 2:
                continue
            if self.literal(setting.list[0]) == '_wide_channels':
                return self.struct_member_names(setting.list[1])
        return None

    def connection_names(self, node):
        connection = self.deref(node)
        if connection is None:
            return None
        output = self.deref(self.child(connection, 1))
        if output is None or get_oper(output) != 'TDqOutput':
            return None
        stage = self.deref(self.child(output, 1))
        return self.stage_wide_names(stage) if stage is not None and get_oper(stage) in STAGE_OPERS else None

    def wide_map_names(self, node: List, input_names, declared):
        lam = self.deref(self.child(node, 2))
        if lam is None or get_oper(lam) != 'lambda' or len(lam.list) < 3:
            return None
        args_list = self.deref(lam.list[1])
        if args_list is None:
            return None

        args = [item.alias for item in args_list.list if isinstance(item, Reference)]
        arg_names = list(input_names) if input_names else []
        if len(arg_names) + 1 == len(args):
            arg_names.append(BLOCK_LENGTH_NAME)
        arg_pos = {alias: pos for pos, alias in enumerate(args)}

        result = [self.expr_name(body, arg_pos, arg_names) for body in lam.list[2:]]
        if declared and len(result) == len(declared):
            result = [name or declared[pos] for pos, name in enumerate(result)]
        if result:
            self.note(node, 2, result, prefix='→ ')
        return result

    def expr_name(self, body, arg_pos, arg_names):
        if isinstance(body, Reference):
            pos = arg_pos.get(body.alias)
            return arg_names[pos] if pos is not None and pos < len(arg_names) else None
        if isinstance(body, List) and get_oper(body) == 'Member' and len(body.list) > 2:
            return self.literal(body.list[2])
        return None

    def map_join_names(self, node: List, env):
        left = self.columns(self.child(node, 1), env)
        renames = {}
        for pos, is_left in ((6, True), (7, False)):
            items = self.deref(self.child(node, pos))
            if items is None:
                return None
            for item_pos in range(0, len(items.list) - 1, 2):
                source = items.list[item_pos]
                target = as_index(items.list[item_pos + 1])
                if target is None or target < 0:
                    return None
                source_index = as_index(source)
                if not is_left:
                    renames[target] = self.literal(source)
                elif left is not None and source_index is not None and 0 <= source_index < len(left):
                    renames[target] = left[source_index]
                else:
                    renames[target] = None
        return [renames.get(pos) for pos in range(max(renames) + 1)] if renames else None

    def index_names(self, node, names):
        index_list = self.deref(node)
        if index_list is None or not names:
            return None

        resolved = []
        for item in index_list.list:
            index = as_index(item)
            if index is None:
                tuple_item = self.deref(item)
                index = as_index(tuple_item.list[0]) if tuple_item and tuple_item.list else None
            if index is None:
                return None
            resolved.append(names[index] if 0 <= index < len(names) else UNKNOWN_NAME)
        return resolved

    @staticmethod
    def note(node: List, pos, names, prefix=''):
        if not names:
            return
        notes = getattr(node, 'child_notes', None) or {}
        notes[pos] = prefix + format_names(names)
        node.child_notes = notes

    def note_indexes(self, node: List, pos, names):
        self.note(node, pos, self.index_names(self.child(node, pos), names))

    def columns(self, node, env, declared=None):
        if isinstance(node, Reference):
            if node.alias in env:
                return env[node.alias]
            node = self.deref(node)
        if not isinstance(node, List):
            return None

        oper = get_oper(node)
        if oper in ('WideSort', 'WideTopSort', 'WideTop'):
            names = self.columns(self.child(node, 1), env)
            self.note_indexes(node, 2, names)
            return names

        if oper in FLOW_PASSTHROUGH_OPERS:
            return self.columns(self.child(node, 1), env, declared)

        if oper == 'WideFromBlocks':
            block_names = list(declared) + [BLOCK_LENGTH_NAME] if declared else None
            names = self.columns(self.child(node, 1), env, block_names)
            return names[:-1] if names and names[-1] == BLOCK_LENGTH_NAME else names

        if oper in WIDE_MAP_OPERS:
            input_names = self.columns(self.child(node, 1), env)
            return self.wide_map_names(node, input_names, declared)

        if oper == 'BlockHashJoinCore':
            left = self.columns(self.child(node, 1), env)
            right = self.columns(self.child(node, 2), env)
            self.note(node, 1, left)
            self.note(node, 2, right)
            self.note_indexes(node, 4, left)
            self.note_indexes(node, 5, right)
            return list(left) + list(right) if left is not None and right is not None else None

        if oper == 'MapJoinCore':
            left = self.columns(self.child(node, 1), env)
            self.note(node, 1, left)
            self.note_indexes(node, 4, left)
            return self.map_join_names(node, env)

        if oper in ('FlatMap', 'OrderedFlatMap'):
            lam = self.deref(self.child(node, 2))
            if lam is not None and get_oper(lam) == 'lambda':
                return self.columns(self.child(lam, 2), env, declared)

        return None

    def annotate_stage(self, stage: List):
        inputs = self.deref(self.child(stage, 1))
        program = self.deref(self.child(stage, 2))
        if inputs is None or program is None or get_oper(program) != 'lambda':
            return

        input_names = []
        for item in inputs.list:
            names = self.connection_names(item)
            connection = self.deref(item)
            if connection is not None and get_oper(connection) in ('DqCnHashShuffle', 'DqCnMerge'):
                self.note_indexes(connection, 2, names)
            input_names.append(names)
        args = self.deref(self.child(program, 1))
        env = {}
        if args is not None:
            notes = {}
            for pos, arg in enumerate(args.list):
                if not isinstance(arg, Reference) or pos >= len(input_names):
                    continue
                names = input_names[pos]
                if names is not None:
                    env[arg.alias] = names
                if names:
                    notes[pos] = format_names(names)
            if notes:
                args.child_notes = notes

        declared = self.stage_wide_names(stage)
        bodies = program.list[2:]
        for pos, body in enumerate(bodies):
            self.columns(body, env, declared if pos == len(bodies) - 1 else None)

    def annotate(self, node):
        if not isinstance(node, List):
            return
        if get_oper(node) in STAGE_OPERS:
            self.annotate_stage(node)
            return
        for item in node.list:
            self.annotate(item)


def parse_and_process(lines, replace_refs_options: ReplaceRefsOptions):
    program = parse(lines)
    ref_table, ref_counts, _ = collect_refs(program)
    replaced_program = List(False)
    replaced_program.list, _ = replace_refs(program.list, ref_table, ref_counts, replace_refs_options)
    simplified_program = List(False)
    simplified_program.list = simplify_blocks(replaced_program.list)
    ColumnResolver(simplified_program).annotate(simplified_program)
    return simplified_program


def htmlmain():
    input = sys.stdin.read()
    program = parse_and_process(input.split('\n'), ReplaceRefsOptions())
    printer = HtmlPrinter()
    print_list(sys.stdout, program, {}, Context(tabstops=False, printer=printer))
    printer.finalize()


def climain():
    import os.path

    argparser = argparse.ArgumentParser()
    argparser.add_argument('-n', '--nodes', default=[], action='append')
    argparser.add_argument('-r', '--repo', default=None)
    argparser.add_argument(
        '--html',
        action='store_true',
        help='Write HTML: <style> for .syntax_* classes, then <pre> with colored AST to stdout',
    )
    argparser.add_argument('-t', '--tabstops', action='store_true', default=False)
    argparser.add_argument(
        '-i',
        '--max-uses-for-inlining',
        type=int,
        default=3,
        help="Don't inline let macros that are used more than this number of times",
    )
    args = argparser.parse_args()

    tabstops = args.tabstops

    node_descrs = {}
    node_files = []

    repo = args.repo
    repo_subpaths = {'ydb': 'ydb', 'yql': 'yql'}

    def checkfile(repo_dir, name):
        return os.path.exists(os.path.join(repo_dir, name))

    if not repo and __file__:
        repo_dir = os.path.dirname(os.path.abspath(__file__))

        while repo_dir:
            if checkfile(repo_dir, '.arcadia.root') and (
                checkfile(repo_dir, 'contrib/ydb') or checkfile(repo_dir, 'ydb')
            ):
                print('Auto-using YQL callable definitions from %s' % repo_dir, file=sys.stderr)
                repo = repo_dir
                break

            if os.path.ismount(repo_dir):
                break
            repo_dir = os.path.dirname(repo_dir)

    if repo:
        if checkfile(repo, 'contrib/ydb'):
            repo_subpaths['ydb'] = 'contrib/ydb'
        node_files += [
            os.path.join(repo, path)
            for path in [
                p.format(**repo_subpaths)
                for p in [
                    '{ydb}/library/yql/dq/expr_nodes/dq_expr_nodes.json',
                    '{ydb}/core/kqp/expr_nodes/kqp_expr_nodes.json',
                    '{yql}/essentials/core/expr_nodes/yql_expr_nodes.json',
                ]
            ]
        ]

    node_files += args.nodes
    for node_file in node_files:
        with open(node_file, 'rt') as inf:
            node_descrs.update(parse_node_file(inf))

    # print('Loaded %d nodes' % len(node_descrs), file=sys.stderr)
    add_hardcoded(node_descrs)
    inherit_children(node_descrs)
    callables = build_callable_index(node_descrs)
    # print('%d callables' % len(callables), file=sys.stderr)

    input = sys.stdin.read()
    program = parse_and_process(
        input.split('\n'),
        ReplaceRefsOptions(max_uses_for_inlining=args.max_uses_for_inlining),
    )
    printer = HtmlPrinter() if args.html else TerminalPrinter()
    print_list(sys.stdout, program, callables, Context(tabstops=tabstops, printer=printer))
    printer.finalize()


if __name__ == '__main__':
    climain()
