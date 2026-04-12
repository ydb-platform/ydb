#!/usr/bin/env python3

import sys
import argparse
import json
from typing import Self
import itertools

if __name__ == '__main__':
    # TODO: move astparse to a shared library
    import os.path
    script_dir = os.path.dirname(os.path.abspath(__file__))
    sys.path.append(os.path.join(script_dir, '../pretty_ast'))  # TODO: move to a library
    from astparse import parse, Element, List, Reference
    from pretty_ast import TerminalPrinter, Context, print_list


def get_oper_from_raw_list(the_list):
    oper = None
    if len(the_list) >= 1 and isinstance(the_list[0], Element):
        item = the_list[0]
        if not item.is_quote and not item.is_quoted_str and isinstance(item.value, str):
            oper = item.value
    return oper


def get_oper(the_list):
    return get_oper_from_raw_list(the_list.list)


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


def replace_refs(the_list, table):
    rebuilt = []

    for item in the_list:
        if isinstance(item, List):
            sub_list = item.list
            oper = get_oper_from_raw_list(sub_list)
            if (
                len(sub_list) > 1
                and oper == 'let'
            ):
                continue
            #print('Replacing: ', oper)
            l = List(item.is_quote)
            l.list = replace_refs(sub_list, table)
            rebuilt.append(l)
            continue

        # rebuilt.append(item)
        # continue

        if isinstance(item, Reference):
            ref_id = item.alias

            if ref_id in table:
                replaced = replace_refs(table[ref_id].definition, table)
                rebuilt += replaced
                continue

        rebuilt.append(item)

    return rebuilt

def scan_for(the_list, oper_name, collector):
    oper = get_oper_from_raw_list(the_list)
    if oper == oper_name:
        collector.append(the_list)

    for item in the_list:
        if isinstance(item, List):
            scan_for(item.list, oper_name, collector)


class YqlType:
    def __init__(self):
        self.optional = False
        self.items = []
        self.names = []
        self.data = None
        self.tuple = False  # tuple or struct
        self.multi = False

def convert_type(expr):
    oper = get_oper(expr)
    if oper in ('FlowType', 'StreamType'):
        return convert_type(expr.list[1])
    elif oper == 'OptionalType':
        subtype = convert_type(expr.list[1])
        subtype.optional = True
        return subtype
    elif oper == 'MultiType' or oper == 'TupleType':
        res = YqlType()
        res.multi = (oper == 'MultiType')
        res.tuple = (oper == 'TupleType')
        res.items = [convert_type(subexpr) for subexpr in expr.list[1:]]
        return res
    elif oper == 'StructType':
        res = YqlType()
        res.tuple = True
        for field in expr.list[1:]:
            res.items.append(convert_type(field.list[1]))
            res.names.append(field.list[0].value)
        return res
    elif oper == 'DataType':
        if not isinstance(expr.list[1], Element) or not expr.list[1].is_quote:
            raise Exception('Malformed DataType')
        res = YqlType()
        res.data = expr.list[1].value
        return res
    else:
        raise Exception("Unsupported type: ", oper)

def yql_type_to_str(yql_type):
    if yql_type.tuple:
        types = [yql_type_to_str(subitem) for subitem in yql_type.items]
        substrings = []
        for type_string, name in itertools.zip_longest(types, yql_type.names):
            s = type_string
            if name:
                s = name + ': ' + s
            substrings.append(s)
        res = '(' + ', '.join(substrings) + ')'
    else:
        res = yql_type.data
    if yql_type.optional:
        res = res + '?'
    return res

def replace_args(the_list, remap_table):
    for item in the_list:
        if isinstance(item, Reference) and item.alias in remap_table:
            item.alias = remap_table[item.alias]
            continue
        if isinstance(item, List):
            replace_args(item.list, remap_table)
            continue

def print_lambda_with_custom_args(yql_lambda, new_args, title=None):
    print()
    if title:
        print('%s:' % title)
    print()
    args = yql_lambda.list[1].list
    remap_table = {}
    if len(args) != len(new_args):
        raise Exception('Arg count mismatch when renaming lambda args: lambda has %d args, names provided for %d args' % (len(args), len(new_args)))
    for idx, arg in enumerate(args):
        remap_table[arg.alias] = new_args[idx]

    replace_args(yql_lambda.list, remap_table) # mutates in-place

    print('```')
    prog = List(False)
    prog.list = [yql_lambda]
    printer = TerminalPrinter()
    print_list(sys.stdout, prog, {}, Context(tabstops=4, printer=printer))
    printer.finalize()
    print('```')

def parse_and_process(file_name):
    with open(file_name, 'rt') as inf:
        input = inf.read().split('\n')
    program = parse(input)
    ref_table, ref_counts, _ = collect_refs(program)

    phy_stages = []
    combiners = []
    scan_for(program.list, 'DqPhyStage', phy_stages)

    print (file_name, file=sys.stderr)
    for stage in phy_stages:
        # The stage content is its 2nd argument; the first one contains references to inputs (including previous stages)
        stage_content = stage[2]
        if isinstance(stage_content, List):
            stage_list = stage_content.list
        else:
            stage_list = [stage_content.alias]
        replaced_stage = replace_refs(stage_list, ref_table)
        aggs = scan_for(replaced_stage, 'DqPhyHashCombine', combiners)

    def print_expr(expr):
        prog = List(False)
        prog.list = [expr]
        printer = TerminalPrinter()
        print_list(sys.stdout, prog, {}, Context(tabstops=4, printer=printer))
        printer.finalize()

    if not combiners:
        return

    print('### ' + os.path.basename(file_name))
    for combiner in combiners:
        _, _, mem_limit, key_lambda, init_lambda, update_lambda, finalize_lambda, type_info = combiner
        is_aggregate = not mem_limit.value
        in_type, key_type, state_type, result_type = type_info.list
        print()
        prefix = '####'
        if is_aggregate:
            print(prefix, 'Aggregator:')
        else:
            print(prefix, 'Combiner:')
        print()

        def type_list(expr):
            converted = convert_type(expr)
            if converted.multi:
                items = [yql_type_to_str(subitem) for subitem in converted.items]
            else:
                items = [yql_type_to_str(converted)]
            return items

        tables = []
        tables.append(('Input', type_list(in_type)))
        tables.append(('Key', type_list(key_type)))
        tables.append(('State', type_list(state_type)))
        tables.append(('Output', type_list(result_type)))
        max_len = max([len(t[1]) for t in tables])
        print('#|')
        for title, row in tables:
            printed_row = ['`' + fld + '`' for fld in row]
            printed_row += ([''] * (max_len - len(row)))
            print('|| ' + title + ' | ' + ' | '.join(printed_row) + ' ||')
        print('|#')

        item_args = ['input_%d' % idx for idx in range(len(type_list(in_type)))]
        key_args = ['key_%d' % idx for idx in range(len(type_list(key_type)))]
        state_args = ['state_%d' % idx for idx in range(len(type_list(state_type)))]

        print()
        print('{% cut "lambdas" %}')
        print_lambda_with_custom_args(key_lambda, item_args, 'Key')
        print_lambda_with_custom_args(init_lambda, key_args + item_args, 'Init state')
        print_lambda_with_custom_args(update_lambda, key_args + item_args + state_args, 'Update state')
        print_lambda_with_custom_args(finalize_lambda, key_args + state_args, 'Build result')
        print('{% endcut %}')



def climain():
    input_files = sys.argv[1:]
    for f in input_files:
        parse_and_process(f)

if __name__ == '__main__':
    climain()
