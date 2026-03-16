# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#

import ast
import sys
from typing import List

from mo_dots import is_data, is_null, literal_field, unliteral_field
from mo_future import text, number_types, binary_type, flatten
from mo_parsing import *
from mo_parsing import whitespaces
from mo_parsing.utils import is_number, listwrap

from mo_sql_parsing import simple_op


class Call(object):
    __slots__ = ["op", "args", "kwargs"]

    def __init__(self, op, args: List, kwargs: Dict):
        self.op = op
        self.args = args
        self.kwargs = kwargs

    def __str__(self):
        return f"{self.op}({self.args}, {self.kwargs})"


IDENT_CHAR = Regex("[@_$0-9A-Za-zÀ-ÖØ-öø-ƿ]").expr.parser_config.include
FIRST_IDENT_CHAR = "".join(set(IDENT_CHAR) - set("0123456789"))
SQL_NULL = Call("null", [], {})

null_locations = []


def keyword(keywords):
    return And([Keyword(k, caseless=True) for k in keywords.split(" ")]).set_parser_name(keywords) / keywords.replace(
        " ", "_"
    )


def flag(keywords):
    """
    RETURN {keywords: True}
    """
    return (keyword(keywords) / True)(keywords.replace(" ", "_"))


def assign(key: str, value: ParserElement):
    return keyword(key).suppress() + Group(value)(key.replace(" ", "_"))


scrub_op = simple_op
fmap = {}


def scrub(result):
    if result is SQL_NULL:
        return SQL_NULL
    elif result == None:
        return None
    elif isinstance(result, text):
        return result
    elif isinstance(result, binary_type):
        return result.decode("utf8")
    elif isinstance(result, number_types):
        return result
    elif isinstance(result, Call):
        kwargs = scrub(result.kwargs)
        args = scrub(result.args)
        op = result.op
        if args is SQL_NULL:
            null_locations.append((kwargs, op))
        return scrub_op(fmap.get(op, op), args, kwargs)
    elif isinstance(result, dict) and not result:
        return result
    elif isinstance(result, list):
        output = [rr for r in result for rr in [scrub(r)] if rr is not None]

        if not output:
            return None
        elif len(output) == 1:
            return output[0]
        else:
            for i, v in enumerate(output):
                if v is SQL_NULL:
                    null_locations.append((output, i))
            return output
    else:
        # ATTEMPT A DICT INTERPRETATION
        try:
            kv_pairs = list(result.items())
        except Exception as c:
            print(c)
        output = {k: vv for k, v in kv_pairs for vv in [scrub(v)] if not is_null(vv)}
        if isinstance(result, dict) or output:
            for k, v in output.items():
                if v is SQL_NULL:
                    null_locations.append((output, k))
            return output
        return scrub(list(result))


def _chunk(values, size):
    acc = []
    for v in values:
        acc.append(v)
        if len(acc) == size:
            yield acc
            acc = []
    if acc:
        yield acc


def to_json_operator(tokens):
    # ARRANGE INTO {op: params} FORMAT
    length = len(tokens.tokens)
    if length == 2:
        if tokens.tokens[1].type.parser_name == "cast":
            return Call("cast", list(tokens), {})
        # UNARY OPERATOR
        op = tokens.tokens[0].type.parser_name
        if is_number(tokens[1]):
            if op == "neg":
                return -tokens[1]
            elif op == "pos":
                return tokens[1]
        return Call(op, [tokens[1]], {})
    elif length == 5:
        # TRINARY OPERATOR
        return Call(tokens.tokens[1].type.parser_name, [tokens[0], tokens[2], tokens[4]], {})

    op = tokens[1]
    if not isinstance(op, text):
        op = op.type.parser_name
    op = binary_ops.get(op, op)
    if op == "eq":
        if tokens[2] is SQL_NULL:
            return Call("missing", tokens[0], {})
        elif tokens[0] is SQL_NULL:
            return Call("missing", tokens[2], {})
    elif op == "neq":
        if tokens[2] is SQL_NULL:
            return Call("exists", tokens[0], {})
        elif tokens[0] is SQL_NULL:
            return Call("exists", tokens[2], {})
    elif op == "eq!":
        if tokens[2] is SQL_NULL:
            return Call("missing", tokens[0], {})
        elif tokens[0] is SQL_NULL:
            return Call("missing", tokens[2], {})
    elif op == "ne!":
        if tokens[2] is SQL_NULL:
            return Call("exists", tokens[0], {})
        elif tokens[0] is SQL_NULL:
            return Call("exists", tokens[2], {})
    elif op == "is":
        if tokens[2] is SQL_NULL:
            return Call("missing", tokens[0], {})
        else:
            return Call("exists", tokens[0], {})
    elif op == "is_not":
        if tokens[2] is SQL_NULL:
            return Call("exists", tokens[0], {})
        else:
            return Call("missing", tokens[0], {})
    elif op == "regexp_i":
        return Call("regexp", [tokens[0], tokens[2]], {"ignore_case": True})
    elif op == "not_regexp_i":
        return Call("not_regexp", [tokens[0], tokens[2]], {"ignore_case": True})

    operands = [tokens[0], tokens[2]]
    binary_op = Call(op, operands, {})

    if op in {"add", "mul", "and", "or", "concat", "binary_and", "binary_or"}:
        # ASSOCIATIVE OPERATORS
        acc = []
        for operand in operands:
            while isinstance(operand, ParseResults) and isinstance(operand.type, Group):
                # PARENTHESES CAUSE EXTRA GROUP LAYERS
                operand = operand[0]
                if isinstance(operand, ParseResults) and isinstance(operand.type, Forward):
                    operand = operand[0]

            if isinstance(operand, Call) and operand.op == op:
                acc.extend(operand.args)
            elif isinstance(operand, list):
                acc.append(operand)
            elif isinstance(operand, dict) and operand.get(op):
                acc.extend(operand.get(op))
            else:
                acc.append(operand)
        binary_op = Call(op, acc, {})
    return binary_op


def to_offset(tokens):
    expr, offset = tokens.tokens
    return Call("get", [expr, *offset], {})


def to_window_mod(tokens):
    expr, window = tokens.tokens
    return Call("value", [expr], {**window})


def as_literal(token):
    if isinstance(token, number_types):
        return token
    if is_data(token) and "literal" in token:
        return token["literal"]
    if isinstance(token, ParseResults) and isinstance(token.type, Group) and token.length() == 1:
        return as_literal(token[0])


def to_tuple_call(token, index, string):
    # IS THIS ONE VALUE IN (), OR MANY?
    tokens = list(token)
    if len(tokens) == 1:
        return tokens
    if all(isinstance(r, number_types) for r in tokens):
        return [tokens]

    candidate = [as_literal(t) for t in tokens]
    if all(candidate):
        return {"literal": candidate}

    return [tokens]


binary_ops = {
    "::": "cast",
    "COLLATE": "collate",
    ":": "get",
    "||": "concat",
    "->": "json_get",
    "->>": "json_get_text",
    "#>": "json_path",
    "#>>": "json_path_text",
    "@>": "json_subsumes",
    "<@": "json_subsumed_by",
    "?": "json_contains",
    "?|": "json_contains_any",
    "?&": "json_contains_all",
    "#-": "json_path_del",
    "*": "mul",
    "/": "div",
    "%": "mod",
    "+": "add",
    "-": "sub",
    "&": "binary_and",
    "|": "binary_or",
    "<": "lt",
    "<=": "lte",
    ">": "gt",
    ">=": "gte",
    "=": "eq",
    "==": "eq",
    "is distinct from": "eq!",  # https://sparkbyexamples.com/apache-hive/hive-relational-arithmetic-logical-operators/
    "is_distinct_from": "eq!",
    "is not distinct from": "ne!",
    "is_not_distinct_from": "ne!",
    "<=>": "eq!",  # https://sparkbyexamples.com/apache-hive/hive-relational-arithmetic-logical-operators/
    "!=": "neq",
    "<>": "neq",
    "!~*": "not_regexp_i",
    "!~": "not_regexp",
    "~*": "regexp_i",
    "~": "regexp",
    "not in": "nin",
    "in": "in",
    "is_not": "neq",
    "is": "eq",
    "similar_to": "similar_to",
    "like": "like",
    "rlike": "rlike",
    "ilike": "ilike",
    "not like": "not_like",
    "not_like": "not_like",
    "not rlike": "not_rlike",
    "not_rlike": "not_rlike",
    "not ilike": "not_ilike",
    "not_ilike": "not_ilike",
    "not_simlilar_to": "not_similar_to",
    "or": "or",
    "and": "and",
    ":=": "assign",
    "union": "union",
    "union_all": "union_all",
    "union all": "union_all",
    "except": "except",
    "minus": "minus",
    "intersect": "intersect",
}

is_set_op = ("union", "union_all", "except", "minus", "intersect")


def to_trim_call(tokens):
    frum = tokens["from"]
    if not frum:
        return Call("trim", [tokens["chars"]], {"direction": tokens["direction"]})
    return Call("trim", [frum], {"characters": tokens["chars"], "direction": tokens["direction"]},)


def to_index_part(tokens):
    value = dict(tokens)
    if len(value) == 1:
        return value["value"]
    return value


def to_kwarg(tokens):
    return {k: v for k, v in [tuple(tokens)]}


def to_literal(t):
    return {"literal": unliteral_field(t[0])}


def to_json_call(tokens):
    # ARRANGE INTO {op: params} FORMAT
    op = tokens["op"].lower()
    op = binary_ops.get(op, op)
    params = tokens["params"]
    if isinstance(params, (dict, str, int, Call)):
        args = [params]
    else:
        args = list(params)

    kwargs = {k: v for k, v in tokens.items() if k not in ("op", "params", "kwargs")}
    more_kwargs = tokens["kwargs"]
    if more_kwargs:
        for kv in list(more_kwargs):
            kwargs.update(kv)

    return ParseResults(tokens.type, tokens.start, tokens.end, [Call(op, args, kwargs)], tokens.failures,)


def to_option(tokens):
    return dict([list(tokens)])


def to_flat_column_type(tokens):
    """
    Unfortunate that we have to do this, but column properties are flattened
    """
    try:
        col_desc = dict(tokens)
        kwargs = listwrap(col_desc["type"])[0].kwargs
        col_desc["character_set"] = kwargs["character_set"]
        del kwargs["character_set"]
        return col_desc
    except Exception:
        return tokens


def to_interval_type(tokens):
    # ARRANGE INTO {op: params} FORMAT
    op = tokens["op"].lower()
    params = tokens["params"]
    if isinstance(params, (dict, str, int, Call)):
        args = [params]
    else:
        args = list(params)

    kwargs = tokens["kwargs"]

    if args:
        if not kwargs:
            return {op: args}
        elif isinstance(kwargs, str):
            return {op: args, kwargs: {}}
        else:
            return {op: args, **kwargs}
    else:
        if not kwargs:
            return op
        elif isinstance(kwargs, str):
            return {op: {}, kwargs: {}}
        else:
            return {op: {}, **kwargs}


def has_something(tokens, index, string):
    if not tokens:
        raise ParseException(tokens.type, index, string, "expecting something to match") from None


def to_interval_call(tokens, index, string):
    # ARRANGE INTO {interval: [amount, type]} FORMAT
    expr = tokens["expr"]
    type = tokens["type"]

    if expr and type:
        return Call("interval", [expr, type], {})
    if expr:
        return Call("interval", [expr], {})
    ago = -1 if tokens["ago"] else 1
    durations = {k: v for k, v in dict(tokens).items() if k not in ("type", "ago", "day-ago")}
    if tokens["day"] and ago == -1 and tokens["day-ago"] == "+":
        durations["day"] *= -1
    result = Call("add", [Call("interval", [ago * v, k], {}) for k, v in durations.items()], {},)

    if len(result.args) == 1:
        result = result.args[0]

    if type:
        return Call("cast", [result, type], {})

    return result


def cast_interval_call(tokens, index, string):
    # ARRANGE INTO {interval: [amount, type]} FORMAT
    expr = tokens["expr"]
    type = tokens["type"]

    while isinstance(expr, ParseResults) and expr.length() == 1:
        expr = expr[0]

    if isinstance(expr, (int, float, str, dict)):
        return Call("interval", [expr, type or "second"], {})

    if isinstance(expr, Call):
        if (
            expr.op == "interval"
            or expr.op == "add"
            and all(isinstance(e, Call) and e.op == "interval" for e in expr.args)
        ):
            if type:
                return Call("cast", [expr, type], {})
            return expr

        type = tokens["type"]
        if type:
            return Call("interval", [expr, type], {})
        return Call("interval", [expr], {})

    acc = []
    for e in list(expr):
        if isinstance(e, Call):
            if e.op == "add":
                acc.extend(e.args)
            else:
                acc.append(e)
    if len(acc) == 1:
        expr = acc[0]
    else:
        expr = Call("add", args=acc, kwargs={})

    if expr.op == "interval" and len(expr.args) == 1:
        expr.args.append(type or "second")
        return expr
    if type:
        return Call("cast", [expr, type], {})
    return expr


def to_case_call(tokens):
    cases = list(tokens["case"])
    elze = tokens["else"]
    if elze != None:
        cases.append(elze)
    return Call("case", cases, {})


def to_switch_call(tokens):
    # CONVERT TO CLASSIC CASE STATEMENT
    value = tokens["value"]
    acc = []
    for c in list(tokens["case"]):
        acc.append(Call("when", [Call("eq", [value] + c.args, {})], c.kwargs))
    elze = tokens["else"]
    if elze != None:
        acc.append(elze)
    return Call("case", acc, {})


def to_when_call(tokens):
    tok = tokens
    return Call("when", [tok["when"]], {"then": tok["then"]})


def to_match_expr(tokens):
    if "expr" in tokens:
        return Call("and", [tokens["cond"], tokens["expr"]])
    return tokens["cond"]


def to_join_call(tokens):
    op = " ".join(tokens["op"])
    join = tokens["join"]
    if join["name"]:
        output = {op: {"name": join["name"], "value": join["value"]}}
    elif op:
        output = {op: join}
    else:
        output = tokens[0]

    output["on"] = tokens["on"]
    output["using"] = tokens["using"]

    if tokens["child"]:
        return [output, *list(tokens["child"])]
    return [output]


def to_expression_call(tokens):
    if set(tokens.keys()) & {"over", "within", "filter"}:
        return

    return ParseResults(tokens.type, tokens.start, tokens.end, listwrap(tokens["value"]), tokens.failures,)


def to_over(tokens):
    if not tokens:
        return {}


def to_alias(tokens):
    cols = tokens["col"]
    name = tokens["name"]
    if cols:
        return {name: cols}
    return name


def to_top_clause(tokens):
    value = tokens["value"]
    if not value:
        return None

    value = value.value()
    if tokens["ties"]:
        output = {}
        output["ties"] = True
        if tokens["percent"]:
            output["percent"] = value
        else:
            output["value"] = value
        return output
    elif tokens["percent"]:
        return {"percent": value}
    else:
        return [value]


def to_row(tokens):
    columns = list(tokens)
    if len(columns) > 1:
        return {"select": [{"value": v[0]} for v in columns]}
    else:
        return {"select": {"value": columns[0]}}


def get_literal(value):
    if isinstance(value, (int, float)):
        return value
    elif isinstance(value, Call):
        return
    elif value is SQL_NULL:
        return value
    elif "literal" in value:
        return value["literal"]


def to_values(tokens):
    rows = list(tokens)
    if len(rows) > 1:
        values = [[get_literal(s["value"]) for s in listwrap(row["select"])] for row in rows]
        if all(flatten(values)):
            return {"from": {"literal": values}}
        return {"union_all": list(tokens)}
    else:
        return rows


def to_stack(tokens):
    width = tokens["width"]
    args = listwrap(tokens["args"])
    return Call("stack", args, {"width": width})


def to_array(tokens):
    types = list(tokens["type"])
    args = listwrap(tokens["args"])
    output = Call("create_array", args, {})
    if types:
        output = Call("cast", [output, Call("array", types, {})], {})
    return output


def to_map(tokens):
    keys = tokens["keys"]
    values = tokens["values"]
    return Call("create_map", [keys, values], {})


def _simpler_struct_value(v):
    try:
        v = v[0]
        if len(v) == 1 and "value" in v:
            return v["value"]
        return v
    except:
        return v


def to_struct(tokens):
    types = list(tokens["types"])
    args = list(_simpler_struct_value(a) for a in tokens["args"])
    output = Call("create_struct", args, {})
    if types:
        output = Call("cast", [output, Call("struct", types, {})], {})
    return output


def to_select_call(tokens):
    expr = tokens["value"]
    if expr == "*":
        return ["*"]
    try:
        call = expr[0][0]  # expecting a forward from the expression
        if call.op == "value":
            return {"name": tokens["name"], "value": call.args, **call.kwargs}
        return tokens
    except:
        name = tokens["name"]
        if not name:
            return tokens
        return {"name": name, "value": expr}


def to_unpivot_column(tokens):
    expr = tokens["value"][0]
    name = tokens["name"]
    if not name:
        return expr
    return {"name": name["literal"], "value": expr}


def to_pivot_column(tokens):
    expr = tokens["value"][0]
    name = tokens["name"]
    if not name:
        return expr
    return {"name": name, "value": expr}


def to_union_call(tokens):
    unions = tokens["union"]
    if isinstance(unions, dict):
        return unions
    elif unions.type.parser_name == "unordered_sql":
        output = dict(unions)  # REMOVE THE Group()
        if not output:
            output = unions[0]
    else:
        unions = list(unions)
        sources = [unions[i] for i in range(0, len(unions), 2)]
        operators = ["_".join(unions[i]) for i in range(1, len(unions), 2)]
        acc = sources[0]
        last_union = None
        for op, so in list(zip(operators, sources[1:])):
            if op == last_union and "union" in op:
                acc[op].append(so)
            else:
                acc = {op: [acc, so]}
            last_union = op

        if not tokens["orderby"] and not tokens["offset"] and not tokens["limit"]:
            return acc
        else:
            output = {"from": acc}

    output["orderby"] = tokens["orderby"]
    output["limit"] = tokens["limit"]
    output["offset"] = tokens["offset"]
    output["fetch"] = tokens["fetch"]
    output["locking"] = tokens["locking"]
    return output


def to_insert_call(tokens):
    options = {k: v for k, v in tokens.items() if k not in ["columns", "table", "query"]}
    query = tokens["query"]
    columns = tokens["columns"]
    try:
        values = query["from"]["literal"]
        if values:
            if columns:
                data = [dict(zip(columns, row)) for row in values]
                return Call("insert", [tokens["table"]], {"values": data, **options})
            else:
                return Call("insert", [tokens["table"]], {"values": values, **options})
    except Exception:
        pass

    return Call("insert", [tokens["table"]], {"columns": columns, "query": query, **options})


def to_replace_call(tokens):
    options = {k: v for k, v in tokens.items() if k not in ["columns", "table", "query"]}
    query = tokens["query"]
    columns = tokens["columns"]
    try:
        values = query["from"]["literal"]
        if values:
            if columns:
                data = [dict(zip(columns, row)) for row in values]
                return Call("replace", [tokens["table"]], {"values": data, **options})
            else:
                return Call("replace", [tokens["table"]], {"values": values, **options})
    except Exception:
        pass

    return Call("replace", [tokens["table"]], {"columns": columns, "query": query, **options})


def to_update_call(tokens):
    value = tokens["value"]
    name = tokens["name"]
    if name:
        value = {"name": name, "value": value}
    set = tokens["set"]
    frum = tokens["from"]
    where = tokens["where"]

    return {"update": value, "set": set, "from": frum, "where": where}


def to_query(tokens):
    output = tokens["query"][0]
    try:
        output["with"] = tokens["with"]
        output["with_recursive"] = tokens["with_recursive"]
        output["using"] = tokens["using"]

        return output
    except Exception as cause:
        return


def to_table(tokens):
    output = dict(tokens)
    if len(list(output.keys())) > 1:
        return output
    return [output["value"]]


def single_literal(tokens):
    val = str(tokens[0])
    start = val.index("'")
    literal = '"""' + val[start + 1 : -1].replace("''", "\\'").replace('"', '\\"') + '"""'
    encoding = val[0:start].lower()
    if encoding:
        return {f"literal": ast.literal_eval(literal), "encoding": encoding}
    else:
        return {"literal": ast.literal_eval(literal)}


def double_literal(tokens):
    val = tokens[0]
    val = '"""' + val[1:-1].replace('""', '\\"') + '"""'
    return {"literal": ast.literal_eval(val)}


def single_regex(tokens):
    val = tokens[0]
    return {"regex": ast.literal_eval(val)}


def literal_regex(tokens):
    val = tokens[0]
    return {"regex": ast.literal_eval(val)}


def bad_operator_on_ordered_sql(token, index, string):
    raise ParseException(
        token.type,
        token.end,  # CHOOSE .end SO IT IS SELECTED ABOVE OTHER ERRORS
        string,
        f"""{string[token.start: token.end].upper()} can not follow any of ORDER BY, OFFSET, or LIMIT""",
    )


# TODO: SET TO TRUE TO ENABLE DOUBLE-QUOTE WARNING. NOT SURE IF True IS A GOOD DEFAULT
emit_warning_for_double_quotes = False


def double_column(tokens):
    global emit_warning_for_double_quotes
    if emit_warning_for_double_quotes:
        emit_warning_for_double_quotes = False
        sys.stderr.write(
            """Double quotes are used to quote column names, not literal strings.  To hide this message: mo_sql_parsing.utils.emit_warning_for_double_quotes = False"""
        )

    val = tokens[0]
    val = '"' + val[1:-1].replace('""', '\\"') + '"'
    un = literal_field(ast.literal_eval(val))
    return un


def backtick_column(tokens):
    val = tokens[0]
    val = '"' + val[1:-1].replace("``", "`").replace('"', '\\"') + '"'
    un = literal_field(ast.literal_eval(val))
    return un


def square_column(tokens):
    val = tokens[0]
    val = '"' + val[1:-1].replace("]]", "]").replace('"', '\\"') + '"'
    un = literal_field(ast.literal_eval(val))
    return un


# NUMBERS
real_num = Regex(r"[+-]?(?:\d+\.\d*|\.\d+)(?:[eE][+-]?\d+)?").set_parser_name("float") / (lambda t: float(t[0]))
real_pos = Regex(r"(?:\d+\.\d*|\.\d+)(?:[eE][+-]?\d+)?").set_parser_name("float") / (lambda t: float(t[0]))


def parse_int(tokens):
    if "e" in tokens[0].lower():
        return int(float(tokens[0]))
    else:
        return int(tokens[0])


int_num = Regex(r"[+-]?\d+(?:[eE]\+?\d+)?").set_parser_name("int") / parse_int
int_pos = Regex(r"\d+(?:[eE]\+?\d+)?").set_parser_name("int") / parse_int
hex_num = Regex(r"0x[0-9a-fA-F]+").set_parser_name("hex") / (lambda t: {"hex": t[0][2:]})

# STRINGS
ansi_string = Regex(r"(?:_utf8mb4|_utf8|_latin1|_ascii|_ucs2|_binary|n|N)?\'(?:\'\'|[^'])*\'") / single_literal
regex_string = (Regex(r'r\"(?:\\\"|[^"])*\"') | Regex(r"r\'(?:\\\'|[^'])*\'")) / literal_regex
mysql_doublequote_string = Regex(r'\"(?:\"\"|[^"])*\"') / double_literal

# BASIC IDENTIFIERS
ansi_ident = Regex(r'\"(?:\"\"|[^"])*\"') / double_column
mysql_backtick_ident = Regex(r"`(?:``|[^`])*`") / backtick_column
sqlserver_ident = Regex(r"\[(?:\]\]|[^\]])*\]") / square_column

copy_params = (
    "ALLOW_DUPLICATE",
    "AWS_KEY_ID",
    "AWS_SECRET_KEY",
    "AWS_TOKEN",
    "AZURE_SAS_TOKEN",
    "BINARY_AS_TEXT",
    "BINARY_FORMAT",
    "COMPRESSION",
    "CREDENTIALS",
    "DATE_FORMAT",
    "DISABLE_AUTO_CONVERT",
    "DISABLE_SNOWFLAKE_DATA",
    "EMPTY_FIELD_AS_NULL",
    "ENABLE_OCTA",
    "ENCODING",
    "ENCRYPTION",
    "ENFORCE_LENGTH",
    "ERROR_ON_COLUMN_COUNT_MISMATCH",
    "ESCAPE_UNENCLOSED_FIELD",
    "ESCAPE",
    "FIELD_DELIMITER",
    "FIELD_OPTIONALLY_ENCLOSED_BY",
    "FILES",
    "FILE_FORMAT",
    "FORCE",
    "FORMAT_NAME",
    "HEADER",
    "IGNORE_UTF8_ERRORS",
    "INCLUDE_QUERY_ID",
    "LOAD_UNCERTAIN_FILES",
    "MASTER_KEY",
    "MATCH_BY_COLUMN_NAME",
    "MAX_FILE_SIZE",
    "NULL_IF",
    "ON_ERROR",
    "OVERWRITE",
    "PATTERN",
    "PRESERVE_SPACE",
    "PURGE",
    "RECORD_DELIMITER",
    "REPLACE_INVALID_CHARACTERS",
    "RETURN_FAILED_ONLY",
    "SINGLE",
    "SIZE_LIMIT",
    "SKIP_BLANK_LINES",
    "SKIP_BYTE_ORDER_MARK",
    "SKIP_HEADER",
    "STORAGE_INTEGRATION",
    "STRIP_NULL_VALUES",
    "STRIP_OUTER_ARRAY",
    "STRIP_OUTER_ELEMENT",
    "TIME_FORMAT",
    "TIMESTAMP_FORMAT",
    "TRIM_SPACE",
    "TYPE",
    "TRUNCATECOLUMNS",
    "VALIDATION_MODE",
)


def no_dashes(tokens, start, string):
    if "-" in tokens[0]:
        index = tokens[0].find("-")
        raise ParseException(
            tokens.type,
            start + index + 1,  # +1 TO ENSURE THIS MESSAGE HAS PRIORITY
            string,
            """Ambiguity: Use backticks (``) around identifiers with dashes, or add space around subtraction operator.""",
        )


digit = Char("0123456789")
with whitespaces.NO_WHITESPACE:
    # repack the expression into a regex for faster parsing ident_w_dash
    ident_w_dash = Regex(
        (Char(FIRST_IDENT_CHAR) + (Regex("(?<=[^ 0-9])\\-(?=[^ 0-9])") | Char(IDENT_CHAR))[...]).__regex__()[1]
    )
    ident_w_dash_warning = ident_w_dash.set_parser_name("identifier_with_dashes") / no_dashes

simple_ident = Word(FIRST_IDENT_CHAR, IDENT_CHAR).set_parser_name("identifier")
sqlserver_local_ident = Word("@" + FIRST_IDENT_CHAR, IDENT_CHAR).set_parser_name("identifier")
