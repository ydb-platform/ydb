# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Initial Author: Beto Dealmeida (beto@dealmeida.net)
#


import re

from mo_dots import split_field, is_data
from mo_future import first, is_text, string_types, text
from mo_parsing.utils import listwrap

from mo_sql_parsing.keywords import RESERVED, join_keywords, precedence, pivot_keywords
from mo_sql_parsing.utils import binary_ops, is_set_op

MAX_PRECEDENCE = 100
VALID = re.compile(r"^[a-zA-Z_]\w*$")


def is_keyword(identifier):
    try:
        RESERVED.parse_string(identifier)
        return True
    except Exception:
        return False


def _should_quote(identifier):
    """
    Return true if a given identifier should be quoted.

    This is usually true when the identifier:

      - is a reserved word
      - contain spaces
      - does not match the regex `[a-zA-Z_]\\w*`

    """
    return identifier != "*" and (not VALID.match(identifier) or is_keyword(identifier))


def escape(ident, quote_char, should_quote):
    """
    Escape identifiers.

    ANSI uses double quotes, but many databases use back quotes.

    """

    def esc(identifier):
        if not should_quote(identifier):
            return identifier

        identifier = identifier.replace(quote_char, 2 * quote_char)
        return f"{quote_char}{identifier}{quote_char}"

    return ".".join(esc(f) for f in split_field(ident))


def Operator(_op, ordered=True):
    op_prec = precedence[binary_ops[_op]]
    op = " {0} ".format(_op).replace("_", " ").upper()

    def func(self, json, prec):

        if isinstance(json, dict):
            # {VARIABLE: VALUE} FORM
            k, v = first(json.items())
            json = [k, {"literal": v}]

        operands = listwrap(json)
        if ordered and len(operands) == 2:
            acc = [self.dispatch(operands[0], op_prec + 0.5), self.dispatch(operands[1], op_prec - 0.5)]
        else:
            acc = [self.dispatch(v, op_prec) for v in operands]

        if prec > op_prec:
            return op.join(acc)
        elif prec == op_prec and not ordered:
            return op.join(acc)
        else:
            return f"({op.join(acc)})"

    return func


def isolate(expr, sql, prec):
    """
    RETURN sql IN PARENTHESIS IF PREEDENCE > prec
    :param expr: expression to isolate
    :param sql: sql to return
    :param prec: current precedence
    """
    if is_text(expr):
        return sql
    ps = [p for k in expr.keys() for p in [precedence.get(k)] if p is not None]
    if not ps:
        return sql
    elif min(ps) >= prec:
        return f"({sql})"
    else:
        return sql


unordered_clauses = [
    "with",
    "distinct_on",
    "select_distinct",
    "select",
    "from",
    "pivot",
    "unpivot",
    "where",
    "groupby",
    "having",
    "union_all",
    "union",
]

ordered_clauses = [
    "orderby",
    "limit",
    "offset",
    "fetch",
]

agg_kwargs = {"distinct", "orderby", "limit", "nulls"}

ordered_query_kwargs = agg_kwargs | set(ordered_clauses)


class Formatter:
    # infix operators
    _mul = Operator("*", ordered=False)
    _div = Operator("/")
    _mod = Operator("%")
    _add = Operator("+", ordered=False)
    _sub = Operator("-")
    _neq = Operator("<>")
    _gt = Operator(">")
    _lt = Operator("<")
    _gte = Operator(">=")
    _lte = Operator("<=")
    _eq = Operator("=")
    _or = Operator("or", ordered=False)
    _and = Operator("and", ordered=False)
    _binary_and = Operator("&", ordered=False)
    _binary_or = Operator("|", ordered=False)
    _like = Operator("like")
    _not_like = Operator("not like")
    _rlike = Operator("rlike")
    _not_rlike = Operator("not rlike")
    _ilike = Operator("ilike")
    _not_ilike = Operator("not ilike")
    _union = Operator("union", ordered=False)
    _union_all = Operator("union all", ordered=False)
    _intersect = Operator("intersect", ordered=False)
    _minus = Operator("minus")
    _except = Operator("except")

    def __init__(self, ansi_quotes=True, should_quote=None):
        self.quote_char = '"' if ansi_quotes else "`"
        self.should_quote = should_quote or _should_quote

    def format(self, json):
        return self.dispatch(json, 50)

    def dispatch(self, json, prec=100):
        if isinstance(json, list):
            return self.sql_list(json, prec=precedence["list"])
        if isinstance(json, dict):
            if len(json) == 0:
                return ""
            elif "delete" in json:
                return self.delete(json, prec)
            elif "literal" in json:
                return self._literal(json, prec)
            elif "substring" in json:
                return self._substring(json, prec)
            elif "generator" in json:
                return self._generator(json, prec)
            elif "group_concat" in json:
                return self._group_concat(json, prec)
            elif "value" in json:
                return self.value(json, prec)
            elif "join" in json:
                return self._join_on(json, prec)
            elif "insert" in json:
                return self.insert(json, prec)
            elif "on_conversion_error" in json:
                return self._on_conversion_error(json, prec)
            elif json.keys() & ordered_query_kwargs:
                return self.ordered_query(json, prec)
            elif json.keys() & set(unordered_clauses):
                return self.unordered_query(json, prec)
            elif "null" in json:
                return "NULL"
            elif "trim" in json:
                return self._trim(json, prec)
            elif "extract" in json:
                return self._extract(json, prec)
            else:
                return self.op(json, prec)
        if isinstance(json, string_types):
            return escape(json, self.quote_char, self.should_quote)
        if json == None:
            return "NULL"

        return text(json)

    def sql_list(self, json, prec=precedence["from"] - 1):
        sql = ", ".join(self.dispatch(element, prec=MAX_PRECEDENCE) for element in json)
        if prec >= precedence["from"]:
            return sql
        else:
            return f"({sql})"

    def value(self, json, prec=precedence["from"]):
        parts = [self.dispatch(json["value"], prec)]
        if "filter" in json:
            parts.append(f"FILTER (WHERE {self.dispatch(json['filter'])})")
        if "over" in json:
            over = json["over"]
            parts.append("OVER")
            window = []
            if "partitionby" in over:
                window.append(self.partitionby(over, precedence["window"]))
            if "orderby" in over:
                window.append(self.orderby(over, precedence["window"]))
            if "range" in over:

                def wordy(v):
                    if v < 0:
                        return [text(abs(v)), "PRECEDING"]
                    elif v > 0:
                        return [text(v), "FOLLOWING"]

                window.append("ROWS")
                range = over["range"]
                min = range.get("min")
                max = range.get("max")

                if min is None:
                    if max is None:
                        window.pop()  # not expected, but deal
                    elif max == 0:
                        window.append("UNBOUNDED PRECEDING")
                    else:
                        window.append("BETWEEN")
                        window.append("UNBOUNDED PRECEDING")
                        window.append("AND")
                        window.extend(wordy(max))
                elif min == 0:
                    if max is None:
                        window.append("UNBOUNDED FOLLOWING")
                    elif max == 0:
                        window.append("CURRENT ROW")
                    else:
                        window.extend(wordy(max))
                else:
                    if max is None:
                        window.append("BETWEEN")
                        window.extend(wordy(min))
                        window.append("AND")
                        window.append("UNBOUNDED FOLLOWING")
                    elif max == 0:
                        window.extend(wordy(min))
                    else:
                        window.append("BETWEEN")
                        window.extend(wordy(min))
                        window.append("AND")
                        window.extend(wordy(max))

            window = " ".join(window)
            parts.append(f"({window})")
        if "within" in json:
            # WITHIN GROUP (
            #             ORDER BY public.persentil.sale
            #           )
            ob = self.orderby(json["within"], 100)
            parts.append(f"WITHIN GROUP ({ob})")
        if "name" in json:
            parts.extend(["AS", self.dispatch(json["name"])])
        if "tablesample" in json:
            parts.append("TABLESAMPLE")
            sample = json["tablesample"]
            sampling_method = sample.get("method")
            if sampling_method:
                parts.append(sampling_method)
            sampling_rows = sample.get("rows")
            if sampling_rows:
                parts.append(f"({sampling_rows} ROWS)")
            sampling_pct = sample.get("percent")
            if sampling_pct:
                parts.append(f"({sampling_pct} PERCENT)")
            sampling_bucket = sample.get("bucket")
            if sampling_bucket:
                bucket_parts = [f"BUCKET {sampling_bucket[0]} OUT OF {sampling_bucket[1]}"]
                sampling_on = sample.get("on")
                if sampling_on:
                    bucket_parts.append(f"ON {self.format(sampling_on)}")
                parts.append("(" + " ".join(bucket_parts) + ")")

        return " ".join(parts)

    def op(self, json, prec):
        if len(json) > 1:
            raise Exception("Operators should have only one key!")
        key, value = list(json.items())[0]

        # check if the attribute exists, and call the corresponding method;
        # note that we disallow keys that start with `_` to avoid giving access
        # to magic methods
        attr = f"_{key}"
        method = getattr(self, attr, None)
        if method and not key.startswith("_"):
            return method(value, prec)

        # treat as regular function call
        if isinstance(value, dict) and len(value) == 0:
            return key.upper() + "()"  # NOT SURE IF AN EMPTY dict SHOULD BE DELT WITH HERE, OR IN self.format()
        else:
            params = ", ".join(self.dispatch(p, precedence["from"]) for p in listwrap(value))
            return f"{key.upper()}({params})"

    def _regexp(self, value, prec):
        return f"{self.dispatch(value[0])} REGEXP {self.dispatch(value[1])}"

    def _not_regexp(self, value, prec):
        return f"{self.dispatch(value[0])} NOT REGEXP {self.dispatch(value[1])}"

    def _binary_not(self, value, prec):
        return "~{0}".format(self.dispatch(value))

    def _not(self, value, prec):
        op_prec = precedence["not"]
        if prec >= op_prec:
            return f"NOT {self.dispatch(value)}"
        else:
            return f"NOT ({self.dispatch(value)})"

    def _exists(self, value, prec):
        sql = self.dispatch(value, precedence["exists"])
        if "from" in value:
            return f"EXISTS {sql}"
        return f"{sql} IS NOT NULL"

    def _missing(self, value, prec):
        return "{0} IS NULL".format(self.dispatch(value, precedence["is"]))

    def _collate(self, pair, prec):
        return "{0} COLLATE {1}".format(self.dispatch(pair[0], precedence["collate"]), pair[1])

    def _substring(self, json, prec):
        if "from" in json.keys():
            return f"SUBSTRING({json['substring']} FROM {json['from']} FOR {json['for']})"
        else:
            # if substring does not contain from and for,  compose normal substring function
            params = ", ".join(self.dispatch(p) for p in json["substring"])
            return f"SUBSTRING({params})"

    def _generator(self, json, prec):
        # TODO: replace with function-using-keywords
        rowcount = self.dispatch(json["rowcount"])
        return f"GENERATOR(ROWCOUNT=>{rowcount})"

    def _group_concat(self, json, prec):
        acc = ["group_concat(", self.dispatch(json["group_concat"]), " "]
        if "orderby" in json.keys():
            acc.append(self.orderby(json, precedence["order"]))
        if "separator" in json.keys():
            acc.append(" SEPARATOR ")
            acc.append(self.dispatch(json["separator"]))
        acc.append(")")
        return "".join(acc)

    def _in(self, json, prec):
        return self._inline_set_op("IN", json, prec)

    def _nin(self, json, prec):
        return self._inline_set_op("NOT IN", json, prec)

    def _inline_set_op(self, sql_op, json, prec):
        member, set = json
        if is_data(set) and "literal" in set:
            set = {"literal": listwrap(set["literal"])}
        else:
            set = listwrap(set)
        sql = self.dispatch(member, precedence["in"]) + f" {sql_op} " + self.dispatch(set, precedence["in"])
        if prec < precedence["in"]:
            sql = f"({sql})"
        return sql

    def _case(self, checks, prec):
        parts = ["CASE"]
        for check in checks if isinstance(checks, list) else [checks]:
            if isinstance(check, dict):
                if "when" in check and "then" in check:
                    parts.extend(["WHEN", self.dispatch(check["when"])])
                    parts.extend(["THEN", self.dispatch(check["then"])])
                else:
                    parts.extend(["ELSE", self.dispatch(check)])
            else:
                parts.extend(["ELSE", self.dispatch(check)])
        parts.append("END")
        return " ".join(parts)

    def casting(self, name, json):
        expr, type = json

        type_name, params = first(type.items())
        if not params:
            type = type_name.upper()
        else:
            type = {type_name.upper(): params}

        return f"{name}({self.dispatch(expr)} AS {self.dispatch(type)})"

    def _cast(self, json, prec):
        return self.casting("CAST", json)

    def _try_cast(self, json, prec):
        return self.casting("TRY_CAST", json)

    def _validate_conversion(self, json, prec):
        return self.casting("VALIDATE_CONVERSION", json)

    def _safe_cast(self, json, prec):
        return self.casting("SAFE_CAST", json)

    def _extract(self, json, prec):
        interval, value = json["extract"]
        i = self.dispatch(interval).upper()
        v = self.dispatch(value)
        return f"EXTRACT({i} FROM {v})"

    def _interval(self, json, prec):
        amount, type = json
        if isinstance(amount, (int, float)):
            # we can quote numbers and the various dbs are fine with that
            amount = f"'{amount}'"
        else:
            amount = self.dispatch(amount, precedence["interval"])
        type = self.dispatch(type, precedence["and"])
        return f"INTERVAL {amount} {type.upper()}"

    def _literal(self, json, prec=0):
        if isinstance(json, list):
            body = ", ".join(self._literal(v, precedence["literal"]) for v in json)
            return f"({body})"
        elif isinstance(json, string_types):
            body = json.replace("'", "''")
            return f"'{body}'"
        elif isinstance(json, dict):
            if isinstance(json["literal"], list):
                body = ", ".join(self._literal(v, precedence["literal"]) for v in json["literal"])
                return f"({body})"

            encoding = ""
            if json.get("encoding"):
                encoding = json["encoding"].upper()
            body = json["literal"].replace("'", "''")
            return f"{encoding}'{body}'"
        else:
            return str(json)

    def _get(self, json, prec):
        v, i = json
        v_sql = self.dispatch(v, prec=precedence["literal"])
        i_sql = self.dispatch(i)
        return f"{v_sql}[{i_sql}]"

    def _between(self, json, prec):
        return "{0} BETWEEN {1} AND {2}".format(
            self.dispatch(json[0], precedence["between"]),
            self.dispatch(json[1], precedence["between"]),
            self.dispatch(json[2], precedence["between"]),
        )

    def _trim(self, json, prec):
        c = json.get("characters")
        d = json.get("direction")
        v = json["trim"]
        acc = ["TRIM("]
        if d:
            acc.append(d.upper())
            acc.append(" ")
        if c:
            acc.append(self.dispatch(c))
            acc.append(" ")
        if c or d:
            acc.append("FROM ")
        acc.append(self.dispatch(v))
        acc.append(")")
        return "".join(acc)

    def _not_between(self, json, prec):
        return "{0} NOT BETWEEN {1} AND {2}".format(
            self.dispatch(json[0], precedence["between"]),
            self.dispatch(json[1], precedence["between"]),
            self.dispatch(json[2], precedence["between"]),
        )

    def _distinct(self, json, prec):
        return "DISTINCT " + ", ".join(self.dispatch(v, precedence["select"]) for v in listwrap(json))

    def _select_distinct(self, json, prec):
        return "SELECT DISTINCT " + ", ".join(self.dispatch(v) for v in listwrap(json))

    def _distinct_on(self, json, prec):
        return "DISTINCT ON (" + ", ".join(self.dispatch(v) for v in listwrap(json)) + ")"

    def pivot(self, json, prec):
        pivot = json["pivot"]
        return self._pivot("PIVOT", pivot, self.dispatch(pivot["aggregate"]))

    def unpivot(self, json, prec):
        pivot = json["unpivot"]
        if "nulls" in pivot:
            nulls = " INCLUDE NULLS" if pivot["nulls"] else " EXCLUDE NULLS"
        else:
            nulls = ""
        return self._pivot(f"UNPIVOT{nulls}", pivot, self.dispatch(pivot["value"]))

    def _pivot(self, op, pivot, value):
        for_ = self.dispatch(pivot["for"])
        in_ = self.dispatch(pivot["in"])
        sql = f"{op} ({value} FOR {for_} IN {in_})"
        if "name" in pivot:
            name = pivot["name"]
            return f"{sql} AS {name}"
        else:
            return sql

    def _join_on(self, json, prec):
        detected_join = join_keywords & set(json.keys())
        if len(detected_join) == 0:
            raise Exception('Fail to detect join type! Detected: "{}" Except one of: "{}"'.format(
                [on_keyword for on_keyword in json if on_keyword != "on"][0], '", "'.join(join_keywords),
            ))

        join_keyword = detected_join.pop()

        acc = []
        acc.append(join_keyword.upper())
        acc.append(self.dispatch(json[join_keyword], precedence["join"]))

        if json.get("on"):
            acc.append("ON")
            acc.append(self.dispatch(json["on"]))
        if json.get("using"):
            acc.append("USING")
            acc.append(self.dispatch(json["using"]))
        return " ".join(acc)

    def _create_array(self, json, prec):
        return "[" + ", ".join(self.dispatch(v) for v in listwrap(json)) + "]"

    def _on_conversion_error(self, json, prec):
        default = self.dispatch(json["on_conversion_error"])
        op = json.keys() - {"on_conversion_error"}
        if len(op) != 1:
            raise Exception("on_conversion_error should have only one key!")
        op = op.pop()
        param, *rest = json[op]
        acc = [f"{op.upper()}({self.dispatch(param)} DEFAULT {default} ON CONVERSION ERROR"]
        for r in rest:
            acc.append(f", {self.dispatch(r)}")
        acc.append(")")
        return "".join(acc)

    def ordered_query(self, json, prec):
        op = None
        if json.keys() & set(unordered_clauses) - {"from"}:
            # regular query
            acc = [self.unordered_query(json, precedence["order"])]
        elif "from" not in json and len(json.keys() - agg_kwargs) == 1:
            # aggreate operation
            op = first(json.keys() - agg_kwargs)
            acc = []
            if json.get("distinct"):
                acc.append("DISTINCT")
            acc.append(self.dispatch(json[op], precedence["order"]))
            if json.get("nulls"):
                acc.append(json.get("nulls").upper())
                acc.append("NULLS")
        else:
            # set-op expression
            acc = [self.dispatch(json["from"], precedence["order"])]

        acc.extend(
            part
            for clause in ordered_clauses
            if clause in json
            for part in [getattr(self, clause)(json, precedence["order"])]
            if part
        )
        try:
            sql = " ".join(acc)
        except Exception as cause:
            print(cause)
        if op:
            return f"{op.upper()}({sql})"
        elif prec >= precedence["order"]:
            return sql
        else:
            return f"({sql})"

    def unordered_query(self, json, prec):
        sql = " ".join(
            part
            for clause in unordered_clauses
            if clause in json
            for part in [getattr(self, clause)(json, precedence[clause] + 1)]
            if part
        )
        if prec > precedence["from"]:
            return sql
        else:
            return f"({sql})"

    def with_(self, json, prec):
        with_ = listwrap(json["with"])
        parts = ", ".join(f"{part['name']} AS (\n{self.dispatch(part['value'])}\n)" for part in with_)
        return f"WITH {parts}"

    def union_all(self, json, prec):
        sql = "\nUNION ALL\n".join(self.dispatch(part) for part in listwrap(json["union_all"]))
        return f"{sql}" if prec > precedence["union_all"] else f"({sql})"

    def union(self, json, prec):
        sql = "\nUNION\n".join(self.dispatch(part) for part in listwrap(json["union"]))
        return f"{sql}" if prec > precedence["union"] else f"({sql})"

    def select(self, json, prec):
        select = json["select"]
        acc = []
        for s in listwrap(select):
            if s == "*":
                acc.append("*")
                continue
            if isinstance(s, str):
                acc.append(self.dispatch(s, precedence["select"]))
            else:
                all_col = s.get("all_columns")
                if all_col or isinstance(all_col, dict):
                    acc.append(self.all_columns(s, precedence["select"]))
                else:
                    acc.append(self.dispatch(s, precedence["select"]))
        param = ", ".join(acc)
        if "top" in json:
            top = self.dispatch(json["top"])
            return f"SELECT TOP ({top}) {param}"
        if "distinct_on" in json:
            return param
        else:
            return f"SELECT {param}"

    def all_columns(self, json, prec):
        others = json.get("except")
        frum = json["all_columns"]
        if frum:
            if others:
                return f"{frum}.* EXCEPT ({self.dispatch(others)})"
            else:
                return f"{frum}.*"
        else:
            if others:
                return f"* EXCEPT ({self.dispatch(others)})"
            else:
                return "*"

    def distinct_on(self, json, prec):
        param = ", ".join(self.dispatch(s) for s in listwrap(json["distinct_on"]))
        return f"SELECT DISTINCT ON ({param})"

    def select_distinct(self, json, prec):
        param = ", ".join(self.dispatch(s, precedence["select"]) for s in listwrap(json["select_distinct"]))
        return f"SELECT DISTINCT {param}"

    def from_(self, json, prec):
        joiner = ", "
        from_ = json["from"]
        if isinstance(from_, dict) and "literal" in from_:
            content = ", ".join(self._literal(row) for row in from_["literal"])
            return f"VALUES {content}"
        if isinstance(from_, dict) and is_set_op & from_.keys():
            source = self.op(from_, precedence["from"])
            return f"FROM {source}"

        from_ = listwrap(from_)
        parts = []
        for v in from_:
            if join_keywords & set(v):
                joiner = " "
                parts.append(self._join_on(v, precedence["from"] - 1))
            else:
                parts.append(self.dispatch(v, precedence["from"] - 1))
        rest = joiner.join(parts)
        return f"FROM {rest}"

    def where(self, json, prec):
        expr = self.dispatch(json["where"])
        return f"WHERE {expr}"

    def groupby(self, json, prec):
        param = ", ".join(self.dispatch(s) for s in listwrap(json["groupby"]))
        return f"GROUP BY {param}"

    def having(self, json, prec):
        return "HAVING {0}".format(self.dispatch(json["having"]))

    def orderby(self, json, prec):
        param = ", ".join(
            (self.dispatch(s["value"], precedence["order"]) + " " + s.get("sort", "").upper()).strip()
            for s in listwrap(json["orderby"])
        )
        return f"ORDER BY {param}"

    def partitionby(self, json, prec):
        param = ", ".join(self.dispatch(s, precedence["order"]) for s in listwrap(json["partitionby"]))
        return f"PARTITION BY {param}"

    def limit(self, json, prec):
        num = self.dispatch(json["limit"], precedence["order"])
        return f"LIMIT {num}"

    def offset(self, json, prec):
        num = self.dispatch(json["offset"], precedence["order"])
        return f"OFFSET {num}"

    def fetch(self, json, prec):
        num = self.dispatch(json["offset"], precedence["order"])
        return f"FETCH {num} ROWS ONLY"

    def delete(self, json, prec):
        acc = ["DELETE FROM ", json["delete"]]
        if "where" in json:
            json = {k: v for k, v in json.items() if k != "delete"}
            acc.append("\n")
            acc.append(self.dispatch(json, prec))
        return "".join(acc)

    def insert(self, json, prec=precedence["from"]):
        acc = ["INSERT"]
        if "overwrite" in json:
            acc.append("OVERWRITE")
        else:
            acc.append("INTO")
        acc.append(json["insert"])

        if "columns" in json:
            acc.append(self.sql_list(json["columns"]))
        if "values" in json:
            values = json["values"]
            if all(isinstance(row, dict) for row in values):
                columns = list(sorted(set(k for row in values for k in row.keys())))
                acc.append(self.sql_list(columns))
                if "if exists" in json:
                    acc.append("IF EXISTS")
                acc.append("VALUES")
                acc.append(",\n".join("(" + ", ".join(self._literal(row[c]) for c in columns) + ")" for row in values))
            else:
                if "if exists" in json:
                    acc.append("IF EXISTS")
                acc.append("VALUES")
                for row in values:
                    acc.append("(" + ", ".join(self._literal(row)) + ")")

        else:
            if json.get("if exists"):
                acc.append("IF EXISTS")
            acc.append(self.dispatch(json["query"]))
        return " ".join(acc)


setattr(Formatter, "with", Formatter.with_)
setattr(Formatter, "from", Formatter.from_)
