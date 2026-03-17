# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#


from mo_sql_parsing.keywords import *
from mo_sql_parsing.utils import *


# https://docs.microsoft.com/en-us/sql/t-sql/queries/select-over-clause-transact-sql?view=sql-server-ver15


def _to_bound_call(tokens):
    zero = tokens["zero"]
    if zero:
        return {"min": 0, "max": 0}

    direction = scrub(tokens["direction"])
    limit = scrub(tokens["limit"])
    if direction == "preceding":
        if limit == "unbounded":
            return {"max": 0}
        elif is_data(limit):
            return {"min": {"neg": limit}, "max": 0}
        else:
            return {"min": -limit, "max": 0}
    else:  # following
        if limit == "unbounded":
            return {"min": 0}
        elif is_data(limit):
            return {"min": {"neg": limit}, "max": 0}
        else:
            return {"min": 0, "max": limit}


def _to_between_call(tokens):
    minn = scrub(tokens["min"])
    maxx = scrub(tokens["max"])

    if maxx.get("max") == 0:
        # following
        return {
            "min": minn.get("min"),
            "max": maxx.get("min"),
        }
    elif minn.get("min") == 0:
        # preceding
        return {"min": minn.get("max"), "max": maxx.get("max")}
    else:
        return {
            "min": minn.get("min"),
            "max": maxx.get("max"),
        }


UNBOUNDED = keyword("unbounded")
PRECEDING = keyword("preceding")
FOLLOWING = keyword("following")
CURRENT_ROW = keyword("current row")
ROWS = keyword("rows")
RANGE = keyword("range")


def window(expr, var_name, sort_column):
    bound_row = (
        CURRENT_ROW("zero") | (UNBOUNDED | int_num)("limit") + (PRECEDING | FOLLOWING)("direction")
    ) / _to_bound_call
    bound_expr = (
        CURRENT_ROW("zero") | (UNBOUNDED | expr)("limit") + (PRECEDING | FOLLOWING)("direction")
    ) / _to_bound_call
    between_row = (BETWEEN + bound_row("min") + AND + bound_row("max")) / _to_between_call
    between_expr = (BETWEEN + bound_expr("min") + AND + bound_expr("max")) / _to_between_call

    row_clause = (ROWS.suppress() + (between_row | bound_row)) | (RANGE.suppress() + (between_expr | bound_expr))

    over_clause = (
        LB
        + Optional(PARTITION_BY + delimited_list(Group(expr))("partitionby"))
        + Optional(ORDER_BY + delimited_list(Group(sort_column))("orderby"))
        + Optional(row_clause("range"))
        + Optional(var_name)
        + RB
    )

    within = (WITHIN_GROUP + LB + Optional(ORDER_BY + delimited_list(Group(sort_column))("orderby")) + RB)("within")
    over = OVER + (over_clause | var_name)("over") / to_over

    window_clause = within + Optional(over) | over

    set_parser_names()
    return window_clause, over_clause
