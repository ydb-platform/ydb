"""
Utils class to deal with the sqlfluff segments manipulations
"""

import re
from typing import Any, Iterable, List, Optional, Tuple

from sqlfluff.core.linter import ParsedString
from sqlfluff.core.parser import BaseSegment

from collate_sqllineage.utils.entities import SubQueryTuple


def is_segment_negligible(segment: BaseSegment) -> bool:
    """
    :param segment: segment to be processed segment to be processed
    :return: True is the segment is negligible
    """
    return (
        segment.is_whitespace
        or segment.is_comment
        or bool(segment.is_meta)
        or (segment.type == "symbol" and segment.raw != "*")
    )


def extract_as_and_target_segment(
    segment: BaseSegment,
) -> Tuple[Optional[BaseSegment], BaseSegment]:
    """
    :param segment: segment to be processed
    :return: a Tuple of "alias_expression" and the target table
    """
    as_segment = segment.get_child("alias_expression")
    sublist = retrieve_segments(segment, False)
    if is_subquery(sublist[0]):
        target = sublist[0]
    elif sublist[0].type == "bracketed":
        target = get_innermost_bracketed(sublist[0])
    elif sublist[0].type == "keyword" and sublist[0].raw_upper == "FROM":
        as_segment, target = extract_as_and_target_segment(sublist[1])
    else:
        target = sublist[0].segments[0]
    return as_segment, target


def get_subqueries(segment: BaseSegment) -> List[SubQueryTuple]:
    """
    Retrieve a list of 'SubQueryTuple' based on the type of the segment.
    :param segment: segment to be processed
    :return: a list of 'SubQueryTuple'
    """
    subquery = []
    if segment.type in ["select_clause"]:
        as_segment = segment.get_child("select_clause_element").get_child(
            "alias_expression"
        )
        select_clause = segment.get_child("select_clause_element")
        case_expression = select_clause.get_child(
            "expression"
        ) and select_clause.get_child("expression").get_child("case_expression")
        target = case_expression or select_clause.get_child("column_reference")
        if target and target.type == "case_expression":
            for when_clause in target.get_children("when_clause"):
                for bracketed_segment in get_bracketed_from(
                    when_clause, to_keyword="THEN", children_segments="expression"
                ):
                    subquery.append(SubQueryTuple(bracketed_segment, None))
                for bracketed_segment in get_bracketed_from(
                    when_clause, from_keyword="THEN", children_segments="expression"
                ):
                    if isinstance(as_segment, BaseSegment):
                        subquery.append(
                            SubQueryTuple(bracketed_segment, get_identifier(as_segment))
                        )
        return subquery
    elif segment.type in ["from_clause", "from_expression", "from_expression_element"]:
        as_segment, target = extract_as_and_target_segment(
            get_inner_from_expression(segment)
        )
        if is_subquery(target):
            as_segment, target = extract_as_and_target_segment(
                get_inner_from_expression(segment)
            )
            subquery = [
                SubQueryTuple(
                    get_innermost_bracketed(target) if not is_union(target) else target,
                    get_identifier(as_segment) if as_segment else None,
                )
            ]
        return subquery
    elif segment.type in ["where_clause"]:
        if segment.get_child("expression"):
            expression_segments = segment.get_child("expression").segments or []
        else:
            expression_segments = []
        bracketed_segments = [s for s in expression_segments if s.type == "bracketed"]
        if (
            bracketed_segments
            and is_subquery(bracketed_segments[0])
            and isinstance(bracketed_segments[0], BaseSegment)
        ):
            return [SubQueryTuple(get_innermost_bracketed(bracketed_segments[0]), None)]
        return []
    elif is_union(segment):
        for s in retrieve_segments(segment, check_bracketed=True):
            if s.type == "bracketed" or s.type == "select_statement":
                subquery.append(SubQueryTuple(s, None))
        return subquery
    else:
        raise NotImplementedError()


def is_subquery(segment: BaseSegment) -> bool:
    """
    :param segment: segment to be processed
    :return: True if the given segment is subquery
    """
    if segment.type == "from_expression_element" or segment.type == "bracketed":
        token = get_innermost_bracketed(
            segment if segment.type == "bracketed" else segment.segments[0]
        )
        # check if innermost parenthesis contains SELECT, SET, or WITH COMPOUND
        sub_token = (
            token.get_child("select_statement")
            or token.get_child("set_expression")
            or token.get_child("with_compound_statement")
            or (
                token.get_child("expression")
                and (
                    token.get_child("expression").get_child("select_statement")
                    or token.get_child("expression").get_child(
                        "with_compound_statement"
                    )
                )
            )
        )
        if sub_token is not None:
            return True
    return False


def get_innermost_bracketed(bracketed_segment: BaseSegment) -> BaseSegment:
    """
    :param bracketed_segment: segment to be processed
    :return:the innermost bracketed segment of a bracketed segment
    """
    # in case of subquery in nested parenthesis, find the innermost one first
    # this should not occur if we clean parentheses like: `SELECT * FROM ((table))`
    while True:
        sub_bracketed_segments = [
            bs.get_child("bracketed")
            for bs in bracketed_segment.segments
            if bs.get_child("bracketed")
        ]
        sub_paren = bracketed_segment.get_child("bracketed") or (
            sub_bracketed_segments[0] if sub_bracketed_segments else None
        )
        if sub_paren is not None:
            bracketed_segment = sub_paren
        else:
            break
    return bracketed_segment


def is_values_clause(segment: BaseSegment) -> bool:
    """
    :param segment: segment to be processed
    :return: True if it is a given segments list o segment contains a 'values_clause' type segment
    """
    if segment.get_child("table_expression") and segment.get_child(
        "table_expression"
    ).get_child("values_clause"):
        return True
    return False


def get_multiple_identifiers(segment: BaseSegment) -> List[BaseSegment]:
    """
    :param segment: segment to be processed
    :return: a list of segments from a 'from_clause' type segment
    """
    if segment.type == "from_clause":
        return [seg for seg in segment.get_children("from_expression")]
    return []


def get_inner_from_expression(segment: BaseSegment) -> BaseSegment:
    """
    :param segment: segment to be processed
    :return: a list of segments from a 'from_expression' or 'from_expression_element' segment
    """
    if segment.get_child("from_expression"):
        if segment.get_child("from_expression").get_child("from_expression_element"):
            return segment.get_child("from_expression").get_child(
                "from_expression_element"
            )
        bracketed_exp = segment.get_child("from_expression").get_child("bracketed")
        if bracketed_exp and isinstance(bracketed_exp, BaseSegment):
            innermost_bracketed = get_innermost_bracketed(bracketed_exp)
            if innermost_bracketed.get_child("from_expression_element"):
                return innermost_bracketed.get_child("from_expression_element")
    elif segment.get_child("from_expression_element"):
        return segment.get_child("from_expression_element")
    return segment


def filter_segments_by_keyword(
    statement: BaseSegment,
    from_keyword: Optional[str] = None,
    to_keyword: Optional[str] = None,
    children_segments: Optional[str] = None,
) -> List[BaseSegment]:
    """
    Filter from the segments' children of a segment by expected children segment
    :param statement: segment to be processed
    :param from_keyword: start in a specific keyword from the segments list
    :param to_keyword: end at a specific keyword from the segments list
    :param children_segments: the filtered children segment, otherwise all of them
    :return: a list of segments' children of a segment if children_segments is not None otherwise all of them
    """
    filtered_segments = []
    can_append = False if from_keyword else True
    for segment in statement.segments:
        if not can_append and segment.type == "keyword" and from_keyword:
            if segment.raw.lower() == from_keyword.lower():
                can_append = True
        if can_append:
            filtered_segments.append(segment)
        if segment.type == "keyword" and to_keyword:
            if segment.raw.lower() == to_keyword.lower():
                break
    return [
        segment
        for segment in filtered_segments
        if (children_segments is None or segment.type == children_segments)
    ]


def get_bracketed_from(
    segment: BaseSegment,
    from_keyword: Optional[str] = None,
    to_keyword: Optional[str] = None,
    children_segments: Optional[str] = None,
) -> List[BaseSegment]:
    """
    Filter from the segments' children of a segment by expected children segment
    :param segment: segment to be processed
    :param from_keyword: start in a specific keyword from the segments list
    :param to_keyword: end at a specific keyword from the segments list
    :param children_segments: the filtered children segment, otherwise all of them
    :return: a list of segments' children of a segment which type is "bracketed"
    """
    return [
        bracketed_segment
        for filtered_segment in filter_segments_by_keyword(
            segment, from_keyword, to_keyword, children_segments
        )
        for bracketed_segment in filtered_segment.get_children("bracketed")
    ]


def find_table_identifier(identifier: BaseSegment) -> Optional[BaseSegment]:
    """
    :param identifier: segment to be processed
    :return: a table_reference or file_reference type segment if it exists in children list, otherwise the identifier
    """
    table_identifier = None
    if identifier.segments:
        for segment in identifier.segments:
            if segment.type in ("table_reference", "file_reference"):
                return segment
            else:
                table_identifier = find_table_identifier(segment)
                if table_identifier:
                    return table_identifier
    return table_identifier


def retrieve_segments(
    segment: BaseSegment, check_bracketed: bool = True
) -> List[BaseSegment]:
    """
    Filter segments for a given segment's segments if they are not negligible
    :param segment: segment to be processed
    :param check_bracketed: process segment if it is of type "bracketed"
    :return: a list of segments
    """
    if segment.type == "bracketed":
        for sgmt in segment.segments:
            if sgmt.type == "set_expression":
                return [sgmt]

    if segment.type == "bracketed" and check_bracketed:
        segments = [
            sg
            for sg in segment.iter_segments(expanding=["expression"], pass_through=True)
        ]
        result = []
        for sgmnt in segments:
            if sgmnt.type == "column_reference":
                result.append(sgmnt)
            else:
                for sg in sgmnt.segments:
                    if not is_segment_negligible(sg):
                        result.append(sg)
        return result
    return [sgmnt for sgmnt in segment.segments if not is_segment_negligible(sgmnt)]


def get_identifier(col_segment: BaseSegment) -> str:
    """
    :param col_segment: column segment
    :return: the table identifier name
    """
    identifiers = retrieve_segments(col_segment)
    col_identifier = identifiers[-1]
    return str(col_identifier.raw)


def is_wildcard(symbol: BaseSegment):
    """
    :param symbol: symbol segment
    :return: True if the symbol segment is a wildcard
    """
    return (
        symbol.type == "wildcard_expression"
        or symbol.type == "symbol"
        and symbol.raw == "*"
    )


def get_table_alias(table_segments: List[BaseSegment]) -> Optional[str]:
    """
    :param table_segments: list of segments to search for an alias
    :return: alias found otherwise None
    """
    alias = None
    if len(table_segments) > 1 and table_segments[1].type == "alias_expression":
        segments = retrieve_segments(table_segments[1])
        alias = segments[1].raw if len(segments) > 1 else segments[0].raw
    elif (
        len(table_segments) == 1 and table_segments[0].type == "from_expression_element"
    ):
        table_and_alias = retrieve_segments(table_segments[0])
        if len(table_and_alias) > 1 and table_and_alias[1].type == "alias_expression":
            segments = retrieve_segments(table_and_alias[1])
            alias = segments[1].raw if len(segments) > 1 else segments[0].raw
    return str(alias) if alias else None


def has_alias(segment: BaseSegment) -> bool:
    """
    :param segment: segment to be processed
    :return: True if the segment contains an alias keyword
    """
    return len([s for s in segment.get_children("keyword") if s.raw_upper == "AS"]) > 0


def retrieve_extra_segment(segment: BaseSegment) -> Iterable[BaseSegment]:
    """
    Yield all the extra segments of the segment if it is a 'from_expression' type.
    :param segment: segment to be processed
    """
    if segment.get_child("from_expression"):
        for sgmnt in segment.get_child("from_expression").segments:
            if sgmnt.type == "join_clause":
                yield sgmnt
        # handle bracketed expression
        bracketed_exp = segment.get_child("from_expression").get_child("bracketed")
        if bracketed_exp and isinstance(bracketed_exp, BaseSegment):
            for sgmnt in bracketed_exp.segments:
                if sgmnt.type == "join_clause":
                    yield sgmnt


def get_grandchild(
    segment: BaseSegment, child: str, grandchild: str
) -> Optional[BaseSegment]:
    """
    :param segment: segment to be processed
    :param child: child segment
    :param grandchild: grand child
    :return: the grandchild segment if found, otherwise, None
    """
    return (
        segment.get_child(child).get_child(grandchild)
        if segment.get_child(child)
        else None
    )


def get_child(segment: BaseSegment, child: str) -> BaseSegment:
    """
    :param segment: segment to be processed
    :param child: child segment
    :return:
    """
    return segment.get_child(child)


def get_grandchildren(segment: BaseSegment, child: str, grandchildren: str) -> Any:
    """
    :param segment: segment to be processed
    :param child: child segment
    :param grandchildren: grand children
    :return: the grandchildren segment list if found, otherwise, empty list
    """
    return (
        segment.get_child(child).get_children(grandchildren)
        if segment.get_child(child)
        else []
    )


def get_statement_segment(parsed_string: ParsedString) -> Optional[BaseSegment]:
    """
    :param parsed_string: parsed string
    :return: first segment from the statement segment of the segments of parsed_string
    """
    return next(
        (
            (
                x.segments[0]
                if x.type == "statement"
                else x.get_child("statement").segments[0]
            )
            for x in getattr(parsed_string.tree, "segments")
            if x.type == "statement" or x.type == "batch"
        ),
        None,
    )


def is_union(segment: BaseSegment) -> bool:
    """
    :param segment: segment to be processed
    :return: True if the segment contains 'UNION' or 'UNION ALL' keyword
    """
    return (
        len(
            [
                s
                for s in segment.raw_segments
                if (s.raw_upper == "UNION" or s.raw_upper == "UNION ALL")
            ]
        )
        > 0
    )


def get_union_subqueries(segment: BaseSegment) -> List[BaseSegment]:
    """
    :param segment: segment to be processed
    :return: a list of subqueries or select statements from a UNION segment
    """
    sub_segments = retrieve_segments(segment, check_bracketed=True)
    return [
        s for s in sub_segments if s.type == "bracketed" or s.type == "select_statement"
    ]


def is_subquery_statement(stmt: str) -> bool:
    parentheses_regex = r"^\(.*\)"
    return bool(re.match(parentheses_regex, stmt))


def remove_statement_parentheses(stmt: str) -> str:
    parentheses_regex = r"^\((.*)\)"
    return re.sub(parentheses_regex, r"\1", stmt)


def clean_parentheses(stmt: str) -> str:
    """
      Clean redundant parentheses from a SQL statement e.g:
        `SELECT col1 FROM (((((((SELECT col1 FROM tab1))))))) dt`
      will be:
        `SELECT col1 FROM (SELECT col1 FROM tab1) dt`

    :param stmt: a SQL str to be cleaned
    """
    redundant_parentheses = r"\(\(([^()]+)\)\)"
    if re.findall(redundant_parentheses, stmt):
        stmt = re.sub(redundant_parentheses, r"(\1)", stmt)
        stmt = clean_parentheses(stmt)
    return stmt
