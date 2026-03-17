"""
Utils class to deal with the sqlfluff segments manipulations

naming convention:
    is_x: BaseSegment -> bool
    find_x: BaseSegment -> Optional[BaseSegment]
    list_x: BaseSegment -> list[BaseSegment]
    extract_x: other pattern
first parameter of each function must be sqlfluff BaseSegment
"""

from sqlfluff.core.parser import BaseSegment

from sqllineage.utils.entities import ColumnQualifierTuple, SubQueryTuple


def is_negligible(segment: BaseSegment) -> bool:
    return (
        segment.is_whitespace
        or segment.is_comment
        or bool(segment.is_meta)
        or (segment.type == "symbol" and segment.raw != "*")
    )


def is_set_expression(segment: BaseSegment) -> bool:
    return segment.type == "set_expression" or any(
        seg.type == "set_expression" for seg in segment.segments
    )


def is_subquery(segment: BaseSegment) -> bool:
    if segment.type in ["from_expression_element", "bracketed"]:
        token = extract_innermost_bracketed(
            segment if segment.type == "bracketed" else segment.segments[0]
        )
        # check if innermost parenthesis contains SELECT
        if token.get_child(
            "select_statement", "set_expression", "with_compound_statement"
        ):
            return True
        elif expression := token.get_child("expression"):
            if expression.get_child("select_statement"):
                return True
    return False


def is_wildcard(segment: BaseSegment) -> bool:
    return segment.type == "wildcard_expression" or (
        segment.type == "symbol" and segment.raw == "*" and segment.get_type() == "star"
    )


def is_teradata_title_phrase(segment: BaseSegment) -> bool:
    """
    Check if a select_clause_element contains Teradata specific TITLE phrase
    Teradata TITLE phrase syntax: column_name (TITLE 'title_name')
    This gets incorrectly parsed as a function by sqlfluff
    """
    if segment.type == "select_clause_element":
        if function := segment.get_child("function"):
            if function_contents := function.get_child("function_contents"):
                if bracketed := function_contents.get_child("bracketed"):
                    if expression := bracketed.get_child("expression"):
                        if data_type := expression.get_child("data_type"):
                            if data_type_identifier := data_type.get_child(
                                "data_type_identifier"
                            ):
                                if data_type_identifier.raw_upper == "TITLE":
                                    return True
    return False


def find_from_expression_element(segment: BaseSegment) -> BaseSegment | None:
    """
    segment can be of type:
        from_clause as grandparent
        from_expression/join_clause as parent
    """
    try:
        from_expression_element = next(
            segment.recursive_crawl("from_expression_element")
        )
    except StopIteration:
        from_expression_element = None
    return from_expression_element


def find_table_identifier(segment: BaseSegment) -> BaseSegment | None:
    """
    recursively find table identifier
    """
    table_identifier = None
    if segment.type in ["table_reference", "file_reference", "object_reference"]:
        return segment
    else:
        for sub_segment in segment.segments:
            if identifier := find_table_identifier(sub_segment):
                return identifier
    return table_identifier


def list_join_clause(segment: BaseSegment) -> list[BaseSegment]:
    """
    traverse from_clause, recursively goes into bracket by default
    """
    if segment.type in ["from_clause", "update_statement"]:
        # for select from subquery, do not recursively go into subquery
        if from_expression := segment.get_child("from_expression"):
            join_clause = from_expression.get_child("join_clause")
            if not join_clause:
                try:
                    next(from_expression.recursive_crawl("select_clause"))
                except StopIteration:
                    pass
                else:
                    # no join at top level, and there is select statement in from_clause
                    return []
        # otherwise, recursively find join_clause
        return list(segment.recursive_crawl("join_clause"))
    return []


def list_expression_from_when_clause(
    when_clause: BaseSegment, sub_type: str
) -> list[BaseSegment]:
    """
    sub_type can be either WHEN or THEN, this is to extract subquery for case when (SELECT ...) then (SELECT ...)
    """
    segments = []
    start_flag = False
    from_keyword, end_keyword = sub_type, "THEN" if sub_type == "WHEN" else None
    for segment in when_clause.segments:
        if segment.type == "keyword" and segment.raw_upper == from_keyword:
            start_flag = True
            continue
        if (
            end_keyword is not None
            and segment.type == "keyword"
            and segment.raw_upper == end_keyword
        ):
            break
        if start_flag and segment.type == "expression":
            segments = segment.get_children("bracketed")
    return segments


def list_subqueries(segment: BaseSegment) -> list[SubQueryTuple]:
    subquery = []
    match segment.type:
        case "select_clause":
            for select_clause_element in segment.get_children("select_clause_element"):
                if expression := select_clause_element.get_child("expression"):
                    if case_expression := expression.get_child("case_expression"):
                        for when_clause in case_expression.get_children("when_clause"):
                            for bracketed_segment in list_expression_from_when_clause(
                                when_clause, sub_type="WHEN"
                            ):
                                subquery.append(SubQueryTuple(bracketed_segment, None))
                            for bracketed_segment in list_expression_from_when_clause(
                                when_clause, sub_type="THEN"
                            ):
                                alias_expression = select_clause_element.get_child(
                                    "alias_expression"
                                )
                                alias = (
                                    extract_identifier(alias_expression)
                                    if alias_expression
                                    else None
                                )
                                subquery.append(SubQueryTuple(bracketed_segment, alias))
                elif function := select_clause_element.get_child("function"):
                    for bracketed in function.recursive_crawl("bracketed"):
                        if is_subquery(bracketed):
                            subquery.append(SubQueryTuple(bracketed, None))
        case "from_expression_element":
            as_segment, target = extract_as_and_target_segment(segment)
            if is_subquery(target):
                subquery = [
                    SubQueryTuple(
                        (
                            extract_innermost_bracketed(target)
                            if not is_set_expression(target)
                            else target
                        ),
                        extract_identifier(as_segment) if as_segment else None,
                    )
                ]
        case "where_clause":
            bracketeds = []
            if expression := segment.get_child("expression"):
                bracketeds = expression.get_children("bracketed")
            elif bracketed_where := segment.get_child("bracketed"):
                if expression := bracketed_where.get_child("expression"):
                    bracketeds = expression.get_children("bracketed")
            subquery = [
                SubQueryTuple(extract_innermost_bracketed(bracketed), None)
                for bracketed in bracketeds
                if is_subquery(bracketed)
            ]
        case "from_clause" | "from_expression":
            if from_expression_element := find_from_expression_element(segment):
                subquery = list_subqueries(from_expression_element)
            for join_clause in list_join_clause(segment):
                if from_expression_element := find_from_expression_element(join_clause):
                    subquery += list_subqueries(from_expression_element)
    return subquery


def list_child_segments(
    segment: BaseSegment, check_bracketed: bool = True
) -> list[BaseSegment]:
    """
    Filter segments for a given segment's children, recursive goes into bracket by default
    """
    if segment.type == "bracketed" and check_bracketed:
        if is_set_expression(segment):
            return [seg for seg in segment.segments if seg.type == "set_expression"]
        else:
            result = []
            for seg in segment.iter_segments(
                expanding=["expression"], pass_through=True
            ):
                if seg.type in ["column_reference", "column_definition"]:
                    result.append(seg)
                else:
                    for s in seg.segments:
                        if not is_negligible(s):
                            result.append(s)
            return result
    else:
        return [seg for seg in segment.segments if not is_negligible(seg)]


def extract_identifier(col_segment: BaseSegment) -> str:
    identifiers = list_child_segments(col_segment)

    # tsql assignment operator syntax: alias_name = expression
    # starting from sqlfluff 3.4.2, the alias_expression contains the alias operator that we need to get rid of
    if col_segment.type == "alias_expression" and col_segment.get_child(
        "alias_operator"
    ):
        identifiers = [seg for seg in identifiers if seg.type != "alias_operator"]

    col_identifier = identifiers[-1]
    return str(col_identifier.raw)


def extract_as_and_target_segment(
    segment: BaseSegment,
) -> tuple[BaseSegment | None, BaseSegment]:
    as_segment = segment.get_child("alias_expression")
    sublist = list_child_segments(segment, False)
    target = sublist[0]
    if target.type == "keyword" and target.raw_upper == "LATERAL":
        target = sublist[1]
    table_expr = target if is_subquery(target) else target.segments[0]
    return as_segment, table_expr


def extract_column_qualifier(segment: BaseSegment) -> ColumnQualifierTuple | None:
    cqt = None
    if is_wildcard(segment):
        identifiers = segment.raw.split(".")
        column = identifiers[-1]
        parent = identifiers[-2] if len(identifiers) > 1 else None
        cqt = ColumnQualifierTuple(column, parent)
    else:
        match segment.type:
            case "column_reference":
                sub_segments = list_child_segments(segment)
                column = sub_segments[-1].raw
                parent = sub_segments[-2].raw if len(sub_segments) > 1 else None
                cqt = ColumnQualifierTuple(column, parent)
            case "identifier":
                cqt = ColumnQualifierTuple(segment.raw, None)
    return cqt


def extract_innermost_bracketed(bracketed_segment: BaseSegment) -> BaseSegment:
    # in case of subquery in nested parenthesis like: `SELECT * FROM ((table))`, find the innermost one first
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
