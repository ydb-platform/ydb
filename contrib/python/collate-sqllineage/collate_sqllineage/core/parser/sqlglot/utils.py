"""
Utils class to deal with the sqlglot expressions manipulations
"""

import re
from typing import List, Optional

from sqlglot import exp
from sqlglot.expressions import Expression

from collate_sqllineage.utils.entities import SubQueryTuple


def is_expression_negligible(expression: Expression) -> bool:
    """
    Check if an expression should be ignored (comments, whitespace, etc.)
    :param expression: expression to be processed
    :return: True if the expression is negligible
    """
    # In sqlglot, comments are typically removed during parsing
    # We might want to filter out certain expression types if needed
    return expression is None


def is_subquery(expression: Expression) -> bool:
    """
    Check if an expression is a subquery
    :param expression: expression to be processed
    :return: True if the given expression is a subquery
    """
    # In sqlglot, subqueries are typically Subquery or Select expressions within parentheses
    return isinstance(expression, (exp.Subquery, exp.Select))


def get_statement_type(statement: Expression) -> str:
    """
    Get the type of SQL statement
    :param statement: parsed sqlglot expression
    :return: statement type as string
    """
    # Map sqlglot expression types to statement types
    if isinstance(statement, exp.Select):
        return "select_statement"
    elif isinstance(statement, exp.Insert):
        return "insert_statement"
    elif isinstance(statement, exp.Update):
        return "update_statement"
    elif isinstance(statement, exp.Delete):
        return "delete_statement"
    elif isinstance(statement, exp.Merge):
        return "merge_statement"
    elif isinstance(statement, exp.Create):
        return "create_statement"
    elif isinstance(statement, exp.Drop):
        return "drop_statement"
    elif isinstance(statement, exp.Alter):
        return "alter_statement"
    elif isinstance(statement, exp.Copy):
        return "copy_statement"
    elif isinstance(statement, exp.Use):
        return "use_statement"
    elif isinstance(statement, exp.Command):
        return "command"
    elif isinstance(statement, exp.Union):
        return "set_expression"
    else:
        return (
            statement.key
            if hasattr(statement, "key")
            else type(statement).__name__.lower()
        )


def is_subquery_statement(stmt: str) -> bool:
    """
    Check if a SQL statement is wrapped in parentheses
    :param stmt: SQL string
    :return: True if wrapped in parentheses
    """
    parentheses_regex = r"^\(.*\)"
    return bool(re.match(parentheses_regex, stmt.strip()))


def remove_statement_parentheses(stmt: str) -> str:
    """
    Remove outer parentheses from a SQL statement
    :param stmt: SQL string
    :return: SQL string without outer parentheses
    """
    parentheses_regex = r"^\((.*)\)"
    return re.sub(parentheses_regex, r"\1", stmt.strip())


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


def get_subqueries(expression: Expression) -> List[SubQueryTuple]:
    """
    Retrieve a list of subqueries from an expression
    :param expression: expression to be processed
    :return: a list of SubQueryTuple
    """
    subqueries = []

    # Find all subquery expressions
    for subquery in expression.find_all(exp.Subquery):
        alias = subquery.alias if hasattr(subquery, "alias") else None
        subqueries.append(SubQueryTuple(subquery, alias))

    # Also check for CTEs (Common Table Expressions)
    for cte in expression.find_all(exp.CTE):
        alias = cte.alias if hasattr(cte, "alias") else None
        subqueries.append(SubQueryTuple(cte, alias))

    return subqueries


def get_table_references(expression: Expression) -> List[exp.Table]:
    """
    Get all table references from an expression
    :param expression: expression to be processed
    :return: list of Table expressions
    """
    return list(expression.find_all(exp.Table))


def get_column_references(expression: Expression) -> List[exp.Column]:
    """
    Get all column references from an expression
    :param expression: expression to be processed
    :return: list of Column expressions
    """
    return list(expression.find_all(exp.Column))


def extract_alias(expression: Expression) -> Optional[str]:
    """
    Extract alias from an expression
    :param expression: expression to be processed
    :return: alias string if exists, None otherwise
    """
    if expression.alias:
        return expression.alias
    elif "alias" in expression.args:
        alias_exp = expression.args.get("alias")
        if alias_exp:
            return getattr(alias_exp, "name", str(alias_exp))
    return None


def is_wildcard(expression: Expression) -> bool:
    """
    Check if expression is a wildcard (*)
    :param expression: expression to be processed
    :return: True if wildcard
    """
    return isinstance(expression, exp.Star)


def get_identifier_name(expression: Expression) -> str:
    """
    Get the identifier name from an expression
    :param expression: expression to be processed
    :return: identifier name as string
    """
    return getattr(expression, "name", None) or str(
        getattr(expression, "this", expression)
    )
