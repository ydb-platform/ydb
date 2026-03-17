from typing import List, Union

from sqlparse.sql import (
    Case,
    Comment,
    Comparison,
    Function,
    Identifier,
    Over,
    Parenthesis,
    TokenList,
    Where,
)
from sqlparse.tokens import DML, Keyword, Wildcard


from collate_sqllineage.utils.entities import SubQueryTuple


def is_token_negligible(token: TokenList) -> bool:
    # utility to skip tokens like whitespace or comment
    return token.is_whitespace or isinstance(token, Comment)


def remove_parenthesis_between_union(token: Parenthesis) -> Parenthesis:
    """
    remove parenthesis around subqueries between union
    Example:
        INPUT: ((SELECT col1 FROM tab2) UNION ALL (SELECT col1 FROM tab3))
        OUTPUT: (SELECT col1 FROM tab2 UNION ALL SELECT col1 FROM tab3)
    """
    # pre-compute the idx of UNION/UNION ALL
    offsets = [-1]
    while True:
        offset, _ = token.token_next_by(
            m=[(Keyword, "UNION ALL"), (Keyword, "UNION")], idx=offsets[-1]
        )
        if offset is not None:
            offsets.append(offset)
        else:
            break
    expand_parenthesis_idx = set()
    for i, offset in enumerate(offsets[1:]):
        if i == 0:
            # first union idx, we need to handle the previous parenthesis
            pre_idx, sub_token = token.token_prev(idx=offset)
            if isinstance(sub_token, Parenthesis):
                expand_parenthesis_idx.add(pre_idx)
        # then all post parenthesis handled for each union
        post_idx, sub_token = token.token_next(idx=offset)
        if isinstance(sub_token, Parenthesis):
            expand_parenthesis_idx.add(post_idx)
    if expand_parenthesis_idx:
        updated_tokens = []
        for i, sub_token in enumerate(token.tokens):
            if i in expand_parenthesis_idx:
                inner_paren = get_innermost_parenthesis(sub_token)
                updated_tokens.extend(inner_paren.tokens[1:-1])
            else:
                updated_tokens.append(sub_token)
        token.tokens = updated_tokens
    return token


def get_innermost_parenthesis(token: Parenthesis) -> Parenthesis:
    # in case of subquery in nested parenthesis, find the innermost one first
    while True:
        idx, sub_paren = token.token_next_by(i=Parenthesis)
        if sub_paren is not None and (
            idx == 1
            or (idx > 1 and all(is_token_negligible(t) for t in token.tokens[1:idx]))
        ):
            # either the subsequent parenthesis is closely followed or there is only whitespace/comment in between
            token = sub_paren
        else:
            break
    return token


def is_subquery(token: TokenList) -> bool:
    flag = False
    if isinstance(token, Parenthesis):
        token = get_innermost_parenthesis(token)
        # check if innermost parenthesis contains SELECT
        _, sub_token = token.token_next_by(m=(DML, "SELECT"))
        if sub_token is not None:
            flag = True
    return flag


def is_values_clause(token: Parenthesis) -> bool:
    for t in token.tokens:
        if is_token_negligible(t):
            continue
        if t.match(Keyword, "VALUES"):
            return True
    return False


def get_subquery_parentheses(
    token: Union[Identifier, Function, Where],
) -> List[SubQueryTuple]:
    """
    Retrieve subquery list
    the returned list is either empty when no subquery parsed or list of [parenthesis, alias] tuple
    """
    subquery = []
    as_idx, as_ = token.token_next_by(m=(Keyword, "AS"))
    sublist = list(token.get_sublists())
    if as_ is not None and len(sublist) == 1:
        # CTE: tbl AS (SELECT 1)
        target = sublist[0]
    else:
        if isinstance(token, Function):
            # CTE without AS: tbl (SELECT 1)
            target = token.tokens[-1]
        elif isinstance(token, Where):
            # WHERE col1 IN (SELECT max(col1) FROM tab2)
            target = token
        else:
            # normal subquery: (SELECT 1) tbl
            target = token.token_first(skip_cm=True)
    if isinstance(target, (Case, Where)):
        # CASE WHEN (SELECT count(*) from tab1) > 0 THEN (SELECT count(*) FROM tab1) ELSE -1
        for tk in target.get_sublists():
            if isinstance(tk, Comparison):
                if is_subquery(tk.left):
                    subquery.append(SubQueryTuple(tk.left, tk.left.get_real_name()))
                if is_subquery(tk.right):
                    subquery.append(SubQueryTuple(tk.right, tk.right.get_real_name()))
            elif is_subquery(tk):
                subquery.append(SubQueryTuple(tk, token.get_real_name()))
    elif is_subquery(target):
        target = remove_parenthesis_between_union(target)
        subquery = [
            SubQueryTuple(get_innermost_parenthesis(target), token.get_real_name())
        ]
    return subquery


def get_parameters(token: Function):
    """
    This is a replacement for sqlparse.sql.Function.get_parameters(), which produces problematic result for:
        if(col1 = 'foo' AND col2 = 'bar', 1, 0)
    This implementation ignores the constant parameter as we don't need them for column lineage for now
    """
    last_token = token.tokens[-1]
    parameters = []
    if isinstance(last_token, Over):
        # special handling for window function
        parameters = token.get_parameters()
    parameters += [
        tk for tk in last_token.tokens if tk.is_group or tk.ttype == Wildcard
    ]
    return parameters
