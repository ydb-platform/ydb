import re
from typing import Optional, Union

from sqlparse.sql import Function, Identifier, Parenthesis

from collate_sqllineage.core.holders import SubQueryLineageHolder
from collate_sqllineage.core.models import Path, SubQuery, Table
from collate_sqllineage.core.parser.sqlparse.models import (
    SqlParseSubQuery,
    SqlParseTable,
)
from collate_sqllineage.core.parser.sqlparse.utils import (
    get_subquery_parentheses,
    is_values_clause,
)


def get_dataset_from_identifier(
    identifier: Identifier, holder: SubQueryLineageHolder
) -> Optional[Union[Path, SubQuery, Table]]:
    first_token = identifier.token_first(skip_cm=True)
    if isinstance(first_token, Function):
        # function() as alias, no dataset involved
        return None
    elif isinstance(first_token, Parenthesis) and is_values_clause(first_token):
        # (VALUES ...) AS alias, no dataset involved
        return None
    dataset: Union[Table, SubQuery, Path]
    path_match = re.match(r"(parquet|csv|json)\.`(.*)`", identifier.value)
    if path_match is not None:
        dataset = Path(path_match.groups()[1])
    else:
        read: Optional[Union[Table, SubQuery]] = None
        subqueries = get_subquery_parentheses(identifier)
        if len(subqueries) > 0:
            # SELECT col1 FROM (SELECT col2 FROM tab1) dt, the subquery will be parsed as Identifier
            # referring https://github.com/andialbrecht/sqlparse/issues/218 for further information
            parenthesis, alias = subqueries[0]
            read = SqlParseSubQuery.of(parenthesis, alias)
        else:
            cte_dict = {s.alias: s for s in holder.cte}
            if "." not in identifier.value:
                cte = cte_dict.get(identifier.get_real_name())
                if cte is not None:
                    # could reference CTE with or without alias
                    read = SqlParseSubQuery.of(
                        cte.query,
                        identifier.get_alias() or identifier.get_real_name(),
                    )
            if read is None:
                read = SqlParseTable.of(identifier)
        dataset = read
    return dataset
