def walk_sql(sql: str, start: int = 0):
    """Yield (index, char, depth) for unquoted chars, tracking paren depth."""
    depth = 0
    quote_char = None
    escape = False
    for i in range(start, len(sql)):
        char = sql[i]
        if escape:
            escape = False
            continue
        if quote_char:
            if char == "\\" and quote_char == "'":
                escape = True
            elif char == quote_char:
                quote_char = None
            continue
        if char in {"'", '"', "`"}:
            quote_char = char
            continue
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        yield i, char, depth


def extract_parenthesized_block(sql: str, start: int) -> tuple[str, int]:
    """Return the content and closing index of the first parenthesized block."""
    block_start = -1
    for i, char, depth in walk_sql(sql, start):
        if char == "(" and depth == 1 and block_start == -1:
            block_start = i + 1
        elif char == ")" and depth == 0 and block_start != -1:
            return sql[block_start:i], i
    raise ValueError("Could not parse parenthesized SQL block")


def split_top_level(sql: str, delimiter: str = ",") -> list[str]:
    """Split SQL on *delimiter* only at the top nesting level."""
    parts = []
    part_start = 0
    for i, char, depth in walk_sql(sql):
        if char == delimiter and depth == 0:
            part = sql[part_start:i].strip()
            if part:
                parts.append(part)
            part_start = i + 1
    tail = sql[part_start:].strip()
    if tail:
        parts.append(tail)
    return parts


def find_top_level_clause(sql: str, clauses: tuple[str, ...]) -> tuple[int, str | None]:
    """Find the first occurrence of any *clause* at top nesting level."""
    upper_sql = sql.upper()
    for i, _char, depth in walk_sql(sql):
        if depth == 0:
            for clause in clauses:
                if upper_sql.startswith(clause, i):
                    return i, clause
    return -1, None
