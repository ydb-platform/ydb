from clickhouse_connect.driver.common import unescape_identifier


def parse_callable(expr) -> tuple[str, tuple[str | int, ...], str]:
    """
    Parses a single level ClickHouse optionally 'callable' function/identifier.  The identifier is returned as the
    first value in the response tuple.  If the expression is callable -- i.e. an identifier followed by 0 or more
    arguments in parentheses, the second returned value is a tuple of the comma separated arguments.  The third and
    final tuple value is any text remaining after the initial expression for further parsing/processing.

    Examples:
      "Tuple(String, Enum('one' = 1, 'two' = 2))" will return "Tuple", ("String", "Enum('one' = 1,'two' = 2)"), ""
      "MergeTree() PARTITION BY key" will return "MergeTree", (), "PARTITION BY key"

    :param expr:  ClickHouse DDL or Column Name expression
    :return: Tuple of the identifier, a tuple of arguments, and remaining text
    """
    expr = expr.strip()
    pos = expr.find("(")
    space = expr.find(" ")
    if pos == -1 and space == -1:
        return expr, (), ""
    if space != -1 and (pos == -1 or space < pos):
        return expr[:space], (), expr[space:].strip()
    name = expr[:pos]
    pos += 1  # Skip first paren
    values = []
    value = ""
    quote: str | None = None
    level = 0

    def add_value():
        try:
            values.append(int(value))
        except ValueError:
            values.append(value)

    while True:
        char = expr[pos]
        pos += 1
        if quote:
            value += char
            if char == "\\" and pos < len(expr):
                value += expr[pos]
                pos += 1
            elif char == quote and pos < len(expr) and expr[pos] == quote:
                value += expr[pos]
                pos += 1
            elif char == quote:
                quote = None
        else:
            if level == 0:
                if char == " ":
                    space = pos
                    temp_char = expr[space]
                    while temp_char == " ":
                        space += 1
                        temp_char = expr[space]
                    if not value or temp_char in "()',=><0":
                        char = temp_char
                        pos = space + 1
                if char == ",":
                    add_value()
                    value = ""
                    continue
                if char == ")":
                    break
            if char in ("'", '"', "`"):
                quote = char
            elif char == "(":
                level += 1
            elif char == ")" and level:
                level -= 1
            value += char
    if value != "":
        add_value()
    return name, tuple(values), expr[pos:].strip()


def parse_enum(expr) -> tuple[tuple[str, ...], tuple[int, ...]]:
    """
    Parse a ClickHouse enum definition expression of the form ('key1' = 1, 'key2' = 2)
    :param expr: ClickHouse enum expression/arguments
    :return: Parallel tuples of string enum keys and integer enum values
    """
    keys = []
    values = []
    pos = expr.find("(") + 1
    in_key = False
    key: list[str] = []
    value: list[str] = []
    while True:
        char = expr[pos]
        pos += 1
        if in_key:
            if char == "'":
                if expr[pos] == "'":
                    key.append("'")
                    pos += 1
                else:
                    keys.append("".join(key))
                    key = []
                    in_key = False
            elif char == "\\" and expr[pos] == "'" and expr[pos : pos + 4] != "' = " and expr[pos:] != "')":
                key.append(expr[pos])
                pos += 1
            else:
                key.append(char)
        elif char not in (" ", "="):
            if char == ",":
                values.append(int("".join(value)))
                value = []
            elif char == ")":
                values.append(int("".join(value)))
                break
            elif char == "'" and not value:
                in_key = True
            else:
                value.append(char)
    sorted_values, sorted_keys = zip(*sorted(zip(values, keys)))
    return tuple(sorted_keys), tuple(sorted_values)


def parse_columns(expr: str, preserve_names: bool = False):
    """
    Parse a ClickHouse column list of the form (col1 String, col2 Array(Tuple(String, Int32))).  This also handles
    unnamed columns (such as Tuple definitions).  Mixed named and unnamed columns are not currently supported.
    :param expr: ClickHouse enum expression/arguments
    :param preserve_names: Keep identifier quoting in returned names. JSON parsing
        uses this to distinguish quoted typed paths from unquoted directives.
    :return: Parallel tuples of column names and column types (strings)
    """
    names = []
    columns = []
    pos = 1
    named = False
    level = 0
    label = ""
    quote = None
    while True:
        char = expr[pos]
        pos += 1
        if quote:
            if char == "\\" and pos < len(expr):
                label += char + expr[pos]
                pos += 1
                continue
            if char == quote and pos < len(expr) and expr[pos] == quote:
                label += char + expr[pos]
                pos += 1
                continue
            if char == quote:
                quote = None
        else:
            if level == 0:
                if char.isspace() or char == "=":
                    if label and not named:
                        names.append(label if preserve_names else unescape_identifier(label))
                        label = ""
                        named = True
                        char = "=" if preserve_names and names[-1].upper() == "SKIP" and char == "=" else ""
                    elif preserve_names and named and names[-1].upper() == "SKIP":
                        if char.isspace():
                            char = "" if not label or label.endswith(" ") else " "
                    else:
                        char = ""
                elif char == ",":
                    columns.append(label.rstrip() if preserve_names and named and names[-1].upper() == "SKIP" else label)
                    named = False
                    label = ""
                    continue
                elif char == ")":
                    if label or named or columns:
                        columns.append(label.rstrip() if preserve_names and named and names[-1].upper() == "SKIP" else label)
                    break
            if char in ("'", '"', "`"):
                quote = char
            elif char == "(":
                level += 1
            elif char == ")":
                level -= 1
        label += char
    return tuple(names), tuple(columns)
