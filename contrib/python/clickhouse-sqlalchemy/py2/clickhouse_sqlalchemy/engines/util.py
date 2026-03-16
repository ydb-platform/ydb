

def parse_columns(str_columns, delimeter=',', quote_symbol='`',
                  escape_symbol='\\'):
    if not str_columns:
        return []

    in_column = False
    quoted = False
    prev_symbol = None
    brackets_count = 0

    rv = []
    col = ''
    for i, x in enumerate(str_columns + delimeter):
        if x == delimeter and not quoted and brackets_count == 0:
            in_column = False
            rv.append(col)
            col = ''

        elif x == ' ' and not in_column:
            continue

        elif x == '(':
            brackets_count += 1
            col += x

        elif x == ')':
            brackets_count -= 1
            col += x

        else:
            if x == quote_symbol:
                if prev_symbol != escape_symbol:
                    if not quoted:
                        quoted = True
                        in_column = True
                    else:
                        quoted = False
                        in_column = False
                else:
                    col = col[:-1] + x

            else:
                if not in_column:
                    in_column = True

                col += x

        prev_symbol = x

    return rv
