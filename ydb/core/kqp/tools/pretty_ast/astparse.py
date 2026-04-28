class List:
    def __init__(self, is_quote):
        self.list = []
        self.is_quote = is_quote


class Element:
    def __init__(self, is_quote, value, is_quoted_str=False):
        self.value = value
        self.is_quote = is_quote
        self.is_quoted_str = is_quoted_str


class Reference:
    def __init__(self, alias):
        self.alias = alias


def read_string(line, pos):
    esc = False
    res = ''
    while pos < len(line):
        if esc:
            res += line[pos]
            pos += 1
            esc = False
            continue
        if line[pos] == '\\':
            esc = True
            pos += 1
            continue
        if line[pos] == '"':
            return res, pos + 1
        res += line[pos]
        pos += 1
    raise Exception("unterminated quoted string")


def read_num(line, pos):
    start = pos
    while pos < len(line):
        if not line[pos].isdigit():
            return int(line[start:pos]), pos
        pos += 1
    return int(line[start:]), pos


def read_keyword(line, pos):
    start = pos
    while pos < len(line):
        if line[pos] == ')' or line[pos].isspace():
            return line[start:pos], pos
        pos += 1
    return line[start:]


def parse(lines):
    curr_stack = [List(False)]
    is_quote = False

    def push(item):
        curr_stack[-1].list.append(item)

    for line in lines:
        line = line.strip()
        if not line:
            continue
        pos = 0
        while pos < len(line):
            if line[pos] == '\'':
                is_quote = True
                pos += 1
                continue

            if line[pos] == '(':
                l = List(is_quote)
                push(l)
                curr_stack.append(l)
                pos += 1
            elif line[pos] == '"':
                tok, pos = read_string(line, pos + 1)
                push(Element(is_quote, tok, is_quoted_str=True))
            elif line[pos].isdigit():
                tok, pos = read_num(line, pos)
                push(Element(is_quote, tok))
            elif line[pos] == ')':
                curr_stack.pop()
                pos += 1
            elif line[pos] == '$':
                tok, pos = read_num(line, pos + 1)
                push(Reference(tok))
            elif line[pos].isspace():
                pos += 1
            else:
                tok, pos = read_keyword(line, pos)
                push(Element(is_quote, tok))
            is_quote = False

    return curr_stack[0]
