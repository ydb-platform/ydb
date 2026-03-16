
def get_inner_spec(spec):
    brackets = 0
    offset = spec.find('(')
    if offset == -1:
        return ''
    i = offset
    for i, ch in enumerate(spec[offset:], offset):
        if ch == '(':
            brackets += 1

        elif ch == ')':
            brackets -= 1

        if brackets == 0:
            break

    return spec[offset + 1:i]
