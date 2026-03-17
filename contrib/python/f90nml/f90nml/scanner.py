import string

# The Fortran character set

alpha = string.ascii_letters
digit = string.digits
alnum = alpha + digit + '_'

blank = ' '
# NOTE: Not all of these can act as token separators, but for now we interpret
#   them as blanks.
blank += '\t\f\r\n'

# Special characters, as defined in the language standard
special = '=+-*/\\()[]{},.:;!"%&~<>?\'`^|$#@'

charset = alnum + blank + special


# Control flags
non_delimited_strings = True
comment_tokens = '!#'


def notchar(chars, ref=charset):
    """Remove `chars` from a reference character set."""
    base = ref
    for c in chars:
        base = ''.join(base.split(c))
    return base


# DFA scanner

M = {}

M['start'] = {}
for d in (
    {c: 'blank' for c in blank},
    {c: 'cmt' for c in comment_tokens},
    {c: 'name' for c in alpha + '_'},
    {c: 'num' for c in digit},
    {"'": 'str_a'},
    {'"': 'str_q'},
    {'.': 'dec'},
    {'+': 'op_plus'},
    {'-': 'op_minus'},
    {c: 'op' for c in notchar('+-."\'' + comment_tokens, special)},
):
    M['start'].update(d)

# Blank (whitespace) tokens

M['blank'] = {}
for d in (
    {c: 'blank' for c in blank},
    {c: 'cmt' for c in comment_tokens},
    {c: 'end' for c in notchar(blank + comment_tokens)},
):
    M['blank'].update(d)

# This doesn't actually get used more than once, but it is correct.
M['cmt'] = {c: 'cmt' for c in notchar('\n')}
M['cmt']['\n'] = 'end'


# Identifiers (keywords, functions, variables, ...)
# NOTE: We permit identifiers to start with _ for preprocessor support
M['name'] = {}
for d in (
    {c: 'name' for c in alnum},
    {c: 'end' for c in notchar(alnum)},
):
    M['name'].update(d)

if non_delimited_strings:
    M['name']["'"] = 'name'
    M['name']['"'] = 'name'


# Apostrophe-delimited strings
M['str_a'] = {c: 'str_a' for c in notchar("'")}
M['str_a']["'"] = 'str_a_esc'

M['str_a_esc'] = {c: 'end' for c in notchar("'")}
M['str_a_esc']["'"] = 'str_a'


# Quote-delimited strings
M['str_q'] = {c: 'str_q' for c in notchar('"')}
M['str_q']['"'] = 'str_q_esc'

M['str_q_esc'] = {c: 'end' for c in notchar('"')}
M['str_q_esc']['"'] = 'str_q'


# Literal numeric
# NOTE: Decimals must be separate due to logicals (.true./.false.)
M['num'] = {}
for d in (
    {c: 'num' for c in digit},
    {c: 'num_float_e' for c in 'eEdD'},
    {'.': 'num_frac'},
    {c: 'num_float_sign' for c in '+-'},
    {'_': 'num_kind'},
    {c: 'end' for c in notchar(digit + '+-._eEdD')},
):
    M['num'].update(d)

M['num_frac'] = {}
for d in (
    {c: 'num_frac' for c in digit},
    {c: 'num_float_e' for c in 'eEdD'},
    {c: 'num_float_sign' for c in '+-'},
    {'_': 'num_kind'},
    {c: 'end' for c in notchar(digit + '+-_eEdD')},
):
    M['num_frac'].update(d)

# Numeric E notation token
M['num_float_e'] = {}
for d in (
    {c: 'num_float_sign' for c in '+-'},
    {c: 'num_float_exp' for c in digit},
    # Error: ^[0-9+-]
):
    M['num_float_e'].update(d)

# Numeric E notation exponent sign
M['num_float_sign'] = {c: 'num_float_exp' for c in digit}
    # Error: ^[0-9]

# Numeric E notation exponent
M['num_float_exp'] = {}
for d in (
    {c: 'num_float_exp' for c in digit},
    {'_': 'num_kind'},
    {c: 'end' for c in notchar(digit + '_')},
):
    M['num_float_exp'].update(d)

# Numeric kind token (_)
M['num_kind'] = {}
for d in (
    {c: 'num_kind_name' for c in alpha},
    {c: 'num_kind_int' for c in digit},
):
    M['num_kind'].update(d)

# Numeric kind as a variable name
# NOTE: This is identical to name, but might be useful for tokenization
M['num_kind_name'] = {}
for d in (
    {c: 'num_kind_name' for c in alnum},
    {c: 'end' for c in notchar(alnum)},
):
    M['num_kind_name'].update(d)

# Numeric kind as coded integer
# XXX: Why is this alnum?  Shouldn't it be digit?
M['num_kind_int'] = {}
for d in (
    {c: 'num_kind_int' for c in alnum},
    {c: 'end' for c in notchar(alnum)},
):
    M['num_kind_int'].update(d)

# ----
# Old numeric stuff.. not sure how it holds up

# Decimal mark
# TODO: Fix me!  This only represents the leading decimal mark.
M['dec'] = {}
for d in (
    {c: 'num' for c in digit},
    {'_': 'num_kind'},
    {c: 'op_keyword' for c in notchar('eEdD', ref=alpha)},
    {c: 'op_kw_test' for c in 'eEdD'},
    {c: 'end' for c in notchar(digit + alpha + '_')},
):
    M['dec'].update(d)

# TODO: These permit "+." and "-." which are not valid!
# TODO: "name" is only handled for +-inf and +-nan.  It could be tightened but
#   the DFA will be unpretty.

M['op_plus'] = {}
for d in (
    {c: 'num' for c in digit},
    {'.': 'num_frac'},
    {c: 'name' for c in alpha},
):
    M['op_plus'].update(d)

M['op_minus'] = {}
for d in (
    {c: 'num' for c in digit},
    {'.': 'num_frac'},
    {c: 'name' for c in alpha},
):
    M['op_minus'].update(d)

M['op_kw_test'] = {}
for d in (
    {c: 'op_keyword' for c in alpha},
    {c: 'num_float_sign' for c in '+-'},
    {c: 'num_float_exp' for c in digit},
    # Error: ^[a-z0-9+-]
):
    M['op_kw_test'].update(d)

# End decimal
#---


# Single-character tokens (operators, declaration, etc)
M['op'] = {c: 'end' for c in charset}


# TODO: We don't have keyword operators, just .true. and .false. values.
M['op_keyword'] = {}
for d in (
    {c: 'op_keyword' for c in alpha},
    {'.': 'op'},
    {c: 'end' for c in notchar(alpha + '.')},
):
    M['op_keyword'].update(d)


def scan(file):
    lexemes = []

    lex = ''
    state = 'start'

    for line in file:
        linelx = []

        for char in line:
            try:
                state = M[state][char]
            except KeyError:
                # This catches potential unicode characters in a string.
                # TODO: What about non-delimited strings?
                if state in ('str_a', 'str_q') and char == '\n':
                    # However, non-closed strings are an error
                    raise

            if state not in ('end', 'cmt'):
                lex += char

            elif (state == 'end'):
                linelx.append(lex)

                # Re-evaluate the current token (as a lookback)
                lex = char
                state = M['start'][char]

            elif (state == 'cmt'):
                # Find the index of the first comment character.
                buf = ''.join(linelx) + lex
                idx = len(buf[buf.rfind('\n') + 1:])

                # Skip per-character iteration and append the comment as blank.
                lex += line[idx:]
                state = 'blank'

                break

        lexemes.extend(linelx)

    # Append any trailing lexeme
    if lex:
        try:
            assert any('end' in M[state][c] for c in M[state])
        except AssertionError:
            print(M[state])
            raise

        lexemes.append(lex)

    return lexemes
