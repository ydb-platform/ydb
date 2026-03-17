# -*- coding: utf-8 -*-
import glob
import os

from .lexer import lex
from .analyzer import analyze, enter_block_ctx
from .errors import NgxParserDirectiveError

# map of external / third-party directives to a parse function
EXTERNAL_PARSERS = {}


# TODO: raise special errors for invalid "if" args
def _prepare_if_args(stmt):
    """Removes parentheses from an "if" directive's arguments"""
    args = stmt['args']
    if args and args[0].startswith('(') and args[-1].endswith(')'):
        args[0] = args[0][1:].lstrip()
        args[-1] = args[-1][:-1].rstrip()
        start = int(not args[0])
        end = len(args) - int(not args[-1])
        args[:] = args[start:end]


def parse(filename, onerror=None, catch_errors=True, ignore=(), single=False,
        comments=False, strict=False, combine=False, check_ctx=True,
        check_args=True):
    """
    Parses an nginx config file and returns a nested dict payload

    :param filename: string contianing the name of the config file to parse
    :param onerror: function that determines what's saved in "callback"
    :param catch_errors: bool; if False, parse stops after first error
    :param ignore: list or tuple of directives to exclude from the payload
    :param combine: bool; if True, use includes to create a single config obj
    :param single: bool; if True, including from other files doesn't happen
    :param comments: bool; if True, including comments to json payload
    :param strict: bool; if True, unrecognized directives raise errors
    :param check_ctx: bool; if True, runs context analysis on directives
    :param check_args: bool; if True, runs arg count analysis on directives
    :returns: a payload that describes the parsed nginx config
    """
    config_dir = os.path.dirname(filename)

    payload = {
        'status': 'ok',
        'errors': [],
        'config': [],
    }

    # start with the main nginx config file/context
    includes = [(filename, ())]  # stores (filename, config context) tuples
    included = {filename: 0} # stores {filename: array index} map

    def _handle_error(parsing, e):
        """Adds representaions of an error to the payload"""
        file = parsing['file']
        error = str(e)
        line = getattr(e, 'lineno', None)

        parsing_error = {'error': error, 'line': line}
        payload_error = {'file': file, 'error': error, 'line': line}
        if onerror is not None:
            payload_error['callback'] = onerror(e)

        parsing['status'] = 'failed'
        parsing['errors'].append(parsing_error)

        payload['status'] = 'failed'
        payload['errors'].append(payload_error)

    def _parse(parsing, tokens, ctx=(), consume=False):
        """Recursively parses nginx config contexts"""
        fname = parsing['file']
        parsed = []

        # parse recursively by pulling from a flat stream of tokens
        for token, lineno, quoted in tokens:
            comments_in_args = []

            # we are parsing a block, so break if it's closing
            if token == '}' and not quoted:
                break

            # if we are consuming, then just continue until end of context
            if consume:
                # if we find a block inside this context, consume it too
                if token == '{' and not quoted:
                    _parse(parsing, tokens, consume=True)
                continue

            # the first token should always(?) be an nginx directive
            directive = token

            if combine:
                stmt = {
                    'file': fname,
                    'directive': directive,
                    'line': lineno,
                    'args': []
                }
            else:
                stmt = {
                    'directive': directive,
                    'line': lineno,
                    'args': []
                }

            # if token is comment
            if directive.startswith('#') and not quoted:
                if comments:
                    stmt['directive'] = '#'
                    stmt['comment'] = token[1:]
                    parsed.append(stmt)
                continue

            # TODO: add external parser checking and handling

            # parse arguments by reading tokens
            args = stmt['args']
            token, __, quoted = next(tokens)  # disregard line numbers of args
            while token not in ('{', ';', '}') or quoted:
                if token.startswith('#') and not quoted:
                    comments_in_args.append(token[1:])
                else:
                    stmt['args'].append(token)

                token, __, quoted = next(tokens)

            # consume the directive if it is ignored and move on
            if stmt['directive'] in ignore:
                # if this directive was a block consume it too
                if token == '{' and not quoted:
                    _parse(parsing, tokens, consume=True)
                continue

            # prepare arguments
            if stmt['directive'] == 'if':
                _prepare_if_args(stmt)

            try:
                # raise errors if this statement is invalid
                analyze(
                    fname=fname, stmt=stmt, term=token, ctx=ctx, strict=strict,
                    check_ctx=check_ctx, check_args=check_args
                )
            except NgxParserDirectiveError as e:
                if catch_errors:
                    _handle_error(parsing, e)

                    # if it was a block but shouldn't have been then consume
                    if e.strerror.endswith(' is not terminated by ";"'):
                        if token != '}' and not quoted:
                            _parse(parsing, tokens, consume=True)
                        else:
                            break

                    # keep on parsin'
                    continue
                else:
                    raise e

            # add "includes" to the payload if this is an include statement
            if not single and stmt['directive'] == 'include':
                pattern = args[0]
                if not os.path.isabs(args[0]):
                    pattern = os.path.join(config_dir, args[0])

                stmt['includes'] = []

                # get names of all included files
                if glob.has_magic(pattern):
                    fnames = glob.glob(pattern)
                    fnames.sort()
                else:
                    try:
                        # if the file pattern was explicit, nginx will check
                        # that the included file can be opened and read
                        open(str(pattern)).close()
                        fnames = [pattern]
                    except Exception as e:
                        fnames = []
                        e.lineno = stmt['line']
                        if catch_errors:
                            _handle_error(parsing, e)
                        else:
                            raise e

                for fname in fnames:
                    # the included set keeps files from being parsed twice
                    # TODO: handle files included from multiple contexts
                    if fname not in included:
                        included[fname] = len(includes)
                        includes.append((fname, ctx))
                    index = included[fname]
                    stmt['includes'].append(index)

            # if this statement terminated with '{' then it is a block
            if token == '{' and not quoted:
                inner = enter_block_ctx(stmt, ctx)  # get context for block
                stmt['block'] = _parse(parsing, tokens, ctx=inner)

            parsed.append(stmt)

            # add all comments found inside args after stmt is added
            for comment in comments_in_args:
                comment_stmt = {
                    'directive': '#',
                    'line': stmt['line'],
                    'args': [],
                    'comment': comment
                }
                parsed.append(comment_stmt)

        return parsed

    # the includes list grows as "include" directives are found in _parse
    for fname, ctx in includes:
        tokens = lex(fname)
        parsing = {
            'file': fname,
            'status': 'ok',
            'errors': [],
            'parsed': []
        }
        try:
            parsing['parsed'] = _parse(parsing, tokens, ctx=ctx)
        except Exception as e:
            _handle_error(parsing, e)

        payload['config'].append(parsing)

    if combine:
        return _combine_parsed_configs(payload)
    else:
        return payload


def _combine_parsed_configs(old_payload):
    """
    Combines config files into one by using include directives.

    :param old_payload: payload that's normally returned by parse()
    :return: the new combined payload
    """
    old_configs = old_payload['config']

    def _perform_includes(block):
        for stmt in block:
            if 'block' in stmt:
                stmt['block'] = list(_perform_includes(stmt['block']))
            if 'includes' in stmt:
                for index in stmt['includes']:
                    config = old_configs[index]['parsed']
                    for stmt in _perform_includes(config):
                        yield stmt
            else:
                yield stmt  # do not yield include stmt itself

    combined_config = {
        'file': old_configs[0]['file'],
        'status': 'ok',
        'errors': [],
        'parsed': []
    }

    for config in old_configs:
        combined_config['errors'] += config.get('errors', [])
        if config.get('status', 'ok') == 'failed':
            combined_config['status'] = 'failed'

    first_config = old_configs[0]['parsed']
    combined_config['parsed'] += _perform_includes(first_config)

    combined_payload = {
        'status': old_payload.get('status', 'ok'),
        'errors': old_payload.get('errors', []),
        'config': [combined_config]
    }
    return combined_payload


def register_external_parser(parser, directives):
    """
    :param parser: parser function
    :param directives: list of directive strings
    :return:
    """
    for directive in directives:
        EXTERNAL_PARSERS[directive] = parser
