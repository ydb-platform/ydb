# -*- coding: utf-8 -*-
from crossplane.lexer import register_external_lexer
from crossplane.builder import register_external_builder
from crossplane.compat import fix_pep_479
from crossplane.errors import NgxParserBaseException
from crossplane.ext.abstract import CrossplaneExtension


class EmplaceIter:
    def __init__(self, it):
        self.it = it
        self.ret = []

    def __iter__(self):
        return self

    def __next__(self):
        if len(self.ret) > 0:
            v = self.ret.pop()
            return v
        return next(self.it)

    next = __next__

    def put_back(self, v):
        self.ret.append(v)



class LuaBlockPlugin(CrossplaneExtension):
    """
    This plugin adds special handling for Lua code block directives (*_by_lua_block)
    We don't need to handle non-block or file directives because those are parsed
    correctly by base Crossplane functionality.
    """
    # todo maybe: populate the actual directive bit masks if analyzer/parser logic is needed
    directives = {
        'access_by_lua_block': [],
        'balancer_by_lua_block': [],
        'body_filter_by_lua_block': [],
        'content_by_lua_block': [],
        'header_filter_by_lua_block': [],
        'init_by_lua_block': [],
        'init_worker_by_lua_block': [],
        'log_by_lua_block': [],
        'rewrite_by_lua_block': [],
        'set_by_lua_block': [],
        'ssl_certificate_by_lua_block': [],
        'ssl_session_fetch_by_lua_block': [],
        'ssl_session_store_by_lua_block': [],
    }

    def register_extension(self):
        register_external_lexer(directives=self.directives, lexer=self.lex)
        register_external_builder(directives=self.directives, builder=self.build)

    @fix_pep_479
    def lex(self, char_iterator, directive):
        if directive == "set_by_lua_block":
            # https://github.com/openresty/lua-nginx-module#set_by_lua_block
            # The sole *_by_lua_block directive that has an arg
            arg = ''
            for char, line in char_iterator:
                if char.isspace():
                    if arg:
                        yield (arg, line, False)
                        break
                    while char.isspace():
                        char, line = next(char_iterator)

                arg += char

        depth = 0
        token = ''

        # check that Lua block starts correctly
        while True:
            char, line = next(char_iterator)
            if not char.isspace():
                break

        if char != "{":
            reason = 'expected { to start Lua block'
            raise LuaBlockParserSyntaxError(reason, filename=None, lineno=line)

        depth += 1

        char_iterator = EmplaceIter(char_iterator)

        # Grab everything in Lua block as a single token
        # and watch for curly brace '{' in strings
        for char, line in char_iterator:
            if char == '-':
                prev_char, prev_line = char, line
                char, comment_line = next(char_iterator)
                if char == '-':
                    token += '-'
                    while char != '\n':
                        token += char
                        char, line = next(char_iterator)
                else:
                    char_iterator.put_back((char, comment_line))
                    char, line = prev_char, prev_line
            elif char == '{':
                depth += 1
            elif char == '}':
                depth -= 1
            elif char in ('"', "'"):
                quote = char
                token += quote
                char, line = next(char_iterator)
                while char != quote:
                    token += quote if char == quote else char
                    char, line = next(char_iterator)

            if depth < 0:
                reason = 'unxpected "}"'
                raise LuaBlockParserSyntaxError(reason, filename=None, lineno=line)

            if depth == 0:
                yield (token, line, True)  # True because this is treated like a string
                yield (';', line, False)
                raise StopIteration
            token += char

    def parse(self, stmt, parsing, tokens, ctx=(), consume=False):
        pass

    def build(self, stmt, padding, indent=4, tabs=False):
        built = stmt['directive']
        if built == 'set_by_lua_block':
            block = stmt['args'][1]
            built += " %s" % stmt['args'][0]
        else:
            block = stmt['args'][0]
        return built + ' {' + block + '}'


class LuaBlockParserSyntaxError(NgxParserBaseException):
    pass
