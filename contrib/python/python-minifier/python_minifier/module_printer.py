import sys

import python_minifier.ast_compat as ast

from .expression_printer import ExpressionPrinter
from .token_printer import Delimiter


class ModulePrinter(ExpressionPrinter):
    """
    Builds the smallest possible exact representation of an ast
    """

    def __init__(self, indent_char='\t', prefer_single_line=False):
        super(ModulePrinter, self).__init__(prefer_single_line=prefer_single_line)
        self.indent_char = indent_char

    def __call__(self, module):
        """
        Generate the source code for an AST

        :param module: The Module to generate code for
        :type module: ast.Module
        :rtype: str

        """

        assert isinstance(module, ast.Module)

        self.visit_Module(module)
        # On Python 2.7, preserve unicode strings to avoid encoding issues
        code = unicode(self.printer) if sys.version_info[0] < 3 else str(self.printer)
        return code.rstrip('\n' + self.indent_char + ';')

    @property
    def code(self):
        # On Python 2.7, preserve unicode strings to avoid encoding issues
        code = unicode(self.printer) if sys.version_info[0] < 3 else str(self.printer)
        return code.rstrip('\n' + self.indent_char + ';')

    # region Simple Statements

    def visit_Exec(self, node):
        assert isinstance(node, ast.Exec)

        self.printer.keyword('exec')
        self._expression(node.body)

        if node.globals:
            self.printer.keyword('in')
            self._expression(node.globals)

        if node.locals:
            self.printer.delimiter(',')
            self._expression(node.locals)

        self.printer.end_statement()

    def visit_Expr(self, node):
        assert isinstance(node, ast.Expr)

        if isinstance(node.value, (ast.Yield, ast.YieldFrom)):
            self._yield_expr(node.value)
        else:
            self._testlist(node.value)

        self.printer.end_statement()

    def visit_Assert(self, node):
        assert isinstance(node, ast.Assert)

        self.printer.keyword('assert')
        self._expression(node.test)

        if node.msg:
            self.printer.delimiter(',')
            self._expression(node.msg)

        self.printer.end_statement()

    def visit_Assign(self, node):
        assert isinstance(node, ast.Assign)

        for target_node in node.targets:
            self._testlist(target_node)
            self.printer.delimiter('=')

        # Yield nodes that are the sole node on the right hand side of an assignment do not need parens
        if isinstance(node.value, (ast.Yield, ast.YieldFrom)):
            self._yield_expr(node.value)
        else:
            self._testlist(node.value)

        self.printer.end_statement()

    def visit_AugAssign(self, node):
        assert isinstance(node, ast.AugAssign)

        self._testlist(node.target)
        self.visit(node.op)
        self.printer.delimiter('=')

        # Yield nodes that are the sole node on the right hand side of an assignment do not need parens
        if isinstance(node.value, (ast.Yield, ast.YieldFrom)):
            self._yield_expr(node.value)
        else:
            self._testlist(node.value)

        self.printer.end_statement()

    def visit_AnnAssign(self, node):
        assert isinstance(node, ast.AnnAssign)

        if node.simple:
            self.visit(node.target)
        else:
            self.printer.delimiter('(')
            self._expression(node.target)
            self.printer.delimiter(')')

        if node.annotation:
            self.printer.delimiter(':')
            self._expression(node.annotation)

        if node.value:
            self.printer.delimiter('=')

            self._expression(node.value)

        self.printer.end_statement()

    def visit_Pass(self, node):
        assert isinstance(node, ast.Pass)

        self.printer.keyword('pass')
        self.printer.end_statement()

    def visit_Delete(self, node):
        assert isinstance(node, ast.Delete)

        self.printer.keyword('del')
        self._exprlist(node.targets)
        self.printer.end_statement()

    def visit_Return(self, node):
        assert isinstance(node, ast.Return)

        self.printer.keyword('return')
        if isinstance(node.value, ast.Tuple):
            if sys.version_info < (3, 8) and [n for n in node.value.elts if isinstance(n, ast.Starred)]:
                self.printer.delimiter('(')
                self._testlist(node.value)
                self.printer.delimiter(')')
            else:
                self._testlist(node.value)
        elif node.value is not None:
            self._testlist(node.value)
        self.printer.end_statement()

    def visit_Print(self, node):
        assert isinstance(node, ast.Print)

        self.printer.keyword('print')

        delimiter = Delimiter(self.printer)

        if node.dest:
            delimiter.new_item()
            self.printer.operator('>>')
            self._expression(node.dest)

        for v in node.values:
            delimiter.new_item()
            self._expression(v)

        if not node.nl:
            self.printer.delimiter(',')

        self.printer.end_statement()

    def visit_Yield(self, node):
        assert isinstance(node, ast.Yield)

        self._yield_expr(node)
        self.printer.end_statement()

    def visit_YieldFrom(self, node):
        assert isinstance(node, ast.YieldFrom)

        self._yield_expr(node)
        self.printer.end_statement()

    def visit_Raise(self, node):
        assert isinstance(node, ast.Raise)

        self.printer.keyword('raise')

        if hasattr(node, 'type'):
            # Python2 raise node

            if node.type:
                self._expression(node.type)
            if node.inst:
                self.printer.delimiter(',')
                self._expression(node.inst)
            if node.tback:
                self.printer.delimiter(',')
                self._expression(node.tback)

        else:
            # Python3

            if node.exc:
                self._expression(node.exc)

            if node.cause:
                self.printer.keyword('from')
                self._expression(node.cause)

        self.printer.end_statement()

    def visit_Break(self, node):
        assert isinstance(node, ast.Break)

        self.printer.keyword('break')
        self.printer.end_statement()

    def visit_Continue(self, node):
        assert isinstance(node, ast.Continue)

        self.printer.keyword('continue')
        self.printer.end_statement()

    def visit_Import(self, node):
        assert isinstance(node, ast.Import)

        self.printer.keyword('import')

        delimiter = Delimiter(self.printer)
        for n in node.names:
            delimiter.new_item()
            self.visit_alias(n)

        self.printer.end_statement()

    def visit_ImportFrom(self, node):
        assert isinstance(node, ast.ImportFrom)

        self.printer.keyword('from')
        for _i in range(node.level):
            self.printer.delimiter('.')
        if node.module is not None:
            self.printer.identifier(node.module)

        self.printer.keyword('import')

        delimiter = Delimiter(self.printer)
        for n in node.names:
            delimiter.new_item()

            if node.module == '__future__' and n.name == 'unicode_literals':
                self.printer.unicode_literals = True

            if n.name == '*':
                self.printer.operator('*')
            else:
                self.visit_alias(n)

        self.printer.end_statement()

    def visit_alias(self, node):
        assert isinstance(node, ast.alias)

        self.printer.identifier(node.name)

        if node.asname:
            self.printer.keyword('as')
            self.printer.identifier(node.asname)

    def visit_Global(self, node):
        assert isinstance(node, ast.Global)

        self.printer.keyword('global')
        delimiter = Delimiter(self.printer)
        for n in node.names:
            delimiter.new_item()
            self.printer.identifier(n)

        self.printer.end_statement()

    def visit_Nonlocal(self, node):
        assert isinstance(node, ast.Nonlocal)

        self.printer.keyword('nonlocal')
        delimiter = Delimiter(self.printer)
        for n in node.names:
            delimiter.new_item()
            self.printer.identifier(n)

        self.printer.end_statement()

    # endregion

    # region Compound Statements

    def visit_If(self, node, el=False):
        assert isinstance(node, ast.If)

        self.printer.newline()

        if el:
            self.printer.keyword('elif')
        else:
            self.printer.keyword('if')

        self._expression(node.test)
        self.printer.delimiter(':')

        self._suite(node.body)

        if node.orelse:
            if len(node.orelse) == 1 and isinstance(node.orelse[0], ast.If):
                # elif
                self.visit_If(node.orelse[0], el=True)
                self.printer.newline()
            else:
                # an else block
                self.printer.keyword('else')
                self.printer.delimiter(':')
                self._suite(node.orelse)

    def visit_For(self, node, is_async=False):
        assert isinstance(node, (ast.For, ast.AsyncFor))

        self.printer.newline()

        if is_async:
            self.printer.keyword('async')

        self.printer.keyword('for')
        self._exprlist([node.target])
        self.printer.keyword('in')
        self._expression(node.iter)
        self.printer.delimiter(':')

        self._suite(node.body)

        if node.orelse:
            self.printer.newline()
            self.printer.keyword('else')
            self.printer.delimiter(':')
            self._suite(node.orelse)

    def visit_While(self, node):
        assert isinstance(node, ast.While)

        self.printer.newline()
        self.printer.keyword('while')
        self._expression(node.test)
        self.printer.delimiter(':')
        self._suite(node.body)

        if node.orelse:
            self.printer.keyword('else')
            self.printer.delimiter(':')
            self._suite(node.orelse)

    def visit_Try(self, node, star=False):
        assert isinstance(node, (ast.Try, ast.TryStar))

        self.printer.newline()
        self.printer.keyword('try')
        self.printer.delimiter(':')
        self._suite(node.body)

        [self.visit_ExceptHandler(n, star) for n in node.handlers]

        if node.orelse:
            self.printer.keyword('else')
            self.printer.delimiter(':')
            self._suite(node.orelse)

        if node.finalbody:
            self.printer.keyword('finally')
            self.printer.delimiter(':')
            self._suite(node.finalbody)

    def visit_TryStar(self, node):
        assert isinstance(node, ast.TryStar)
        self.visit_Try(node, star=True)

    def visit_TryFinally(self, node):
        assert isinstance(node, ast.TryFinally)

        if len(node.body) == 1 and isinstance(node.body[0], ast.TryExcept):
            self.visit_TryExcept(node.body[0])
        else:
            self.printer.newline()
            self.printer.keyword('try')
            self.printer.delimiter(':')
            self._suite(node.body)

        if node.finalbody:
            self.printer.keyword('finally')
            self.printer.delimiter(':')
            self._suite(node.finalbody)

    def visit_TryExcept(self, node):
        assert isinstance(node, ast.TryExcept)

        self.printer.newline()
        self.printer.keyword('try')
        self.printer.delimiter(':')
        self._suite(node.body)

        [self.visit_ExceptHandler(n) for n in node.handlers]

        if node.orelse:
            self.printer.keyword('else')
            self.printer.delimiter(':')
            self._suite(node.orelse)

    def visit_ExceptHandler(self, node, star=False):
        assert isinstance(node, ast.ExceptHandler)

        self.printer.keyword('except')

        if star:
            self.printer.operator('*')

        if node.type is not None:
            self._expression(node.type)

        if node.name is not None:
            self.printer.keyword('as')

            if isinstance(node.name, str):
                self.printer.identifier(node.name)
            else:
                self._expression(node.name)

        self.printer.delimiter(':')

        self._suite(node.body)

    def visit_With(self, node, is_async=False):
        assert isinstance(node, (ast.With, ast.AsyncWith))

        self.printer.newline()

        if is_async:
            self.printer.keyword('async')

        self.printer.keyword('with')

        delimiter = Delimiter(self.printer)
        if hasattr(node, 'items'):
            for item in node.items:
                delimiter.new_item()

                if self.precedence(item.context_expr) != 0 and self.precedence(item.context_expr) <= self.precedence(
                    node
                ):
                    self.printer.delimiter('(')
                    self.visit_withitem(item)
                    self.printer.delimiter(')')
                else:
                    self.visit_withitem(item)
        else:
            self.visit_withitem(node)

        self.printer.delimiter(':')
        self._suite(node.body)

    def visit_withitem(self, node):
        assert isinstance(node, (ast.withitem, ast.With))

        self._expression(node.context_expr)

        if node.optional_vars is not None:
            self.printer.keyword('as')
            self._expression(node.optional_vars)

    def visit_FunctionDef(self, node, is_async=False):
        assert isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))

        self.printer.newline()

        for d in node.decorator_list:
            self.printer.operator('@')
            self._expression(d)
            self.printer.newline()

        if is_async:
            self.printer.keyword('async')

        self.printer.keyword('def')
        self.printer.identifier(node.name)

        if hasattr(node, 'type_params') and node.type_params:
            self.printer.delimiter('[')
            delimiter = Delimiter(self.printer)
            for type_param in node.type_params:
                delimiter.new_item()
                self.visit(type_param)
            self.printer.delimiter(']')

        self.printer.delimiter('(')
        self.visit_arguments(node.args)
        self.printer.delimiter(')')

        if hasattr(node, 'returns') and node.returns is not None:
            self.printer.delimiter('->')
            self._expression(node.returns)
            self.printer.delimiter(':')
        else:
            self.printer.delimiter(':')

        if hasattr(node, 'docstring') and node.docstring is not None:
            self._suite([ast.Expr(value=ast.Str(s=node.docstring))] + node.body)
        else:
            self._suite(node.body)

    def visit_ClassDef(self, node):
        assert isinstance(node, ast.ClassDef)

        self.printer.newline()

        for d in node.decorator_list:
            self.printer.operator('@')
            self._expression(d)
            self.printer.newline()

        self.printer.keyword('class')
        self.printer.identifier(node.name)

        if hasattr(node, 'type_params') and node.type_params:
            self.printer.delimiter('[')
            delimiter = Delimiter(self.printer)
            for type_param in node.type_params:
                delimiter.new_item()
                self.visit(type_param)
            self.printer.delimiter(']')

        with Delimiter(self.printer, add_parens=True) as delimiter:

            for b in node.bases:
                delimiter.new_item()
                self._expression(b)

            if hasattr(node, 'starargs') and node.starargs is not None:
                delimiter.new_item()
                self.printer.operator('*')
                self._expression(node.starargs)

            if hasattr(node, 'keywords'):
                for kw in node.keywords:
                    delimiter.new_item()
                    self.visit_keyword(kw)

            if hasattr(node, 'kwargs') and node.kwargs is not None:
                delimiter.new_item()
                self.printer.operator('**')
                self.visit(node.kwargs)

        self.printer.delimiter(':')

        if hasattr(node, 'docstring') and node.docstring is not None:
            self._suite([ast.Expr(value=ast.Str(s=node.docstring))] + node.body)
        else:
            self._suite(node.body)

    # endregion

    # region Pattern Matching

    def pattern(self, pattern_node):
        assert isinstance(pattern_node, (ast.MatchValue, ast.MatchAs, ast.MatchStar, ast.MatchOr, ast.MatchSingleton, ast.MatchClass, ast.MatchSequence, ast.MatchMapping))
        self.visit(pattern_node)

    def visit_Match(self, node):
        assert isinstance(node, ast.Match)

        self.printer.newline()

        self.printer.keyword('match')

        self._expression(node.subject)

        self.printer.delimiter(':')

        self._suite(node.cases)

    def visit_match_case(self, node):
        assert isinstance(node, ast.match_case)
        self.printer.keyword('case')

        if isinstance(node.pattern, ast.MatchSequence):
            self.visit_MatchSequence(node.pattern, omit_brackets=True)
        else:
            self.pattern(node.pattern)

        if node.guard is not None:
            self.printer.keyword('if')
            self.visit(node.guard)

        self.printer.delimiter(':')
        self._suite(node.body)

    def visit_MatchValue(self, node):
        assert isinstance(node, ast.MatchValue)
        self.visit(node.value)

    def visit_MatchSingleton(self, node):
        assert isinstance(node, ast.MatchSingleton)

        self.printer.identifier(repr(node.value))

    def visit_MatchStar(self, node):
        assert isinstance(node, ast.MatchStar)

        self.printer.operator('*')

        if node.name is None:
            self.printer.identifier('_')
        else:
            self.printer.identifier(node.name)

    def visit_MatchSequence(self, node, omit_brackets=False):
        assert isinstance(node, ast.MatchSequence)

        if len(node.patterns) < 2 or not omit_brackets:
            self.printer.delimiter('[')

        delimiter = Delimiter(self.printer)
        for pattern in node.patterns:
            delimiter.new_item()
            self.pattern(pattern)

        if len(node.patterns) < 2 or not omit_brackets:
            self.printer.delimiter(']')

    def visit_MatchMapping(self, node):
        self.printer.delimiter('{')

        delimiter = Delimiter(self.printer)
        for k, p in zip(node.keys, node.patterns):
            delimiter.new_item()

            self._expression(k)
            self.printer.delimiter(':')

            self.pattern(p)

        if node.rest is not None:
            delimiter.new_item()

            self.printer.operator('**')
            self.printer.identifier(node.rest)

        self.printer.delimiter('}')

    def visit_MatchClass(self, node):
        assert isinstance(node, ast.MatchClass)

        self.visit(node.cls)
        self.printer.delimiter('(')

        delimiter = Delimiter(self.printer)
        for pattern in node.patterns:
            delimiter.new_item()
            self.pattern(pattern)

        for kwd, pattern in zip(node.kwd_attrs, node.kwd_patterns):
            delimiter.new_item()

            self.printer.identifier(kwd)
            self.printer.delimiter('=')

            self.pattern(pattern)

        self.printer.delimiter(')')

    def visit_MatchAs(self, node):
        assert isinstance(node, ast.MatchAs)

        if node.pattern is not None:
            if isinstance(node.pattern, ast.MatchAs):
                self.printer.delimiter('(')
                self.pattern(node.pattern)
                self.printer.delimiter(')')
            else:
                self.pattern(node.pattern)

            self.printer.keyword('as')

        if node.name is None:
            self.printer.identifier('_')
        else:
            self.printer.identifier(node.name)

    def visit_MatchOr(self, node):
        assert isinstance(node, ast.MatchOr)

        delimiter = Delimiter(self.printer, delimiter='|')
        for pattern in node.patterns:
            delimiter.new_item()

            if isinstance(pattern, (ast.MatchAs, ast.MatchOr)):
                self.printer.delimiter('(')
                self.pattern(pattern)
                self.printer.delimiter(')')
            else:
                self.pattern(pattern)

    # endregion

    # region async and await

    def visit_AsyncFunctionDef(self, node):
        assert isinstance(node, ast.AsyncFunctionDef)
        self.visit_FunctionDef(node, is_async=True)

    def visit_AsyncFor(self, node):
        assert isinstance(node, ast.AsyncFor)
        self.visit_For(node, is_async=True)

    def visit_AsyncWith(self, node):
        assert isinstance(node, ast.AsyncWith)
        self.visit_With(node, is_async=True)

    # endregion

    # region Generic Types
    def visit_TypeAlias(self, node):
        assert isinstance(node, ast.TypeAlias)
        self.printer.keyword('type')
        self.visit_Name(node.name)

        if hasattr(node, 'type_params') and node.type_params:
            self.printer.delimiter('[')
            delimiter = Delimiter(self.printer)
            for param in node.type_params:
                delimiter.new_item()
                self.visit(param)
            self.printer.delimiter(']')

        self.printer.delimiter('=')
        self._expression(node.value)
        self.printer.end_statement()

    def visit_TypeVar(self, node):
        assert isinstance(node, ast.TypeVar)
        assert isinstance(node.name, str)
        self.printer.identifier(node.name)

        if node.bound:
            self.printer.delimiter(':')
            self._expression(node.bound)

        if hasattr(node, 'default_value') and node.default_value is not None:
            self.printer.delimiter('=')
            self._expression(node.default_value)

    def visit_TypeVarTuple(self, node):
        assert isinstance(node, ast.TypeVarTuple)
        self.printer.operator('*')
        self.printer.identifier(node.name)

        if hasattr(node, 'default_value') and node.default_value is not None:
            self.printer.delimiter('=')
            self._expression(node.default_value)

    def visit_ParamSpec(self, node):
        assert isinstance(node, ast.ParamSpec)
        self.printer.operator('*')
        self.printer.operator('*')
        self.printer.identifier(node.name)

        if hasattr(node, 'default_value') and node.default_value is not None:
            self.printer.delimiter('=')
            self._expression(node.default_value)

    # endregion

    def visit_Module(self, node):
        if hasattr(node, 'docstring') and node.docstring is not None:
            # Python 3.6 added a docstring field! Really useful for every use case except this one...
            # Put the docstring back into the body
            self._suite_body([ast.Expr(value=ast.Str(s=node.docstring))] + node.body)
        else:
            self._suite_body(node.body)

    def _suite(self, node_list):

        compound_statements = [
            'For',
            'While',
            'Try',
            'TryStar',
            'If',
            'With',
            'ClassDef',
            'TryFinally',
            'TryExcept',
            'FunctionDef',
            'AsyncFunctionDef',
            'AsyncFor',
            'AsyncWith',
            'Match',
            'match_case'
        ]

        if [node for node in node_list if node.__class__.__name__ in compound_statements]:
            self.printer.enter_block()
            self._suite_body(node_list)
            self.printer.leave_block()
        else:
            self.printer.indent += 1
            self._suite_body(node_list)
            self.printer.indent -= 1
            self.printer.newline()

    def _suite_body(self, node_list):

        statements = {
            'Assign': self.visit_Assign,
            'AnnAssign': self.visit_AnnAssign,
            'AugAssign': self.visit_AugAssign,
            'Expr': self.visit_Expr,
            'Delete': self.visit_Delete,
            'Pass': self.visit_Pass,
            'Import': self.visit_Import,
            'ImportFrom': self.visit_ImportFrom,
            'Global': self.visit_Global,
            'Nonlocal': self.visit_Nonlocal,
            'Assert': self.visit_Assert,
            'Break': self.visit_Break,
            'Continue': self.visit_Continue,
            'Return': self.visit_Return,
            'Raise': self.visit_Raise,
            'Yield': self.visit_Yield,
            'YieldFrom': self.visit_YieldFrom,
            'For': self.visit_For,
            'While': self.visit_While,
            'Try': self.visit_Try,
            'TryStar': self.visit_TryStar,
            'If': self.visit_If,
            'With': self.visit_With,
            'ClassDef': self.visit_ClassDef,
            'FunctionDef': self.visit_FunctionDef,
            'AsyncFunctionDef': self.visit_AsyncFunctionDef,
            'AsyncFor': self.visit_AsyncFor,
            'AsyncWith': self.visit_AsyncWith,
            'TryFinally': self.visit_TryFinally,
            'TryExcept': self.visit_TryExcept,
            'Print': self.visit_Print,
            'Exec': self.visit_Exec,
            'Match': self.visit_Match,
            'match_case': self.visit_match_case,
            'TypeAlias': self.visit_TypeAlias
        }

        for node in node_list:
            statements[node.__class__.__name__](node)
