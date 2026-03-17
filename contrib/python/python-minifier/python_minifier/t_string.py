"""
Template String (T-String) unparsing

T-strings in Python 3.14 follow PEP 750 and are based on PEP 701,
which means they don't have the quote restrictions of older f-strings.

This implementation is much simpler than f_string.py because:
- No quote tracking needed (PEP 701 benefits)
- No pep701 parameter needed (always true for t-strings)
- No Outer vs Inner distinction needed
- Always use all quote types
"""

import python_minifier.ast_compat as ast

from python_minifier import UnstableMinification
from python_minifier.ast_compare import CompareError, compare_ast
from python_minifier.expression_printer import ExpressionPrinter
from python_minifier.ministring import MiniString
from python_minifier.token_printer import TokenTypes
from python_minifier.util import is_constant_node


class TString(object):
    """
    A Template String (t-string)

    Much simpler than f-strings because PEP 701 eliminates quote restrictions
    """

    def __init__(self, node):
        assert isinstance(node, ast.TemplateStr)
        self.node = node
        # Always use all quotes - no restrictions due to PEP 701
        self.allowed_quotes = ['"', "'", '"""', "'''"]

    def is_correct_ast(self, code):
        """Check if the generated code produces the same AST"""
        try:
            c = ast.parse(code, 'TString candidate', mode='eval')
            compare_ast(self.node, c.body)
            return True
        except Exception:
            return False

    def complete_debug_specifier(self, partial_specifier_candidates, value_node):
        """Complete debug specifier candidates for an Interpolation node"""
        assert isinstance(value_node, ast.Interpolation)

        conversion = ''
        if value_node.conversion == 115:  # 's'
            conversion = '!s'
        elif value_node.conversion == 114 and value_node.format_spec is not None:
            # This is the default for debug specifiers, unless there's a format_spec
            conversion = '!r'
        elif value_node.conversion == 97:  # 'a'
            conversion = '!a'

        conversion_candidates = [x + conversion for x in partial_specifier_candidates]

        if value_node.format_spec is not None:
            # Handle format specifications in debug specifiers
            if isinstance(value_node.format_spec, ast.JoinedStr):
                import python_minifier.f_string
                format_specs = python_minifier.f_string.FormatSpec(value_node.format_spec, self.allowed_quotes, pep701=True).candidates()
                conversion_candidates = [c + ':' + fs for c in conversion_candidates for fs in format_specs]

        return [x + '}' for x in conversion_candidates]

    def candidates(self):
        """Generate all possible representations"""
        actual_candidates = []

        # Normal t-string candidates
        actual_candidates.extend(self._generate_candidates_with_processor('t', self.str_for))

        # Raw t-string candidates (if we detect backslashes)
        if self._contains_literal_backslashes():
            actual_candidates.extend(self._generate_candidates_with_processor('rt', self.raw_str_for))

        return filter(self.is_correct_ast, actual_candidates)

    def _generate_candidates_with_processor(self, prefix, str_processor):
        """Generate t-string candidates using the given prefix and string processor function."""
        candidates = []

        for quote in self.allowed_quotes:
            quote_candidates = ['']
            debug_specifier_candidates = []

            for v in self.node.values:
                if is_constant_node(v, ast.Constant) and isinstance(v.value, str):
                    # String literal part - check for debug specifiers

                    # Could this be used as a debug specifier?
                    if len(quote_candidates) < 10:
                        import re
                        debug_specifier = re.match(r'.*=\s*$', v.value)
                        if debug_specifier:
                            # Maybe! Save for potential debug specifier completion
                            try:
                                debug_specifier_candidates = [x + '{' + v.value for x in quote_candidates]
                            except Exception:
                                continue

                    try:
                        quote_candidates = [x + str_processor(v.value, quote) for x in quote_candidates]
                    except Exception:
                        continue

                elif isinstance(v, ast.Interpolation):
                    # Interpolated expression part - check for debug completion
                    try:
                        # Try debug specifier completion
                        completed = self.complete_debug_specifier(debug_specifier_candidates, v)

                        # Regular interpolation processing
                        interpolation_candidates = InterpolationValue(v).get_candidates()
                        quote_candidates = [x + y for x in quote_candidates for y in interpolation_candidates] + completed

                        debug_specifier_candidates = []
                    except Exception:
                        continue
                else:
                    raise RuntimeError('Unexpected TemplateStr value: %r' % v)

            candidates.extend([prefix + quote + x + quote for x in quote_candidates])

        return candidates

    def str_for(self, s, quote):
        """Convert string literal to properly escaped form"""
        # Use MiniString for optimal string representation
        # Always allowed due to PEP 701 - no backslash restrictions
        mini_s = str(MiniString(s, quote)).replace('{', '{{').replace('}', '}}')

        if mini_s == '':
            return '\\\n'
        return mini_s

    def raw_str_for(self, s):
        """
        Generate string representation for raw t-strings.
        Don't escape backslashes like MiniString does.
        """
        return s.replace('{', '{{').replace('}', '}}')

    def _contains_literal_backslashes(self):
        """
        Check if this t-string contains literal backslashes in constant values.
        This indicates it may need to be a raw t-string.
        """
        for node in ast.walk(self.node):
            if is_constant_node(node, ast.Str):
                if '\\' in node.s:
                    return True
        return False

    def __str__(self):
        """Generate the shortest valid t-string representation"""
        if len(self.node.values) == 0:
            return 't' + min(self.allowed_quotes, key=len) * 2

        candidates = list(self.candidates())

        # Validate all candidates
        for candidate in candidates:
            try:
                minified_t_string = ast.parse(candidate, 'python_minifier.t_string output', mode='eval').body
            except SyntaxError as syntax_error:
                raise UnstableMinification(syntax_error, '', candidate)

            try:
                compare_ast(self.node, minified_t_string)
            except CompareError as compare_error:
                raise UnstableMinification(compare_error, '', candidate)

        if not candidates:
            raise ValueError('Unable to create representation for t-string')

        return min(candidates, key=len)


class InterpolationValue(ExpressionPrinter):
    """
    A Template String Interpolation Part

    Handles ast.Interpolation nodes (equivalent to FormattedValue for f-strings)
    """

    def __init__(self, node):
        super(InterpolationValue, self).__init__()

        assert isinstance(node, ast.Interpolation)
        self.node = node
        # Always use all quotes - no restrictions due to PEP 701
        self.allowed_quotes = ['"', "'", '"""', "'''"]
        self.candidates = ['']

    def get_candidates(self):
        """Generate all possible representations of this interpolation"""

        self.printer.delimiter('{')

        if self.is_curly(self.node.value):
            self.printer.delimiter(' ')

        self._expression(self.node.value)

        # Handle conversion specifiers
        if self.node.conversion == 115:  # 's'
            self.printer.append('!s', TokenTypes.Delimiter)
        elif self.node.conversion == 114:  # 'r'
            self.printer.append('!r', TokenTypes.Delimiter)
        elif self.node.conversion == 97:  # 'a'
            self.printer.append('!a', TokenTypes.Delimiter)

        # Handle format specifications
        if self.node.format_spec is not None:
            self.printer.delimiter(':')

            # Format spec is a JoinedStr (f-string) in the AST
            if isinstance(self.node.format_spec, ast.JoinedStr):
                import python_minifier.f_string
                # Use f-string processing for format specs
                format_candidates = python_minifier.f_string.OuterFString(
                    self.node.format_spec, pep701=True
                ).candidates()
                # Remove the f/rf prefix and quotes to get just the format part
                format_parts = []
                for fmt in format_candidates:
                    # Handle both f"..." and rf"..." patterns
                    if fmt.startswith('rf'):
                        # Remove rf prefix and outer quotes
                        inner = fmt[2:]
                    elif fmt.startswith('f'):
                        # Remove f prefix and outer quotes
                        inner = fmt[1:]
                    else:
                        continue

                    if (inner.startswith('"') and inner.endswith('"')) or \
                       (inner.startswith("'") and inner.endswith("'")):
                        format_parts.append(inner[1:-1])
                    elif (inner.startswith('"""') and inner.endswith('"""')) or \
                         (inner.startswith("'''") and inner.endswith("'''")):
                        format_parts.append(inner[3:-3])
                    else:
                        format_parts.append(inner)

                if format_parts:
                    self._append(format_parts)
            else:
                # Simple constant format spec
                self.printer.append(str(self.node.format_spec), TokenTypes.Delimiter)

        self.printer.delimiter('}')

        self._finalize()
        return self.candidates

    def is_curly(self, node):
        """Check if expression starts with curly braces (needs space)"""
        if isinstance(node, (ast.SetComp, ast.DictComp, ast.Set, ast.Dict)):
            return True

        if isinstance(node, (ast.Expr, ast.Attribute, ast.Subscript)):
            return self.is_curly(node.value)

        if isinstance(node, (ast.Compare, ast.BinOp)):
            return self.is_curly(node.left)

        if isinstance(node, ast.Call):
            return self.is_curly(node.func)

        if isinstance(node, ast.BoolOp):
            return self.is_curly(node.values[0])

        if isinstance(node, ast.IfExp):
            return self.is_curly(node.body)

        return False

    def visit_Constant(self, node):
        """Handle constant values in interpolations"""
        if isinstance(node.value, str):
            # Use Str class from f_string module for string handling
            from python_minifier.f_string import Str
            self.printer.append(str(Str(node.value, self.allowed_quotes, pep701=True)), TokenTypes.NonNumberLiteral)
        elif isinstance(node.value, bytes):
            # Use Bytes class from f_string module for bytes handling
            from python_minifier.f_string import Bytes
            self.printer.append(str(Bytes(node.value, self.allowed_quotes)), TokenTypes.NonNumberLiteral)
        else:
            # Other constants (numbers, None, etc.)
            super().visit_Constant(node)

    def visit_TemplateStr(self, node):
        """Handle nested t-strings"""
        assert isinstance(node, ast.TemplateStr)
        if self.printer.previous_token in [TokenTypes.Identifier, TokenTypes.Keyword, TokenTypes.SoftKeyword]:
            self.printer.delimiter(' ')
        # Nested t-string - no quote restrictions due to PEP 701
        self._append(TString(node).candidates())

    def visit_JoinedStr(self, node):
        """Handle nested f-strings in t-strings"""
        assert isinstance(node, ast.JoinedStr)
        if self.printer.previous_token in [TokenTypes.Identifier, TokenTypes.Keyword, TokenTypes.SoftKeyword]:
            self.printer.delimiter(' ')

        import python_minifier.f_string
        # F-strings nested in t-strings also benefit from PEP 701
        self._append(python_minifier.f_string.OuterFString(node, pep701=True).candidates())

    def visit_Lambda(self, node):
        """Handle lambda expressions in interpolations"""
        self.printer.delimiter('(')
        super().visit_Lambda(node)
        self.printer.delimiter(')')

    def _finalize(self):
        """Finalize the current printer state"""
        self.candidates = [x + str(self.printer) for x in self.candidates]
        self.printer._code = ''

    def _append(self, candidates):
        """Append multiple candidate strings"""
        self._finalize()
        self.candidates = [x + y for x in self.candidates for y in candidates]
