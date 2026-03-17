"""A reconstructor for HCL2 implemented using Lark's experimental reconstruction functionality"""

import re
from typing import List, Dict, Callable, Optional, Union, Any, Tuple

from lark import Lark, Tree
from lark.grammar import Terminal, Symbol
from lark.lexer import Token, PatternStr, TerminalDef
from lark.reconstruct import Reconstructor
from lark.tree_matcher import is_discarded_terminal
from lark.visitors import Transformer_InPlace
from regex import regex

from hcl2.const import START_LINE_KEY, END_LINE_KEY
from hcl2.parser import reconstruction_parser


# function to remove the backslashes within interpolated portions
def reverse_quotes_within_interpolation(interp_s: str) -> str:
    """
    A common operation is to `json.dumps(s)` where s is a string to output in
    HCL. This is useful for automatically escaping any quotes within the
    string, but this escapes quotes within interpolation incorrectly. This
    method removes any erroneous escapes within interpolated segments of a
    string.
    """
    return re.sub(r"\$\{(.*)}", lambda m: m.group(0).replace('\\"', '"'), interp_s)


class WriteTokensAndMetaTransformer(Transformer_InPlace):
    """
    Inserts discarded tokens into their correct place, according to the rules
    of grammar, and annotates with metadata during reassembly. The metadata
    tracked here include the terminal which generated a particular string
    output, and the rule that that terminal was matched on.

    This is a modification of lark.reconstruct.WriteTokensTransformer
    """

    tokens: Dict[str, TerminalDef]
    term_subs: Dict[str, Callable[[Symbol], str]]

    def __init__(
        self,
        tokens: Dict[str, TerminalDef],
        term_subs: Dict[str, Callable[[Symbol], str]],
    ) -> None:
        super().__init__()
        self.tokens = tokens
        self.term_subs = term_subs

    def __default__(self, data, children, meta):
        """
        This method is called for every token the transformer visits.
        """

        if not getattr(meta, "match_tree", False):
            return Tree(data, children)
        iter_args = iter(
            [child[2] if isinstance(child, tuple) else child for child in children]
        )
        to_write = []
        for sym in meta.orig_expansion:
            if is_discarded_terminal(sym):
                try:
                    value = self.term_subs[sym.name](sym)
                except KeyError as exc:
                    token = self.tokens[sym.name]
                    if not isinstance(token.pattern, PatternStr):
                        raise NotImplementedError(
                            f"Reconstructing regexps not supported yet: {token}"
                        ) from exc

                    value = token.pattern.value

                # annotate the leaf with the specific rule (data) and terminal
                # (sym) it was generated from
                to_write.append((data, sym, value))
            else:
                item = next(iter_args)
                if isinstance(item, list):
                    to_write += item
                else:
                    if isinstance(item, Token):
                        # annotate the leaf with the specific rule (data) and
                        # terminal (sym) it was generated from
                        to_write.append((data, sym, item))
                    else:
                        to_write.append(item)

        return to_write


class HCLReconstructor(Reconstructor):
    """This class converts a Lark.Tree AST back into a string representing the underlying HCL code."""

    def __init__(
        self,
        parser: Lark,
        term_subs: Optional[Dict[str, Callable[[Symbol], str]]] = None,
    ):
        Reconstructor.__init__(self, parser, term_subs)

        self.write_tokens: WriteTokensAndMetaTransformer = (
            WriteTokensAndMetaTransformer(
                {token.name: token for token in self.tokens}, term_subs or {}
            )
        )

        # these variables track state during reconstruction to enable us to make
        # informed decisions about formatting output. They are primarily used
        # by the _should_add_space(...) method.
        self._last_char_space = True
        self._last_terminal: Union[Terminal, None] = None
        self._last_rule: Union[Tree, Token, None] = None
        self._deferred_item = None

    def should_be_wrapped_in_spaces(self, terminal: Terminal) -> bool:
        """Whether given terminal should be wrapped in spaces"""
        return terminal.name in {
            "IF",
            "IN",
            "FOR",
            "FOR_EACH",
            "FOR_OBJECT_ARROW",
            "COLON",
            "QMARK",
            "BINARY_OP",
        }

    def _is_equals_sign(self, terminal) -> bool:
        return (
            isinstance(self._last_rule, Token)
            and self._last_rule.value in ("attribute", "object_elem")
            and self._last_terminal == Terminal("EQ")
            and terminal != Terminal("NL_OR_COMMENT")
        )

    # pylint: disable=too-many-branches, too-many-return-statements
    def _should_add_space(self, rule, current_terminal, is_block_label: bool = False):
        """
        This method documents the situations in which we add space around
        certain tokens while reconstructing the generated HCL.

        Additional rules can be added here if the generated HCL has
        improper whitespace (affecting parse OR affecting ability to perfectly
        reconstruct a file down to the whitespace level.)

        It has the following information available to make its decision:

          - the last token (terminal) we output
          - the last rule that token belonged to
          - the current token (terminal) we're about to output
          - the rule the current token belongs to

        This should be sufficient to make a spacing decision.
        """

        # we don't need to add multiple spaces
        if self._last_char_space:
            return False

        # we don't add a space at the start of the file
        if not self._last_terminal or not self._last_rule:
            return False

        if self._is_equals_sign(current_terminal):
            return True

        if is_block_label and isinstance(rule, Token) and rule.value == "string":
            if (
                current_terminal == self._last_terminal == Terminal("DBLQUOTE")
                or current_terminal == Terminal("DBLQUOTE")
                and self._last_terminal == Terminal("NAME")
            ):
                return True

        # if we're in a ternary or binary operator, add space around the operator
        if (
            isinstance(rule, Token)
            and rule.value
            in [
                "conditional",
                "binary_operator",
            ]
            and self.should_be_wrapped_in_spaces(current_terminal)
        ):
            return True

        # if we just left a ternary or binary operator, add space around the
        # operator unless there's a newline already
        if (
            isinstance(self._last_rule, Token)
            and self._last_rule.value
            in [
                "conditional",
                "binary_operator",
            ]
            and self.should_be_wrapped_in_spaces(self._last_terminal)
            and current_terminal != Terminal("NL_OR_COMMENT")
        ):
            return True

        # if we're in a for or if statement and find a keyword, add a space
        if (
            isinstance(rule, Token)
            and rule.value
            in [
                "for_object_expr",
                "for_cond",
                "for_intro",
            ]
            and self.should_be_wrapped_in_spaces(current_terminal)
        ):
            return True

        # if we've just left a for or if statement and find a keyword, add a
        # space, unless we have a newline
        if (
            isinstance(self._last_rule, Token)
            and self._last_rule.value
            in [
                "for_object_expr",
                "for_cond",
                "for_intro",
            ]
            and self.should_be_wrapped_in_spaces(self._last_terminal)
            and current_terminal != Terminal("NL_OR_COMMENT")
        ):
            return True

        # if we're in a block
        if (isinstance(rule, Token) and rule.value == "block") or (
            isinstance(rule, str) and re.match(r"^__block_(star|plus)_.*", rule)
        ):
            # always add space before the starting brace
            if current_terminal == Terminal("LBRACE"):
                return True

            # always add space before the closing brace
            if current_terminal == Terminal(
                "RBRACE"
            ) and self._last_terminal != Terminal("LBRACE"):
                return True

            # always add space between string literals
            if current_terminal == Terminal("STRING_CHARS"):
                return True

        # if we just opened a block, add a space, unless the block is empty
        # or has a newline
        if (
            isinstance(self._last_rule, Token)
            and self._last_rule.value == "block"
            and self._last_terminal == Terminal("LBRACE")
            and current_terminal not in [Terminal("RBRACE"), Terminal("NL_OR_COMMENT")]
        ):
            return True

        # if we're in a tuple or function arguments (this rule matches commas between items)
        if isinstance(self._last_rule, str) and re.match(
            r"^__(tuple|arguments)_(star|plus)_.*", self._last_rule
        ):

            # string literals, decimals, and identifiers should always be
            # preceded by a space if they're following a comma in a tuple or
            # function arg
            if current_terminal in [
                Terminal("DBLQUOTE"),
                Terminal("DECIMAL"),
                Terminal("NAME"),
                Terminal("NEGATIVE_DECIMAL"),
            ]:
                return True

        # the catch-all case, we're not sure, so don't add a space
        return False

    def _reconstruct(self, tree, is_block_label=False):
        unreduced_tree = self.match_tree(tree, tree.data)
        res = self.write_tokens.transform(unreduced_tree)
        for item in res:
            # any time we encounter a child tree, we recurse
            if isinstance(item, Tree):
                yield from self._reconstruct(
                    item, (unreduced_tree.data == "block" and item.data != "body")
                )

            # every leaf should be a tuple, which contains information about
            # which terminal the leaf represents
            elif isinstance(item, tuple):
                rule, terminal, value = item

                # first, handle any deferred items
                if self._deferred_item is not None:
                    (
                        deferred_rule,
                        deferred_terminal,
                        deferred_value,
                    ) = self._deferred_item

                    # if we deferred a comma and the next character ends a
                    # parenthesis or block, we can throw it out
                    if deferred_terminal == Terminal("COMMA") and terminal in [
                        Terminal("RPAR"),
                        Terminal("RBRACE"),
                    ]:
                        pass
                    # in any other case, we print the deferred item
                    else:
                        yield deferred_value

                        # and do our bookkeeping
                        self._last_terminal = deferred_terminal
                        self._last_rule = deferred_rule
                        if deferred_value and not deferred_value[-1].isspace():
                            self._last_char_space = False

                    # clear the deferred item
                    self._deferred_item = None

                # potentially add a space before the next token
                if self._should_add_space(rule, terminal, is_block_label):
                    yield " "
                    self._last_char_space = True

                # potentially defer the item if needed
                if terminal in [Terminal("COMMA")]:
                    self._deferred_item = item
                else:
                    # otherwise print the next token
                    yield value

                    # and do our bookkeeping so we can make an informed
                    # decision about formatting next time
                    self._last_terminal = terminal
                    self._last_rule = rule
                    if value:
                        self._last_char_space = value[-1].isspace()

            else:
                raise RuntimeError(f"Unknown bare token type: {item}")

    def reconstruct(self, tree, postproc=None, insert_spaces=False):
        """Convert a Lark.Tree AST back into a string representation of HCL."""
        return Reconstructor.reconstruct(
            self,
            tree,
            postproc,
            insert_spaces,
        )


class HCLReverseTransformer:
    """
    The reverse of hcl2.transformer.DictTransformer. This method attempts to
    convert a dict back into a working AST, which can be written back out.
    """

    @staticmethod
    def _name_to_identifier(name: str) -> Tree:
        """Converts a string to a NAME token within an identifier rule."""
        return Tree(Token("RULE", "identifier"), [Token("NAME", name)])

    @staticmethod
    def _escape_interpolated_str(interp_s: str) -> str:
        if interp_s.strip().startswith("<<-") or interp_s.strip().startswith("<<"):
            # For heredoc strings, preserve their format exactly
            return reverse_quotes_within_interpolation(interp_s)
        # Escape backslashes first (very important to do this first)
        escaped = interp_s.replace("\\", "\\\\")
        # Escape quotes
        escaped = escaped.replace('"', '\\"')
        # Escape control characters
        escaped = escaped.replace("\n", "\\n")
        escaped = escaped.replace("\r", "\\r")
        escaped = escaped.replace("\t", "\\t")
        escaped = escaped.replace("\b", "\\b")
        escaped = escaped.replace("\f", "\\f")
        # find each interpolation within the string and remove the backslashes
        interp_s = reverse_quotes_within_interpolation(f"{escaped}")
        return interp_s

    @staticmethod
    def _block_has_label(block: dict) -> bool:
        return len(block.keys()) == 1

    def __init__(self):
        pass

    def transform(self, hcl_dict: dict) -> Tree:
        """Given a dict, return a Lark.Tree representing the HCL AST."""
        level = 0
        body = self._transform_dict_to_body(hcl_dict, level)
        start = Tree(Token("RULE", "start"), [body])
        return start

    @staticmethod
    def _is_string_wrapped_tf(interp_s: str) -> bool:
        """
        Determines whether a string is a complex HCL data structure
        wrapped in ${ interpolation } characters.
        """
        if not interp_s.startswith("${") or not interp_s.endswith("}"):
            return False

        nested_tokens = []
        for match in re.finditer(r"\$?\{|}", interp_s):
            if match.group(0) in ["${", "{"]:
                nested_tokens.append(match.group(0))
            elif match.group(0) == "}":
                nested_tokens.pop()

            # if we exit ${ interpolation } before the end of the string,
            # this interpolated string has string parts and can't represent
            # a valid HCL expression on its own (without quotes)
            if len(nested_tokens) == 0 and match.end() != len(interp_s):
                return False

        return True

    @classmethod
    def _unwrap_interpolation(cls, value: str) -> str:
        if cls._is_string_wrapped_tf(value):
            return value[2:-1]
        return value

    def _newline(self, level: int, count: int = 1) -> Tree:
        return Tree(
            Token("RULE", "new_line_or_comment"),
            [Token("NL_OR_COMMENT", f"\n{'  ' * level}") for _ in range(count)],
        )

    def _build_string_rule(self, string: str, level: int = 0) -> Tree:
        # grammar in hcl2.lark defines that a string is built of any number of string parts,
        #   each string part can be either interpolation expression, escaped interpolation string
        #   or regular string
        # this method build hcl2 string rule based on arbitrary string,
        #   splitting such string into individual parts and building a lark tree out of them
        #
        result = []

        pattern = regex.compile(r"(\${1,2}\{(?:[^{}]|(?R))*\})")
        parts = [part for part in pattern.split(string) if part != ""]
        # e.g. 'aaa$${bbb}ccc${"ddd-${eee}"}' -> ['aaa', '$${bbb}', 'ccc', '${"ddd-${eee}"}']
        # 'aa-${"bb-${"cc-${"dd-${5 + 5}"}"}"}' -> ['aa-', '${"bb-${"cc-${"dd-${5 + 5}"}"}"}']

        for part in parts:
            if part.startswith("$${") and part.endswith("}"):
                result.append(Token("ESCAPED_INTERPOLATION", part))

            # unwrap interpolation expression and recurse into it
            elif part.startswith("${") and part.endswith("}"):
                part = part[2:-1]
                if part.startswith('"') and part.endswith('"'):
                    part = part[1:-1]
                    part = self._transform_value_to_expr_term(part, level)
                else:
                    part = Tree(
                        Token("RULE", "expr_term"),
                        [Tree(Token("RULE", "identifier"), [Token("NAME", part)])],
                    )

                result.append(Tree(Token("RULE", "interpolation"), [part]))

            else:
                result.append(Token("STRING_CHARS", part))

        result = [Tree(Token("RULE", "string_part"), [element]) for element in result]
        return Tree(Token("RULE", "string"), result)

    def _is_block(self, value: Any) -> bool:
        if isinstance(value, dict):
            block_body = value
            if START_LINE_KEY in block_body.keys() or END_LINE_KEY in block_body.keys():
                return True

            try:
                # if block is labeled, actual body might be nested
                # pylint: disable=W0612
                block_label, block_body = next(iter(value.items()))
            except StopIteration:
                # no more potential labels = nothing more to check
                return False

            return self._is_block(block_body)

        if isinstance(value, list):
            if len(value) > 0:
                return self._is_block(value[0])

        return False

    def _calculate_block_labels(self, block: dict) -> Tuple[List[str], dict]:
        # if block doesn't have a label
        if len(block.keys()) != 1:
            return [], block

        # otherwise, find the label
        curr_label = list(block)[0]
        potential_body = block[curr_label]

        # __start_line__ and __end_line__ metadata are not labels
        if (
            START_LINE_KEY in potential_body.keys()
            or END_LINE_KEY in potential_body.keys()
        ):
            return [curr_label], potential_body

        # recurse and append the label
        next_label, block_body = self._calculate_block_labels(potential_body)
        return [curr_label] + next_label, block_body

    # pylint:disable=R0914
    def _transform_dict_to_body(self, hcl_dict: dict, level: int) -> Tree:
        # we add a newline at the top of a body within a block, not the root body
        # >2 here is to ignore the __start_line__ and __end_line__ metadata
        if level > 0 and len(hcl_dict) > 2:
            children = [self._newline(level)]
        else:
            children = []

        # iterate through each attribute or sub-block of this block
        for key, value in hcl_dict.items():
            if key in [START_LINE_KEY, END_LINE_KEY]:
                continue

            # construct the identifier, whether that be a block type name or an attribute key
            identifier_name = self._name_to_identifier(key)

            # first, check whether the value is a "block"
            if self._is_block(value):
                for block_v in value:
                    block_labels, block_body_dict = self._calculate_block_labels(
                        block_v
                    )
                    block_label_trees = [
                        self._build_string_rule(block_label, level)
                        for block_label in block_labels
                    ]
                    block_body = self._transform_dict_to_body(
                        block_body_dict, level + 1
                    )

                    # create our actual block to add to our own body
                    block = Tree(
                        Token("RULE", "block"),
                        [identifier_name] + block_label_trees + [block_body],
                    )
                    children.append(block)
                    # add empty line after block
                    new_line = self._newline(level - 1)
                    # add empty line with indentation for next element in the block
                    new_line.children.append(self._newline(level).children[0])

                    children.append(new_line)

            # if the value isn't a block, it's an attribute
            else:
                expr_term = self._transform_value_to_expr_term(value, level)
                attribute = Tree(
                    Token("RULE", "attribute"),
                    [identifier_name, Token("EQ", " ="), expr_term],
                )
                children.append(attribute)
                children.append(self._newline(level))

        # since we're leaving a block body here, reduce the indentation of the
        # final newline if it exists
        if (
            len(children) > 0
            and isinstance(children[-1], Tree)
            and children[-1].data.type == "RULE"
            and children[-1].data.value == "new_line_or_comment"
        ):
            children[-1] = self._newline(level - 1)

        return Tree(Token("RULE", "body"), children)

    # pylint: disable=too-many-branches, too-many-return-statements too-many-statements
    def _transform_value_to_expr_term(self, value, level) -> Union[Token, Tree]:
        """Transforms a value from a dictionary into an "expr_term" (a value in HCL2)

        Anything passed to this function is treated "naively". Any lists passed
        are assumed to be tuples, and any dicts passed are assumed to be objects.
        No more checks will be performed for either to see if they are "blocks"
        as this check happens in `_transform_dict_to_body`.
        """

        # for lists, recursively turn the child elements into expr_terms and
        # store within a tuple
        if isinstance(value, list):
            tuple_tree = Tree(
                Token("RULE", "tuple"),
                [
                    self._transform_value_to_expr_term(tuple_v, level)
                    for tuple_v in value
                ],
            )
            return Tree(Token("RULE", "expr_term"), [tuple_tree])

        if value is None:
            return Tree(
                Token("RULE", "expr_term"),
                [Tree(Token("RULE", "identifier"), [Token("NAME", "null")])],
            )

        # for dicts, recursively turn the child k/v pairs into object elements
        # and store within an object
        if isinstance(value, dict):
            elements = []

            # if the object has elements, put it on a newline
            if len(value) > 0:
                elements.append(self._newline(level + 1))

            # iterate through the items and add them to the object
            for i, (k, dict_v) in enumerate(value.items()):
                if k in [START_LINE_KEY, END_LINE_KEY]:
                    continue

                value_expr_term = self._transform_value_to_expr_term(dict_v, level + 1)
                k = self._unwrap_interpolation(k)
                elements.append(
                    Tree(
                        Token("RULE", "object_elem"),
                        [
                            Tree(
                                Token("RULE", "object_elem_key"),
                                [Tree(Token("RULE", "identifier"), [Token("NAME", k)])],
                            ),
                            Token("EQ", " ="),
                            value_expr_term,
                        ],
                    )
                )

                # add indentation appropriately
                if i < len(value) - 1:
                    elements.append(self._newline(level + 1))
                else:
                    elements.append(self._newline(level))
            return Tree(
                Token("RULE", "expr_term"), [Tree(Token("RULE", "object"), elements)]
            )

        # treat booleans appropriately
        if isinstance(value, bool):
            return Tree(
                Token("RULE", "expr_term"),
                [
                    Tree(
                        Token("RULE", "identifier"),
                        [Token("NAME", "true" if value else "false")],
                    )
                ],
            )

        # store integers as literals, digit by digit
        if isinstance(value, int):
            return Tree(
                Token("RULE", "expr_term"),
                [
                    Tree(
                        Token("RULE", "int_lit"),
                        [Token("DECIMAL", digit) for digit in str(value)],
                    )
                ],
            )

        if isinstance(value, float):
            value = str(value)
            literal = []

            if value[0] == "-":
                # pop two first chars - minus and a digit
                literal.append(Token("NEGATIVE_DECIMAL", value[:2]))
                value = value[2:]

            while value != "":
                char = value[0]

                if char == ".":
                    # current char marks beginning of decimal part: pop all remaining chars and end the loop
                    literal.append(Token("DOT", char))
                    literal.extend(Token("DECIMAL", char) for char in value[1:])
                    break

                if char == "e":
                    # current char marks beginning of e-notation: pop all remaining chars and end the loop
                    literal.append(Token("EXP_MARK", value))
                    break

                literal.append(Token("DECIMAL", char))
                value = value[1:]

            return Tree(
                Token("RULE", "expr_term"),
                [Tree(Token("RULE", "float_lit"), literal)],
            )

        # store strings as single literals
        if isinstance(value, str):
            # potentially unpack a complex syntax structure
            if self._is_string_wrapped_tf(value):
                # we have to unpack it by parsing it
                wrapped_value = re.match(r"\$\{(.*)}", value).group(1)  # type:ignore
                ast = reconstruction_parser().parse(f"value = {wrapped_value}")

                if ast.data != Token("RULE", "start"):
                    raise RuntimeError("Token must be `start` RULE")

                body = ast.children[0]
                if body.data != Token("RULE", "body"):
                    raise RuntimeError("Token must be `body` RULE")

                attribute = body.children[0]
                if attribute.data != Token("RULE", "attribute"):
                    raise RuntimeError("Token must be `attribute` RULE")

                if attribute.children[1] != Token("EQ", " ="):
                    raise RuntimeError("Token must be `EQ (=)` rule")

                parsed_value = attribute.children[2]
                return parsed_value

            # otherwise it's a string
            return Tree(
                Token("RULE", "expr_term"),
                [self._build_string_rule(self._escape_interpolated_str(value), level)],
            )

        # otherwise, we don't know the type
        raise RuntimeError(f"Unknown type to transform {type(value)}")
