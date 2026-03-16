from cucumber_expressions.argument import Argument
from cucumber_expressions.ast import Node, NodeType
from cucumber_expressions.errors import (
    AlternativeMayNotBeEmpty,
    AlternativeMayNotExclusivelyContainOptionals,
    OptionalIsNotAllowedInOptional,
    OptionalMayNotBeEmpty,
    ParameterIsNotAllowedInOptional,
    UndefinedParameterTypeError,
)
from cucumber_expressions.expression_parser import CucumberExpressionParser
from cucumber_expressions.parameter_type import ParameterType
from cucumber_expressions.tree_regexp import TreeRegexp

ESCAPE_PATTERN = rb"([\\^\[({$.|?*+})\]])"


class CucumberExpression:
    def __init__(self, expression, parameter_type_registry):
        self.expression = expression
        self.parameter_type_registry = parameter_type_registry
        self.parameter_types: list[ParameterType] = []
        self.tree_regexp = TreeRegexp(
            self.rewrite_to_regex(CucumberExpressionParser().parse(self.expression)),
        )

    def match(self, text: str) -> list[Argument] | None:
        return Argument.build(self.tree_regexp, text, self.parameter_types)

    @property
    def source(self):
        return self.expression

    @property
    def regexp(self) -> str:
        return self.tree_regexp.regexp.pattern

    def rewrite_to_regex(self, node: Node):
        if node.ast_type == NodeType.TEXT:
            return self.escape_regex(node.text)
        if node.ast_type == NodeType.OPTIONAL:
            return self.rewrite_optional(node)
        if node.ast_type == NodeType.ALTERNATION:
            return self.rewrite_alternation(node)
        if node.ast_type == NodeType.ALTERNATIVE:
            return self.rewrite_alternative(node)
        if node.ast_type == NodeType.PARAMETER:
            return self.rewrite_parameter(node)
        if node.ast_type == NodeType.EXPRESSION:
            return self.rewrite_expression(node)
        # Can't happen as long as the switch case is exhaustive
        raise Exception(node.ast_type)

    @staticmethod
    def escape_regex(expression) -> str:
        return expression.translate({i: "\\" + chr(i) for i in ESCAPE_PATTERN})

    def rewrite_optional(self, node: Node):
        _possible_node_with_params = self.get_possible_node_with_parameters(node)
        if _possible_node_with_params:
            raise ParameterIsNotAllowedInOptional(
                _possible_node_with_params,
                self.expression,
            )
        _possible_node_with_optionals = self.get_possible_node_with_optionals(node)
        if _possible_node_with_optionals:
            raise OptionalIsNotAllowedInOptional(
                _possible_node_with_optionals,
                self.expression,
            )
        if self.are_nodes_empty(node):
            raise OptionalMayNotBeEmpty(node, self.expression)
        regex = "".join([self.rewrite_to_regex(_node) for _node in node.nodes])
        return rf"(?:{regex})?"

    def rewrite_alternation(self, node: Node):
        for alternative in node.nodes:
            if not alternative.nodes:
                raise AlternativeMayNotBeEmpty(alternative, self.expression)
            if self.are_nodes_empty(alternative):
                raise AlternativeMayNotExclusivelyContainOptionals(
                    alternative,
                    self.expression,
                )
        regex = "|".join([self.rewrite_to_regex(_node) for _node in node.nodes])
        return f"(?:{regex})"

    def rewrite_alternative(self, node: Node):
        return "".join([self.rewrite_to_regex(_node) for _node in node.nodes])

    def rewrite_parameter(self, node: Node):
        name = node.text
        parameter_type = self.parameter_type_registry.lookup_by_type_name(name)

        if not parameter_type:
            raise UndefinedParameterTypeError(node, self.expression, name)

        self.parameter_types.append(parameter_type)

        regexps = parameter_type.regexps
        if len(regexps) == 1:
            return rf"({regexps[0]})"
        return rf"((?:{')|(?:'.join(regexps)}))"

    def rewrite_expression(self, node: Node):
        regex = "".join([self.rewrite_to_regex(_node) for _node in node.nodes])
        return rf"^{regex}$"

    def are_nodes_empty(self, node: Node) -> bool:
        return not any(self.get_nodes_with_ast_type(node, NodeType.TEXT))

    def get_possible_node_with_parameters(self, node: Node) -> Node | None:
        results = self.get_nodes_with_ast_type(node, NodeType.PARAMETER)
        return results[0] if results else None

    def get_possible_node_with_optionals(self, node: Node) -> Node | None:
        results = self.get_nodes_with_ast_type(node, NodeType.OPTIONAL)
        return results[0] if results else None

    @staticmethod
    def get_nodes_with_ast_type(node: Node, ast_type: NodeType) -> list[Node]:
        return [ast_node for ast_node in node.nodes if ast_node.ast_type == ast_type]
