from cucumber_expressions.generated_expression import GeneratedExpression
from cucumber_expressions.parameter_type import ParameterType

# 256 generated expressions ought to be enough for anybody
MAX_EXPRESSIONS = 256


class CombinatorialGeneratedExpressionFactory:
    def __init__(self, expression_template, parameter_type_combinations):
        self.expression_template = expression_template
        self.parameter_type_combinations = parameter_type_combinations

    def generate_expressions(self) -> list[GeneratedExpression]:
        generated_expressions = []
        self.generate_permutations(generated_expressions, 0, [])
        return generated_expressions

    def generate_permutations(
        self,
        generated_expressions: list[GeneratedExpression],
        depth: int,
        current_parameter_types: list[ParameterType],
    ):
        if len(generated_expressions) >= MAX_EXPRESSIONS:
            return
        if depth == len(self.parameter_type_combinations):
            generated_expressions.append(
                GeneratedExpression(self.expression_template, current_parameter_types),
            )
            return
        for parameter_type_combination in self.parameter_type_combinations[depth]:
            if len(generated_expressions) >= MAX_EXPRESSIONS:
                return
            new_current_parameter_types = current_parameter_types.copy()
            new_current_parameter_types.append(parameter_type_combination)
            self.generate_permutations(
                generated_expressions,
                depth + 1,
                new_current_parameter_types,
            )
