import abc
from dataclasses import dataclass
from typing import List, Dict, Union, Any, Optional, Tuple, Callable

import toloka.client.project.template_builder as tb


@dataclass
class ConditionContext:
    @dataclass
    class ArgData:
        final_name: str
        is_output: bool

    name_to_arg_data: Dict[str, ArgData]

    def get_toloka_data(self, name: str) -> Union[tb.InputData, tb.OutputData]:
        assert name in self.name_to_arg_data, (
            f'no argument "{name}" in condition context, may be you are trying to reference'
            f'argument which is declared later in argument list'
        )
        arg_data = self.name_to_arg_data[name]
        data = tb.OutputData if arg_data.is_output else tb.InputData
        return data(path=arg_data.final_name)


class Condition:
    @abc.abstractmethod
    def to_toloka_condition(self, ctx: ConditionContext) -> tb.conditions.BaseConditionV1:
        ...


@dataclass
class ConditionEquals(Condition):
    what: str
    to: Any

    def to_toloka_condition(self, ctx: ConditionContext) -> tb.conditions.BaseConditionV1:
        return tb.EqualsConditionV1(
            data=ctx.get_toloka_data(self.what),
            to=self.to,
        )


class Consequence:
    ...


@dataclass
class If:
    condition: Condition
    then: Union['If', Consequence]
    else_: Optional[Union['If', Consequence]] = None

    @staticmethod
    def get_consequence(
        branch: Optional[Union['If', Consequence]],
        view_gen: Callable[[Consequence], tb.view.BaseViewV1],
        condition_context: ConditionContext,
    ) -> Optional[Union['If', Consequence]]:
        if branch is None:
            return None
        if isinstance(branch, If):
            return branch.to_toloka_if(view_gen, condition_context)
        if isinstance(branch, Consequence):
            return view_gen(branch)
        assert False, f'unexpected consequence: {branch}'

    @staticmethod
    def generate_list_sequence(name: str, value_consequences: List[Tuple[Any, Consequence]]) -> 'If':
        first_if = None
        current_if = None

        for value, consequence in value_consequences:
            if_ = If(
                condition=ConditionEquals(what=name, to=value),
                then=consequence,
                else_=None,
            )

            if first_if is None:
                first_if = if_

            if current_if is not None:
                current_if.else_ = if_

            current_if = if_

        return first_if

    def to_toloka_if(
        self,
        view_gen: Callable[[Consequence], tb.view.BaseViewV1],
        condition_context: ConditionContext,
    ) -> tb.IfHelperV1:
        return tb.IfHelperV1(
            condition=self.condition.to_toloka_condition(condition_context),
            then=self.get_consequence(self.then, view_gen, condition_context),
            else_=self.get_consequence(self.else_, view_gen, condition_context),
        )
