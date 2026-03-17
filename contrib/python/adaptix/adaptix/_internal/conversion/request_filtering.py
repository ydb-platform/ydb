from adaptix._internal.provider.loc_stack_filtering import DirectMediator, LocStack, LocStackChecker


class FromCtxParam(LocStackChecker):
    def __init__(self, field_id: str):
        if not field_id.isidentifier():
            raise ValueError("param_name must be a valid python identifier to exactly match parameter")
        self._field_id = field_id

    def check_loc_stack(self, mediator: DirectMediator, loc_stack: LocStack) -> bool:
        return len(loc_stack) == 1 and loc_stack.last.field_id == self._field_id
