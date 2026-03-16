from ...provider.essential import CannotProvide
from ...provider.loc_stack_filtering import DirectMediator, LocStack, LocStackChecker
from ...provider.shape_provider import InputShapeRequest, OutputShapeRequest


class AnyModelLSC(LocStackChecker):
    def check_loc_stack(self, mediator: DirectMediator, loc_stack: LocStack) -> bool:
        try:
            mediator.provide(InputShapeRequest(loc_stack=loc_stack))
        except CannotProvide:
            pass
        else:
            return True

        try:
            mediator.provide(OutputShapeRequest(loc_stack=loc_stack))
        except CannotProvide:
            return False
        return True
