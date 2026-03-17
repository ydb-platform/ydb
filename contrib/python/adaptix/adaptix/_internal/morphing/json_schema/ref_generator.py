from ...provider.loc_stack_filtering import LocStack
from .definitions import JSONSchema
from .resolver import RefGenerator


class BuiltinRefGenerator(RefGenerator):
    def generate_ref(self, json_schema: JSONSchema, loc_stack: LocStack) -> str:
        return str(loc_stack.last.type)
