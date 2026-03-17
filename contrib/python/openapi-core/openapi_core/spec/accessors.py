from contextlib import contextmanager

from dictpath.accessors import DictOrListAccessor


class SpecAccessor(DictOrListAccessor):

    def __init__(self, dict_or_list, dereferencer):
        super(SpecAccessor, self).__init__(dict_or_list)
        self.dereferencer = dereferencer

    @contextmanager
    def open(self, parts):
        content = self.dict_or_list
        for part in parts:
            content = content[part]
            if '$ref' in content:
                content = self.dereferencer.dereference(
                    content)
        try:
            yield content
        finally:
            pass
