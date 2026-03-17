class _Name(object):
    """Parses a name in camelCase, PascalCase, snake_case, or kebab-case into its segments."""

    def __init__(self, raw_name: str):
        #
        self._segments = []
        segment = []
        for ch in raw_name:
            if ch.isupper():
                if segment:
                    self._segments.append("".join(segment))
                segment = [ch.lower()]
            elif ch.islower():
                segment.append(ch)
            else:
                if segment:
                    self._segments.append("".join(segment))
                segment = []
        if segment:
            self._segments.append("".join(segment))

    def to_snake_case(self) -> str:
        return "_".join(self._segments)

    def to_header_case(self) -> str:
        return "-".join([s.capitalize() for s in self._segments])


class Casing(object):

    @staticmethod
    def to_header_case(name: str) -> str:
        """
        Convert a name from camelCase, PascalCase, snake_case, or kebab-case to header-case.
        :param name:
        :return:
        """
        return _Name(name).to_header_case()
