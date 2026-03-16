from datamodel_code_generator.format import CustomCodeFormatter


class WrongFormatterName(CustomCodeFormatter):
    """Invalid formatter: correct name is CodeFormatter."""
    def apply(self, code: str) -> str:
        return f'# a comment\n{code}'
