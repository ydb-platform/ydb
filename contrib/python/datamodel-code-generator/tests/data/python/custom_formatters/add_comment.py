from datamodel_code_generator.format import CustomCodeFormatter


class CodeFormatter(CustomCodeFormatter):
    """Simple correct formatter. Adding a comment to top of code."""
    def apply(self, code: str) -> str:
        return f'# a comment\n{code}'
