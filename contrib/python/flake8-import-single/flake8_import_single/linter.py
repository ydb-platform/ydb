import ast

from flake8_import_single import version


class ImportFinder(ast.NodeVisitor):
    def __init__(self, *args, **kwargs):
        super(ImportFinder, self).__init__(*args, **kwargs)
        self.errors = []

    def visit_ImportFrom(self, node):
        if len(node.names) > 1:
            self.errors.append((node.lineno, node.col_offset))


class Linter(object):
    name = 'flake8-import-single'
    version = version

    def __init__(self, tree):
        self.tree = tree

    def run(self):
        parser = ImportFinder()
        parser.visit(self.tree)

        for error in parser.errors:
            yield (
                error[0],
                error[1],
                'IS001 found multiple imports on a single line.',
                Linter,
            )
