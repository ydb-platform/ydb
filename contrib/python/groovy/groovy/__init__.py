import importlib.resources

from pathlib import Path

from groovy.transpiler import TranspilerError, transpile

__version__ = (importlib.resources.files(__package__) / 'version.txt').read_text().strip()

__all__ = ["__version__", "transpile", "TranspilerError"]
