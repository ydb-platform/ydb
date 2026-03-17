from typing import List
from typing import Type
import _ast

from .ast_profle_transformer import AstProfileTransformer
from .profmod_extractor import ProfmodExtractor

__docstubs__: str


class AstTreeProfiler:

    def __init__(
            self,
            script_file: str,
            prof_mod: List[str],
            profile_imports: bool,
            ast_transformer_class_handler: Type = AstProfileTransformer,
            profmod_extractor_class_handler: Type = ProfmodExtractor) -> None:
        ...

    def profile(self) -> (_ast.Module):
        ...
