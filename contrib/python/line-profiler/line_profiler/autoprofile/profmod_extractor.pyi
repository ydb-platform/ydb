import _ast
from typing import List
from typing import Dict


class ProfmodExtractor:

    def __init__(self, tree: _ast.Module, script_file: str,
                 prof_mod: List[str]) -> None:
        ...

    def run(self) -> (Dict[int, str]):
        ...
