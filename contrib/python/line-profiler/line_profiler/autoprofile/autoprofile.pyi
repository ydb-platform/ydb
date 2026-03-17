from typing import List

PROFILER_LOCALS_NAME: str


def run(script_file: str,
        ns: dict,
        prof_mod: List[str],
        profile_imports: bool = False) -> None:
    ...
