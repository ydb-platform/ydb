from .types import (
    DetailTypes,
    Sort,
)
from complexipy.rust import (
    FileComplexity,
    FunctionComplexity,
)
from rich.align import (
    Align,
)
from rich.console import (
    Console,
)
from rich.table import Table


def output_summary(
    console: Console,
    file_level: bool,
    files: list[FileComplexity],
    max_complexity: int,
    details: DetailTypes,
    path: str,
    sort: Sort,
) -> bool:
    if file_level:
        table, has_success, total_complexity = create_table_file_level(
            files, max_complexity, details, sort
        )
    else:
        table, has_success, total_complexity = create_table_function_level(
            files, max_complexity, details, sort
        )

    if details == DetailTypes.low and table.row_count < 1:
        console.print(f"No {'file' if file_level else 'function'}{'s' if len(files) > 1 else ''} were found with complexity greater than {max_complexity}.")
    else:
        console.print(Align.center(table))
    console.print(f":brain: Total Cognitive Complexity in {path}: {total_complexity}")

    return has_success


def create_table_file_level(
    files: list[FileComplexity], max_complexity: int, details: DetailTypes, sort: Sort
) -> tuple[Table, bool, int]:
    has_success = True

    table = Table(
        title="Summary", show_header=True, header_style="bold magenta", show_lines=True
    )
    table.add_column("Path")
    table.add_column("File")
    table.add_column("Complexity")
    total_complexity = 0

    if sort != Sort.name:
        files.sort(key=lambda x: x.complexity)

        if sort == Sort.desc:
            files.reverse()

    for file in files:
        total_complexity += file.complexity
        if file.complexity > max_complexity and max_complexity != 0:
            table.add_row(
                f"{file.path}",
                f"[green]{file.file_name}[/green]",
                f"[red]{file.complexity}[/red]",
            )
            has_success = False
        elif details != DetailTypes.low or max_complexity == 0:
            table.add_row(
                f"{file.path}",
                f"[green]{file.file_name}[/green]",
                f"[blue]{file.complexity}[/blue]",
            )
    return table, has_success, total_complexity


def create_table_function_level(
    files: list[FileComplexity],
    complexity: int,
    details: DetailTypes,
    sort: bool = False,
) -> tuple[Table, bool, int]:
    has_success = True
    all_functions: list[tuple[str, str, FunctionComplexity]] = []
    total_complexity = 0

    table = Table(
        title="Summary", show_header=True, header_style="bold magenta", show_lines=True
    )
    table.add_column("Path")
    table.add_column("File")
    table.add_column("Function")
    table.add_column("Complexity")

    for file in files:
        total_complexity += file.complexity
        for function in file.functions:
            total_complexity += function.complexity
            all_functions.append((file.path, file.file_name, function))

    if sort != Sort.name:
        all_functions.sort(key=lambda x: x[2].complexity)

        if sort == Sort.desc:
            all_functions.reverse()

    for function in all_functions:
        if function[2].complexity > complexity and complexity != 0:
            table.add_row(
                f"{function[0]}",
                f"[green]{function[1]}[/green]",
                f"[green]{function[2].name}[/green]",
                f"[red]{function[2].complexity}[/red]",
            )
            has_success = False
        elif details != DetailTypes.low or complexity == 0:
            table.add_row(
                f"{function[0]}",
                f"[green]{function[1]}[/green]",
                f"[green]{function[2].name}[/green]",
                f"[blue]{function[2].complexity}[/blue]",
            )
    return table, has_success, total_complexity


def has_success_file_level(files: list[FileComplexity], max_complexity: int) -> bool:
    for file in files:
        if file.complexity > max_complexity and max_complexity != 0:
            return False
    return True


def has_success_function_level(files: list[FileComplexity], complexity: int) -> bool:
    for file in files:
        for function in file.functions:
            if function.complexity > complexity and complexity != 0:
                return False
    return True
