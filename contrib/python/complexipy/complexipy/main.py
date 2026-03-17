from .types import (
    DetailTypes,
    Level,
    Sort,
)
from .utils import (
    output_summary,
    has_success_file_level,
    has_success_function_level,
)
from complexipy import (
    rust,
)
from complexipy.rust import (
    FileComplexity,
)
import os
from pathlib import (
    Path,
)
import re
from rich.console import (
    Console,
)
import time
import typer

root_dir = Path(__file__).resolve().parent.parent
app = typer.Typer(name="complexipy")
console = Console()
version = "0.4.0"


@app.command()
def main(
    path: str = typer.Argument(
        help="Path to the directory or file to analyze, it can be a local path or a git repository URL.",
    ),
    max_complexity: int = typer.Option(
        15,
        "--max-complexity",
        "-c",
        help="The maximum complexity allowed per file, set this value as 0 to set it as unlimited. Default is 15.",
    ),
    output: bool = typer.Option(
        False, "--output", "-o", help="Output the results to a CSV file."
    ),
    details: DetailTypes = typer.Option(
        DetailTypes.normal.value,
        "--details",
        "-d",
        help="Specify how detailed should be output, it can be 'low' or 'normal'. Default is 'normal'.",
    ),
    level: Level = typer.Option(
        Level.function.value,
        "--level",
        "-l",
        help="Specify the level of measurement, it can be 'function' or 'file'. Default is 'function'.",
    ),
    quiet: bool = typer.Option(
        False, "--quiet", "-q", help="Suppress the output to the console."
    ),
    sort: Sort = typer.Option(
        Sort.asc.value,
        "--sort",
        "-s",
        help="Sort the output by complexity, it can be 'asc', 'desc' or 'name'. Default is 'asc'.",
    ),
):
    is_dir = Path(path).is_dir()
    _url_pattern = (
        r"^(https:\/\/|http:\/\/|www\.|git@)(github|gitlab)\.com(\/[\w.-]+){2,}$"
    )
    is_url = bool(re.match(_url_pattern, path))
    invocation_path = os.getcwd()
    file_level = level == Level.file

    console.rule(f":octopus: complexipy {version}")
    start_time = time.time()
    files: list[FileComplexity] = rust.main(
        path, is_dir, is_url, max_complexity, file_level
    )
    execution_time = time.time() - start_time
    output_csv_path = f"{invocation_path}/complexipy.csv"

    if output and file_level:
        rust.output_csv_file_level(output_csv_path, files, sort.value)
        console.print(f"Results saved in {output_csv_path}")
    if output and not file_level:
        rust.output_csv_function_level(output_csv_path, files, sort.value)
        console.print(f"Results saved in {output_csv_path}")

    # Summary
    if not quiet:
        has_success = output_summary(
            console, file_level, files, max_complexity, details, path, sort
        )
    if quiet and not file_level:
        has_success = has_success_function_level(files, max_complexity)
    if quiet and file_level:
        has_success = has_success_file_level(files, max_complexity)

    console.print(
        f"{len(files)} file{'s' if len(files)> 1 else ''} analyzed in {execution_time:.4f} seconds"
    )
    console.rule(":tada: Analysis completed! :tada:")

    if not has_success:
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
