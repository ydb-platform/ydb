from datetime import datetime
from pathlib import Path
from typing import List, Union

import typer

from . import BasePath, FluidPath, Pathy
from .about import __version__

app: typer.Typer = typer.Typer(help=f"Pathy command line interface. (v{__version__})")


@app.command()
def cp(from_location: str, to_location: str) -> None:
    """
    Copy a blob or folder of blobs from one bucket to another.
    """
    from_path: FluidPath = Pathy.fluid(from_location)
    if not from_path.exists():
        raise ValueError(f"from_path is not an existing Path or Pathy: {from_path}")
    to_path: FluidPath = Pathy.fluid(to_location)
    if from_path.is_dir():
        to_path.mkdir(parents=True, exist_ok=True)
        for blob in from_path.rglob("*"):
            if not blob.is_file():
                continue
            to_blob = to_path / str(blob.relative_to(from_path))
            to_blob.write_bytes(blob.read_bytes())
    elif from_path.is_file():
        # Copy prefix from the source if the to_path has none.
        #
        # e.g. "cp ./file.txt gs://bucket-name/" writes "gs://bucket-name/file.txt"
        sep: str = to_path.pathmod.sep  # type:ignore
        if isinstance(to_path, Pathy) and to_location.endswith(sep):
            to_path = to_path / from_path

        to_path.parent.mkdir(parents=True, exist_ok=True)
        to_path.write_bytes(from_path.read_bytes())


@app.command()
def mv(from_location: str, to_location: str) -> None:
    """
    Move a blob or folder of blobs from one path to another.
    """
    from_path: FluidPath = Pathy.fluid(from_location)
    to_path: FluidPath = Pathy.fluid(to_location)

    if from_path.is_file():
        # Copy prefix from the source if the to_path has none.
        #
        # e.g. "cp ./file.txt gs://bucket-name/" writes "gs://bucket-name/file.txt"
        sep: str = to_path.pathmod.sep  # type:ignore
        if isinstance(to_path, Pathy) and to_location.endswith(sep):
            to_path = to_path / from_path
        to_path.parent.mkdir(parents=True, exist_ok=True)
        to_path.write_bytes(from_path.read_bytes())
        from_path.unlink()
        return

    if from_path.is_dir():
        to_path.mkdir(parents=True, exist_ok=True)
        to_unlink: List[Union[Pathy, BasePath, Path]] = []
        for blob in from_path.rglob("*"):
            if not blob.is_file():
                continue
            to_blob = to_path / str(blob.relative_to(from_path))
            to_blob.write_bytes(blob.read_bytes())
            to_unlink.append(blob)
        for unlink in to_unlink:
            unlink.unlink()
        if from_path.is_dir():
            from_path.rmdir()


@app.command()
def rm(
    location: str,
    recursive: bool = typer.Option(
        False, "--recursive", "-r", help="Recursively remove files and folders."
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Print removed files and folders."
    ),
) -> None:
    """
    Remove a blob or folder of blobs from a given location.
    """
    path: FluidPath = Pathy.fluid(location)
    if not path.exists():
        typer.echo(f"rm: {path}: No such file or directory")
        raise typer.Exit(1)

    if path.is_dir():
        if not recursive:
            typer.echo(f"rm: {path}: is a directory")
            raise typer.Exit(1)
        selector = path.rglob("*") if recursive else path.glob("*")
        to_unlink = [b for b in selector if b.is_file()]
        for blob in to_unlink:
            if verbose:
                typer.echo(str(blob))
            blob.unlink()
        if path.exists():
            if verbose:
                typer.echo(str(path))
            path.rmdir()
    elif path.is_file():
        if verbose:
            typer.echo(str(path))
        path.unlink()


@app.command()
def ls(
    location: str,
    long: bool = typer.Option(
        False,
        "--long",
        "-l",
        help="Print long style entries with updated time and size shown.",
    ),
) -> None:
    """
    List the blobs that exist at a given location.
    """
    path: FluidPath = Pathy.fluid(location)
    if not path.exists() or path.is_file():
        typer.echo(f"ls: {path}: No such file or directory")
        raise typer.Exit(1)
    now = datetime.now()
    for blob_stat in path.ls():
        print_name = str(path / blob_stat.name)
        if not long:
            typer.echo(print_name)
            continue
        time_str = ""
        if blob_stat.last_modified is not None:
            then = datetime.fromtimestamp(blob_stat.last_modified)
            if now.year != then.year:
                time_str = "%d %b, %Y"
            else:
                time_str = "%d %b, %H:%M"
            time_str = then.strftime(time_str)
        typer.echo("{0:10}{1:15}{2:10}".format(blob_stat.size, time_str, print_name))


if __name__ == "__main__":
    app()

__all__ = ()
