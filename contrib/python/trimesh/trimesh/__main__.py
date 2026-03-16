import argparse


def main():
    """
    A simple command line utility for accessing trimesh functions.

    To display a mesh:
      > trimesh hi.stl

    To convert a mesh:
      > trimesh hi.stl -e hey.glb

    To print some information about a mesh:
      > trimesh hi.stl --statistics
    """
    from .exchange.load import load

    parser = argparse.ArgumentParser()
    parser.add_argument("file_name", nargs="?")

    parser.add_argument(
        "-i",
        "--interact",
        action="store_true",
        help="Get an interactive terminal with trimesh and loaded geometry",
    )
    parser.add_argument("-e", "--export", help="Export a loaded geometry to a new file.")

    args = parser.parse_args()

    if args.file_name is None:
        parser.print_help()
        return
    else:
        scene = load(args.file_name)

    summary(scene)

    if args.export is not None:
        scene.export(args.export)

    if args.interact:
        return interactive(scene)

    scene.show()


def summary(geom):
    """ """
    print(geom)


def interactive(scene):
    """
    Run an interactive session with a loaded scene and trimesh.

    This uses the standard library `code.InteractiveConsole`
    """
    local = locals()

    from code import InteractiveConsole

    # filter out junk variables.
    InteractiveConsole(locals={k: v for k, v in local.items() if k != "local"}).interact()


if __name__ == "__main__":
    main()
