#  Copyright (c) 2021-2024, Manfred Moitzi
#  License: MIT License
from __future__ import annotations

import pathlib
import tempfile
from typing import Callable, Optional, TYPE_CHECKING, Type, Sequence
import abc
import sys
import os
import glob
import signal
import time
import logging
from pathlib import Path

# --------------------------------------------------------------------------------------
# Only imports from the core package here - no add-ons!
#
#   The command `ezdxf -V` must always work!
#
# Imports depending on additional packages like Pillow, Matplotlib, PySide6, ...
# have to be local imports, see 'draw' command as an example.
# --------------------------------------------------------------------------------------
import ezdxf
from ezdxf import recover
from ezdxf.lldxf import const
from ezdxf.lldxf.validator import is_dxf_file, is_binary_dxf_file, dxf_info
from ezdxf.dwginfo import dwg_file_info

if TYPE_CHECKING:
    from ezdxf.entities import DXFGraphic
    from ezdxf.addons.drawing.properties import Properties, LayerProperties

__all__ = ["get", "add_parsers"]

logger = logging.getLogger("ezdxf")


def get(cmd: str) -> Optional[Callable]:
    cls = _commands.get(cmd)
    if cls:
        return cls.run
    return None


def add_parsers(subparsers) -> None:
    for cmd in _commands.values():  # in order of registration
        try:
            cmd.add_parser(subparsers)
        except ImportError:
            logger.info(f"ImportError - '{cmd.NAME}' command not available")


def is_dxf_r12_file(filename: str) -> bool:
    try:
        with open(filename, "rt", errors="ignore") as fp:
            info = dxf_info(fp)
    except IOError:
        return False
    return info.version <= const.DXF12


class Command:
    """abstract base class for launcher commands"""

    NAME = "command"

    @staticmethod
    @abc.abstractmethod
    def add_parser(subparsers) -> None:
        pass

    @staticmethod
    @abc.abstractmethod
    def run(args) -> None:
        pass


_commands: dict[str, Type[Command]] = dict()


def register(cls: Type[Command]):
    """Register a launcher sub-command."""
    _commands[cls.NAME] = cls
    return cls


@register
class Audit(Command):
    """Launcher sub-command: audit"""

    NAME = "audit"

    @staticmethod
    def add_parser(subparsers):
        parser = subparsers.add_parser(Audit.NAME, help="audit and repair DXF files")
        parser.add_argument(
            "files",
            metavar="FILE",
            nargs="+",
            help="audit DXF files",
        )
        parser.add_argument(
            "-s",
            "--save",
            action="store_true",
            help='save recovered files with extension ".rec.dxf" ',
        )
        parser.add_argument(
            "-x",
            "--explore",
            action="store_true",
            help="filters invalid DXF tags, this may load corrupted files but "
            "data loss is very likely",
        )

    @staticmethod
    def run(args):
        def build_outname(name: str) -> str:
            p = Path(name)
            return str(p.parent / (p.stem + ".rec.dxf"))

        def log_fixes(auditor):
            for error in auditor.fixes:
                logger.info("fixed:" + error.message)

        def log_errors(auditor):
            for error in auditor.errors:
                logger.error(error.message)

        def _audit(filename: str) -> None:
            msg = f"auditing file: {filename}"
            print(msg)
            logger.info(msg)
            if args.explore:
                logger.info("explore mode - skipping invalid tags")
            loader = recover.explore if args.explore else recover.readfile
            try:
                doc, auditor = loader(filename)
            except IOError:
                msg = "Not a DXF file or a generic I/O error."
                print(msg)
                logger.error(msg)
                return  # keep on processing additional files
            except const.DXFStructureError as e:
                msg = f"Invalid or corrupted DXF file: {str(e)}"
                print(msg)
                logger.error(msg)
                return  # keep on processing additional files

            if auditor.has_errors:
                auditor.print_error_report()
                log_errors(auditor)
            if auditor.has_fixes:
                auditor.print_fixed_errors()
                log_fixes(auditor)

            if auditor.has_errors is False and auditor.has_fixes is False:
                print("No errors found.")
            else:
                print(
                    f"Found {len(auditor.errors)} errors, "
                    f"applied {len(auditor.fixes)} fixes"
                )

            if args.save:
                outname = build_outname(filename)
                try:
                    doc.saveas(outname)
                except IOError as e:
                    print(f"Can not save recovered file '{outname}':\n{str(e)}")
                else:
                    print(f"Saved recovered file as: '{outname}'")

        for pattern in args.files:
            names = list(glob.glob(pattern))
            if len(names) == 0:
                msg = f"File(s) '{pattern}' not found."
                print(msg)
                logger.error(msg)
                continue
            for filename in names:
                if not os.path.exists(filename):
                    msg = f"File '{filename}' not found."
                    print(msg)
                    logger.error(msg)
                    continue
                if not is_dxf_file(filename):
                    msg = f"File '{filename}' is not a DXF file."
                    print(msg)
                    logger.error(msg)
                    continue
                _audit(filename)


def load_document(filename: str):
    try:
        doc, auditor = recover.readfile(filename)
    except IOError:
        msg = f'Not a DXF file or a generic I/O error: "{filename}"'
        print(msg, file=sys.stderr)
        sys.exit(2)
    except const.DXFStructureError:
        msg = f'Invalid or corrupted DXF file: "{filename}"'
        print(msg, file=sys.stderr)
        sys.exit(3)

    if auditor.has_errors:
        # But is most likely good enough for rendering.
        msg = f"Audit process found {len(auditor.errors)} unrecoverable error(s)."
        print(msg)
        logger.error(msg)
    if auditor.has_fixes:
        msg = f"Audit process fixed {len(auditor.fixes)} error(s)."
        print(msg)
        logger.info(msg)
    return doc, auditor


HELP_LTYPE = (
    "select the line type rendering method, default is approximate. "
    "Approximate uses the closest approximation available to the "
    "backend, the accurate method renders as accurately as possible "
    "but this approach is slower."
)
HELP_LWSCALE = (
    "set custom line weight scaling, default is 0 to disable line " "weights at all"
)


@register
class Draw(Command):
    """Launcher sub-command: draw"""

    NAME = "draw"

    @staticmethod
    def add_parser(subparsers):
        parser = subparsers.add_parser(
            Draw.NAME, help="draw and save DXF as a bitmap or vector image"
        )
        parser.add_argument(
            "file",
            metavar="FILE",
            nargs="?",
            help="DXF file to view or convert",
        )
        parser.add_argument(
            "--backend",
            default="matplotlib",
            choices=["matplotlib", "qt", "mupdf", "custom_svg"],
            help="choose the backend to use for rendering",
        )
        parser.add_argument(
            "--formats",
            action="store_true",
            help="show all supported export formats and exit",
        )
        parser.add_argument(
            "-l",
            "--layout",
            default="Model",
            help='select the layout to draw, default is "Model"',
        )
        parser.add_argument(
            "--background",
            default="DEFAULT",
            choices=[
                "DEFAULT",
                "WHITE",
                "BLACK",
                "PAPERSPACE",
                "MODELSPACE",
                "OFF",
                "CUSTOM",
            ],
            help="choose the background color to use",
        )
        parser.add_argument(
            "--all-layers-visible",
            action="store_true",
            help="draw all layers including the ones marked as invisible",
        )
        parser.add_argument(
            "--all-entities-visible",
            action="store_true",
            help="draw all entities including the ones marked as invisible "
            "(some entities are individually marked as invisible even "
            "if the layer is visible)",
        )
        parser.add_argument(
            "-o",
            "--out",
            required=False,
            type=pathlib.Path,
            help="output filename for export",
        )
        parser.add_argument(
            "--dpi",
            type=int,
            default=300,
            help="target render resolution, default is 300",
        )
        parser.add_argument(
            "-f",
            "--force",
            action="store_true",
            help="overwrite the destination if it already exists",
        )
        parser.add_argument(
            "-v",
            "--verbose",
            action="store_true",
            help="give more output",
        )

    @staticmethod
    def run(args):
        try:
            from ezdxf.addons.drawing import RenderContext, Frontend
            from ezdxf.addons.drawing.config import Configuration, BackgroundPolicy
            from ezdxf.addons.drawing.file_output import (
                open_file,
                MatplotlibFileOutput,
                PyQtFileOutput,
                SvgFileOutput,
                MuPDFFileOutput,
            )
        except ImportError as e:
            print(str(e))
            sys.exit(1)

        if args.backend == "matplotlib":
            try:
                file_output = MatplotlibFileOutput(args.dpi)
            except ImportError as e:
                print(str(e))
                sys.exit(1)
        elif args.backend == "qt":
            try:
                file_output = PyQtFileOutput(args.dpi)
            except ImportError as e:
                print(str(e))
                sys.exit(1)
        elif args.backend == "mupdf":
            try:
                file_output = MuPDFFileOutput(args.dpi)
            except ImportError as e:
                print(str(e))
                sys.exit(1)
        elif args.backend == "custom_svg":
            # has no additional dependencies
            file_output = SvgFileOutput(args.dpi)
        else:
            raise ValueError(args.backend)

        verbose = args.verbose
        if verbose:
            logging.basicConfig(level=logging.INFO)

        if args.formats:
            print(f"formats supported by {args.backend}:")
            for extension, description in file_output.supported_formats():
                print(f"  {extension}: {description}")
            sys.exit(0)

        if args.file:
            filename = args.file
        else:
            print("argument FILE is required")
            sys.exit(1)

        print(f'loading file "{filename}"...')
        doc, _ = load_document(filename)

        try:
            layout = doc.layouts.get(args.layout)
        except KeyError:
            print(
                f'Could not find layout "{args.layout}". '
                f"Valid layouts: {[l.name for l in doc.layouts]}"
            )
            sys.exit(1)

        ctx = RenderContext(doc)
        config = Configuration().with_changes(
            background_policy=BackgroundPolicy[args.background]
        )
        out = file_output.backend()

        if args.all_layers_visible:
            def override_layer_properties(layer_properties: Sequence[LayerProperties]) -> None:
                for properties in layer_properties:
                    properties.is_visible = True

            ctx.set_layer_properties_override(override_layer_properties)

        frontend = Frontend(ctx, out, config=config)

        if args.all_entities_visible:
            def override_entity_properties(entity: DXFGraphic, properties: Properties) -> None:
                properties.is_visible = True
            frontend.push_property_override_function(override_entity_properties)

        t0 = time.perf_counter()
        if verbose:
            print(f"drawing layout '{layout.name}' ...")
        frontend.draw_layout(layout, finalize=True)
        t1 = time.perf_counter()
        if verbose:
            print(f"took {t1-t0:.4f} seconds")

        if args.out is not None:
            if pathlib.Path(args.out).suffix not in {
                f".{ext}" for ext, _ in file_output.supported_formats()
            }:
                print(
                    f'the format of the output path "{args.out}" '
                    f"is not supported by the backend {args.backend}"
                )
                sys.exit(1)

            if args.out.exists() and not args.force:
                print(f'the destination "{args.out}" already exists. Not writing')
                sys.exit(1)
            else:
                print(f'exporting to "{args.out}"...')
                t0 = time.perf_counter()
                file_output.save(args.out)
                t1 = time.perf_counter()
                if verbose:
                    print(f"took {t1 - t0:.4f} seconds")

        else:
            print(f"exporting to temporary file...")
            output_dir = pathlib.Path(tempfile.mkdtemp(prefix="ezdxf_draw"))
            output_path = output_dir / f"output.{file_output.default_format()}"
            file_output.save(output_path)
            print(f'saved to "{output_path}"')
            if verbose:
                print("opening viewer...")
            open_file(output_path)


@register
class View(Command):
    """Launcher sub-command: view"""

    NAME = "view"

    @staticmethod
    def add_parser(subparsers):
        parser = subparsers.add_parser(
            View.NAME, help="view DXF files by the PyQt viewer"
        )
        parser.add_argument(
            "file",
            metavar="FILE",
            nargs="?",
            help="DXF file to view",
        )
        parser.add_argument(
            "-l",
            "--layout",
            default="Model",
            help='select the layout to draw, default is "Model"',
        )
        # disable lineweight at all by default:
        parser.add_argument(
            "--lwscale",
            type=float,
            default=0,
            help=HELP_LWSCALE,
        )

    @staticmethod
    def run(args):
        # Import on demand for a quicker startup:
        try:
            from ezdxf.addons.xqt import QtWidgets
        except ImportError as e:
            print(str(e))
            sys.exit(1)
        from ezdxf.addons.drawing.qtviewer import CADViewer
        from ezdxf.addons.drawing.config import Configuration

        config = Configuration(
            lineweight_scaling=args.lwscale,
        )

        signal.signal(signal.SIGINT, signal.SIG_DFL)  # handle Ctrl+C properly
        app = QtWidgets.QApplication(sys.argv)
        app.setStyle("Fusion")
        set_app_icon(app)
        viewer = CADViewer.from_config(config)
        filename = args.file
        if filename:
            doc, auditor = load_document(filename)
            viewer.set_document(
                doc,
                auditor,
                layout=args.layout,
            )
        sys.exit(app.exec())


@register
class Browse(Command):
    """Launcher sub-command: browse"""

    NAME = "browse"

    @staticmethod
    def add_parser(subparsers):
        parser = subparsers.add_parser(Browse.NAME, help="browse DXF file structure")
        parser.add_argument(
            "file",
            metavar="FILE",
            nargs="?",
            help="DXF file to browse",
        )
        parser.add_argument(
            "-l", "--line", type=int, required=False, help="go to line number"
        )
        parser.add_argument(
            "-g",
            "--handle",
            required=False,
            help="go to entity by HANDLE, HANDLE has to be a hex value without "
            "any prefix like 'fefe'",
        )

    @staticmethod
    def run(args):
        try:
            from ezdxf.addons.xqt import QtWidgets
        except ImportError as e:
            print(str(e))
            sys.exit(1)
        from ezdxf.addons import browser

        signal.signal(signal.SIGINT, signal.SIG_DFL)  # handle Ctrl+C properly
        app = QtWidgets.QApplication(sys.argv)
        app.setStyle("Fusion")
        set_app_icon(app)
        main_window = browser.DXFStructureBrowser(
            args.file,
            line=args.line,
            handle=args.handle,
            resource_path=resources_path(),
        )
        main_window.show()
        sys.exit(app.exec())


@register
class BrowseAcisData(Command):
    """Launcher sub-command: browse-acis"""

    NAME = "browse-acis"

    @staticmethod
    def add_parser(subparsers):
        parser = subparsers.add_parser(
            BrowseAcisData.NAME, help="browse ACIS structures in DXF files"
        )
        parser.add_argument(
            "file",
            metavar="FILE",
            nargs="?",
            help="DXF file to browse",
        )
        parser.add_argument(
            "-g",
            "--handle",
            required=False,
            help="go to entity by HANDLE, HANDLE has to be a hex value without "
            "any prefix like 'fefe'",
        )

    @staticmethod
    def run(args):
        try:
            from ezdxf.addons.xqt import QtWidgets
        except ImportError as e:
            print(str(e))
            sys.exit(1)
        from ezdxf.addons.acisbrowser.browser import AcisStructureBrowser

        signal.signal(signal.SIGINT, signal.SIG_DFL)  # handle Ctrl+C properly
        app = QtWidgets.QApplication(sys.argv)
        app.setStyle("Fusion")
        set_app_icon(app)
        main_window = AcisStructureBrowser(
            args.file,
            handle=args.handle,
        )
        main_window.show()
        sys.exit(app.exec())


@register
class Strip(Command):
    """Launcher sub-command: strip"""

    NAME = "strip"

    @staticmethod
    def add_parser(subparsers):
        parser = subparsers.add_parser(Strip.NAME, help="strip comments from DXF files")
        parser.add_argument(
            "file",
            metavar="FILE",
            nargs="+",
            help='DXF file to process, wildcards "*" and "?" are supported',
        )
        parser.add_argument(
            "-b",
            "--backup",
            action="store_true",
            required=False,
            help='make a backup copy with extension ".bak" from the '
            "DXF file, overwrites existing backup files",
        )
        parser.add_argument(
            "-t",
            "--thumbnail",
            action="store_true",
            required=False,
            help="strip THUMBNAILIMAGE section",
        )
        parser.add_argument(
            "--handles",
            action="store_true",
            required=False,
            help="remove handles from DXF R12 or older files",
        )
        parser.add_argument(
            "-v",
            "--verbose",
            action="store_true",
            required=False,
            help="give more output",
        )

    @staticmethod
    def run(args):
        from ezdxf.tools.strip import strip

        for pattern in args.file:
            for filename in glob.glob(pattern):
                codes = [999]
                if args.handles:
                    if is_dxf_r12_file(filename):
                        codes.extend([5, 105])
                    else:
                        print(
                            f"Cannot remove handles from DXF R13 or later: {filename}"
                        )
                strip(
                    filename,
                    backup=args.backup,
                    thumbnail=args.thumbnail,
                    verbose=args.verbose,
                    codes=codes,
                )


@register
class Config(Command):
    """Launcher sub-command: config"""

    NAME = "config"

    @staticmethod
    def add_parser(subparsers):
        parser = subparsers.add_parser(Config.NAME, help="manage config files")
        parser.add_argument(
            "-p",
            "--print",
            action="store_true",
            help="print configuration",
        )
        parser.add_argument(
            "-w",
            "--write",
            metavar="FILE",
            help="write configuration",
        )
        parser.add_argument(
            "--home",
            action="store_true",
            help="create config file 'ezdxf.ini' in the user home directory "
            "'~/.config/ezdxf', $XDG_CONFIG_HOME is supported if set",
        )
        parser.add_argument(
            "--reset",
            action="store_true",
            help="factory reset, delete default config files 'ezdxf.ini'",
        )

    @staticmethod
    def run(args):
        from ezdxf import options

        action = False
        if args.reset:
            options.reset()
            options.delete_default_config_files()
            action = True
        if args.home:
            options.write_home_config()
            action = True
        if args.write:
            action = True
            filepath = Path(args.write).expanduser()
            try:
                options.write_file(str(filepath))
                print(f"configuration written to: {filepath}")
            except IOError as e:
                print(str(e))
        if args.print or action is False:
            options.print()


def load_every_document(filename: str):
    def io_error() -> str:
        msg = f'Not a DXF file or a generic I/O error: "{filename}"'
        print(msg, file=sys.stderr)
        return msg

    def structure_error() -> str:
        msg = f'Invalid or corrupted DXF file: "{filename}"'
        print(msg, file=sys.stderr)
        return msg

    binary_fmt = False
    if is_binary_dxf_file(filename):
        try:
            doc = ezdxf.readfile(filename)
        except IOError:
            raise const.DXFLoadError(io_error())
        except const.DXFStructureError:
            raise const.DXFLoadError(structure_error())
        auditor = doc.audit()
        binary_fmt = True
    else:
        try:
            doc, auditor = recover.readfile(filename)
        except IOError:
            raise const.DXFLoadError(io_error())
        except const.DXFStructureError:
            dwginfo = dwg_file_info(filename)
            if dwginfo.version != "invalid":
                print(
                    f"This is a DWG file!!!\n"
                    f'Filename: "{filename}"\n'
                    f"Format: DWG\n"
                    f"Release: {dwginfo.release}\n"
                    f"DWG Version: {dwginfo.version}\n"
                )
                raise const.DXFLoadError()
            raise const.DXFLoadError(structure_error())
    return doc, auditor, binary_fmt


@register
class Info(Command):
    """Launcher sub-command: info"""

    NAME = "info"

    @staticmethod
    def add_parser(subparsers):
        parser = subparsers.add_parser(
            Info.NAME,
            help="show information and optional stats of DXF files as "
            "loaded by ezdxf, this may not represent the original "
            "content of the file, use the browse command to "
            "see the original content",
        )
        parser.add_argument(
            "file",
            metavar="FILE",
            nargs="+",
            help='DXF file to process, wildcards "*" and "?" are supported',
        )
        parser.add_argument(
            "-v",
            "--verbose",
            action="store_true",
            required=False,
            help="give more output",
        )
        parser.add_argument(
            "-s",
            "--stats",
            action="store_true",
            required=False,
            help="show content stats",
        )

    @staticmethod
    def run(args):
        from ezdxf.document import info

        def process(fn: str):
            try:
                doc, auditor, binary_fmt = load_every_document(fn)
            except const.DXFLoadError:
                pass
            else:
                fmt = "Binary" if binary_fmt else "ASCII"
                print(
                    "\n".join(
                        info(
                            doc,
                            verbose=args.verbose,
                            content=args.stats,
                            fmt=fmt,
                        )
                    )
                )
                if auditor.has_fixes:
                    print(f"Audit process fixed {len(auditor.fixes)} error(s).")
                if auditor.has_errors:
                    print(
                        f"Audit process found {len(auditor.errors)} unrecoverable error(s)."
                    )
                print()

        for pattern in args.file:
            file_count = 0
            for filename in glob.glob(pattern):
                if os.path.isdir(filename):
                    dir_pattern = os.path.join(filename, "*.dxf")
                    for filename2 in glob.glob(dir_pattern):
                        process(filename2)
                        file_count += 1
                else:
                    process(filename)
                    file_count += 1

            if file_count == 0:
                sys.stderr.write(f'No matching files for pattern: "{pattern}"\n')


@register
class HPGL(Command):
    """Launcher sub-command: hpgl"""

    NAME = "hpgl"

    @staticmethod
    def add_parser(subparsers):
        parser = subparsers.add_parser(
            HPGL.NAME, help=f"view and/or convert HPGL/2 plot files to various formats"
        )
        parser.add_argument(
            "file",
            metavar="FILE",
            nargs="?",
            default="",
            help=f"view and/or convert HPGL/2 plot files, wildcards (*, ?) supported in command line mode",
        )
        parser.add_argument(
            "-e",
            "--export",
            metavar="FORMAT",
            required=False,
            help=f"convert HPGL/2 plot file to SVG, PDF or DXF from the command line (no gui)",
        )
        parser.add_argument(
            "-r",
            "--rotate",
            type=int,
            choices=(0, 90, 180, 270),
            default=0,
            required=False,
            help="rotate page about 90, 180 or 270 degrees (no gui)",
        )
        parser.add_argument(
            "-x",
            "--mirror_x",
            action="store_true",
            required=False,
            help="mirror page in x-axis direction, (no gui)",
        )
        parser.add_argument(
            "-y",
            "--mirror_y",
            action="store_true",
            required=False,
            help="mirror page in y-axis direction, (no gui)",
        )
        parser.add_argument(
            "-m",
            "--merge_control",
            type=int,
            required=False,
            default=2,
            choices=(0, 1, 2),
            help="provides control over the order of filled polygons, 0=off (print order), "
            "1=luminance (order by luminance), 2=auto (default)",
        )
        parser.add_argument(
            "-f",
            "--force",
            action="store_true",
            required=False,
            help="inserts the mandatory 'enter HPGL/2 mode' escape sequence into the data "
            "stream; use this flag when no HPGL/2 data was found and you are sure the "
            "file is a HPGL/2 plot file",
        )
        parser.add_argument(
            "--aci",
            action="store_true",
            required=False,
            help="use pen numbers as ACI colors and assign colors by layer (DXF only)",
        )
        parser.epilog = (
            "Note that plot files are intended to be plotted on white paper."
        )
        parser.add_argument(
            "--dpi",
            type=int,
            required=False,
            default=96,
            help="pixel density in dots per inch (PNG only)",
        )
        parser.epilog = (
            "Note that plot files are intended to be plotted on white paper."
        )

    @staticmethod
    def run(args):
        if args.export:
            if os.path.exists(args.file):
                filenames = [args.file]
            else:
                filenames = glob.glob(args.file)
            for filename in filenames:
                export_hpgl2(Path(filename), args)
        else:
            launch_hpgl2_viewer(args.file, args.force)


def export_hpgl2(filepath: Path, args) -> None:
    from ezdxf.addons.hpgl2 import api as hpgl2
    from ezdxf.addons.drawing.dxf import ColorMode

    fmt = args.export.upper()
    start_msg = f"converting HPGL/2 plot file '{filepath.name}' to {fmt}"
    try:
        data = filepath.read_bytes()
    except IOError as e:
        print(str(e), file=sys.stderr)
        return
    if args.force:
        data = b"%1B" + data
    export_path = filepath.with_suffix(f".{fmt.lower()}")
    if fmt == "DXF":
        print(start_msg)
        color_mode = ColorMode.ACI if args.aci else ColorMode.RGB
        doc = hpgl2.to_dxf(
            data,
            rotation=args.rotate,
            mirror_x=args.mirror_x,
            mirror_y=args.mirror_y,
            color_mode=color_mode,
            merge_control=args.merge_control,
        )
        try:
            doc.saveas(export_path)
        except IOError as e:
            print(str(e), file=sys.stderr)

    elif fmt == "SVG":
        print(start_msg)
        svg_string = hpgl2.to_svg(
            data,
            rotation=args.rotate,
            mirror_x=args.mirror_x,
            mirror_y=args.mirror_y,
            merge_control=args.merge_control,
        )
        try:
            export_path.write_text(svg_string)
        except IOError as e:
            print(str(e), file=sys.stderr)
    elif fmt == "PDF":
        print(start_msg)
        pdf_bytes = hpgl2.to_pdf(
            data,
            rotation=args.rotate,
            mirror_x=args.mirror_x,
            mirror_y=args.mirror_y,
            merge_control=args.merge_control,
        )
        try:
            export_path.write_bytes(pdf_bytes)
        except IOError as e:
            print(str(e), file=sys.stderr)
    elif fmt == "PNG":
        print(start_msg)
        png_bytes = hpgl2.to_pixmap(
            data,
            rotation=args.rotate,
            mirror_x=args.mirror_x,
            mirror_y=args.mirror_y,
            merge_control=args.merge_control,
            fmt="png",
            dpi=args.dpi,
        )
        try:
            export_path.write_bytes(png_bytes)
        except IOError as e:
            print(str(e), file=sys.stderr)
    else:
        print(f"invalid export format: {fmt}")
        exit(1)
    print(f"file '{export_path.name}' successfully written")


def launch_hpgl2_viewer(filename: str, force: bool) -> None:
    try:
        from ezdxf.addons.xqt import QtWidgets
    except ImportError as e:
        print(str(e))
        exit(1)
    from ezdxf.addons.hpgl2.viewer import HPGL2Viewer

    signal.signal(signal.SIGINT, signal.SIG_DFL)  # handle Ctrl+C properly
    app = QtWidgets.QApplication(sys.argv)
    app.setStyle("Fusion")
    set_app_icon(app)
    viewer = HPGL2Viewer()
    viewer.show()
    if filename and os.path.exists(filename):
        viewer.load_plot_file(filename, force=force)
    sys.exit(app.exec())


def set_app_icon(app):
    from ezdxf.addons.xqt import QtGui, QtCore

    app_icon = QtGui.QIcon()
    p = resources_path()
    app_icon.addFile(str(p / "16x16.png"), QtCore.QSize(16, 16))
    app_icon.addFile(str(p / "24x24.png"), QtCore.QSize(24, 24))
    app_icon.addFile(str(p / "32x32.png"), QtCore.QSize(32, 32))
    app_icon.addFile(str(p / "48x48.png"), QtCore.QSize(48, 48))
    app_icon.addFile(str(p / "64x64.png"), QtCore.QSize(64, 64))
    app_icon.addFile(str(p / "256x256.png"), QtCore.QSize(256, 256))
    app.setWindowIcon(app_icon)


def resources_path():
    from pathlib import Path

    return Path(__file__).parent / "resources"
