import argparse
import importlib
import json
from typing import Dict, Protocol, Optional, Callable
import sys
from .view import generate_oas


class YamlModule(Protocol):
    """
    Yaml Module type hint
    """

    def dump(self, data) -> str:
        pass


yaml: Optional[YamlModule]

try:
    import yaml
except ImportError:
    yaml = None


def application_type(value):
    """
    Return aiohttp application defined in the value.
    """
    try:
        module_name, app_name = value.split(":")
    except ValueError:
        module_name, app_name = value, "app"

    module = importlib.import_module(module_name)
    try:
        if app_name.endswith("()"):
            app_name = app_name.strip("()")
            factory_app = getattr(module, app_name)
            return factory_app()
        return getattr(module, app_name)

    except AttributeError as error:
        raise argparse.ArgumentTypeError(error) from error


def base_oas_file_type(value) -> Dict:
    """
    Load base oas file
    """
    try:
        with open(value) as oas_file:
            data = oas_file.read()
    except OSError as error:
        raise argparse.ArgumentTypeError(error) from error

    return json.loads(data)


def format_type(value) -> Callable:
    """
    Date Dumper one of (json, yaml)
    """
    dumpers = {"json": lambda data: json.dumps(data, sort_keys=False, indent=4)}
    if yaml is not None:
        dumpers["yaml"] = lambda data: yaml.dump(data, sort_keys=False)

    try:
        return dumpers[value]
    except KeyError:
        raise argparse.ArgumentTypeError(
            f"Wrong format value. (allowed values: {tuple(dumpers.keys())})"
        ) from None


def setup(parser: argparse.ArgumentParser):
    parser.add_argument(
        "apps",
        metavar="APP",
        type=application_type,
        nargs="*",
        help="The name of the module containing the asyncio.web.Application."
        " By default the variable named 'app' is loaded but you can define"
        " an other variable name ending the name of module with : characters"
        " and the name of variable. Example: my_package.my_module:my_app"
        " If your asyncio.web.Application is returned by a function, you can"
        " use the syntax: my_package.my_module:my_app()",
    )
    parser.add_argument(
        "-b",
        "--base-oas-file",
        metavar="FILE",
        dest="base",
        type=base_oas_file_type,
        help="A file that will be used as base to generate OAS",
        default={},
    )
    parser.add_argument(
        "-o",
        "--output",
        metavar="FILE",
        type=argparse.FileType("w"),
        help="File to write the output",
        default=sys.stdout,
    )

    if yaml:
        help_output_format = (
            "The output format, can be 'json' or 'yaml' (default is json)"
        )
    else:
        help_output_format = "The output format, only 'json' is available install pyyaml to have yaml output format"

    parser.add_argument(
        "-f",
        "--format",
        metavar="FORMAT",
        dest="formatter",
        type=format_type,
        help=help_output_format,
        default=format_type("json"),
    )

    parser.set_defaults(func=show_oas)


def show_oas(args: argparse.Namespace):
    """
    Display Open API Specification on the stdout.
    """
    spec = args.base
    spec.update(generate_oas(args.apps))
    print(args.formatter(spec), file=args.output)
