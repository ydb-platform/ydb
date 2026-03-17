import argparse
import json
import os

from .template import DocxTemplate, TemplateError

TEMPLATE_ARG = "template_path"
JSON_ARG = "json_path"
OUTPUT_ARG = "output_filename"
OVERWRITE_ARG = "overwrite"
QUIET_ARG = "quiet"


def make_arg_parser():
    parser = argparse.ArgumentParser(
        usage="python -m docxtpl [-h] [-o] [-q] {} {} {}".format(
            TEMPLATE_ARG, JSON_ARG, OUTPUT_ARG
        ),
        description="Make docx file from existing template docx and json data.",
    )
    parser.add_argument(
        TEMPLATE_ARG, type=str, help="The path to the template docx file."
    )
    parser.add_argument(
        JSON_ARG, type=str, help="The path to the json file with the data."
    )
    parser.add_argument(
        OUTPUT_ARG, type=str, help="The filename to save the generated docx."
    )
    parser.add_argument(
        "-" + OVERWRITE_ARG[0],
        "--" + OVERWRITE_ARG,
        action="store_true",
        help="If output file already exists, overwrites without asking for confirmation",
    )
    parser.add_argument(
        "-" + QUIET_ARG[0],
        "--" + QUIET_ARG,
        action="store_true",
        help="Do not display unnecessary messages",
    )
    return parser


def get_args(parser):
    try:
        parsed_args = vars(parser.parse_args())
        return parsed_args
    # Argument errors raise a SystemExit with code 2. Normal usage of the
    # --help or -h flag raises a SystemExit with code 0.
    except SystemExit as e:
        if e.code == 0:
            raise SystemExit
        else:
            raise RuntimeError(
                "Correct usage is:\n{parser.usage}".format(parser=parser)
            )


def is_argument_valid(arg_name, arg_value, overwrite):
    # Basic checks for the arguments
    if arg_name == TEMPLATE_ARG:
        return os.path.isfile(arg_value) and arg_value.endswith(".docx")
    elif arg_name == JSON_ARG:
        return os.path.isfile(arg_value) and arg_value.endswith(".json")
    elif arg_name == OUTPUT_ARG:
        return arg_value.endswith(".docx") and check_exists_ask_overwrite(
            arg_value, overwrite
        )
    elif arg_name in [OVERWRITE_ARG, QUIET_ARG]:
        return arg_value in [True, False]


def check_exists_ask_overwrite(arg_value, overwrite):
    # If output file does not exist or command was run with overwrite option,
    # returns True, else asks for overwrite confirmation. If overwrite is
    # confirmed returns True, else raises OSError.
    if os.path.exists(arg_value) and not overwrite:
        try:
            msg = (
                "File %s already exists, would you like to overwrite the existing file? (y/n)"
                % arg_value
            )
            if input(msg).lower() == "y":
                return True
            else:
                raise OSError
        except OSError:
            raise RuntimeError(
                "File %s already exists, please choose a different name." % arg_value
            )
    else:
        return True


def validate_all_args(parsed_args):
    overwrite = parsed_args[OVERWRITE_ARG]
    # Raises AssertionError if any of the arguments is not validated
    try:
        for arg_name, arg_value in parsed_args.items():
            if not is_argument_valid(arg_name, arg_value, overwrite):
                raise AssertionError
    except AssertionError:
        raise RuntimeError(
            'The specified {arg_name} "{arg_value}" is not valid.'.format(
                arg_name=arg_name, arg_value=arg_value
            )
        )


def get_json_data(json_path):
    with open(json_path) as file:
        try:
            json_data = json.load(file)
            return json_data
        except json.JSONDecodeError as e:
            print(
                "There was an error on line {e.lineno}, column {e.colno} while trying to parse file {json_path}".format(
                    e=e, json_path=json_path
                )
            )
            raise RuntimeError("Failed to get json data.")


def make_docxtemplate(template_path):
    try:
        return DocxTemplate(template_path)
    except TemplateError:
        raise RuntimeError("Could not create docx template.")


def render_docx(doc, json_data):
    try:
        doc.render(json_data)
        return doc
    except TemplateError:
        raise RuntimeError("An error ocurred while trying to render the docx")


def save_file(doc, parsed_args):
    try:
        output_path = parsed_args[OUTPUT_ARG]
        doc.save(output_path)
        if not parsed_args[QUIET_ARG]:
            print(
                "Document successfully generated and saved at {output_path}".format(
                    output_path=output_path
                )
            )
    except OSError as e:
        print("{e.strerror}. Could not save file {e.filename}.".format(e=e))
        raise RuntimeError("Failed to save file.")


def main():
    parser = make_arg_parser()
    # Everything is in a try-except block that catches a RuntimeError that is
    # raised if any of the individual functions called cause an error
    # themselves, terminating the main function.
    parsed_args = get_args(parser)
    try:
        validate_all_args(parsed_args)
        json_data = get_json_data(os.path.abspath(parsed_args[JSON_ARG]))
        doc = make_docxtemplate(os.path.abspath(parsed_args[TEMPLATE_ARG]))
        doc = render_docx(doc, json_data)
        save_file(doc, parsed_args)
    except RuntimeError as e:
        print("Error: " + e.__str__())
        return
    finally:
        if not parsed_args[QUIET_ARG]:
            print("Exiting program!")


if __name__ == "__main__":
    main()
