import argparse
import jsondiff
import sys

def load_file(serializer, file_path):
    with open(file_path) as f:
        parsed = None
        try:
            parsed = serializer.deserialize_file(f)
        except ValueError:
            print(f"{file_path} is not valid {serializer.file_format}")
        except FileNotFoundError:
            print(f"{file_path} does not exist")
    return parsed

def main():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument("first")
    parser.add_argument("second")
    parser.add_argument("-p", "--patch", action="store_true", default=False)
    parser.add_argument("-s", "--syntax", choices=(jsondiff.builtin_syntaxes.keys()), default="compact",
                        help="Diff syntax controls how differences are rendered")
    parser.add_argument("-i", "--indent", action="store", type=int, default=None,
                        help="Number of spaces to indent. None is compact, no indentation.")
    parser.add_argument("-f", "--format", choices=("json", "yaml"), default="json",
                        help="Specify file format for input and dump")

    args = parser.parse_args()

    serializer = jsondiff.Serializer(args.format, args.indent)

    parsed_first = load_file(serializer, args.first)
    parsed_second = load_file(serializer, args.second)

    if not (parsed_first and parsed_second):
        return 1

    if args.patch:
        x = jsondiff.patch(
            parsed_first,
            parsed_second,
            marshal=True,
            syntax=args.syntax
        )
    else:
        x = jsondiff.diff(
            parsed_first,
            parsed_second,
            marshal=True,
            syntax=args.syntax
        )

    serializer.serialize_data(x, sys.stdout)

    return 0

if __name__ == '__main__':
    ret = main()
    sys.exit(ret)
