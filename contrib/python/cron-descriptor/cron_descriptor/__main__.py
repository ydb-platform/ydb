import argparse

from cron_descriptor import CasingTypeEnum, ExpressionDescriptor, Options

parser = argparse.ArgumentParser(prog="cron_descriptor")
parser.add_argument("expression")
parser.add_argument("-c", "--casing",
                    choices=[v for v in vars(CasingTypeEnum)
                             if not v.startswith("_")],
                    default="Sentence")
parser.add_argument("-v", "--verbose", action="store_true")
parser.add_argument("-W", "--one-indexed-week", action="store_true")
parser.add_argument("-H", "--use-24-hour-time-format", action="store_true")

args = parser.parse_args()

options = Options()
options.casing_type = getattr(CasingTypeEnum, args.casing)
options.verbose = args.verbose
options.day_of_week_start_index_zero = not args.one_indexed_week
options.use_24hour_time_format = args.use_24_hour_time_format

descriptor = ExpressionDescriptor(args.expression, options)

print(str(descriptor))
