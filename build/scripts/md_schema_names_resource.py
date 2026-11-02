import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import process_command_files


sys.stdout.write(";".join(process_command_files.iter_args(sys.argv[1:])))
