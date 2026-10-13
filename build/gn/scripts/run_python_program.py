import argparse
import os
import subprocess
import sys


# parser = argparse.ArgumentParser()
# parser.add_argument("--source-root", required=True)
# parser.add_argument("--package-dir", required=True)
# parser.add_argument("--main", required=True)
# args, extra_args = parser.parse_known_args()

# source_root = os.path.abspath(args.source_root)
# package_dir = os.path.join(source_root, args.package_dir)
# main_file = os.path.join(package_dir, args.main)

# env = os.environ.copy()
# python_path = [source_root, package_dir]
# if "PYTHONPATH" in env:
#     python_path.append(env["PYTHONPATH"])
# env["PYTHONPATH"] = os.pathsep.join(python_path)

# cmd = [sys.executable, main_file] + extra_args

# sys.exit(subprocess.call(cmd, env=env))

print(sys.argv)
