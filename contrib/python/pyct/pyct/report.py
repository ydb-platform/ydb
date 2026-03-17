#!/usr/bin/env python
# Import and print the library version and filesystem location for each Python package or shell command specified
#
# bash usage: $ report numpy pandas python conda
# python usage: >>> from report import report; report("numpy","pandas","python","conda")

import os.path
import importlib
import subprocess
import platform
import sys

def report(*packages):
    """Import and print location and version information for specified Python packages"""
    accepted_commands = ['python','conda']
    for package in packages:
        loc = "not installed in this environment"
        ver = "unknown"

        try:
            module = importlib.import_module(package)
            loc =  os.path.dirname(module.__file__)

            try:
                ver = str(module.__version__)
            except Exception:
                pass

        except (ImportError, ModuleNotFoundError):
            if package in accepted_commands:
                try:
                    # See if there is a command by that name and check its --version if so
                    try:
                        loc = subprocess.check_output('command -v {}'.format(package), shell=True).decode().splitlines()[0].strip()
                    except Exception:
                        # .exe in case powershell (otherwise wouldn't need it)
                        loc = subprocess.check_output( 'where.exe {}'.format(package), shell=True).decode().splitlines()[0].strip()
                    out = ""
                    try:
                        out = subprocess.check_output([package, '--version'], stderr=subprocess.STDOUT)
                    except subprocess.CalledProcessError as e:
                        out = e.output

                    # Assume first word in output with a period and digits is the version
                    for s in out.decode().split():
                        if '.' in s and str.isdigit(s[0]) and sum(str.isdigit(c) for c in s)>=2:
                            ver=s.strip()
                            break
                except Exception:
                    pass
            elif package == 'system':
                try:
                    ver = platform.platform(terse=True)
                    loc = "OS: " + platform.platform()
                except Exception:
                    pass
            else:
                pass

        print("{0:30} # {1}".format(package + "=" + ver,loc))


def main():
    report(*(sys.argv[1:]))

if __name__ == "__main__":
    main()
