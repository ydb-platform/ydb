import os
import sys


class Installer:
    """Helper to install fancycompleter in PYTHONSTARTUP"""

    def __init__(self, basepath, force):
        fname = os.path.join(basepath, "python_startup.py")
        self.filename = os.path.expanduser(fname)
        self.force = force

    def check(self):
        PYTHONSTARTUP = os.environ.get("PYTHONSTARTUP")
        if PYTHONSTARTUP:
            return f"PYTHONSTARTUP already defined: {PYTHONSTARTUP}"
        if os.path.exists(self.filename):
            return f"{self.filename} already exists"

    def install(self):
        import textwrap

        error = self.check()
        if error and not self.force:
            print(error)
            print("Use --force to overwrite.")
            return False
        with open(self.filename, "w") as f:
            f.write(
                textwrap.dedent(
                    """
                import fancycompleter
                fancycompleter.interact(persist_history=True)
                """
                )
            )
        self.set_env_var()
        return True

    def set_env_var(self):
        if sys.platform == "win32":
            os.system(f'SETX PYTHONSTARTUP "{self.filename}"')
            print(f"%PYTHONSTARTUP% set to {self.filename}")
        else:
            print(f"startup file written to {self.filename}")
            print("Append this line to your ~/.bashrc:")
            print(f"    export PYTHONSTARTUP={self.filename}")


if __name__ == "__main__":

    def usage():
        print("Usage: python -m fancycompleter install [-f|--force]")
        sys.exit(1)

    cmd = None
    force = False
    for item in sys.argv[1:]:
        if item in ("install",):
            cmd = item
        elif item in ("-f", "--force"):
            force = True
        else:
            usage()

    if cmd == "install":
        installer = Installer("~", force)
        installer.install()
    else:
        usage()
