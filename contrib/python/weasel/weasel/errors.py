class ErrorsWithCodes(type):
    def __getattribute__(self, code):
        msg = super().__getattribute__(code)
        if code.startswith("__"):  # python system attributes like __class__
            return msg
        else:
            return "[{code}] {msg}".format(code=code, msg=msg)


class Warnings(metaclass=ErrorsWithCodes):
    # File system
    W801 = "Could not clean/remove the temp directory at {dir}: {msg}."
    W802 = (
        "Remote storage is not yet supported for Python 3.12 with "
        "cloudpathlib. Please use Python 3.11 or earlier for remote storage."
    )


class Errors(metaclass=ErrorsWithCodes):
    # API - Datastructure
    E001 = (
        "Can't write to frozen dictionary. This is likely an internal "
        "error. Are you writing to a default function argument?"
    )
    E002 = (
        "Can't write to frozen list. Maybe you're trying to modify a computed "
        "property or default function argument?"
    )

    # Workflow
    E501 = "Can not execute command '{str_command}'. Do you have '{tool}' installed?"

    # File system
    E801 = "The tar file pulled from the remote attempted an unsafe path " "traversal."
