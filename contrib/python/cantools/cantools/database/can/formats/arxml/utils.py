# utility functions that are helpful when dealing with ARXML files


def parse_number_string(in_string: str, allow_float: bool=False) \
    -> int | float:
    """Convert a string representing numeric value that is specified
    within an ARXML file to either an integer or a floating point object

    This is surprisingly complicated:

    - Some ARXML files use "true" and "false" synonymous to 1 and 0
    - ARXML uses the C notation (leading 0) to specify octal numbers
      whereas python only accepts the "0o" prefix
    - Some ARXML editors seem to sometimes include a dot in integer
      numbers (e.g., they produce "123.0" instead of "123")
    """
    ret: None | int | float = None
    in_string = in_string.strip().lower()

    if len(in_string) > 0:
        # the string literals "true" and "false" are interpreted as 1 and 0
        if in_string == 'true':
            ret = 1

        if in_string == 'false':
            ret = 0

        # note: prefer parsing as integer first to prevent floating-point precision issues in large numbers.
        # 1. try int parsing from octal notation without an "o" after the leading 0.
        if len(in_string) > 1 and in_string[0] == '0' and in_string[1].isdigit():
            # interpret strings starting with a 0 as octal because
            # python's int(*, 0) does not for some reason.
            ret = int(in_string, 8)

        # 2. try int parsing with auto-detected base.
        if ret is None:
            # handles python integer literals
            # see https://docs.python.org/3/reference/lexical_analysis.html#integers
            try:
                ret = int(in_string, 0)
            except ValueError:
                pass

        # 3. try float parsing from hex string.
        if ret is None and in_string.startswith('0x'):
            ret = float.fromhex(in_string)

        # 4. try float parsing from 'normal' float string
        if ret is None:
            # throws an error, if non-numeric
            # but handles for example scientific notation
            ret = float(in_string)

        # check for not allowed non-integer values
        if not allow_float:
            if ret != int(ret):
                raise ValueError('Floating point value specified where integer '
                                 'is required')
            # if an integer is required but a .0 floating point value is
            # specified, we accept the input anyway. (this seems to be an
            # ambiguity in the AUTOSAR specification.)
            ret = int(ret)
    else:
        ret = 0

    return ret

