import zipfile

import olefile

from msoffcrypto import exceptions


def OfficeFile(file):
    """Return an office file object based on the format of given file.

    Args:
        file (:obj:`_io.BufferedReader`): Input file.

    Returns:
        BaseOfficeFile object.

    Examples:
        >>> with open("tests/inputs/example_password.docx", "rb") as f:
        ...     officefile = OfficeFile(f)
        ...     officefile.keyTypes
        ('password', 'private_key', 'secret_key')

        >>> with open("tests/inputs/example_password.docx", "rb") as f:
        ...     officefile = OfficeFile(f)
        ...     officefile.load_key(password="Password1234_", verify_password=True)

        >>> with open("README.md", "rb") as f:
        ...     officefile = OfficeFile(f)
        Traceback (most recent call last):
            ...
        msoffcrypto.exceptions.FileFormatError: ...

        >>> with open("tests/inputs/example_password.docx", "rb") as f:
        ...     officefile = OfficeFile(f)
        ...     officefile.load_key(password="0000", verify_password=True)
        Traceback (most recent call last):
            ...
        msoffcrypto.exceptions.InvalidKeyError: ...

    Given file handle will not be closed, the file position will most certainly
    change.
    """
    file.seek(0)  # required by isOleFile
    if olefile.isOleFile(file):
        ole = olefile.OleFileIO(file)
    elif zipfile.is_zipfile(file):  # Heuristic
        from msoffcrypto.format.ooxml import OOXMLFile

        return OOXMLFile(file)
    else:
        raise exceptions.FileFormatError("Unsupported file format")

    # TODO: Make format specifiable by option in case of obstruction
    # Try this first; see https://github.com/nolze/msoffcrypto-tool/issues/17
    if ole.exists("EncryptionInfo"):
        from msoffcrypto.format.ooxml import OOXMLFile

        return OOXMLFile(file)
    # MS-DOC: The WordDocument stream MUST be present in the file.
    # https://msdn.microsoft.com/en-us/library/dd926131(v=office.12).aspx
    elif ole.exists("wordDocument"):
        from msoffcrypto.format.doc97 import Doc97File

        return Doc97File(file)
    # MS-XLS: A file MUST contain exactly one Workbook Stream, ...
    # https://msdn.microsoft.com/en-us/library/dd911009(v=office.12).aspx
    elif ole.exists("Workbook"):
        from msoffcrypto.format.xls97 import Xls97File

        return Xls97File(file)
    # MS-PPT: A required stream whose name MUST be "PowerPoint Document".
    # https://docs.microsoft.com/en-us/openspecs/office_file_formats/ms-ppt/1fc22d56-28f9-4818-bd45-67c2bf721ccf
    elif ole.exists("PowerPoint Document"):
        from msoffcrypto.format.ppt97 import Ppt97File

        return Ppt97File(file)
    else:
        raise exceptions.FileFormatError("Unrecognized file format")
