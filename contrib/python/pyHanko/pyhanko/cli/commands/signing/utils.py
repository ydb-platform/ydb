import getpass
from typing import BinaryIO, Optional

import click

from pyhanko.pdf_utils import crypt
from pyhanko.pdf_utils.crypt import AuthStatus
from pyhanko.pdf_utils.incremental_writer import IncrementalPdfFileWriter


class OpenForSigning:
    def __init__(self, infile_path: str, lenient: bool):
        self.infile_path = infile_path
        self.lenient = lenient
        self.handle: Optional[BinaryIO] = None

    def __enter__(self) -> IncrementalPdfFileWriter:
        self.handle = infile = open(self.infile_path, 'rb')
        writer = IncrementalPdfFileWriter(infile, strict=not self.lenient)

        # TODO make this an option higher up the tree
        if writer.prev.encrypted:
            sh = writer.prev.security_handler
            if isinstance(sh, crypt.StandardSecurityHandler):
                pdf_pass = getpass.getpass(
                    prompt='Password for encrypted file \'%s\': '
                    % self.infile_path
                )
                auth = writer.encrypt(pdf_pass)
                if auth.status == AuthStatus.FAILED:
                    raise click.ClickException(
                        "Invalid password for encrypted file"
                    )
            elif isinstance(sh, crypt.PubKeySecurityHandler):
                raise click.ClickException(
                    "Public-key document encryption is not supported in the CLI"
                )
            else:  # pragma: nocover
                raise click.ClickException(
                    "Input file appears to be encrypted, but appropriate "
                    "credentials are not available."
                )
        return writer

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.handle:
            self.handle.close()


def open_for_signing(infile_path: str, lenient: bool):
    return OpenForSigning(infile_path, lenient)


def get_text_params(ctx):
    text_params = None
    stamp_url = ctx.obj.stamp_url
    if stamp_url is not None:
        text_params = {'url': stamp_url}
    return text_params
