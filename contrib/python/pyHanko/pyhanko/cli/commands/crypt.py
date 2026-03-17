import getpass

import click

from pyhanko.cli._root import cli_root
from pyhanko.cli.runtime import pyhanko_exception_manager
from pyhanko.cli.utils import _warn_empty_passphrase, readable_file
from pyhanko.keys import load_certs_from_pemder
from pyhanko.pdf_utils import crypt
from pyhanko.pdf_utils.crypt import StandardSecurityHandler
from pyhanko.pdf_utils.crypt.permissions import PubKeyPermissions
from pyhanko.pdf_utils.reader import PdfFileReader
from pyhanko.pdf_utils.writer import copy_into_new_writer

__all__ = ['decrypt', 'encrypt_file']


@cli_root.command(help='encrypt PDF files (AES-256 only)', name='encrypt')
@click.argument('infile', type=readable_file)
@click.argument('outfile', type=click.Path(writable=True, dir_okay=False))
@click.option(
    '--password',
    help='password to encrypt the file with',
    required=False,
    type=str,
)
@click.option(
    '--recipient',
    required=False,
    multiple=True,
    help='certificate(s) corresponding to entities that '
    'can decrypt the output file',
    type=click.Path(readable=True, dir_okay=False),
)
def encrypt_file(infile, outfile, password, recipient):
    if password and recipient:
        raise click.ClickException(
            "Specify either a password or a list of recipients."
        )
    elif not password and not recipient:
        password = getpass.getpass(prompt='Output file password: ')

    recipient_certs = None
    if recipient:
        recipient_certs = list(load_certs_from_pemder(cert_files=recipient))

    with pyhanko_exception_manager():
        with open(infile, 'rb') as inf:
            r = PdfFileReader(inf)
            w = copy_into_new_writer(r)

            if recipient_certs:
                w.encrypt_pubkey(recipient_certs)
            else:
                w.encrypt(owner_pass=password)

            with open(outfile, 'wb') as outf:
                w.write(outf)


@cli_root.group(
    help='decrypt PDF files (any standard PDF encryption scheme)',
    name='decrypt',
)
def decrypt():
    pass


decrypt_force_flag = click.option(
    '--force',
    help='ignore access restrictions (use at your own risk)',
    required=False,
    type=bool,
    is_flag=True,
    default=False,
)


@decrypt.command(help='decrypt using password', name='password')
@click.argument('infile', type=readable_file)
@click.argument('outfile', type=click.Path(writable=True, dir_okay=False))
@click.option(
    '--password',
    help='password to decrypt the file with',
    required=False,
    type=str,
)
@decrypt_force_flag
def decrypt_with_password(infile, outfile, password, force):
    with pyhanko_exception_manager():
        with open(infile, 'rb') as inf:
            r = PdfFileReader(inf)
            if r.security_handler is None:
                raise click.ClickException("File is not encrypted.")
            elif not isinstance(r.security_handler, StandardSecurityHandler):
                raise click.ClickException(
                    "File is not encrypted with the standard (password-based) security handler"
                )
            if not password:
                password = getpass.getpass(prompt='File password: ')
            auth_result = r.decrypt(password)
            if auth_result.status == crypt.AuthStatus.USER and not force:
                raise click.ClickException(
                    "Password specified was the user password, not "
                    "the owner password. Pass --force to decrypt the "
                    "file anyway."
                )
            elif auth_result.status == crypt.AuthStatus.FAILED:
                raise click.ClickException("Password didn't match.")
            w = copy_into_new_writer(r)
            with open(outfile, 'wb') as outf:
                w.write(outf)


@decrypt.command(help='decrypt using private key (PEM/DER)', name='pemder')
@click.argument('infile', type=readable_file)
@click.argument('outfile', type=click.Path(writable=True, dir_okay=False))
@click.option(
    '--key',
    type=readable_file,
    required=True,
    help='file containing the recipient\'s private key (PEM/DER)',
)
@click.option(
    '--cert',
    help='file containing the recipient\'s certificate (PEM/DER)',
    type=readable_file,
    required=True,
)
@click.option(
    '--passfile',
    required=False,
    type=click.File('rb'),
    help='file containing the passphrase for the private key',
    show_default='stdin',
)
@click.option(
    '--no-pass',
    help='assume the private key file is unencrypted',
    type=bool,
    is_flag=True,
    default=False,
    show_default=True,
)
@decrypt_force_flag
def decrypt_with_pemder(infile, outfile, key, cert, passfile, force, no_pass):
    if passfile is not None:
        passphrase = passfile.read()
        passfile.close()
    elif not no_pass:
        passphrase = getpass.getpass(prompt='Key passphrase: ').encode('utf-8')
        if not passphrase:
            _warn_empty_passphrase()
            passphrase = None
    else:
        passphrase = None

    sedk = crypt.SimpleEnvelopeKeyDecrypter.load(
        key, cert, key_passphrase=passphrase
    )

    _decrypt_pubkey(sedk, infile, outfile, force)


def _decrypt_pubkey(
    sedk: crypt.SimpleEnvelopeKeyDecrypter, infile, outfile, force
):
    with pyhanko_exception_manager():
        with open(infile, 'rb') as inf:
            r = PdfFileReader(inf)
            if r.security_handler is None:
                raise click.ClickException("File is not encrypted.")
            if not isinstance(r.security_handler, crypt.PubKeySecurityHandler):
                raise click.ClickException(
                    "File was not encrypted with a public-key security handler."
                )
            auth_result = r.decrypt_pubkey(sedk)
            if auth_result.status == crypt.AuthStatus.USER:
                if (
                    not force
                    and auth_result.permission_flags
                    and not (
                        PubKeyPermissions.ALLOW_ENCRYPTION_CHANGE
                        in auth_result.permission_flags
                    )
                ):
                    raise click.ClickException(
                        "Change of encryption is typically not allowed with "
                        "user access. Pass --force to decrypt the file anyway."
                    )
            elif auth_result.status == crypt.AuthStatus.FAILED:
                raise click.ClickException("Failed to decrypt the file.")
            w = copy_into_new_writer(r)
            with open(outfile, 'wb') as outf:
                w.write(outf)


@decrypt.command(help='decrypt using private key (PKCS#12)', name='pkcs12')
@click.argument('infile', type=readable_file)
@click.argument('outfile', type=click.Path(writable=True, dir_okay=False))
@click.argument('pfx', type=readable_file)
@click.option(
    '--passfile',
    required=False,
    type=click.File('r'),
    help='file containing the passphrase for the PKCS#12 file',
    show_default='stdin',
)
@decrypt_force_flag
def decrypt_with_pkcs12(infile, outfile, pfx, passfile, force):
    if passfile is None:
        passphrase = getpass.getpass(prompt='Key passphrase: ').encode('utf-8')
    else:
        passphrase = passfile.readline().strip().encode('utf-8')
        passfile.close()
    sedk = crypt.SimpleEnvelopeKeyDecrypter.load_pkcs12(
        pfx, passphrase=passphrase
    )

    _decrypt_pubkey(sedk, infile, outfile, force)
