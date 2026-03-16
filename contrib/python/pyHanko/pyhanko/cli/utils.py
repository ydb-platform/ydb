import logging
from typing import Optional, Tuple

import click

from pyhanko.sign import fields

logger = logging.getLogger("cli")


def _warn_empty_passphrase():
    click.echo(
        click.style(
            "WARNING: passphrase is empty. If you intended to use an "
            "unencrypted private key, use --no-pass instead.",
            bold=True,
        )
    )


readable_file = click.Path(exists=True, readable=True, dir_okay=False)
writable_file = click.Path(writable=True, dir_okay=False)


def _index_page(page):
    try:
        page_ix = int(page)
        if not page_ix:
            raise ValueError
        if page_ix > 0:
            # subtract 1 from the total, since that's what people expect
            # when referring to a page index
            return page_ix - 1
        else:
            # keep negative indexes as-is.
            return page_ix
    except ValueError:
        raise click.ClickException(
            "Sig field parameter PAGE should be a nonzero integer, "
            "not %s." % page
        )


def parse_field_location_spec(
    spec: str, require_full_spec: bool = True
) -> Tuple[str, Optional[fields.SigFieldSpec]]:
    try:
        page, box, name = spec.split('/')
    except ValueError:
        if require_full_spec:
            raise click.ClickException(
                "Sig field spec should be of the form PAGE/X1,Y1,X2,Y2/NAME."
            )
        else:
            # interpret the entire string as a field name
            return spec, None

    page_ix = _index_page(page)

    try:
        x1, y1, x2, y2 = map(int, box.split(','))
    except ValueError:
        raise click.ClickException(
            "Sig field parameters X1,Y1,X2,Y2 should be four integers."
        )

    return name, fields.SigFieldSpec(
        sig_field_name=name, on_page=page_ix, box=(x1, y1, x2, y2)
    )
