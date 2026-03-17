"""Common commandline options for `fio`"""


from collections import defaultdict

import click


src_crs_opt = click.option('--src-crs', '--src_crs', help="Source CRS.")
dst_crs_opt = click.option('--dst-crs', '--dst_crs', help="Destination CRS.")


def cb_layer(ctx, param, value):
    """Let --layer be a name or index."""
    if value is None or not value.isdigit():
        return value
    else:
        return int(value)


def cb_multilayer(ctx, param, value):
    """
    Transform layer options from strings ("1:a,1:b", "2:a,2:c,2:z") to
    {
    '1': ['a', 'b'],
    '2': ['a', 'c', 'z']
    }
    """
    out = defaultdict(list)
    for raw in value:
        for v in raw.split(','):
            ds, name = v.split(':')
            out[ds].append(name)
    return out


def cb_key_val(ctx, param, value):
    """
    click callback to validate `--opt KEY1=VAL1 --opt KEY2=VAL2` and collect
    in a dictionary like the one below, which is what the CLI function receives.
    If no value or `None` is received then an empty dictionary is returned.

        {
            'KEY1': 'VAL1',
            'KEY2': 'VAL2'
        }

    Note: `==VAL` breaks this as `str.split('=', 1)` is used.

    """
    if not value:
        return {}
    else:
        out = {}
        for pair in value:
            if "=" not in pair:
                raise click.BadParameter(
                    f"Invalid syntax for KEY=VAL arg: {pair}"
                )
            else:
                k, v = pair.split("=", 1)
                k = k.lower()
                v = v.lower()
                out[k] = None if v.lower() in ["none", "null", "nil", "nada"] else v
        return out


def validate_multilayer_file_index(files, layerdict):
    """
    Ensure file indexes provided in the --layer option are valid
    """
    for key in layerdict.keys():
        if key not in [str(k) for k in range(1, len(files) + 1)]:
            layer = key + ":" + layerdict[key][0]
            raise click.BadParameter(f"Layer {layer} does not exist")


creation_opt = click.option(
    "--co",
    "--profile",
    "creation_options",
    metavar="NAME=VALUE",
    multiple=True,
    callback=cb_key_val,
    help="Driver specific creation options. See the documentation for the selected output driver for more information.",
)


open_opt = click.option(
    "--oo",
    "open_options",
    metavar="NAME=VALUE",
    multiple=True,
    callback=cb_key_val,
    help="Driver specific open options. See the documentation for the selected output driver for more information.",
)
