"""``$ rio rm``"""


import click

import rasterio.shutil
from rasterio.errors import DriverRegistrationError, RasterioIOError


@click.command()
@click.argument('path')
@click.confirmation_option(
    help="Confirm delete without prompting",
    prompt="Are you sure you want to delete the dataset?",
    expose_value=True)
@click.option(
    '-f', '--format', '--driver', 'driver',
    help="Explicitly delete with this driver rather than probing for the "
         "appropriate driver.")
def rm(path, yes, driver):

    """Delete a dataset.

    Invoking the shell's '$ rm <path>' on a dataset can be used to
    delete a dataset referenced by a file path, but it won't handle
    deleting side car files.  This command is aware of datasets and
    their sidecar files.
    """

    try:
        rasterio.shutil.delete(path, driver=driver)
    except (DriverRegistrationError, RasterioIOError) as e:
        raise click.ClickException(str(e))
