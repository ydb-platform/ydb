import logging
import xml.etree.ElementTree as ET

from fiona.env import require_gdal_version
from fiona.ogrext import _get_metadata_item

log = logging.getLogger(__name__)


class MetadataItem:
    # since GDAL 2.0
    CREATION_FIELD_DATA_TYPES = "DMD_CREATIONFIELDDATATYPES"
    # since GDAL 2.3
    CREATION_FIELD_DATA_SUB_TYPES = "DMD_CREATIONFIELDDATASUBTYPES"
    CREATION_OPTION_LIST = "DMD_CREATIONOPTIONLIST"
    LAYER_CREATION_OPTION_LIST = "DS_LAYER_CREATIONOPTIONLIST"
    # since GDAL 2.0
    DATASET_OPEN_OPTIONS = "DMD_OPENOPTIONLIST"
    # since GDAL 2.0
    EXTENSIONS = "DMD_EXTENSIONS"
    EXTENSION = "DMD_EXTENSION"
    VIRTUAL_IO = "DCAP_VIRTUALIO"
    # since GDAL 2.0
    NOT_NULL_FIELDS = "DCAP_NOTNULL_FIELDS"
    # since gdal 2.3
    NOT_NULL_GEOMETRY_FIELDS = "DCAP_NOTNULL_GEOMFIELDS"
    # since GDAL 3.2
    UNIQUE_FIELDS = "DCAP_UNIQUE_FIELDS"
    # since GDAL 2.0
    DEFAULT_FIELDS = "DCAP_DEFAULT_FIELDS"
    OPEN = "DCAP_OPEN"
    CREATE = "DCAP_CREATE"


def _parse_options(xml):
    """Convert metadata xml to dict"""
    options = {}
    if len(xml) > 0:

        root = ET.fromstring(xml)
        for option in root.iter('Option'):

            option_name = option.attrib['name']
            opt = {}
            opt.update((k, v) for k, v in option.attrib.items() if not k == 'name')

            values = []
            for value in option.iter('Value'):
                values.append(value.text)
            if len(values) > 0:
                opt['values'] = values

            options[option_name] = opt

    return options


@require_gdal_version('2.0')
def dataset_creation_options(driver):
    """ Returns dataset creation options for driver

    Parameters
    ----------
    driver : str

    Returns
    -------
    dict
        Dataset creation options

    """

    xml = _get_metadata_item(driver, MetadataItem.CREATION_OPTION_LIST)

    if xml is None:
        return {}

    if len(xml) == 0:
        return {}

    return _parse_options(xml)


@require_gdal_version('2.0')
def layer_creation_options(driver):
    """ Returns layer creation options for driver

    Parameters
    ----------
    driver : str

    Returns
    -------
    dict
        Layer creation options

    """
    xml = _get_metadata_item(driver, MetadataItem.LAYER_CREATION_OPTION_LIST)

    if xml is None:
        return {}

    if len(xml) == 0:
        return {}

    return _parse_options(xml)


@require_gdal_version('2.0')
def dataset_open_options(driver):
    """ Returns dataset open options for driver

    Parameters
    ----------
    driver : str

    Returns
    -------
    dict
        Dataset open options

    """
    xml = _get_metadata_item(driver, MetadataItem.DATASET_OPEN_OPTIONS)

    if xml is None:
        return {}

    if len(xml) == 0:
        return {}

    return _parse_options(xml)


@require_gdal_version('2.0')
def print_driver_options(driver):
    """ Print driver options for dataset open, dataset creation, and layer creation.

    Parameters
    ----------
    driver : str

    """

    for option_type, options in [("Dataset Open Options", dataset_open_options(driver)),
                                 ("Dataset Creation Options", dataset_creation_options(driver)),
                                 ("Layer Creation Options", layer_creation_options(driver))]:

        print(f"{option_type}:")
        if len(options) == 0:
            print("\tNo options available.")
        else:
            for option_name in options:
                print(f"\t{option_name}:")
                if 'description' in options[option_name]:
                    print(f"\t\tDescription: {options[option_name]['description']}")
                if 'type' in options[option_name]:
                    print(f"\t\tType: {options[option_name]['type']}")
                if 'values' in options[option_name] and len(options[option_name]['values']) > 0:
                    print(f"\t\tAccepted values: {','.join(options[option_name]['values'])}")
                for attr_text, attribute in [('Default value', 'default'),
                                             ('Required', 'required'),
                                             ('Alias', 'aliasOf'),
                                             ('Min', 'min'),
                                             ('Max', 'max'),
                                             ('Max size', 'maxsize'),
                                             ('Scope', 'scope'),
                                             ('Alternative configuration option', 'alt_config_option')]:
                    if attribute in options[option_name]:
                        print(f"\t\t{attr_text}: {options[option_name][attribute]}")
        print("")


@require_gdal_version('2.0')
def extensions(driver):
    """ Returns file extensions supported by driver

    Parameters
    ----------
    driver : str

    Returns
    -------
    list
        List with file extensions or None if not specified by driver

    """

    exts = _get_metadata_item(driver, MetadataItem.EXTENSIONS)

    if exts is None:
        return None

    return [ext for ext in exts.split(" ") if len(ext) > 0]


def extension(driver):
    """ Returns file extension of driver

    Parameters
    ----------
    driver : str

    Returns
    -------
    str
        File extensions or None if not specified by driver

    """

    return _get_metadata_item(driver, MetadataItem.EXTENSION)


@require_gdal_version('2.0')
def supports_vsi(driver):
    """ Returns True if driver supports GDAL's VSI*L API

    Parameters
    ----------
    driver : str

    Returns
    -------
    bool

    """
    virtual_io = _get_metadata_item(driver, MetadataItem.VIRTUAL_IO)
    return virtual_io is not None and virtual_io.upper() == "YES"


@require_gdal_version('2.0')
def supported_field_types(driver):
    """ Returns supported field types

    Parameters
    ----------
    driver : str

    Returns
    -------
    list
        List with supported field types or None if not specified by driver

    """
    field_types_str = _get_metadata_item(driver, MetadataItem.CREATION_FIELD_DATA_TYPES)

    if field_types_str is None:
        return None

    return [field_type for field_type in field_types_str.split(" ") if len(field_type) > 0]


@require_gdal_version('2.3')
def supported_sub_field_types(driver):
    """ Returns supported sub field types

    Parameters
    ----------
    driver : str

    Returns
    -------
    list
        List with supported field types or None if not specified by driver

    """
    field_types_str = _get_metadata_item(driver, MetadataItem.CREATION_FIELD_DATA_SUB_TYPES)

    if field_types_str is None:
        return None

    return [field_type for field_type in field_types_str.split(" ") if len(field_type) > 0]
