from udsoncan.exceptions import ConfigError
from udsoncan.BaseService import BaseService

from udsoncan.typing import IOConfigEntry, IOConfig, CodecDefinition

from typing import Any, Union, List, Dict, cast


def validate_int(value: Any, min: int = 0, max: int = 0xFF, name: str = 'value'):
    if not isinstance(value, int):
        raise ValueError("%s must be a valid integer" % (name))
    if value < min or value > max:
        raise ValueError("%s must be an integer between 0x%X and 0x%X" % (name, min, max))


def fetch_io_entry_from_config(did: int, ioconfig: IOConfig) -> IOConfigEntry:
    if did not in ioconfig:
        if 'default' in ioconfig:
            selected = ioconfig['default']
        else:
            raise ConfigError(key=did, msg='Actual Input/Output configuration contains no definition for data identifier 0x%04x' % did)
    else:
        selected = ioconfig[did]

    if not isinstance(selected, dict):
        selected = cast(IOConfigEntry, {
            'codec': selected
        })
    return selected


def check_io_config_composite_entry(didname: str, entry: IOConfigEntry) -> None:
    if 'codec'not in entry:
        raise ConfigError('codec', msg='Configuration for Input/Output identifier %s is missing a codec')

    if 'mask' in entry:
        mask_def = entry['mask']

        for mask_name in mask_def:
            if not isinstance(mask_def[mask_name], int):
                raise ValueError('In Input/Output configuration for did %s, mask "%s" is not an integer' % (didname, mask_name))

            if mask_def[mask_name] < 0:
                raise ValueError('In Input/Output configuration for did %s, mask "%s" is not a positive integer' % (didname, mask_name))

    if 'mask_size' in entry:
        if not isinstance(entry['mask_size'], int):
            raise ValueError('mask_size in Input/Output configuration for did %s must be a valid integer' % (didname))

        if entry['mask_size'] < 0:
            raise ValueError('mask_size in Input/Output configuration for did %s must be greater than 0' % (didname))

        if 'mask' in entry:
            mask_def = entry['mask']
            for mask_name in mask_def:
                if mask_def[mask_name] > 2**(entry['mask_size'] * 8) - 1:
                    raise ValueError(
                        'In Input/Output configuration for did %s, mask "%s" cannot fit in %d bytes (defined by mask_size)' % (didname, mask_name, entry['mask_size']))


# Make sure that the actual client configuration contains valid definitions for given Input/Output Data Identifiers
def check_io_config(didlist: Union[int, List[int]], ioconfig: Dict[Any, Any]) -> IOConfig:
    didlist = [didlist] if not isinstance(didlist, list) else didlist

    if 'input_output' in ioconfig:
        ioconfig = ioconfig['input_output']

    if not isinstance(ioconfig, dict):
        raise ConfigError('input_output', msg='Configuration of Input/Output section must be a dict.')

    for did in didlist:
        io_config_entry = fetch_io_entry_from_config(did, ioconfig)
        didstr = '"default"' if did not in ioconfig else '0x%04x' % did
        check_io_config_composite_entry(didstr, io_config_entry)

    return cast(IOConfig, ioconfig)
