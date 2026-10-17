from elftools.elf.elffile import ELFFile

import yatest.common

UDF_PATH = 'yql/essentials/udfs/test/policy_probe/libpolicy_probe_udf.so'

ENTRY_POINTS = frozenset(
    [
        'AbiVersion',
        'Register',
    ]
)

SERVICE_SYMBOLS = frozenset(
    [
        'UdfAllocate',
        'UdfAllocateWithSize',
        'UdfArrowAllocate',
        'UdfArrowFree',
        'UdfArrowReallocate',
        'UdfFree',
        'UdfFreeWithSize',
        'UdfRegisterObject',
        'UdfTerminate',
        'UdfUnregisterObject',
    ]
)


def defined_symbols(names):
    with open(yatest.common.binary_path(UDF_PATH), 'rb') as f:
        elf = ELFFile(f)
        sections = [elf.get_section_by_name('.dynsym'), elf.get_section_by_name('.symtab')]
        return {
            symbol.name
            for section in sections
            if section is not None
            for symbol in section.iter_symbols()
            if symbol.name in names and symbol['st_shndx'] != 'SHN_UNDEF'
        }


def test_entry_points_are_defined():
    missing = ENTRY_POINTS - defined_symbols(ENTRY_POINTS)
    assert not missing, 'the udf defines no ' + ', '.join(sorted(missing))


def test_service_symbols_are_left_to_the_host():
    defined = defined_symbols(SERVICE_SYMBOLS)
    assert not defined, 'a service policy got linked into the udf, defining ' + ', '.join(sorted(defined))
