#-------------------------------------------------------------------------------
# elftools: elf/notes.py
#
# ELF notes
#
# Eli Bendersky (eliben@gmail.com)
# This code is in the public domain
#-------------------------------------------------------------------------------
from ..common.py3compat import bytes2hex, bytes2str
from ..common.utils import struct_parse, roundup
from ..construct import CString


def iter_notes(elffile, offset, size):
    """ Yield all the notes in a section or segment.
    """
    end = offset + size
    while offset < end:
        note = struct_parse(
            elffile.structs.Elf_Nhdr,
            elffile.stream,
            stream_pos=offset)
        note['n_offset'] = offset
        offset += elffile.structs.Elf_Nhdr.sizeof()
        elffile.stream.seek(offset)
        # n_namesz is 4-byte aligned.
        disk_namesz = roundup(note['n_namesz'], 2)
        note['n_name'] = bytes2str(
            CString('').parse(elffile.stream.read(disk_namesz)))
        offset += disk_namesz

        desc_data = elffile.stream.read(note['n_descsz'])
        note['n_descdata'] = desc_data
        if note['n_type'] == 'NT_GNU_ABI_TAG':
            note['n_desc'] = struct_parse(elffile.structs.Elf_abi,
                                          elffile.stream,
                                          offset)
        elif note['n_type'] == 'NT_GNU_BUILD_ID':
            note['n_desc'] = bytes2hex(desc_data)
        elif note['n_type'] == 'NT_GNU_GOLD_VERSION':
            note['n_desc'] = bytes2str(desc_data)
        elif note['n_type'] == 'NT_PRPSINFO':
            note['n_desc'] = struct_parse(elffile.structs.Elf_Prpsinfo,
                                          elffile.stream,
                                          offset)
        elif note['n_type'] == 'NT_FILE':
            note['n_desc'] = struct_parse(elffile.structs.Elf_Nt_File,
                                          elffile.stream,
                                          offset)
        elif note['n_type'] == 'NT_GNU_PROPERTY_TYPE_0':
            off = offset
            props = []
            while off < end:
                p = struct_parse(elffile.structs.Elf_Prop, elffile.stream, off)
                off += roundup(p.pr_datasz + 8, 2 if elffile.elfclass == 32 else 3)
                props.append(p)
            note['n_desc'] = props
        else:
            note['n_desc'] = desc_data
        offset += roundup(note['n_descsz'], 2)
        note['n_size'] = offset - note['n_offset']
        yield note
