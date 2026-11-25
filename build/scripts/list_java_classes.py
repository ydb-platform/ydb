import io
import struct
import sys
import zipfile

# Sizes of the constant pool entries that carry no string, by tag (JVMS 4.4)
_CONSTANT_SIZES = {
    3: 4,
    4: 4,
    5: 8,
    6: 8,
    7: 2,
    8: 2,
    9: 4,
    10: 4,
    11: 4,
    12: 4,
    15: 3,
    16: 2,
    17: 4,
    18: 4,
    19: 2,
    20: 2,
}
_CONSTANT_UTF8 = 1
_WIDE_CONSTANTS = (5, 6)


def read_source_file(data):
    """Return the name of the source file a class was compiled from: its SourceFile attribute (JVMS 4.7.10)."""
    strings = {}
    (count,) = struct.unpack_from('>H', data, 8)
    pos = 10
    index = 1
    while index < count:
        tag = data[pos]
        pos += 1
        if tag == _CONSTANT_UTF8:
            (length,) = struct.unpack_from('>H', data, pos)
            strings[index] = data[pos + 2 : pos + 2 + length]
            pos += 2 + length
        else:
            pos += _CONSTANT_SIZES[tag]
        # long and double take two entries
        index += 2 if tag in _WIDE_CONSTANTS else 1

    def skip_attributes(pos):
        (count,) = struct.unpack_from('>H', data, pos)
        pos += 2
        for _ in range(count):
            (length,) = struct.unpack_from('>I', data, pos + 2)
            pos += 6 + length
        return pos

    # access_flags, this_class, super_class, interfaces
    (interfaces,) = struct.unpack_from('>H', data, pos + 6)
    pos += 8 + 2 * interfaces
    # fields, then methods
    for _ in range(2):
        (members,) = struct.unpack_from('>H', data, pos)
        pos += 2
        for _ in range(members):
            pos = skip_attributes(pos + 6)

    (attributes,) = struct.unpack_from('>H', data, pos)
    pos += 2
    for _ in range(attributes):
        name, length = struct.unpack_from('>HI', data, pos)
        if strings[name] == b'SourceFile':
            (source,) = struct.unpack_from('>H', data, pos + 6)
            # Modified UTF-8 (JVMS 4.4.7): a character outside the BMP is stored as a surrogate pair,
            # each half encoded on its own, so the halves are decoded as is and then joined
            halves = strings[source].decode('utf8', 'surrogatepass')
            return halves.encode('utf16', 'surrogatepass').decode('utf16')
        pos += 6 + length
    return None


def load_sources(cpsf):
    """Return the sources of the module that lie in the source tree, named `<package path>/<file name>`."""
    with io.open(cpsf, encoding='utf8') as afile:
        return set(line.split(':', 1)[0] for line in afile if line.strip())


def list_classes(jar, sources, shade_prefix=''):
    """
    Return the class entries of the jar that were compiled from the given sources. Classes of generated
    sources are not listed: such sources live in the build root, so there is nothing in the repository
    to attribute their coverage to. Neither are the dependencies merged in by UBERJAR().
    UBERJAR_HIDING_PREFIX() moves classes to another package, so shade_prefix is cut off to find the source.
    """
    classes = []
    with zipfile.ZipFile(jar) as jf:
        for name in jf.namelist():
            if not name.endswith('.class'):
                continue
            origin = name[len(shade_prefix) :] if shade_prefix and name.startswith(shade_prefix) else name
            source = read_source_file(jf.read(name))
            if source and origin[: origin.rfind('/') + 1] + source in sources:
                classes.append(name)
    return sorted(classes)


def main():
    jar, cpsf, output = sys.argv[1:4]
    with io.open(output, 'w', encoding='utf8') as afile:
        afile.writelines(name + '\n' for name in list_classes(jar, load_sources(cpsf), *sys.argv[4:]))


if __name__ == '__main__':
    main()
