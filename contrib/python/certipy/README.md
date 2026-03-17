# Certipy

A simple python tool for creating certificate authorities and certificates on
the fly.

## Introduction

Certipy was made to simplify the certificate creation process. To that end,
Certipy exposes methods for creating and managing certificate authorities,
certificates, signing and building trust bundles. Behind the scenes Certipy:

* Manages records of all certificates it creates
  * External certs can be imported and managed by Certipy
  * Maintains signing hierarchy
* Persists certificates to files with appropriate permissions

## Usage

### Command line

Creating a certificate authority:

Certipy defaults to writing certs and certipy.json into a folder called `out`
in your current directory.

```
$ certipy foo
FILES {'ca': '', 'cert': 'out/foo/foo.crt', 'key': 'out/foo/foo.key'}
IS_CA True
SERIAL 0
SIGNEES None
PARENT_CA
```

Creating and signing a key-cert pair:

```
$ certipy bar --ca-name foo
FILES {'ca': 'out/foo/foo.crt', 'key': 'out/bar/bar.key', 'cert': 'out/bar/bar.crt'}
IS_CA False
SERIAL 0
SIGNEES None
PARENT_CA foo
```

Removal:

```
certipy --rm bar
Deleted:
FILES {'ca': 'out/foo/foo.crt', 'key': 'out/bar/bar.key', 'cert': 'out/bar/bar.crt'}
IS_CA False
SERIAL 0
SIGNEES None
PARENT_CA foo
```

### Code

Creating a certificate authority:

```
from certipy import Certipy

certipy = Certipy(store_dir='/tmp')
certipy.create_ca('foo')
record = certipy.store.get_record('foo')
```

Creating and signing a key-cert pair:

```
certipy.create_signed_pair('bar', 'foo')
record = certipy.store.get_record('bar')
```

Creating trust:

```
certipy.create_ca_bundle('ca-bundle.crt')

# or to trust specific certs only:
certipy.create_ca_bundle_for_names('ca-bundle.crt', ['bar'])
```

Removal:

```
record = certipy.remove_files('bar')
```

Records are dicts with the following structure:

```
{
  'serial': 0,
  'is_ca': true,
  'parent_ca': 'ca_name',
  'signees': {
    'signee_name': 1
  },
  'files': {
    'key': 'path/to/key.key',
    'cert': 'path/to/cert.crt',
    'ca': 'path/to/ca.crt',
  }
}
```

The `signees` will be empty for non-CA certificates. The `signees` field
is stored as a python `Counter`. These relationships are used to build trust
bundles.

Information in Certipy is generally passed around as records which point to
actual files. For most `_record` methods, there are generally equivalent
`_file` methods that operate on files themselves. The former will only affect
records in Certipy's store and the latter will affect both (something happens
to the file, the record for it should change, too).

### Release

Certipy is released under BSD license. For more details see the LICENSE file.

LLNL-CODE-754897
