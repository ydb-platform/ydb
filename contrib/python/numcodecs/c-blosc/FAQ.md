## Which is the compatibility policy for the Blosc library and format?

### Compatibility among libraries with different minor versions

The compatibility between minor versions (e.g. 2.0 <-> 2.1) will always be *both* backward and forward.  This means that any version of the library will be able to read any data produced with another version differing only in the patch level or minor version. Not being able to achieve that will be considered a bug, and action will be taken to fix that as soon as possible (see e.g.https://github.com/Blosc/c-blosc/issues/215).

### Compatibility among libraries with different major versions

There will be an *attempt* (but not an absolute guarantee) of backward compatibility, but it won’t be an effort for guaranteeing forward compatibility.  For example, Blosc2 2.1 is be able to read any data produced by Blosc 1.x, but Blosc 1.x are not able to read data produced by Blosc2 2.x.
