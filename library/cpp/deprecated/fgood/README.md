Some ancient wrappers on top of FILE*, and some string manupulation functions.

Alternatives are as follows.

For TFILEPtr. Use TIFStream or TOFStream if you need IO. For some rare use cases a TFileMap might also do.

For fput/fget/getline. Use streams API.

For struct ffb and struct prnstr. Just don't use them. Even if you can figure out what they do.

For sf family of functions and TLineSplitter. Just use Split* from util/string/split.h

For TSFReader. Use TMapTsvFile.

For read_or_die family of functions. Use streams API.
