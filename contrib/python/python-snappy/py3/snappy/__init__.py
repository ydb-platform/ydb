from __future__ import absolute_import

from .snappy import (
	compress,
	decompress,
	uncompress,
	stream_compress,
	stream_decompress,
	StreamCompressor,
	StreamDecompressor,
	UncompressError,
	isValidCompressed,
)

from .hadoop_snappy import (
    stream_compress as hadoop_stream_compress,
    stream_decompress as hadoop_stream_decompress,
)
