/**
* Copyright (c) 2016-present, Gregory Szorc
* All rights reserved.
*
* This software may be modified and distributed under the terms
* of the BSD license. See the LICENSE file for details.
*/

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include "structmember.h"

#define ZSTD_STATIC_LINKING_ONLY
#define ZDICT_STATIC_LINKING_ONLY
#include <zstd.h>
#include <zdict.h>

/* Remember to change the string in zstandard/__init__ as well */
#define PYTHON_ZSTANDARD_VERSION "0.14.1"

typedef enum {
	compressorobj_flush_finish,
	compressorobj_flush_block,
} CompressorObj_Flush;

/*
   Represents a ZstdCompressionParameters type.

   This type holds all the low-level compression parameters that can be set.
*/
typedef struct {
	PyObject_HEAD
	ZSTD_CCtx_params* params;
} ZstdCompressionParametersObject;

extern PyTypeObject ZstdCompressionParametersType;

/*
   Represents a FrameParameters type.

   This type is basically a wrapper around ZSTD_frameParams.
*/
typedef struct {
	PyObject_HEAD
	unsigned long long frameContentSize;
	unsigned long long windowSize;
	unsigned dictID;
	char checksumFlag;
} FrameParametersObject;

extern PyTypeObject FrameParametersType;

/*
   Represents a ZstdCompressionDict type.

   Instances hold data used for a zstd compression dictionary.
*/
typedef struct {
	PyObject_HEAD

	/* Pointer to dictionary data. Owned by self. */
	void* dictData;
	/* Size of dictionary data. */
	size_t dictSize;
	ZSTD_dictContentType_e dictType;
	/* k parameter for cover dictionaries. Only populated by train_cover_dict(). */
	unsigned k;
	/* d parameter for cover dictionaries. Only populated by train_cover_dict(). */
	unsigned d;
	/* Digested dictionary, suitable for reuse. */
	ZSTD_CDict* cdict;
	ZSTD_DDict* ddict;
} ZstdCompressionDict;

extern PyTypeObject ZstdCompressionDictType;

/*
   Represents a ZstdCompressor type.
*/
typedef struct {
	PyObject_HEAD

	/* Number of threads to use for operations. */
	unsigned int threads;
	/* Pointer to compression dictionary to use. NULL if not using dictionary
	   compression. */
	ZstdCompressionDict* dict;
	/* Compression context to use. Populated during object construction. */
	ZSTD_CCtx* cctx;
	/* Compression parameters in use. */
	ZSTD_CCtx_params* params;
} ZstdCompressor;

extern PyTypeObject ZstdCompressorType;

typedef struct {
	PyObject_HEAD

	ZstdCompressor* compressor;
	ZSTD_outBuffer output;
	int finished;
} ZstdCompressionObj;

extern PyTypeObject ZstdCompressionObjType;

typedef struct {
	PyObject_HEAD

	ZstdCompressor* compressor;
	PyObject* writer;
	ZSTD_outBuffer output;
	size_t outSize;
	int entered;
	int closed;
	int writeReturnRead;
	unsigned long long bytesCompressed;
} ZstdCompressionWriter;

extern PyTypeObject ZstdCompressionWriterType;

typedef struct {
	PyObject_HEAD

	ZstdCompressor* compressor;
	PyObject* reader;
	Py_buffer buffer;
	Py_ssize_t bufferOffset;
	size_t inSize;
	size_t outSize;

	ZSTD_inBuffer input;
	ZSTD_outBuffer output;
	int finishedOutput;
	int finishedInput;
	PyObject* readResult;
} ZstdCompressorIterator;

extern PyTypeObject ZstdCompressorIteratorType;

typedef struct {
	PyObject_HEAD

	ZstdCompressor* compressor;
	PyObject* reader;
	Py_buffer buffer;
	size_t readSize;

	int entered;
	int closed;
	unsigned long long bytesCompressed;

	ZSTD_inBuffer input;
	ZSTD_outBuffer output;
	int finishedInput;
	int finishedOutput;
	PyObject* readResult;
} ZstdCompressionReader;

extern PyTypeObject ZstdCompressionReaderType;

typedef struct {
	PyObject_HEAD

	ZstdCompressor* compressor;
	ZSTD_inBuffer input;
	ZSTD_outBuffer output;
	Py_buffer inBuffer;
	int finished;
	size_t chunkSize;
} ZstdCompressionChunker;

extern PyTypeObject ZstdCompressionChunkerType;

typedef enum {
	compressionchunker_mode_normal,
	compressionchunker_mode_flush,
	compressionchunker_mode_finish,
} CompressionChunkerMode;

typedef struct {
	PyObject_HEAD

	ZstdCompressionChunker* chunker;
	CompressionChunkerMode mode;
} ZstdCompressionChunkerIterator;

extern PyTypeObject ZstdCompressionChunkerIteratorType;

typedef struct {
	PyObject_HEAD

	ZSTD_DCtx* dctx;
	ZstdCompressionDict* dict;
	size_t maxWindowSize;
	ZSTD_format_e format;
} ZstdDecompressor;

extern PyTypeObject ZstdDecompressorType;

typedef struct {
	PyObject_HEAD

	ZstdDecompressor* decompressor;
	size_t outSize;
	int finished;
} ZstdDecompressionObj;

extern PyTypeObject ZstdDecompressionObjType;

typedef struct {
	PyObject_HEAD

	/* Parent decompressor to which this object is associated. */
	ZstdDecompressor* decompressor;
	/* Object to read() from (if reading from a stream). */
	PyObject* reader;
	/* Size for read() operations on reader. */
	size_t readSize;
	/* Whether a read() can return data spanning multiple zstd frames. */
	int readAcrossFrames;
	/* Buffer to read from (if reading from a buffer). */
	Py_buffer buffer;

	/* Whether the context manager is active. */
	int entered;
	/* Whether we've closed the stream. */
	int closed;

	/* Number of bytes decompressed and returned to user. */
	unsigned long long bytesDecompressed;

	/* Tracks data going into decompressor. */
	ZSTD_inBuffer input;

	/* Holds output from read() operation on reader. */
	PyObject* readResult;

	/* Whether all input has been sent to the decompressor. */
	int finishedInput;
	/* Whether all output has been flushed from the decompressor. */
	int finishedOutput;
} ZstdDecompressionReader;

extern PyTypeObject ZstdDecompressionReaderType;

typedef struct {
	PyObject_HEAD

	ZstdDecompressor* decompressor;
	PyObject* writer;
	size_t outSize;
	int entered;
	int closed;
	int writeReturnRead;
} ZstdDecompressionWriter;

extern PyTypeObject ZstdDecompressionWriterType;

typedef struct {
	PyObject_HEAD

	ZstdDecompressor* decompressor;
	PyObject* reader;
	Py_buffer buffer;
	Py_ssize_t bufferOffset;
	size_t inSize;
	size_t outSize;
	size_t skipBytes;
	ZSTD_inBuffer input;
	ZSTD_outBuffer output;
	Py_ssize_t readCount;
	int finishedInput;
	int finishedOutput;
} ZstdDecompressorIterator;

extern PyTypeObject ZstdDecompressorIteratorType;

typedef struct {
	int errored;
	PyObject* chunk;
} DecompressorIteratorResult;

typedef struct {
	/* The public API is that these are 64-bit unsigned integers. So these can't
	 * be size_t, even though values larger than SIZE_MAX or PY_SSIZE_T_MAX may 
	 * be nonsensical for this platform. */
	unsigned long long offset;
	unsigned long long length;
} BufferSegment;

typedef struct {
	PyObject_HEAD

	PyObject* parent;
	BufferSegment* segments;
	Py_ssize_t segmentCount;
} ZstdBufferSegments;

extern PyTypeObject ZstdBufferSegmentsType;

typedef struct {
	PyObject_HEAD

	PyObject* parent;
	void* data;
	Py_ssize_t dataSize;
	unsigned long long offset;
} ZstdBufferSegment;

extern PyTypeObject ZstdBufferSegmentType;

typedef struct {
	PyObject_HEAD

	Py_buffer parent;
	void* data;
	unsigned long long dataSize;
	BufferSegment* segments;
	Py_ssize_t segmentCount;
	int useFree;
} ZstdBufferWithSegments;

extern PyTypeObject ZstdBufferWithSegmentsType;

/**
 * An ordered collection of BufferWithSegments exposed as a squashed collection.
 *
 * This type provides a virtual view spanning multiple BufferWithSegments
 * instances. It allows multiple instances to be "chained" together and
 * exposed as a single collection. e.g. if there are 2 buffers holding
 * 10 segments each, then o[14] will access the 5th segment in the 2nd buffer.
 */
typedef struct {
	PyObject_HEAD

	/* An array of buffers that should be exposed through this instance. */
	ZstdBufferWithSegments** buffers;
	/* Number of elements in buffers array. */
	Py_ssize_t bufferCount;
	/* Array of first offset in each buffer instance. 0th entry corresponds
	   to number of elements in the 0th buffer. 1st entry corresponds to the
	   sum of elements in 0th and 1st buffers. */
	Py_ssize_t* firstElements;
} ZstdBufferWithSegmentsCollection;

extern PyTypeObject ZstdBufferWithSegmentsCollectionType;

int set_parameter(ZSTD_CCtx_params* params, ZSTD_cParameter param, int value);
int set_parameters(ZSTD_CCtx_params* params, ZstdCompressionParametersObject* obj);
int to_cparams(ZstdCompressionParametersObject* params, ZSTD_compressionParameters* cparams);
FrameParametersObject* get_frame_parameters(PyObject* self, PyObject* args, PyObject* kwargs);
int ensure_ddict(ZstdCompressionDict* dict);
int ensure_dctx(ZstdDecompressor* decompressor, int loadDict);
ZstdCompressionDict* train_dictionary(PyObject* self, PyObject* args, PyObject* kwargs);
ZstdBufferWithSegments* BufferWithSegments_FromMemory(void* data, unsigned long long dataSize, BufferSegment* segments, Py_ssize_t segmentsSize);
Py_ssize_t BufferWithSegmentsCollection_length(ZstdBufferWithSegmentsCollection*);
int cpu_count(void);
size_t roundpow2(size_t);
int safe_pybytes_resize(PyObject** obj, Py_ssize_t size);
