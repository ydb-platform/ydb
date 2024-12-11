#pragma once

#include "public.h"

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NCompression {

////////////////////////////////////////////////////////////////////////////////

//! Each compression frame (i.e. blob compressed via single call to IDictionaryCompressor) contains this header.
struct TDictionaryCompressionFrameInfo
{
    ui64 ContentSize;
};

////////////////////////////////////////////////////////////////////////////////

//! Compressor interface that is aware of compression context.
//! Thread affinity: single-threaded.
struct IDictionaryCompressor
    : public TRefCounted
{
    //! Returns ref to compressed data. Memory will be allocated via #pool.
    virtual TRef Compress(
        TChunkedMemoryPool* pool,
        TRef input) = 0;
};

DEFINE_REFCOUNTED_TYPE(IDictionaryCompressor)

//! Decompressor interface that is aware of decompression context.
//! Thread affinity: single-threaded.
struct IDictionaryDecompressor
    : public TRefCounted
{
    //! Decompresses #input into #ouput.
    //! Memory for output must be pre-allocated, its size can be inferred from frame info.
    virtual void Decompress(
        TRef input,
        TMutableRef output) = 0;
};

DEFINE_REFCOUNTED_TYPE(IDictionaryDecompressor)

////////////////////////////////////////////////////////////////////////////////

//! Dictionary digested and ready for compression.
//! Stores preprocessed dictionary data and can be used to create instance of IDictionaryCompressor.
//! May be used concurrently from multiple compressors.
//! Thread affinity: any.
struct IDigestedCompressionDictionary
    : public TRefCounted
{
    virtual i64 GetMemoryUsage() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IDigestedCompressionDictionary)

//! Dictionary digested and ready for decompression.
//! Stores preprocessed dictionary data and can be used to create instance of IDictionaryDecompressor.
//! May be used concurrently from multiple decompressors.
//! Thread affinity: any.
struct IDigestedDecompressionDictionary
    : public TRefCounted
{
    virtual i64 GetMemoryUsage() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IDigestedDecompressionDictionary)

////////////////////////////////////////////////////////////////////////////////

struct IDictionaryCompressionCodec
{
    virtual ~IDictionaryCompressionCodec() = default;

    virtual int GetMinDictionarySize() const = 0;

    virtual int GetMaxCompressionLevel() const = 0;
    virtual int GetDefaultCompressionLevel() const = 0;

    //! Trains compression dictionary of size not exceeding #dictionarySize.
    //! This dictionary may then be digested for (de)compression.
    //! NB: May return null if training failed, e.g. due to lack of #samples
    //! or no sufficient profit from using dictionary on them.
    virtual TErrorOr<TSharedRef> TrainCompressionDictionary(
        i64 dictionarySize,
        const std::vector<TSharedRef>& samples) const = 0;

    //! NB: Digested dictionary data will not be copied.
    //! (De)compressor will reference digested dictionary for safe access.
    virtual IDictionaryCompressorPtr CreateDictionaryCompressor(
        const IDigestedCompressionDictionaryPtr& digestedCompressionDictionary) const = 0;
    virtual IDictionaryDecompressorPtr CreateDictionaryDecompressor(
        const IDigestedDecompressionDictionaryPtr& digestedDecompressionDictionary) const = 0;

    //! NB: These functions provide means for creating digested (de)compression dictionary,
    //! i.e. a special in-memory representation of a dictionary that can then be used within (de)compressor
    //! for fast (de)compression of string values of any size.
    //! First one must get estimation on size of the digested dictionary with one of the two methods below,
    //! so memory blob of at least that size can be provided to one of the two construction methods.
    //! #compressionLevel determines compression level that will be applied for each compression with that dictionary later on.
    virtual i64 EstimateDigestedCompressionDictionarySize(i64 dictionarySize, int compressionLevel) const = 0;
    virtual i64 EstimateDigestedDecompressionDictionarySize(i64 dictionarySize) const = 0;
    //! NB: Raw #compressionDictionary data will be copied and stored within digested dictionary in a preprocessed form,
    //! so the memory corresponding to #compressionDictionary can be freed. Digested dictionary will own the memory,
    //! referenced by #storage.
    //! These methods may throw.
    virtual IDigestedCompressionDictionaryPtr ConstructDigestedCompressionDictionary(
        const TSharedRef& compressionDictionary,
        TSharedMutableRef storage,
        int compressionLevel) const = 0;
    virtual IDigestedDecompressionDictionaryPtr ConstructDigestedDecompressionDictionary(
        const TSharedRef& decompressionDictionary,
        TSharedMutableRef storage) const = 0;

    //! Parses header of compressed frame #input and returns specified frame info.
    //! This method may throw.
    virtual TDictionaryCompressionFrameInfo GetFrameInfo(TRef input) const = 0;
};

IDictionaryCompressionCodec* GetDictionaryCompressionCodec();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCompression
