#pragma once

#include "public.h"
#include "stream.h"

namespace NYT::NCompression::NDetail {

////////////////////////////////////////////////////////////////////////////////

void ZstdCompress(int level, TSource* source, TBlob* output);
void ZstdDecompress(TSource* source, TBlob* output);

////////////////////////////////////////////////////////////////////////////////

constexpr i64 ZstdFrameSignatureSize = 4;
constexpr auto ZstdDataFrameSignature = TStringBuf(
    "\x28\xb5\x2f\xfd",
    ZstdFrameSignatureSize);

constexpr i64 ZstdSyncTagPrefixSize = 24;
constexpr i64 ZstdSyncTagSize = 32;
constexpr auto ZstdSyncTagPrefix = TStringBuf(
    // 8-byte zstd skippable frame magic number
    "\x50\x2a\x4d\x18\x18\x00\x00\x00"
    // 16-byte sync tag magic
    "\xf6\x79\x9c\x4e\xd1\x09\x90\x7e"
    "\x29\x91\xd9\xe6\xbe\xe4\x84\x40",
    ZstdSyncTagPrefixSize);
constexpr auto ZstdSyncFrameSignature = TStringBuf(
    ZstdSyncTagPrefix.begin(),
    ZstdFrameSignatureSize);

DEFINE_ENUM(EZstdFrameType,
    (Data)
    (Sync)
    (Unknown)
);

//! Detects the type of zstd frame from its signature.
//! #buffer must be at least #ZstdFrameSignatureSize bytes long.
EZstdFrameType DetectZstdFrameType(TRef buffer);

//! Returns the (file) offset of the first zstd sync tag in #buffer
//! assuming the buffer starts at #bufferStartOffset (in file).
//! Returns null if no sync tag is found.
std::optional<i64> FindLastZstdSyncTagOffset(TRef buffer, i64 bufferStartOffset);

////////////////////////////////////////////////////////////////////////////////

int ZstdGetMinDictionarySize();

int ZstdGetMaxCompressionLevel();
int ZstdGetDefaultCompressionLevel();

////////////////////////////////////////////////////////////////////////////////

//! See codec.h for clarification on these functions.

TErrorOr<TSharedRef> ZstdTrainCompressionDictionary(
    i64 dictionarySize,
    const std::vector<TSharedRef>& samples);

IDictionaryCompressorPtr ZstdCreateDictionaryCompressor(
    const IDigestedCompressionDictionaryPtr& digestedCompressionDictionary);

IDictionaryDecompressorPtr ZstdCreateDictionaryDecompressor(
    const IDigestedDecompressionDictionaryPtr& digestedDecompressionDictionary);

i64 ZstdEstimateDigestedCompressionDictionarySize(i64 dictionarySize, int compressionLevel);
i64 ZstdEstimateDigestedDecompressionDictionarySize(i64 dictionarySize);

IDigestedCompressionDictionaryPtr ZstdConstructDigestedCompressionDictionary(
    const TSharedRef& compressionDictionary,
    TSharedMutableRef storage,
    int compressionLevel);
IDigestedDecompressionDictionaryPtr ZstdConstructDigestedDecompressionDictionary(
    const TSharedRef& decompressionDictionary,
    TSharedMutableRef storage);

TDictionaryCompressionFrameInfo ZstdGetFrameInfo(TRef input);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCompression::NDetail
