#pragma once

#include "public.h"
#include "stream.h"

namespace NYT::NCompression::NDetail {

////////////////////////////////////////////////////////////////////////////////

void ZstdCompress(int level, TSource* source, TBlob* output);
void ZstdDecompress(TSource* source, TBlob* output);

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

IDigestedCompressionDictionaryPtr ZstdCreateDigestedCompressionDictionary(
    const TSharedRef& compressionDictionary,
    int compressionLevel);

IDigestedDecompressionDictionaryPtr ZstdCreateDigestedDecompressionDictionary(
    const TSharedRef& compressionDictionary);

TDictionaryCompressionFrameInfo ZstdGetFrameInfo(TRef input);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCompression::NDetail
