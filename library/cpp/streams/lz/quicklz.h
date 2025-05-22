#pragma once

#include <library/cpp/streams/lz/common/compressor.h>

TAutoPtr<IInputStream> TryOpenQuickLzDecompressor(const TDecompressSignature& s, IInputStream* input);
TAutoPtr<IInputStream> TryOpenQuickLzDecompressor(const TDecompressSignature& s, TAutoPtr<IInputStream>& input);
