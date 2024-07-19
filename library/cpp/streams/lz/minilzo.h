#pragma once

#include <library/cpp/streams/lz/common/compressor.h>

TAutoPtr<IInputStream> TryOpenMiniLzoDecompressor(const TDecompressSignature& s, IInputStream* input);
TAutoPtr<IInputStream> TryOpenMiniLzoDecompressor(const TDecompressSignature& s, TAutoPtr<IInputStream>& input);
