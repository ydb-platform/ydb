#pragma once

#include <util/generic/buffer.h>
#include <util/system/types.h>

namespace NYql::NFmr {

// Appends a YSON control record `<table_index=tableIndex;>#;` to buffer, in the same
// binary format TMkqlReaderImpl (yt_codec_io.cpp) expects when switching between the
// input tables multiplexed into a single physical byte stream.
void AppendTableIndexMarker(TBuffer& buffer, ui32 tableIndex);

} // namespace NYql::NFmr
