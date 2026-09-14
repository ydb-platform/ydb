#include "yql_yt_table_index_marker.h"

#include <library/cpp/yson/detail.h>
#include <library/cpp/yson/varint.h>

#include <util/generic/strbuf.h>
#include <util/stream/buffer.h>

namespace NYql::NFmr {

void AppendTableIndexMarker(TBuffer& buffer, ui32 tableIndex) {
    static constexpr TStringBuf TableIndexKey = "table_index";

    TBufferOutput out(buffer);
    out.Write(NYson::NDetail::BeginAttributesSymbol);
    out.Write(NYson::NDetail::StringMarker);
    NYson::WriteVarInt32(&out, static_cast<i32>(TableIndexKey.size()));
    out.Write(TableIndexKey.data(), TableIndexKey.size());
    out.Write(NYson::NDetail::KeyValueSeparatorSymbol);
    out.Write(NYson::NDetail::Int64Marker);
    NYson::WriteVarInt64(&out, static_cast<i64>(tableIndex));
    out.Write(NYson::NDetail::EndAttributesSymbol);
    out.Write(NYson::NDetail::EntitySymbol);
    out.Write(NYson::NDetail::ListItemSeparatorSymbol);
}

} // namespace NYql::NFmr
