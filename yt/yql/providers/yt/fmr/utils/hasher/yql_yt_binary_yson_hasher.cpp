#include "yql_yt_binary_yson_hasher.h"
#include <util/digest/multi.h>
#include <util/str_stl.h>

using namespace NYson::NDetail;

namespace NYql::NFmr {

namespace {

ui64 HashYsonValueFromReader(TYsonReader& reader) {
    char marker = reader.ReadByte();
    switch (marker) {
        case EntitySymbol:
            return 0;
        case FalseMarker:
            return THash<bool>{}(false);
        case TrueMarker:
            return THash<bool>{}(true);
        case Int64Marker:
            return THash<i64>{}(reader.ReadVarInt64());
        case Uint64Marker:
            return THash<ui64>{}(reader.ReadVarUint64());
        case DoubleMarker:
            return THash<double>{}(reader.ReadDouble());
        case StringMarker: {
            TStringBuf str = reader.ReadString();
            return THash<TStringBuf>{}(str);
        }
        default:
            ythrow yexception() << "Unsupported YSON marker for hashing: 0x"
                                << IntToString<16>(static_cast<unsigned char>(marker));
    }
}

} // namespace

ui64 HashYsonValue(TStringBuf ysonData) {
    TYsonReader reader(ysonData);
    return HashYsonValueFromReader(reader);
}

ui64 HashKeyColumns(TStringBuf blobData, const TRowIndexMarkup& rowMarkup, size_t numKeyColumns) {
    Y_ENSURE(rowMarkup.size() >= numKeyColumns + 1, "Row markup too small for requested key columns");
    ui64 h = 0;
    for (size_t i = 0; i < numKeyColumns; ++i) {
        const TColumnOffsetRange& range = rowMarkup[i];
        ui64 colHash = 0;
        if (range.IsValid()) {
            colHash = HashYsonValue(SliceRange(blobData, range));
        }
        h = CombineHashes(h, colHash);
    }
    return h;
}

} // namespace NYql::NFmr
