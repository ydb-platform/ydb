#ifndef KIKIMR_DISABLE_S3_OPS

#include "export_data_format.h"

#include "export_data_format.h"
#include "export_iface.h"
#include "type_serialization.h"

#include <library/cpp/string_utils/quote/quote.h>
#include <yql/essentials/types/binary_json/read.h>

#include <util/generic/hash.h>
#include <util/generic/yexception.h>
#include <util/stream/buffer.h>
#include <util/stream/output.h>

namespace NKikimr::NDataShard {

namespace {

class TDataFormatYdbDump: public IExportDataFormat {
    using TTagToColumn = IExport::TTableColumns;
    using TTagToIndex = THashMap<ui32, ui32>; // index in IScan::TRow

public:

TDataFormatYdbDump(TYdbDumpExportSettings&& settings)
    : Columns(std::move(settings.Columns))
{
}

~TDataFormatYdbDump() = default;

bool ColumnsOrder(const TVector<ui32>& tags) override {
    Y_ENSURE(tags.size() == Columns.size());

    Indices.clear();
    for (ui32 i = 0; i < tags.size(); ++i) {
        const ui32 tag = tags.at(i);
        auto it = Columns.find(tag);
        Y_ENSURE(it != Columns.end());
        Y_ENSURE(Indices.emplace(tag, i).second);
    }

    return true;
}

TMaybe<TBuffer> Collect(const NTable::IScan::TRow& row) override {
    TBuffer rowBuffer;
    TBufferOutput out(rowBuffer);
    ErrorString.clear();

    bool needsComma = false;
    for (const auto& [tag, column] : Columns) {
        auto it = Indices.find(tag);
        Y_ENSURE(it != Indices.end());
        Y_ENSURE(it->second < (*row).size());
        const auto& cell = (*row)[it->second];

        if (needsComma) {
            out << ",";
        } else {
            needsComma = true;
        }

        if (cell.IsNull()) {
            out << "null";
            continue;
        }

        bool serialized = true;
        switch (column.Type.GetTypeId()) {
        case NScheme::NTypeIds::Int32:
            serialized = cell.ToStream<i32>(out, ErrorString);
            break;
        case NScheme::NTypeIds::Uint32:
            serialized = cell.ToStream<ui32>(out, ErrorString);
            break;
        case NScheme::NTypeIds::Int64:
            serialized = cell.ToStream<i64>(out, ErrorString);
            break;
        case NScheme::NTypeIds::Uint64:
            serialized = cell.ToStream<ui64>(out, ErrorString);
            break;
        case NScheme::NTypeIds::Uint8:
        //case NScheme::NTypeIds::Byte:
            out << static_cast<ui32>(cell.AsValue<ui8>());
            break;
        case NScheme::NTypeIds::Int8:
            out << static_cast<i32>(cell.AsValue<i8>());
            break;
        case NScheme::NTypeIds::Int16:
            serialized = cell.ToStream<i16>(out, ErrorString);
            break;
        case NScheme::NTypeIds::Uint16:
            serialized = cell.ToStream<ui16>(out, ErrorString);
            break;
        case NScheme::NTypeIds::Bool:
            serialized = cell.ToStream<bool>(out, ErrorString);
            break;
        case NScheme::NTypeIds::Double:
            serialized = cell.ToStream<double>(out, ErrorString);
            break;
        case NScheme::NTypeIds::Float:
            serialized = cell.ToStream<float>(out, ErrorString);
            break;
        case NScheme::NTypeIds::Date:
            out << TInstant::Days(cell.AsValue<ui16>());
            break;
        case NScheme::NTypeIds::Datetime:
            out << TInstant::Seconds(cell.AsValue<ui32>());
            break;
        case NScheme::NTypeIds::Timestamp:
            out << TInstant::MicroSeconds(cell.AsValue<ui64>());
            break;
        case NScheme::NTypeIds::Interval:
            serialized = cell.ToStream<i64>(out, ErrorString);
            break;
        case NScheme::NTypeIds::Date32:
            serialized = cell.ToStream<i32>(out, ErrorString);
            break;
        case NScheme::NTypeIds::Datetime64:
        case NScheme::NTypeIds::Timestamp64:
        case NScheme::NTypeIds::Interval64:
            serialized = cell.ToStream<i64>(out, ErrorString);
            break;
        case NScheme::NTypeIds::Decimal:
            serialized = DecimalToStream(cell.AsValue<std::pair<ui64, i64>>(), out, ErrorString, column.Type);
            break;
        case NScheme::NTypeIds::DyNumber:
            serialized = DyNumberToStream(cell.AsBuf(), out, ErrorString);
            break;
        case NScheme::NTypeIds::String:
        case NScheme::NTypeIds::String4k:
        case NScheme::NTypeIds::String2m:
        case NScheme::NTypeIds::Utf8:
        case NScheme::NTypeIds::Json:
        case NScheme::NTypeIds::Yson:
            out << '"' << CGIEscapeRet(cell.AsBuf()) << '"';
            break;
        case NScheme::NTypeIds::JsonDocument:
            out << '"' << CGIEscapeRet(NBinaryJson::SerializeToJson(cell.AsBuf())) << '"';
            break;
        case NScheme::NTypeIds::Pg:
            serialized = PgToStream(cell.AsBuf(), column.Type, out, ErrorString);
            break;
        case NScheme::NTypeIds::Uuid:
            serialized = UuidToStream(cell.AsValue<std::pair<ui64, ui64>>(), out, ErrorString);
            break;
        default:
            Y_ENSURE(false, "Unsupported type");
        }

        if (!serialized) {
            return Nothing();
        }
    }

    out << "\n";

    return rowBuffer;
}

void Clear() override {
}

size_t GetReadyOutputBytes() const override {
    // Rows are serialized and returned directly from Collect, nothing is buffered here.
    return 0;
}

TString GetError() const override {
    return ErrorString;
}

TMaybe<TBuffer> Flush(bool last) override {
    Y_UNUSED(last);

    return TBuffer();
}

private:
    const TTagToColumn Columns;

    TTagToIndex Indices;
    TString ErrorString;
};


} // namespace

IExportDataFormat* CreateExportDataFormat(TYdbDumpExportSettings&& settings) {
    return new TDataFormatYdbDump(std::move(settings));
}

} // namespace NKikimr::NDataShard
#endif // KIKIMR_DISABLE_S3_OPS
