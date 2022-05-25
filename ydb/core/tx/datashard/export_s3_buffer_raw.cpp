#ifndef KIKIMR_DISABLE_S3_OPS

#include "export_common.h"
#include "export_s3_buffer_raw.h"

#include <ydb/core/tablet_flat/flat_row_state.h>
#include <ydb/library/binary_json/read.h>
#include <ydb/public/lib/scheme_types/scheme_type_id.h>

#include <library/cpp/string_utils/quote/quote.h>

#include <util/datetime/base.h>
#include <util/stream/buffer.h>

namespace NKikimr {
namespace NDataShard {

TS3BufferRaw::TS3BufferRaw(const TTagToColumn& columns, ui64 rowsLimit, ui64 bytesLimit)
    : Columns(columns)
    , RowsLimit(rowsLimit)
    , BytesLimit(bytesLimit)
    , Rows(0)
    , BytesRead(0)
{
}

void TS3BufferRaw::ColumnsOrder(const TVector<ui32>& tags) {
    Y_VERIFY(tags.size() == Columns.size());

    Indices.clear();
    for (ui32 i = 0; i < tags.size(); ++i) {
        const ui32 tag = tags.at(i);
        auto it = Columns.find(tag);
        Y_VERIFY(it != Columns.end());
        Y_VERIFY(Indices.emplace(tag, i).second);
    }
}

void TS3BufferRaw::Collect(const NTable::IScan::TRow& row, IOutputStream& out) {
    bool needsComma = false;
    for (const auto& [tag, column] : Columns) {
        auto it = Indices.find(tag);
        Y_VERIFY(it != Indices.end());
        Y_VERIFY(it->second < (*row).size());
        const auto& cell = (*row)[it->second];

        BytesRead += cell.Size();

        if (needsComma) {
            out << ",";
        } else {
            needsComma = true;
        }

        if (cell.IsNull()) {
            out << "null";
            continue;
        }

        switch (column.Type) {
        case NScheme::NTypeIds::Int32:
            out << cell.AsValue<i32>();
            break;
        case NScheme::NTypeIds::Uint32:
            out << cell.AsValue<ui32>();
            break;
        case NScheme::NTypeIds::Int64:
            out << cell.AsValue<i64>();
            break;
        case NScheme::NTypeIds::Uint64:
            out << cell.AsValue<ui64>();
            break;
        case NScheme::NTypeIds::Uint8:
        //case NScheme::NTypeIds::Byte:
            out << static_cast<ui32>(cell.AsValue<ui8>());
            break;
        case NScheme::NTypeIds::Int8:
            out << static_cast<i32>(cell.AsValue<i8>());
            break;
        case NScheme::NTypeIds::Int16:
            out << cell.AsValue<i16>();
            break;
        case NScheme::NTypeIds::Uint16:
            out << cell.AsValue<ui16>();
            break;
        case NScheme::NTypeIds::Bool:
            out << cell.AsValue<bool>();
            break;
        case NScheme::NTypeIds::Double:
            out << cell.AsValue<double>();
            break;
        case NScheme::NTypeIds::Float:
            out << cell.AsValue<float>();
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
            out << cell.AsValue<i64>();
            break;
        case NScheme::NTypeIds::Decimal:
            out << DecimalToString(cell.AsValue<std::pair<ui64, i64>>());
            break;
        case NScheme::NTypeIds::DyNumber:
            out << DyNumberToString(cell.AsBuf());
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
        default:
            Y_FAIL("Unsupported type");
        }
    }

    out << "\n";
    ++Rows;
}

bool TS3BufferRaw::Collect(const NTable::IScan::TRow& row) {
    TBufferOutput out(Buffer);
    Collect(row, out);
    return true;
}

IEventBase* TS3BufferRaw::PrepareEvent(bool last, NExportScan::IBuffer::TStats& stats) {
    stats.Rows = Rows;
    stats.BytesRead = BytesRead;

    auto buffer = Flush(true);
    if (!buffer) {
        return nullptr;
    }

    stats.BytesSent = buffer->Size();
    return new TEvExportScan::TEvBuffer<TBuffer>(std::move(*buffer), last);
}

void TS3BufferRaw::Clear() {
    Y_VERIFY(Flush(false));
}

bool TS3BufferRaw::IsFilled() const {
    return Rows >= GetRowsLimit() || Buffer.Size() >= GetBytesLimit();
}

TString TS3BufferRaw::GetError() const {
    Y_FAIL("unreachable");
}

TMaybe<TBuffer> TS3BufferRaw::Flush(bool) {
    Rows = 0;
    BytesRead = 0;
    return std::exchange(Buffer, TBuffer());
}

NExportScan::IBuffer* CreateS3ExportBufferRaw(
        const IExport::TTableColumns& columns, ui64 rowsLimit, ui64 bytesLimit)
{
    return new TS3BufferRaw(columns, rowsLimit, bytesLimit);
}

} // NDataShard
} // NKikimr

#endif // KIKIMR_DISABLE_S3_OPS
