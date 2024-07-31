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
    Y_ABORT_UNLESS(tags.size() == Columns.size());

    Indices.clear();
    for (ui32 i = 0; i < tags.size(); ++i) {
        const ui32 tag = tags.at(i);
        auto it = Columns.find(tag);
        Y_ABORT_UNLESS(it != Columns.end());
        Y_ABORT_UNLESS(Indices.emplace(tag, i).second);
    }
}

bool TS3BufferRaw::Collect(const NTable::IScan::TRow& row, IOutputStream& out) {
    bool needsComma = false;
    for (const auto& [tag, column] : Columns) {
        auto it = Indices.find(tag);
        Y_ABORT_UNLESS(it != Indices.end());
        Y_ABORT_UNLESS(it->second < (*row).size());
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
            serialized = DecimalToStream(cell.AsValue<std::pair<ui64, i64>>(), out, ErrorString);
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
            serialized = PgToStream(cell.AsBuf(), column.Type.GetTypeDesc(), out, ErrorString);
            break;
        case NScheme::NTypeIds::Uuid:
            serialized = UuidToStream(cell.AsValue<std::pair<ui64, ui64>>(), out, ErrorString);
            break;
        default:
            Y_ABORT("Unsupported type");
        }

        if (!serialized) {
            return false;
        }
    }

    out << "\n";
    ++Rows;

    return true;
}

bool TS3BufferRaw::Collect(const NTable::IScan::TRow& row) {
    TBufferOutput out(Buffer);
    ErrorString.clear();
    return Collect(row, out);
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
    Y_ABORT_UNLESS(Flush(false));
}

bool TS3BufferRaw::IsFilled() const {
    return Rows >= GetRowsLimit() || Buffer.Size() >= GetBytesLimit();
}

TString TS3BufferRaw::GetError() const {
    return ErrorString;
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
