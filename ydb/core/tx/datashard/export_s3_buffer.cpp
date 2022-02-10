#ifndef KIKIMR_DISABLE_S3_OPS 
 
#include "export_common.h" 
#include "export_s3.h" 
#include "export_s3_buffer.h" 
 
#include <ydb/core/tablet_flat/flat_row_state.h>
#include <ydb/library/binary_json/read.h>
#include <ydb/public/lib/scheme_types/scheme_type_id.h>
#include <library/cpp/string_utils/quote/quote.h>
 
#include <util/datetime/base.h> 
 
namespace NKikimr { 
namespace NDataShard {
 
using namespace NExportScan; 
using TTableColumns = TS3Export::TTableColumns; 
 
class TS3Buffer: public IBuffer { 
    using TTagToColumn = TTableColumns; 
    using TTagToIndex = THashMap<ui32, ui32>; // index in IScan::TRow 
 
public: 
    explicit TS3Buffer(const TTagToColumn& columns, ui64 rowsLimit, ui64 bytesLimit) 
        : Columns(columns) 
        , RowsLimit(rowsLimit) 
        , BytesLimit(bytesLimit) 
        , Rows(0) 
        , BytesRead(0) 
    { 
    } 
 
    void ColumnsOrder(const TVector<ui32>& tags) override { 
        Y_VERIFY(tags.size() == Columns.size()); 
 
        Indices.clear(); 
        for (ui32 i = 0; i < tags.size(); ++i) { 
            const ui32 tag = tags.at(i); 
            auto it = Columns.find(tag); 
            Y_VERIFY(it != Columns.end()); 
            Y_VERIFY(Indices.emplace(tag, i).second); 
        } 
    } 
 
    void Collect(const NTable::IScan::TRow& row) override { 
        TStringOutput stream(Buffer); 
 
        bool needsComma = false; 
        for (const auto& [tag, column] : Columns) { 
            auto it = Indices.find(tag); 
            Y_VERIFY(it != Indices.end()); 
            Y_VERIFY(it->second < (*row).size()); 
            const auto& cell = (*row)[it->second]; 
 
            BytesRead += cell.Size(); 
 
            if (needsComma) { 
                stream << ","; 
            } else { 
                needsComma = true; 
            } 
 
            if (cell.IsNull()) { 
                stream << "null"; 
                continue; 
            } 
 
            switch (column.Type) { 
            case NScheme::NTypeIds::Int32: 
                stream << cell.AsValue<i32>(); 
                break; 
            case NScheme::NTypeIds::Uint32: 
                stream << cell.AsValue<ui32>(); 
                break; 
            case NScheme::NTypeIds::Int64: 
                stream << cell.AsValue<i64>(); 
                break; 
            case NScheme::NTypeIds::Uint64: 
                stream << cell.AsValue<ui64>(); 
                break; 
            case NScheme::NTypeIds::Uint8: 
            //case NScheme::NTypeIds::Byte: 
                stream << static_cast<ui32>(cell.AsValue<ui8>()); 
                break; 
            case NScheme::NTypeIds::Int8: 
                stream << static_cast<i32>(cell.AsValue<i8>()); 
                break; 
            case NScheme::NTypeIds::Int16: 
                stream << cell.AsValue<i16>(); 
                break; 
            case NScheme::NTypeIds::Uint16: 
                stream << cell.AsValue<ui16>(); 
                break; 
            case NScheme::NTypeIds::Bool: 
                stream << cell.AsValue<bool>(); 
                break; 
            case NScheme::NTypeIds::Double: 
                stream << cell.AsValue<double>(); 
                break; 
            case NScheme::NTypeIds::Float: 
                stream << cell.AsValue<float>(); 
                break; 
            case NScheme::NTypeIds::Date: 
                stream << TInstant::Days(cell.AsValue<ui16>()); 
                break; 
            case NScheme::NTypeIds::Datetime: 
                stream << TInstant::Seconds(cell.AsValue<ui32>()); 
                break; 
            case NScheme::NTypeIds::Timestamp: 
                stream << TInstant::MicroSeconds(cell.AsValue<ui64>()); 
                break; 
            case NScheme::NTypeIds::Interval: 
                stream << cell.AsValue<i64>(); 
                break; 
            case NScheme::NTypeIds::Decimal: 
                stream << DecimalToString(cell.AsValue<std::pair<ui64, i64>>()); 
                break; 
            case NScheme::NTypeIds::DyNumber: 
                stream << DyNumberToString(cell.AsBuf()); 
                break; 
            case NScheme::NTypeIds::String: 
            case NScheme::NTypeIds::String4k: 
            case NScheme::NTypeIds::String2m: 
            case NScheme::NTypeIds::Utf8: 
            case NScheme::NTypeIds::Json: 
            case NScheme::NTypeIds::Yson: 
                stream << '"' << CGIEscapeRet(cell.AsBuf()) << '"'; 
                break; 
            case NScheme::NTypeIds::JsonDocument: 
                stream << '"' << CGIEscapeRet(NBinaryJson::SerializeToJson(cell.AsBuf())) << '"'; 
                break; 
            default: 
                Y_FAIL("Unsupported type"); 
            }; 
        } 
 
        stream << "\n"; 
        ++Rows; 
    } 
 
    IEventBase* PrepareEvent(bool last) override { 
        return new TEvExportScan::TEvBuffer<TString>(Flush(), last); 
    } 
 
    void Clear() override { 
        Flush(); 
    } 
 
    TString Flush() { 
        Rows = 0; 
        BytesRead = 0; 
        return std::exchange(Buffer, TString()); 
    } 
 
    bool IsFilled() const override { 
        return GetRows() >= RowsLimit || GetBytesSent() >= BytesLimit; 
    } 
 
    ui64 GetRows() const override { 
        return Rows; 
    } 
 
    ui64 GetBytesRead() const override { 
        return BytesRead; 
    } 
 
    ui64 GetBytesSent() const override { 
        return Buffer.size(); 
    } 
 
private: 
    const TTagToColumn Columns; 
    const ui64 RowsLimit; 
    const ui64 BytesLimit; 
 
    TTagToIndex Indices; 
 
    ui64 Rows; 
    ui64 BytesRead; 
    TString Buffer; 
 
}; // TS3Buffer 
 
IBuffer* CreateS3ExportBuffer(const TTableColumns& columns, ui64 rowsLimit, ui64 bytesLimit) { 
    return new TS3Buffer(columns, rowsLimit, bytesLimit); 
} 
 
} // NDataShard
} // NKikimr 
 
#endif // KIKIMR_DISABLE_S3_OPS 
