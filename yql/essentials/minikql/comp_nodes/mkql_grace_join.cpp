#include "mkql_counters.h"
#include "mkql_grace_join.h"
#include "mkql_grace_join_imp.h"

#include <yql/essentials/public/udf/udf_data_type.h>
#include <yql/essentials/public/udf/udf_value.h>
#include <yql/essentials/public/decimal/yql_decimal_serialize.h>
#include <yql/essentials/minikql/computation/mkql_custom_list.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/comp_nodes/mkql_factories.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_codegen.h> // Y_IGNORE
#include <yql/essentials/minikql/computation/mkql_computation_node_pack.h>
#include <yql/essentials/minikql/computation/mkql_llvm_base.h> // Y_IGNORE

#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/minikql/mkql_string_util.h>

#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/parser/pg_catalog/catalog.h>

#include <chrono>
#include <limits>
#include <utility>

namespace NKikimr::NMiniKQL {

namespace {

const ui32 PartialJoinBatchSize = 100000; // Number of tuples for one join batch

struct TColumnDataPackInfo {
    ui32 ColumnIdx = 0;                                 // Column index in tuple
    ui32 Bytes = 0;                                     // Size in bytes for fixed size values
    TType* MKQLType;                                    // Data type of the column in term of compute nodes data flows
    NUdf::EDataSlot DataType = NUdf::EDataSlot::Uint32; // Data type of the column for standard types (TDataType)
    TString Name;                                       // Name of the type column
    bool IsKeyColumn = false;                           // True if this columns is key for join
    bool IsString = false;                              // True if value is string
    bool IsPgType = false;                              // True if column is PG type
    bool IsPresortSupported = false;                    // True if pg type supports presort and can be interpreted as string value
    bool IsIType = false;                               // True if column need to be processed via IHash, IEquate interfaces
    ui32 Offset = 0;                                    // Offset of column in packed data
    // TValuePacker Packer; // Packer for composite data types
};

struct TGraceJoinPacker {
    ui64 NullsBitmapSize = 0;                                     // Number of ui64 values for nulls bitmap
    ui64 TuplesPacked = 0;                                        // Total number of packed tuples
    ui64 TuplesBatchPacked = 0;                                   // Number of tuples packed during current join batch
    ui64 TuplesUnpacked = 0;                                      // Total number of unpacked tuples
    ui64 BatchSize = PartialJoinBatchSize;                        // Batch size for partial table packing and join
    std::chrono::time_point<std::chrono::system_clock> StartTime; // Start time of execution
    std::chrono::time_point<std::chrono::system_clock> EndTime;   // End time of execution
    std::vector<ui64> TupleIntVals;                               // Packed value of all fixed length values of table tuple.  Keys columns should be packed first.
    std::vector<ui32> TupleStrSizes;                              // Sizes of all packed strings
    std::vector<char*> TupleStrings;                              // All values of tuple strings
    std::vector<TType*> ColumnTypes;                              // Types of all columns
    std::vector<std::shared_ptr<TValuePacker>> Packers;           // Packers for composite data types
    const THolderFactory& HolderFactory;                          // To use during unpacking
    std::vector<TColumnDataPackInfo> ColumnsPackInfo;             // Information about columns packing
    std::unique_ptr<NGraceJoin::TTable> TablePtr;                 // Table to pack data
    std::vector<NUdf::TUnboxedValue> TupleHolder;                 // Storage for tuple data
    std::vector<NUdf::TUnboxedValue*> TuplePtrs;                  // Storage for tuple data pointers to use in FetchValues
    std::vector<std::string> TupleStringHolder;                   // Storage for complex tuple data types serialized to strings
    std::vector<NUdf::TUnboxedValue> IColumnsHolder;              // Storage for interface-based types (IHash, IEquate)
    NGraceJoin::TupleData JoinTupleData;                          // TupleData to get join results
    ui64 TotalColumnsNum = 0;                                     // Total number of columns to pack
    ui64 TotalIntColumnsNum = 0;                                  // Total number of int columns
    ui64 TotalStrColumnsNum = 0;                                  // Total number of string columns
    ui64 TotalIColumnsNum = 0;                                    // Total number of interface-based columns
    ui64 KeyIntColumnsNum = 0;                                    // Total number of key int columns in original table
    ui64 PackedKeyIntColumnsNum = 0;                              // Length of ui64 array containing data of all key int columns after packing
    ui64 KeyStrColumnsNum = 0;                                    // Total number of key string columns
    ui64 KeyIColumnsNum = 0;                                      // Total number of interface-based columns
    ui64 DataIntColumnsNum = TotalIntColumnsNum - KeyIntColumnsNum;
    ui64 PackedDataIntColumnsNum = 0; // Length of ui64 array containing data of all non-key int columns after packing
    ui64 DataStrColumnsNum = TotalStrColumnsNum - KeyStrColumnsNum;
    ui64 DataIColumnsNum = TotalIColumnsNum - KeyIColumnsNum;
    std::vector<NGraceJoin::TColTypeInterface> ColumnInterfaces;
    bool IsAny;                               // Flag to support any join attribute
    const NUdf::TLoggerPtr Logger;            // Logger instance
    const NUdf::TLogComponentId LogComponent; // Id of current component for logging. GracejJoin here
    inline void Pack();                       // Packs new tuple from TupleHolder and TuplePtrs to TupleIntVals, TupleStrSizes, TupleStrings
    inline void UnPack();                     // Unpacks packed values from TupleIntVals, TupleStrSizes, TupleStrings into TupleHolder and TuplePtrs
    TGraceJoinPacker(const std::vector<TType*>& columnTypes, const std::vector<ui32>& keyColumns,
                     const THolderFactory& holderFactory, bool isAny, NUdf::TLoggerPtr logger, NUdf::TLogComponentId logComponent);
};

TColumnDataPackInfo GetPackInfo(TType* type) {
    NUdf::TDataTypeId colTypeId;
    TColumnDataPackInfo res;

    res.MKQLType = type;

    TType* colType;
    if (type->IsOptional()) {
        colType = AS_TYPE(TOptionalType, type)->GetItemType();
    } else {
        colType = type;
    }

    if (type->GetKind() == TType::EKind::Pg) {
        TPgType* pgType = AS_TYPE(TPgType, type);

        res.IsPgType = true;
        if (pgType->IsPresortSupported()) {
            res.IsPresortSupported = true;
            res.IsString = true;
            res.DataType = NUdf::EDataSlot::String;
            res.Name = pgType->GetName();
        } else {
            res.IsIType = true;
        }
        return res;
    }

    if (colType->GetKind() != TType::EKind::Data) {
        res.IsString = true;
        res.DataType = NUdf::EDataSlot::String;
        return res;
    }

    colTypeId = AS_TYPE(TDataType, colType)->GetSchemeType();

    NUdf::EDataSlot dataType = NUdf::GetDataSlot(colTypeId);
    res.DataType = dataType;

    const NYql::NUdf::TDataTypeInfo& ti = GetDataTypeInfo(dataType);
    res.Name = ti.Name;

    switch (dataType) {
        case NUdf::EDataSlot::Bool:
            res.Bytes = sizeof(bool);
            break;
        case NUdf::EDataSlot::Int8:
            res.Bytes = sizeof(i8);
            break;
        case NUdf::EDataSlot::Uint8:
            res.Bytes = sizeof(ui8);
            break;
        case NUdf::EDataSlot::Int16:
            res.Bytes = sizeof(i16);
            break;
        case NUdf::EDataSlot::Uint16:
            res.Bytes = sizeof(ui16);
            break;
        case NUdf::EDataSlot::Int32:
            res.Bytes = sizeof(i32);
            break;
        case NUdf::EDataSlot::Uint32:
            res.Bytes = sizeof(ui32);
            break;
        case NUdf::EDataSlot::Int64:
            res.Bytes = sizeof(i64);
            break;
        case NUdf::EDataSlot::Uint64:
            res.Bytes = sizeof(ui64);
            break;
        case NUdf::EDataSlot::Float:
            res.Bytes = sizeof(float);
            break;
        case NUdf::EDataSlot::Double:
            res.Bytes = sizeof(double);
            break;
        case NUdf::EDataSlot::Date:
            res.Bytes = sizeof(ui16);
            break;
        case NUdf::EDataSlot::Datetime:
            res.Bytes = sizeof(ui32);
            break;
        case NUdf::EDataSlot::Timestamp:
            res.Bytes = sizeof(ui64);
            break;
        case NUdf::EDataSlot::Interval:
            res.Bytes = sizeof(i64);
            break;
        case NUdf::EDataSlot::TzDate:
            res.Bytes = 4;
            break;
        case NUdf::EDataSlot::TzDatetime:
            res.Bytes = 6;
            break;
        case NUdf::EDataSlot::TzTimestamp:
            res.Bytes = 10;
            break;
        case NUdf::EDataSlot::Decimal:
            res.Bytes = 16;
            break;
        case NUdf::EDataSlot::Date32:
            res.Bytes = 4;
            break;
        case NUdf::EDataSlot::Datetime64:
            res.Bytes = 8;
            break;
        case NUdf::EDataSlot::Timestamp64:
            res.Bytes = 8;
            break;
        case NUdf::EDataSlot::Interval64:
            res.Bytes = 8;
            break;
        case NUdf::EDataSlot::Uuid:
        case NUdf::EDataSlot::DyNumber:
        case NUdf::EDataSlot::JsonDocument:
        case NUdf::EDataSlot::String:
        case NUdf::EDataSlot::Utf8:
        case NUdf::EDataSlot::Yson:
        case NUdf::EDataSlot::Json:
            res.IsString = true;
            break;
        default: {
            MKQL_ENSURE(false, "Unknown data type.");
            res.IsString = true;
        }
    }

    return res;
}

void TGraceJoinPacker::Pack() {
    TuplesPacked++;
    std::fill(TupleIntVals.begin(), TupleIntVals.end(), 0);
    std::fill(TupleStrings.begin(), TupleStrings.end(), nullptr);
    std::fill(TupleStrSizes.begin(), TupleStrSizes.end(), 0);

    for (ui64 i = 0; i < ColumnsPackInfo.size(); i++) {
        const TColumnDataPackInfo& pi = ColumnsPackInfo[i];
        ui32 offset = pi.Offset;

        NYql::NUdf::TUnboxedValue value = *TuplePtrs[pi.ColumnIdx];
        if (!value) { // Null value
            ui64 currNullsIdx = (i + 1) / (sizeof(ui64) * 8);
            ui64 remShift = ((i + 1) - currNullsIdx * (sizeof(ui64) * 8));
            ui64 bitMask = ui64(0x1) << remShift;
            TupleIntVals[currNullsIdx] |= bitMask;
            if (pi.IsKeyColumn) {
                TupleIntVals[0] |= ui64(0x1);
            }
            continue;
        }
        TType* type = pi.MKQLType;

        TType* colType;
        if (type->IsOptional()) {
            colType = AS_TYPE(TOptionalType, type)->GetItemType();
        } else {
            colType = type;
        }

        if (colType->GetKind() != TType::EKind::Data) {
            if (pi.IsIType) { // Interface-based type
                IColumnsHolder[offset] = value;
            } else {
                TStringBuf strBuf = Packers[pi.ColumnIdx]->Pack(value);
                TupleStringHolder[i] = strBuf;
                TupleStrings[offset] = TupleStringHolder[i].data();
                TupleStrSizes[offset] = TupleStringHolder[i].size();
            }
            continue;
        }

        char* buffPtr = reinterpret_cast<char*>(TupleIntVals.data()) + offset;
        switch (pi.DataType)
        {
            case NUdf::EDataSlot::Bool:
                WriteUnaligned<bool>(buffPtr, value.Get<bool>());
                break;
            case NUdf::EDataSlot::Int8:
                WriteUnaligned<i8>(buffPtr, value.Get<i8>());
                break;
            case NUdf::EDataSlot::Uint8:
                WriteUnaligned<ui8>(buffPtr, value.Get<ui8>());
                break;
            case NUdf::EDataSlot::Int16:
                WriteUnaligned<i16>(buffPtr, value.Get<i16>());
                break;
            case NUdf::EDataSlot::Uint16:
                WriteUnaligned<ui16>(buffPtr, value.Get<ui16>());
                break;
            case NUdf::EDataSlot::Int32:
                WriteUnaligned<i32>(buffPtr, value.Get<i32>());
                break;
            case NUdf::EDataSlot::Uint32:
                WriteUnaligned<ui32>(buffPtr, value.Get<ui32>());
                break;
            case NUdf::EDataSlot::Int64:
                WriteUnaligned<i64>(buffPtr, value.Get<i64>());
                break;
            case NUdf::EDataSlot::Uint64:
                WriteUnaligned<ui64>(buffPtr, value.Get<ui64>());
                break;
            case NUdf::EDataSlot::Float:
                WriteUnaligned<float>(buffPtr, value.Get<float>());
                break;
            case NUdf::EDataSlot::Double:
                WriteUnaligned<double>(buffPtr, value.Get<double>());
                break;
            case NUdf::EDataSlot::Date:
                WriteUnaligned<ui16>(buffPtr, value.Get<ui16>());
                break;
            case NUdf::EDataSlot::Datetime:
                WriteUnaligned<ui32>(buffPtr, value.Get<ui32>());
                break;
            case NUdf::EDataSlot::Timestamp:
                WriteUnaligned<ui64>(buffPtr, value.Get<ui64>());
                break;
            case NUdf::EDataSlot::Interval:
                WriteUnaligned<i64>(buffPtr, value.Get<i64>());
                break;
            case NUdf::EDataSlot::Date32:
                WriteUnaligned<i32>(buffPtr, value.Get<i32>());
                break;
            case NUdf::EDataSlot::Datetime64:
                WriteUnaligned<i64>(buffPtr, value.Get<i64>());
                break;
            case NUdf::EDataSlot::Timestamp64:
                WriteUnaligned<i64>(buffPtr, value.Get<i64>());
                break;
            case NUdf::EDataSlot::Interval64:
                WriteUnaligned<i64>(buffPtr, value.Get<i64>());
                break;

            case NUdf::EDataSlot::TzDate: {
                WriteUnaligned<ui16>(buffPtr, value.Get<ui16>());
                WriteUnaligned<ui16>(buffPtr + sizeof(ui16), value.GetTimezoneId());
                break;
            }
            case NUdf::EDataSlot::TzDatetime: {
                WriteUnaligned<ui32>(buffPtr, value.Get<ui32>());
                WriteUnaligned<ui16>(buffPtr + sizeof(ui32), value.GetTimezoneId());
                break;
            }
            case NUdf::EDataSlot::TzTimestamp: {
                WriteUnaligned<ui64>(buffPtr, value.Get<ui64>());
                WriteUnaligned<ui16>(buffPtr + sizeof(ui64), value.GetTimezoneId());
                break;
            }
            case NUdf::EDataSlot::Decimal: {
                NYql::NDecimal::Serialize(value.GetInt128(), buffPtr);
                break;
            }
            default: {
                auto str = TuplePtrs[pi.ColumnIdx]->AsStringRef();
                TupleStrings[offset] = str.Data();
                TupleStrSizes[offset] = str.Size();
            }
        }
    }
}

void TGraceJoinPacker::UnPack() {
    TuplesUnpacked++;
    for (ui64 i = 0; i < ColumnsPackInfo.size(); i++) {
        const TColumnDataPackInfo& pi = ColumnsPackInfo[i];
        ui32 offset = pi.Offset;
        NYql::NUdf::TUnboxedValue& value = *TuplePtrs[pi.ColumnIdx];
        if (JoinTupleData.AllNulls) {
            value = NYql::NUdf::TUnboxedValue();
            continue;
        }
        ui64 currNullsIdx = (i + 1) / (sizeof(ui64) * 8);
        ui64 remShift = ((i + 1) - currNullsIdx * (sizeof(ui64) * 8));
        ui64 bitMask = ui64(0x1) << remShift;
        if (TupleIntVals[currNullsIdx] & bitMask) {
            value = NYql::NUdf::TUnboxedValue();
            continue;
        }

        TType* type = pi.MKQLType;

        TType* colType;
        if (type->IsOptional()) {
            colType = AS_TYPE(TOptionalType, type)->GetItemType();
        } else {
            colType = type;
        }

        if (colType->GetKind() != TType::EKind::Data) {
            if (colType->GetKind() == TType::EKind::Pg) {
                if (pi.IsIType) { // Interface-based type
                    value = IColumnsHolder[offset];
                    continue;
                }
            }
            value = Packers[pi.ColumnIdx]->Unpack(TStringBuf(TupleStrings[offset], TupleStrSizes[offset]), HolderFactory);
            continue;
        }

        char* buffPtr = reinterpret_cast<char*>(TupleIntVals.data()) + offset;
        switch (pi.DataType)
        {
            case NUdf::EDataSlot::Bool:
                value = NUdf::TUnboxedValuePod(ReadUnaligned<bool>(buffPtr));
                break;
            case NUdf::EDataSlot::Int8:
                value = NUdf::TUnboxedValuePod(ReadUnaligned<i8>(buffPtr));
                break;
            case NUdf::EDataSlot::Uint8:
                value = NUdf::TUnboxedValuePod(ReadUnaligned<ui8>(buffPtr));
                break;
            case NUdf::EDataSlot::Int16:
                value = NUdf::TUnboxedValuePod(ReadUnaligned<i16>(buffPtr));
                break;
            case NUdf::EDataSlot::Uint16:
                value = NUdf::TUnboxedValuePod(ReadUnaligned<ui16>(buffPtr));
                break;
            case NUdf::EDataSlot::Int32:
                value = NUdf::TUnboxedValuePod(ReadUnaligned<i32>(buffPtr));
                break;
            case NUdf::EDataSlot::Uint32:
                value = NUdf::TUnboxedValuePod(ReadUnaligned<ui32>(buffPtr));
                break;
            case NUdf::EDataSlot::Int64:
                value = NUdf::TUnboxedValuePod(ReadUnaligned<i64>(buffPtr));
                break;
            case NUdf::EDataSlot::Uint64:
                value = NUdf::TUnboxedValuePod(ReadUnaligned<ui64>(buffPtr));
                break;
            case NUdf::EDataSlot::Float:
                value = NUdf::TUnboxedValuePod(ReadUnaligned<float>(buffPtr));
                break;
            case NUdf::EDataSlot::Double:
                value = NUdf::TUnboxedValuePod(ReadUnaligned<double>(buffPtr));
                break;
            case NUdf::EDataSlot::Date:
                value = NUdf::TUnboxedValuePod(ReadUnaligned<ui16>(buffPtr));
                break;
            case NUdf::EDataSlot::Datetime:
                value = NUdf::TUnboxedValuePod(ReadUnaligned<ui32>(buffPtr));
                break;
            case NUdf::EDataSlot::Timestamp:
                value = NUdf::TUnboxedValuePod(ReadUnaligned<ui64>(buffPtr));
                break;
            case NUdf::EDataSlot::Interval:
                value = NUdf::TUnboxedValuePod(ReadUnaligned<i64>(buffPtr));
                break;
            case NUdf::EDataSlot::Date32:
                value = NUdf::TUnboxedValuePod(ReadUnaligned<i32>(buffPtr));
                break;
            case NUdf::EDataSlot::Datetime64:
                value = NUdf::TUnboxedValuePod(ReadUnaligned<i64>(buffPtr));
                break;
            case NUdf::EDataSlot::Timestamp64:
                value = NUdf::TUnboxedValuePod(ReadUnaligned<i64>(buffPtr));
                break;
            case NUdf::EDataSlot::Interval64:
                value = NUdf::TUnboxedValuePod(ReadUnaligned<i64>(buffPtr));
                break;
            case NUdf::EDataSlot::TzDate: {
                value = NUdf::TUnboxedValuePod(ReadUnaligned<ui16>(buffPtr));
                value.SetTimezoneId(ReadUnaligned<ui16>(buffPtr + sizeof(ui16)));
                break;
            }
            case NUdf::EDataSlot::TzDatetime: {
                value = NUdf::TUnboxedValuePod(ReadUnaligned<ui32>(buffPtr));
                value.SetTimezoneId(ReadUnaligned<ui16>(buffPtr + sizeof(ui32)));
                break;
            }
            case NUdf::EDataSlot::TzTimestamp: {
                value = NUdf::TUnboxedValuePod(ReadUnaligned<ui64>(buffPtr));
                value.SetTimezoneId(ReadUnaligned<ui16>(buffPtr + sizeof(ui64)));
                break;
            }
            case NUdf::EDataSlot::Decimal: {
                const auto des = NYql::NDecimal::Deserialize(buffPtr, sizeof(NYql::NDecimal::TInt128));
                MKQL_ENSURE(!NYql::NDecimal::IsError(des.first), "Bad packed data: invalid decimal.");
                value = NUdf::TUnboxedValuePod(des.first);
                break;
            }
            default: {
                value = MakeString(NUdf::TStringRef(TupleStrings[offset], TupleStrSizes[offset]));
            }
        }
    }
}

TGraceJoinPacker::TGraceJoinPacker(const std::vector<TType*>& columnTypes, const std::vector<ui32>& keyColumns,
                                   const THolderFactory& holderFactory, bool isAny, NUdf::TLoggerPtr logger = nullptr, NUdf::TLogComponentId logComponent = 0)
    : ColumnTypes(columnTypes)
    , HolderFactory(holderFactory)
    , IsAny(isAny)
    , Logger(std::move(logger))
    , LogComponent(logComponent)
{
    ui64 nColumns = ColumnTypes.size();
    ui64 nKeyColumns = keyColumns.size();

    for (const auto& keyColumn : keyColumns) {
        auto colType = columnTypes[keyColumn];
        auto packInfo = GetPackInfo(colType);
        packInfo.ColumnIdx = keyColumn;
        packInfo.IsKeyColumn = true;
        ColumnsPackInfo.push_back(packInfo);
    }

    for (ui32 i = 0; i < columnTypes.size(); i++) {
        auto colType = columnTypes[i];
        auto packInfo = GetPackInfo(colType);
        packInfo.ColumnIdx = i;

        ui32 keyColNums = std::count_if(keyColumns.begin(), keyColumns.end(), [&](ui32 k) { return k == i; });

        Packers.push_back(std::make_shared<TValuePacker>(true, colType));
        if (keyColNums == 0) {
            ColumnsPackInfo.push_back(packInfo);
        }
    }

    nColumns = ColumnsPackInfo.size();

    ui64 totalIntColumnsNum = std::count_if(ColumnsPackInfo.begin(), ColumnsPackInfo.end(), [](TColumnDataPackInfo a) { return !a.IsString && !a.IsPgType; });
    ui64 totalIColumnsNum = std::count_if(ColumnsPackInfo.begin(), ColumnsPackInfo.end(), [](TColumnDataPackInfo a) { return a.IsIType; });
    ui64 totalStrColumnsNum = nColumns - totalIntColumnsNum - totalIColumnsNum;

    ui64 keyIntColumnsNum = std::count_if(ColumnsPackInfo.begin(), ColumnsPackInfo.end(), [](TColumnDataPackInfo a) { return (a.IsKeyColumn && !a.IsString && !a.IsPgType); });
    ui64 keyIColumnsNum = std::count_if(ColumnsPackInfo.begin(), ColumnsPackInfo.end(), [](TColumnDataPackInfo a) { return (a.IsKeyColumn && a.IsIType); });
    ui64 keyStrColumnsNum = nKeyColumns - keyIntColumnsNum - keyIColumnsNum;

    TotalColumnsNum = nColumns;
    TotalIntColumnsNum = totalIntColumnsNum;
    TotalStrColumnsNum = totalStrColumnsNum;
    TotalIColumnsNum = totalIColumnsNum;

    KeyIntColumnsNum = keyIntColumnsNum;
    KeyStrColumnsNum = keyStrColumnsNum;
    KeyIColumnsNum = keyIColumnsNum;

    DataIntColumnsNum = TotalIntColumnsNum - KeyIntColumnsNum;
    DataStrColumnsNum = TotalStrColumnsNum - KeyStrColumnsNum;
    DataIColumnsNum = TotalIColumnsNum - KeyIColumnsNum;

    NullsBitmapSize = ((nColumns + 1) / (8 * sizeof(ui64)) + 1);

    TupleIntVals.resize(2 * totalIntColumnsNum + NullsBitmapSize);
    TupleStrings.resize(totalStrColumnsNum);
    TupleStrSizes.resize(totalStrColumnsNum);

    JoinTupleData.IntColumns = TupleIntVals.data();
    JoinTupleData.StrColumns = TupleStrings.data();
    JoinTupleData.StrSizes = TupleStrSizes.data();

    TupleHolder.resize(nColumns);
    TupleStringHolder.resize(nColumns);
    IColumnsHolder.resize(nColumns);

    JoinTupleData.IColumns = IColumnsHolder.data();

    std::transform(TupleHolder.begin(), TupleHolder.end(), std::back_inserter(TuplePtrs), [](NUdf::TUnboxedValue& v) { return std::addressof(v); });

    ui32 currIntOffset = NullsBitmapSize * sizeof(ui64);
    ui32 currStrOffset = 0;
    ui32 currIOffset = 0;
    std::vector<NGraceJoin::TColTypeInterface> ctiv;

    bool prevKeyColumn = false;

    ui32 keyIntOffset = currIntOffset;

    for (auto& p : ColumnsPackInfo) {
        if (!p.IsString && !p.IsIType) {
            if (prevKeyColumn && !p.IsKeyColumn) {
                currIntOffset = ((currIntOffset + sizeof(ui64) - 1) / sizeof(ui64)) * sizeof(ui64);
            }
            prevKeyColumn = p.IsKeyColumn;
            p.Offset = currIntOffset;
            currIntOffset += p.Bytes;
            if (p.IsKeyColumn) {
                keyIntOffset = currIntOffset;
            }

        } else if (p.IsString) {
            p.Offset = currStrOffset;
            currStrOffset++;
        } else if (p.IsIType) {
            p.Offset = currIOffset;
            currIOffset++;
            NGraceJoin::TColTypeInterface cti{.HashI = MakeHashImpl(p.MKQLType), .EquateI = MakeEquateImpl(p.MKQLType), .Packer = std::make_shared<TValuePacker>(true, p.MKQLType), .HolderFactory = HolderFactory};
            ColumnInterfaces.push_back(cti);
        }
    }

    PackedKeyIntColumnsNum = (keyIntOffset + sizeof(ui64) - 1) / sizeof(ui64) - NullsBitmapSize;
    PackedDataIntColumnsNum = (currIntOffset + sizeof(ui64) - 1) / sizeof(ui64) - PackedKeyIntColumnsNum - NullsBitmapSize;

    NGraceJoin::TColTypeInterface* cti_p = nullptr;

    if (TotalIColumnsNum > 0) {
        cti_p = ColumnInterfaces.data();
    }

    TablePtr = std::make_unique<NGraceJoin::TTable>(
        Logger, LogComponent,
        PackedKeyIntColumnsNum, KeyStrColumnsNum, PackedDataIntColumnsNum,
        DataStrColumnsNum, KeyIColumnsNum, DataIColumnsNum, NullsBitmapSize, cti_p, IsAny);
}

class TGraceJoinSpillingSupportState: public TComputationValue<TGraceJoinSpillingSupportState> {
    using TBase = TComputationValue<TGraceJoinSpillingSupportState>;
    enum class EOperatingMode {
        InMemory,
        Spilling,
        ProcessSpilled
    };

public:
    TGraceJoinSpillingSupportState(TMemoryUsageInfo* memInfo,
                                   IComputationWideFlowNode* flowLeft, IComputationWideFlowNode* flowRight,
                                   EJoinKind joinKind, EAnyJoinSettings anyJoinSettings, const std::vector<ui32>& leftKeyColumns, const std::vector<ui32>& rightKeyColumns,
                                   const std::vector<ui32>& leftRenames, const std::vector<ui32>& rightRenames,
                                   const std::vector<TType*>& leftColumnsTypes, const std::vector<TType*>& rightColumnsTypes, TComputationContext& ctx,
                                   const bool isSelfJoin, bool isSpillingAllowed, NUdf::TLoggerPtr logger, NUdf::TLogComponentId logComponent)
        : TBase(memInfo)
        , FlowLeft_(flowLeft)
        , FlowRight_(flowRight)
        , JoinKind_(joinKind)
        , LeftKeyColumns_(leftKeyColumns)
        , RightKeyColumns_(rightKeyColumns)
        , LeftRenames_(leftRenames)
        , RightRenames_(rightRenames)
        , LeftPacker_(std::make_unique<TGraceJoinPacker>(leftColumnsTypes, leftKeyColumns, ctx.HolderFactory, (anyJoinSettings == EAnyJoinSettings::Left || anyJoinSettings == EAnyJoinSettings::Both || joinKind == EJoinKind::RightSemi || joinKind == EJoinKind::RightOnly), logger, logComponent))
        , RightPacker_(std::make_unique<TGraceJoinPacker>(rightColumnsTypes, rightKeyColumns, ctx.HolderFactory, (anyJoinSettings == EAnyJoinSettings::Right || anyJoinSettings == EAnyJoinSettings::Both || joinKind == EJoinKind::LeftSemi || joinKind == EJoinKind::LeftOnly), logger, logComponent))
        , JoinedTablePtr_(std::make_unique<NGraceJoin::TTable>(logger, logComponent))
        , JoinCompleted_(std::make_unique<bool>(false))
        , PartialJoinCompleted_(std::make_unique<bool>(false))
        , HaveMoreLeftRows_(std::make_unique<bool>(true))
        , HaveMoreRightRows_(std::make_unique<bool>(true))
        , IsSelfJoin_(isSelfJoin)
        , SelfJoinSameKeys_(isSelfJoin && (leftKeyColumns == rightKeyColumns))
        , IsSpillingAllowed_(isSpillingAllowed)
        , Logger_(std::move(logger))
        , LogComponent_(logComponent)
    {
        UDF_LOG(Logger_, LogComponent_, GRACEJOIN_DEBUG, TStringBuilder() << (const void*)&*JoinedTablePtr_ << "# AnyJoinSettings=" << (int)anyJoinSettings << " JoinKind_=" << (int)joinKind);
        if (IsSelfJoin_) {
            LeftPacker_->BatchSize = std::numeric_limits<ui64>::max();
            RightPacker_->BatchSize = std::numeric_limits<ui64>::max();
        }
        if (ctx.CountersProvider) {
            // id will be assigned externally in future versions
            TString id = TString(Operator_Join) + "0";
            CounterOutputRows_ = ctx.CountersProvider->GetCounter(id, Counter_OutputRows, /*deriv=*/false);
        }
    }

    EFetchResult FetchValues(TComputationContext& ctx, NUdf::TUnboxedValue* const* output) {
        while (true) {
            switch (GetMode()) {
                case EOperatingMode::InMemory: {
                    auto r = DoCalculateInMemory(ctx, output);
                    if (GetMode() == EOperatingMode::InMemory) {
                        return r;
                    }
                    break;
                }
                case EOperatingMode::Spilling: {
                    auto r = DoCalculateWithSpilling(ctx, output);
                    if (r == EFetchResult::One) {
                        return r;
                    }
                    if (GetMode() == EOperatingMode::Spilling) {
                        return EFetchResult::Yield;
                    }
                    break;
                }
                case EOperatingMode::ProcessSpilled: {
                    return ProcessSpilledData(ctx, output);
                }
            }
        }
        MKQL_ENSURE(false, "Unreachable");
    }

private:
    bool CanSkipRightOnLeftFinished() const {
        return !IsSelfJoin_ && LeftPacker_->TuplesPacked == 0 && NGraceJoin::ShouldSkipRightIfLeftEmpty(JoinKind_);
    }

    bool CanSkipLeftOnRightFinished() const {
        return !IsSelfJoin_ && RightPacker_->TuplesPacked == 0 && NGraceJoin::ShouldSkipLeftIfRightEmpty(JoinKind_);
    }

    EOperatingMode GetMode() const {
        return Mode_;
    }

    bool HasMemoryForProcessing() const {
        return !TlsAllocState->IsMemoryYellowZoneEnabled();
    }

    bool IsSwitchToSpillingModeCondition() const {
        return !HasMemoryForProcessing();
    }

    void SwitchMode(EOperatingMode mode, TComputationContext& ctx) {
        LogMemoryUsage();
        switch (mode) {
            case EOperatingMode::InMemory: {
                UDF_LOG(Logger_, LogComponent_, NUdf::ELogLevel::Info, TStringBuilder() << (const void*)&*JoinedTablePtr_ << "# switching Memory mode to InMemory");
                MKQL_ENSURE(false, "Internal logic error");
                break;
            }
            case EOperatingMode::Spilling: {
                UDF_LOG(Logger_, LogComponent_, NUdf::ELogLevel::Info, TStringBuilder() << (const void*)&*JoinedTablePtr_ << "# switching Memory mode to Spilling");
                MKQL_ENSURE(EOperatingMode::InMemory == Mode_, "Internal logic error");
                auto spiller = ctx.SpillerFactory->CreateSpiller();
                RightPacker_->TablePtr->InitializeBucketSpillers(spiller);
                LeftPacker_->TablePtr->InitializeBucketSpillers(spiller);
                break;
            }
            case EOperatingMode::ProcessSpilled: {
                UDF_LOG(Logger_, LogComponent_, NUdf::ELogLevel::Info, TStringBuilder() << (const void*)&*JoinedTablePtr_ << "# switching Memory mode to ProcessSpilled");
                SpilledBucketsJoinOrder_.reserve(NGraceJoin::NumberOfBuckets);
                for (ui32 i = 0; i < NGraceJoin::NumberOfBuckets; ++i) {
                    SpilledBucketsJoinOrder_.push_back(i);
                }

                std::sort(SpilledBucketsJoinOrder_.begin(), SpilledBucketsJoinOrder_.end(), [&](ui32 lhs, ui32 rhs) {
                    auto lhs_in_memory = LeftPacker_->TablePtr->IsBucketInMemory(lhs) + RightPacker_->TablePtr->IsBucketInMemory(lhs);
                    auto rhs_in_memory = LeftPacker_->TablePtr->IsBucketInMemory(rhs) + RightPacker_->TablePtr->IsBucketInMemory(rhs);

                    return lhs_in_memory > rhs_in_memory;
                });
                MKQL_ENSURE(EOperatingMode::Spilling == Mode_, "Internal logic error");
                break;
            }
        }
        Mode_ = mode;
    }

    EFetchResult FetchAndPackData(TComputationContext& ctx, NUdf::TUnboxedValue* const* output) {
        const NKikimr::NMiniKQL::EFetchResult resultLeft = FlowLeft_->FetchValues(ctx, LeftPacker_->TuplePtrs.data());
        NKikimr::NMiniKQL::EFetchResult resultRight;

        if (resultLeft == EFetchResult::One) {
            if (LeftPacker_->TuplesPacked == 0) {
                LeftPacker_->StartTime = std::chrono::system_clock::now();
            }
            LeftPacker_->Pack();
            {
                auto added = LeftPacker_->TablePtr->AddTuple(LeftPacker_->TupleIntVals.data(), LeftPacker_->TupleStrings.data(), LeftPacker_->TupleStrSizes.data(), LeftPacker_->IColumnsHolder.data(), *RightPacker_->TablePtr);
                if (added == NGraceJoin::TTable::EAddTupleResult::Added) {
                    ++LeftPacker_->TuplesBatchPacked;
                } else if (added == NGraceJoin::TTable::EAddTupleResult::AnyMatch) {
                    // row dropped
                } else if (JoinKind_ == EJoinKind::Inner || JoinKind_ == EJoinKind::Right || JoinKind_ == EJoinKind::RightSemi || JoinKind_ == EJoinKind::RightOnly || JoinKind_ == EJoinKind::LeftSemi) {
                    // row dropped
                } else { // Left, LeftOnly, Full, Exclusion: output row
                    for (size_t i = 0; i < LeftRenames_.size() / 2; i++) {
                        auto& valPtr = output[LeftRenames_[2 * i + 1]];
                        if (valPtr) {
                            *valPtr = *LeftPacker_->TuplePtrs[LeftRenames_[2 * i]];
                        }
                    }
                    for (size_t i = 0; i < RightRenames_.size() / 2; i++) {
                        auto& valPtr = output[RightRenames_[2 * i + 1]];
                        if (valPtr) {
                            *valPtr = NYql::NUdf::TUnboxedValue();
                        }
                    }
                    CounterOutputRows_.Inc();
                    return EFetchResult::One;
                }
            }
        }

        if (IsSelfJoin_) {
            resultRight = resultLeft;
            if (!SelfJoinSameKeys_) {
                std::copy_n(LeftPacker_->TupleHolder.begin(), LeftPacker_->TotalColumnsNum, RightPacker_->TupleHolder.begin());
            }
        } else {
            resultRight = FlowRight_->FetchValues(ctx, RightPacker_->TuplePtrs.data());
        }

        if (resultRight == EFetchResult::One) {
            if (RightPacker_->TuplesPacked == 0) {
                RightPacker_->StartTime = std::chrono::system_clock::now();
            }

            if (!SelfJoinSameKeys_) {
                RightPacker_->Pack();
                auto added = RightPacker_->TablePtr->AddTuple(RightPacker_->TupleIntVals.data(), RightPacker_->TupleStrings.data(), RightPacker_->TupleStrSizes.data(), RightPacker_->IColumnsHolder.data(), *LeftPacker_->TablePtr);
                if (added == NGraceJoin::TTable::EAddTupleResult::Added) {
                    ++RightPacker_->TuplesBatchPacked;
                } else if (added == NGraceJoin::TTable::EAddTupleResult::AnyMatch) {
                    // row dropped
                } else if (JoinKind_ == EJoinKind::Inner || JoinKind_ == EJoinKind::Left || JoinKind_ == EJoinKind::LeftSemi || JoinKind_ == EJoinKind::LeftOnly || JoinKind_ == EJoinKind::RightSemi) {
                    // row dropped
                } else { // Right, RightOnly, Full, Exclusion: output row
                    for (size_t i = 0; i < LeftRenames_.size() / 2; i++) {
                        auto& valPtr = output[LeftRenames_[2 * i + 1]];
                        if (valPtr) {
                            *valPtr = NYql::NUdf::TUnboxedValue();
                        }
                    }
                    for (size_t i = 0; i < RightRenames_.size() / 2; i++) {
                        auto& valPtr = output[RightRenames_[2 * i + 1]];
                        if (valPtr) {
                            *valPtr = *RightPacker_->TuplePtrs[RightRenames_[2 * i]];
                        }
                    }
                    CounterOutputRows_.Inc();
                    return EFetchResult::One;
                }
            }
        }

        if (resultLeft == EFetchResult::Finish && CanSkipRightOnLeftFinished() ||
            resultRight == EFetchResult::Finish && CanSkipLeftOnRightFinished()) {
            IsEarlyExitDueToEmptyInput_ = true;
            return EFetchResult::Finish;
        }

        if (resultLeft == EFetchResult::Yield || resultRight == EFetchResult::Yield) {
            return EFetchResult::Yield;
        }

        if (resultLeft == EFetchResult::Finish) {
            *HaveMoreLeftRows_ = false;
        }

        if (resultRight == EFetchResult::Finish) {
            *HaveMoreRightRows_ = false;
        }

        return EFetchResult::Finish;
    }

    void UnpackJoinedData(NUdf::TUnboxedValue* const* output) {
        LeftPacker_->UnPack();
        RightPacker_->UnPack();

        auto& valsLeft = LeftPacker_->TupleHolder;
        auto& valsRight = RightPacker_->TupleHolder;

        for (size_t i = 0; i < LeftRenames_.size() / 2; i++) {
            auto& valPtr = output[LeftRenames_[2 * i + 1]];
            if (valPtr) {
                *valPtr = valsLeft[LeftRenames_[2 * i]];
            }
        }

        for (size_t i = 0; i < RightRenames_.size() / 2; i++) {
            auto& valPtr = output[RightRenames_[2 * i + 1]];
            if (valPtr) {
                *valPtr = valsRight[RightRenames_[2 * i]];
            }
        }

        CounterOutputRows_.Inc();
    }

    void LogMemoryUsage() const {
        const auto memoryUsageLogLevel = NUdf::ELogLevel::Info;
        if (!Logger_->IsActive(LogComponent_, memoryUsageLogLevel)) {
            return;
        }

        const auto used = TlsAllocState->GetUsed();
        const auto limit = TlsAllocState->GetLimit();
        TStringBuilder logmsg;
        logmsg << "Memory usage: ";
        if (limit) {
            logmsg << (used * 100 / limit) << "%=";
        }
        logmsg << (used / 1_MB) << "MB/" << (limit / 1_MB) << "MB";

        UDF_LOG(Logger_, LogComponent_, memoryUsageLogLevel, logmsg);
    }

    EFetchResult DoCalculateInMemory(TComputationContext& ctx, NUdf::TUnboxedValue* const* output) {
        // Collecting data for join and perform join (batch or full)
        while (!*JoinCompleted_) {
            if (*PartialJoinCompleted_) {
                // Returns join results (batch or full)
                while (JoinedTablePtr_->NextJoinedData(LeftPacker_->JoinTupleData, RightPacker_->JoinTupleData)) {
                    UnpackJoinedData(output);
                    return EFetchResult::One;
                }

                // Resets batch state for batch join
                if (!*HaveMoreRightRows_) {
                    *PartialJoinCompleted_ = false;
                    LeftPacker_->TuplesBatchPacked = 0;
                    LeftPacker_->TablePtr->Clear(); // Clear table content, ready to collect data for next batch
                    JoinedTablePtr_->Clear();
                    JoinedTablePtr_->ResetIterator();
                }

                if (!*HaveMoreLeftRows_) {
                    *PartialJoinCompleted_ = false;
                    RightPacker_->TuplesBatchPacked = 0;
                    RightPacker_->TablePtr->Clear(); // Clear table content, ready to collect data for next batch
                    JoinedTablePtr_->Clear();
                    JoinedTablePtr_->ResetIterator();
                }
            }

            if (!*HaveMoreRightRows_ && !*HaveMoreLeftRows_) {
                *JoinCompleted_ = true;
                break;
            }

            auto isYield = FetchAndPackData(ctx, output);
            if (IsEarlyExitDueToEmptyInput_) {
                *HaveMoreLeftRows_ = false;
                *HaveMoreRightRows_ = false;
                return EFetchResult::Finish;
            }
            if (isYield == EFetchResult::One) {
                return isYield;
            }
            if (IsSpillingAllowed_ && ctx.SpillerFactory && IsSwitchToSpillingModeCondition()) {
                SwitchMode(EOperatingMode::Spilling, ctx);
                return EFetchResult::Yield;
            }
            if (isYield != EFetchResult::Finish) {
                return isYield;
            }

            if (!*PartialJoinCompleted_ && ((!*HaveMoreRightRows_ && (!*HaveMoreLeftRows_ || LeftPacker_->TuplesBatchPacked >= LeftPacker_->BatchSize)) ||
                                            (!*HaveMoreLeftRows_ && RightPacker_->TuplesBatchPacked >= RightPacker_->BatchSize))) {
                UDF_LOG(Logger_, LogComponent_, GRACEJOIN_TRACE, TStringBuilder() << (const void*)&*JoinedTablePtr_ << '#' << " HaveLeft " << *HaveMoreLeftRows_ << " LeftPacked " << LeftPacker_->TuplesBatchPacked << " LeftBatch " << LeftPacker_->BatchSize << " HaveRight " << *HaveMoreRightRows_ << " RightPacked " << RightPacker_->TuplesBatchPacked << " RightBatch " << RightPacker_->BatchSize);

                auto& leftTable = *LeftPacker_->TablePtr;
                auto& rightTable = SelfJoinSameKeys_ ? *LeftPacker_->TablePtr : *RightPacker_->TablePtr;
                if (IsSpillingAllowed_ && ctx.SpillerFactory && !JoinedTablePtr_->TryToPreallocateMemoryForJoin(leftTable, rightTable, JoinKind_, *HaveMoreLeftRows_, *HaveMoreRightRows_)) {
                    SwitchMode(EOperatingMode::Spilling, ctx);
                    return EFetchResult::Yield;
                }

                *PartialJoinCompleted_ = true;
                LeftPacker_->StartTime = std::chrono::system_clock::now();
                RightPacker_->StartTime = std::chrono::system_clock::now();
                JoinedTablePtr_->Join(leftTable, rightTable, JoinKind_, *HaveMoreLeftRows_, *HaveMoreRightRows_);
                JoinedTablePtr_->ResetIterator();
                LeftPacker_->EndTime = std::chrono::system_clock::now();
                RightPacker_->EndTime = std::chrono::system_clock::now();
            }
        }

        return EFetchResult::Finish;
    }

    bool TryToReduceMemoryAndWait() {
        if (!IsSpillingFinished()) {
            return true;
        }
        i32 largestBucketsPairIndex = 0;
        ui64 largestBucketsPairSize = 0;
        for (ui32 bucket = 0; bucket < NGraceJoin::NumberOfBuckets; ++bucket) {
            ui64 leftBucketSize = LeftPacker_->TablePtr->GetSizeOfBucket(bucket);
            ui64 rightBucketSize = RightPacker_->TablePtr->GetSizeOfBucket(bucket);
            ui64 totalSize = leftBucketSize + rightBucketSize;
            if (totalSize > largestBucketsPairSize) {
                largestBucketsPairSize = totalSize;
                largestBucketsPairIndex = bucket;
            }
        }

        bool isWaitingLeftForReduce = LeftPacker_->TablePtr->TryToReduceMemoryAndWait(largestBucketsPairIndex);
        bool isWaitingRightForReduce = RightPacker_->TablePtr->TryToReduceMemoryAndWait(largestBucketsPairIndex);

        return isWaitingLeftForReduce || isWaitingRightForReduce;
    }

    void UpdateSpilling() {
        LeftPacker_->TablePtr->UpdateSpilling();
        RightPacker_->TablePtr->UpdateSpilling();
    }

    bool IsSpillingFinished() const {
        return LeftPacker_->TablePtr->IsSpillingFinished() && RightPacker_->TablePtr->IsSpillingFinished();
    }

    bool IsReadyForSpilledDataProcessing() const {
        return LeftPacker_->TablePtr->IsSpillingAcceptingDataRequests() && RightPacker_->TablePtr->IsSpillingAcceptingDataRequests();
    }

    bool IsRestoringSpilledBuckets() const {
        return LeftPacker_->TablePtr->IsRestoringSpilledBuckets() || RightPacker_->TablePtr->IsRestoringSpilledBuckets();
    }

    EFetchResult DoCalculateWithSpilling(TComputationContext& ctx, NUdf::TUnboxedValue* const* output) {
        UpdateSpilling();

        ui32 cnt = 0;
        while (*HaveMoreLeftRows_ || *HaveMoreRightRows_) {
            if ((cnt++ % NGraceJoin::SpillingRowLimit) == 0) {
                if (!HasMemoryForProcessing() && !IsSpillingFinalized_) {
                    bool isWaitingForReduce = TryToReduceMemoryAndWait();

                    if (isWaitingForReduce) {
                        return EFetchResult::Yield;
                    }
                }
            }
            auto isYield = FetchAndPackData(ctx, output);
            if (isYield != EFetchResult::Finish) {
                return isYield;
            }
        }

        if (!*HaveMoreLeftRows_ && !*HaveMoreRightRows_) {
            if (!IsSpillingFinished()) {
                return EFetchResult::Yield;
            }
            if (!IsSpillingFinalized_) {
                LeftPacker_->TablePtr->FinalizeSpilling();
                RightPacker_->TablePtr->FinalizeSpilling();
                IsSpillingFinalized_ = true;

                UpdateSpilling();
            }
            if (!IsReadyForSpilledDataProcessing()) {
                return EFetchResult::Yield;
            }

            SwitchMode(EOperatingMode::ProcessSpilled, ctx);
            return EFetchResult::Finish;
        }
        return EFetchResult::Yield;
    }

    EFetchResult ProcessSpilledData(TComputationContext&, NUdf::TUnboxedValue* const* output) {
        while (SpilledBucketsJoinOrderCurrentIndex_ != NGraceJoin::NumberOfBuckets) {
            UpdateSpilling();
            if (IsRestoringSpilledBuckets()) {
                return EFetchResult::Yield;
            }

            ui32 nextBucketToJoin = SpilledBucketsJoinOrder_[SpilledBucketsJoinOrderCurrentIndex_];

            if (LeftPacker_->TablePtr->IsSpilledBucketWaitingForExtraction(nextBucketToJoin)) {
                LeftPacker_->TablePtr->PrepareBucket(nextBucketToJoin);
            }

            if (RightPacker_->TablePtr->IsSpilledBucketWaitingForExtraction(nextBucketToJoin)) {
                RightPacker_->TablePtr->PrepareBucket(nextBucketToJoin);
            }

            if (!LeftPacker_->TablePtr->IsBucketInMemory(nextBucketToJoin)) {
                LeftPacker_->TablePtr->StartLoadingBucket(nextBucketToJoin);
            }

            if (!RightPacker_->TablePtr->IsBucketInMemory(nextBucketToJoin)) {
                RightPacker_->TablePtr->StartLoadingBucket(nextBucketToJoin);
            }

            if (LeftPacker_->TablePtr->IsBucketInMemory(nextBucketToJoin) && RightPacker_->TablePtr->IsBucketInMemory(nextBucketToJoin)) {
                if (*PartialJoinCompleted_) {
                    while (JoinedTablePtr_->NextJoinedData(LeftPacker_->JoinTupleData, RightPacker_->JoinTupleData, nextBucketToJoin + 1)) {
                        UnpackJoinedData(output);
                        return EFetchResult::One;
                    }

                    LeftPacker_->TuplesBatchPacked = 0;
                    LeftPacker_->TablePtr->ClearBucket(nextBucketToJoin); // Clear content of returned bucket
                    LeftPacker_->TablePtr->ShrinkBucket(nextBucketToJoin);

                    RightPacker_->TuplesBatchPacked = 0;
                    RightPacker_->TablePtr->ClearBucket(nextBucketToJoin); // Clear content of returned bucket
                    RightPacker_->TablePtr->ShrinkBucket(nextBucketToJoin);

                    JoinedTablePtr_->Clear();
                    JoinedTablePtr_->ResetIterator();
                    *PartialJoinCompleted_ = false;

                    SpilledBucketsJoinOrderCurrentIndex_++;
                } else {
                    *PartialJoinCompleted_ = true;
                    LeftPacker_->StartTime = std::chrono::system_clock::now();
                    RightPacker_->StartTime = std::chrono::system_clock::now();
                    if (SelfJoinSameKeys_) {
                        JoinedTablePtr_->Join(*LeftPacker_->TablePtr, *LeftPacker_->TablePtr, JoinKind_, *HaveMoreLeftRows_, *HaveMoreRightRows_, nextBucketToJoin, nextBucketToJoin + 1);
                    } else {
                        JoinedTablePtr_->Join(*LeftPacker_->TablePtr, *RightPacker_->TablePtr, JoinKind_, *HaveMoreLeftRows_, *HaveMoreRightRows_, nextBucketToJoin, nextBucketToJoin + 1);
                    }

                    JoinedTablePtr_->ResetIterator();
                    LeftPacker_->EndTime = std::chrono::system_clock::now();
                    RightPacker_->EndTime = std::chrono::system_clock::now();
                }
            }
        }
        return EFetchResult::Finish;
    }

    EOperatingMode Mode_ = EOperatingMode::InMemory;

    IComputationWideFlowNode* const FlowLeft_;
    IComputationWideFlowNode* const FlowRight_;

    const EJoinKind JoinKind_;
    const std::vector<ui32> LeftKeyColumns_;
    const std::vector<ui32> RightKeyColumns_;
    const std::vector<ui32> LeftRenames_;
    const std::vector<ui32> RightRenames_;
    const std::vector<TType*> LeftColumnsTypes_;
    const std::vector<TType*> RightColumnsTypes_;
    const std::unique_ptr<TGraceJoinPacker> LeftPacker_;
    const std::unique_ptr<TGraceJoinPacker> RightPacker_;
    const std::unique_ptr<NGraceJoin::TTable> JoinedTablePtr_;
    const std::unique_ptr<bool> JoinCompleted_;
    const std::unique_ptr<bool> PartialJoinCompleted_;
    const std::unique_ptr<bool> HaveMoreLeftRows_;
    const std::unique_ptr<bool> HaveMoreRightRows_;
    const bool IsSelfJoin_;
    const bool SelfJoinSameKeys_;
    const bool IsSpillingAllowed_;

    bool IsSpillingFinalized_ = false;
    bool IsEarlyExitDueToEmptyInput_ = false;

    NYql::NUdf::TCounter CounterOutputRows_;
    ui32 SpilledBucketsJoinOrderCurrentIndex_ = 0;
    std::vector<ui32> SpilledBucketsJoinOrder_;

    const NUdf::TLoggerPtr Logger_;
    const NUdf::TLogComponentId LogComponent_;
};

class TGraceJoinWrapper: public TStatefulWideFlowCodegeneratorNode<TGraceJoinWrapper> {
    using TBaseComputation = TStatefulWideFlowCodegeneratorNode<TGraceJoinWrapper>;

public:
    TGraceJoinWrapper(TComputationMutables& mutables, IComputationWideFlowNode* flowLeft, IComputationWideFlowNode* flowRight,
                      EJoinKind joinKind, EAnyJoinSettings anyJoinSettings, std::vector<ui32>&& leftKeyColumns, std::vector<ui32>&& rightKeyColumns,
                      std::vector<ui32>&& leftRenames, std::vector<ui32>&& rightRenames,
                      std::vector<TType*>&& leftColumnsTypes, std::vector<TType*>&& rightColumnsTypes,
                      std::vector<EValueRepresentation>&& outputRepresentations, bool isSelfJoin, bool isSpillingAllowed)
        : TBaseComputation(mutables, /*source=*/nullptr, EValueRepresentation::Boxed)
        , FlowLeft_(flowLeft)
        , FlowRight_(flowRight)
        , JoinKind_(joinKind)
        , AnyJoinSettings_(anyJoinSettings)
        , LeftKeyColumns_(std::move(leftKeyColumns))
        , RightKeyColumns_(std::move(rightKeyColumns))
        , LeftRenames_(std::move(leftRenames))
        , RightRenames_(std::move(rightRenames))
        , LeftColumnsTypes_(std::move(leftColumnsTypes))
        , RightColumnsTypes_(std::move(rightColumnsTypes))
        , OutputRepresentations_(std::move(outputRepresentations))
        , IsSelfJoin_(isSelfJoin)
        , IsSpillingAllowed_(isSpillingAllowed)
    {
    }

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue* const* output) const {
        if (state.IsInvalid()) {
            MakeSpillingSupportState(ctx, state);
        }

        return static_cast<TGraceJoinSpillingSupportState*>(state.AsBoxed().Get())->FetchValues(ctx, output);
    }
#ifndef MKQL_DISABLE_CODEGEN
    ICodegeneratorInlineWideNode::TGenerateResult DoGenGetValues(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const override {
        auto& context = ctx.Codegen.GetContext();

        const auto valueType = Type::getInt128Ty(context);
        const auto indexType = Type::getInt32Ty(context);

        const auto arrayType = ArrayType::get(valueType, OutputRepresentations_.size());
        const auto fieldsType = ArrayType::get(PointerType::getUnqual(valueType), OutputRepresentations_.size());

        const auto atTop = &ctx.Func->getEntryBlock().back();

        const auto values = new AllocaInst(arrayType, 0U, "values", atTop);
        const auto fields = new AllocaInst(fieldsType, 0U, "fields", atTop);

        ICodegeneratorInlineWideNode::TGettersList getters(OutputRepresentations_.size());

        Value* initV = UndefValue::get(arrayType);
        Value* initF = UndefValue::get(fieldsType);
        std::vector<Value*> pointers;
        pointers.reserve(getters.size());
        for (auto i = 0U; i < getters.size(); ++i) {
            pointers.emplace_back(GetElementPtrInst::CreateInBounds(arrayType, values, {ConstantInt::get(indexType, 0), ConstantInt::get(indexType, i)}, (TString("ptr_") += ToString(i)).c_str(), atTop));
            initV = InsertValueInst::Create(initV, ConstantInt::get(valueType, 0), {i}, (TString("zero_") += ToString(i)).c_str(), atTop);
            initF = InsertValueInst::Create(initF, pointers.back(), {i}, (TString("insert_") += ToString(i)).c_str(), atTop);

            getters[i] = [i, values, indexType, arrayType, valueType](const TCodegenContext& ctx, BasicBlock*& block) {
                Y_UNUSED(ctx);
                const auto pointer = GetElementPtrInst::CreateInBounds(arrayType, values, {ConstantInt::get(indexType, 0), ConstantInt::get(indexType, i)}, (TString("ptr_") += ToString(i)).c_str(), block);
                return new LoadInst(valueType, pointer, (TString("load_") += ToString(i)).c_str(), block);
            };
        }

        new StoreInst(initV, values, atTop);
        new StoreInst(initF, fields, atTop);

        TLLVMFieldsStructure<TComputationValue<TNull>> fieldsStruct(context);
        const auto stateType = StructType::get(context, fieldsStruct.GetFieldsArray());

        const auto statePtrType = PointerType::getUnqual(stateType);

        const auto make = BasicBlock::Create(context, "make", ctx.Func);
        const auto main = BasicBlock::Create(context, "main", ctx.Func);

        BranchInst::Create(make, main, IsInvalid(statePtr, block, context), block);
        block = make;

        const auto ptrType = PointerType::getUnqual(StructType::get(context));
        const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block);
        EmitFunctionCall<&TGraceJoinWrapper::MakeSpillingSupportState>(Type::getVoidTy(context), {self, ctx.Ctx, statePtr}, ctx, block);
        BranchInst::Create(main, block);

        block = main;

        for (ui32 i = 0U; i < OutputRepresentations_.size(); ++i) {
            ValueCleanup(OutputRepresentations_[i], pointers[i], ctx, block);
        }

        new StoreInst(initV, values, block);

        const auto state = new LoadInst(valueType, statePtr, "state", block);
        const auto half = CastInst::Create(Instruction::Trunc, state, Type::getInt64Ty(context), "half", block);
        const auto stateArg = CastInst::Create(Instruction::IntToPtr, half, statePtrType, "state_arg", block);

        const auto result = EmitFunctionCall<&TGraceJoinSpillingSupportState::FetchValues>(Type::getInt32Ty(context), {stateArg, ctx.Ctx, fields}, ctx, block);

        for (ui32 i = 0U; i < OutputRepresentations_.size(); ++i) {
            ValueRelease(OutputRepresentations_[i], pointers[i], ctx, block);
        }

        return {result, std::move(getters)};
    }
#endif
private:
    void RegisterDependencies() const final {
        FlowDependsOnBoth(FlowLeft_, FlowRight_);
    }

    void MakeSpillingSupportState(TComputationContext& ctx, NUdf::TUnboxedValue& state) const {
        NYql::NUdf::TLoggerPtr logger = ctx.MakeLogger();
        NYql::NUdf::TLogComponentId logComponent = logger->RegisterComponent("GraceJoin");
        UDF_LOG(logger, logComponent, NUdf::ELogLevel::Debug, TStringBuilder() << "State initialized");

        state = ctx.HolderFactory.Create<TGraceJoinSpillingSupportState>(
            FlowLeft_, FlowRight_, JoinKind_, AnyJoinSettings_, LeftKeyColumns_, RightKeyColumns_,
            LeftRenames_, RightRenames_, LeftColumnsTypes_, RightColumnsTypes_,
            ctx, IsSelfJoin_, IsSpillingAllowed_, logger, logComponent);
    }

    IComputationWideFlowNode* const FlowLeft_;
    IComputationWideFlowNode* const FlowRight_;
    const EJoinKind JoinKind_;
    const EAnyJoinSettings AnyJoinSettings_;
    const std::vector<ui32> LeftKeyColumns_;
    const std::vector<ui32> RightKeyColumns_;
    const std::vector<ui32> LeftRenames_;
    const std::vector<ui32> RightRenames_;
    const std::vector<TType*> LeftColumnsTypes_;
    const std::vector<TType*> RightColumnsTypes_;
    const std::vector<EValueRepresentation> OutputRepresentations_;
    const bool IsSelfJoin_;
    const bool IsSpillingAllowed_;
};

} // namespace

IComputationNode* WrapGraceJoinCommon(TCallable& callable, const TComputationNodeFactoryContext& ctx, bool isSelfJoin, bool isSpillingAllowed) {
    const auto leftFlowNodeIndex = 0;
    const auto rightFlowNodeIndex = 1;
    const auto joinKindNodeIndex = isSelfJoin ? 1 : 2;
    const auto leftKeyColumnsNodeIndex = joinKindNodeIndex + 1;
    const auto rightKeyColumnsNodeIndex = leftKeyColumnsNodeIndex + 1;
    const auto leftRenamesNodeIndex = rightKeyColumnsNodeIndex + 1;
    const auto rightRenamesNodeIndex = leftRenamesNodeIndex + 1;
    const auto anyJoinSettingsIndex = rightRenamesNodeIndex + 1;

    const auto leftFlowNode = callable.GetInput(leftFlowNodeIndex);
    const auto joinKindNode = callable.GetInput(joinKindNodeIndex);
    const auto leftKeyColumnsNode = AS_VALUE(TTupleLiteral, callable.GetInput(leftKeyColumnsNodeIndex));
    const auto rightKeyColumnsNode = AS_VALUE(TTupleLiteral, callable.GetInput(rightKeyColumnsNodeIndex));
    const auto leftRenamesNode = AS_VALUE(TTupleLiteral, callable.GetInput(leftRenamesNodeIndex));
    const auto rightRenamesNode = AS_VALUE(TTupleLiteral, callable.GetInput(rightRenamesNodeIndex));
    const EAnyJoinSettings anyJoinSettings = GetAnyJoinSettings(AS_VALUE(TDataLiteral, callable.GetInput(anyJoinSettingsIndex))->AsValue().Get<ui32>());

    const auto leftFlowComponents = GetWideComponents(AS_TYPE(TFlowType, leftFlowNode));
    const ui32 rawJoinKind = AS_VALUE(TDataLiteral, joinKindNode)->AsValue().Get<ui32>();

    const auto flowLeft = dynamic_cast<IComputationWideFlowNode*>(LocateNode(ctx.NodeLocator, callable, 0));
    IComputationWideFlowNode* flowRight = nullptr;
    if (!isSelfJoin) {
        flowRight = dynamic_cast<IComputationWideFlowNode*>(LocateNode(ctx.NodeLocator, callable, 1));
    }

    const auto outputFlowComponents = GetWideComponents(AS_TYPE(TFlowType, callable.GetType()->GetReturnType()));
    std::vector<EValueRepresentation> outputRepresentations;
    outputRepresentations.reserve(outputFlowComponents.size());
    for (auto outputFlowComponent : outputFlowComponents) {
        outputRepresentations.emplace_back(GetValueRepresentation(outputFlowComponent));
    }

    std::vector<ui32> leftKeyColumns;
    std::vector<ui32> leftRenames;
    std::vector<ui32> rightKeyColumns;
    std::vector<ui32> rightRenames;
    std::vector<TType*> leftColumnsTypes(leftFlowComponents.begin(), leftFlowComponents.end());
    std::vector<TType*> rightColumnsTypes;
    if (isSelfJoin) {
        rightColumnsTypes = {leftColumnsTypes};
    } else {
        const auto rightFlowNode = callable.GetInput(rightFlowNodeIndex);
        const auto rightFlowComponents = GetWideComponents(AS_TYPE(TFlowType, rightFlowNode));
        rightColumnsTypes = {rightFlowComponents.begin(), rightFlowComponents.end()};
    }

    leftKeyColumns.reserve(leftKeyColumnsNode->GetValuesCount());
    for (ui32 i = 0; i < leftKeyColumnsNode->GetValuesCount(); ++i) {
        leftKeyColumns.emplace_back(AS_VALUE(TDataLiteral, leftKeyColumnsNode->GetValue(i))->AsValue().Get<ui32>());
    }

    leftRenames.reserve(leftRenamesNode->GetValuesCount());
    for (ui32 i = 0; i < leftRenamesNode->GetValuesCount(); ++i) {
        leftRenames.emplace_back(AS_VALUE(TDataLiteral, leftRenamesNode->GetValue(i))->AsValue().Get<ui32>());
    }

    rightKeyColumns.reserve(rightKeyColumnsNode->GetValuesCount());
    for (ui32 i = 0; i < rightKeyColumnsNode->GetValuesCount(); ++i) {
        rightKeyColumns.emplace_back(AS_VALUE(TDataLiteral, rightKeyColumnsNode->GetValue(i))->AsValue().Get<ui32>());
    }

    if (isSelfJoin) {
        MKQL_ENSURE(leftKeyColumns.size() == rightKeyColumns.size(), "Number of key columns for self join should be equal");
    }

    rightRenames.reserve(rightRenamesNode->GetValuesCount());
    for (ui32 i = 0; i < rightRenamesNode->GetValuesCount(); ++i) {
        rightRenames.emplace_back(AS_VALUE(TDataLiteral, rightRenamesNode->GetValue(i))->AsValue().Get<ui32>());
    }

    return new TGraceJoinWrapper(
        ctx.Mutables, flowLeft, flowRight, GetJoinKind(rawJoinKind), anyJoinSettings,
        std::move(leftKeyColumns), std::move(rightKeyColumns), std::move(leftRenames), std::move(rightRenames),
        std::move(leftColumnsTypes), std::move(rightColumnsTypes), std::move(outputRepresentations), isSelfJoin, isSpillingAllowed);
}

IComputationNode* WrapGraceJoin(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 8, "Expected 8 args");

    return WrapGraceJoinCommon(callable, ctx, /*isSelfJoin=*/false, HasSpillingFlag(callable));
}

IComputationNode* WrapGraceSelfJoin(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 7, "Expected 7 args");

    return WrapGraceJoinCommon(callable, ctx, /*isSelfJoin=*/true, HasSpillingFlag(callable));
}

} // namespace NKikimr::NMiniKQL
