#include "mkql_grace_join.h"
#include "mkql_grace_join_imp.h"

#include <ydb/library/yql/public/udf/udf_data_type.h>
#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/public/decimal/yql_decimal_serialize.h>
#include <ydb/library/yql/minikql/computation/mkql_custom_list.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/comp_nodes/mkql_factories.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/computation/mkql_computation_node_pack.h>
#include <ydb/library/yql/minikql/computation/mkql_llvm_base.h>  // Y_IGNORE

#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>

#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/parser/pg_catalog/catalog.h>

#include <chrono>
#include <limits>

namespace NKikimr {
namespace NMiniKQL {

namespace {

const ui32 PartialJoinBatchSize = 100000; // Number of tuples for one join batch

struct TColumnDataPackInfo {
    ui32 ColumnIdx = 0; // Column index in tuple
    ui32 Bytes = 0; // Size in bytes for fixed size values
    TType* MKQLType; // Data type of the column in term of compute nodes data flows
    NUdf::EDataSlot DataType = NUdf::EDataSlot::Uint32; // Data type of the column for standard types (TDataType)
    TString Name; // Name of the type column
    bool IsKeyColumn = false; // True if this columns is key for join
    bool IsString = false; // True if value is string
    bool IsPgType = false; // True if column is PG type
    bool IsPresortSupported = false; // True if pg type supports presort and can be interpreted as string value
    bool IsIType = false; // True if column need to be processed via IHash, IEquate interfaces
    ui32 Offset = 0; // Offset of column in packed data
//    TValuePacker Packer; // Packer for composite data types
};

struct TGraceJoinPacker {
    ui64 NullsBitmapSize = 0; // Number of ui64 values for nulls bitmap
    ui64 TuplesPacked = 0; // Total number of packed tuples
    ui64 TuplesBatchPacked = 0; // Number of tuples packed during current join batch
    ui64 TuplesUnpacked = 0; // Total number of unpacked tuples
    ui64 BatchSize = PartialJoinBatchSize; // Batch size for partial table packing and join
    std::chrono::time_point<std::chrono::system_clock> StartTime; // Start time of execution
    std::chrono::time_point<std::chrono::system_clock> EndTime; // End time of execution
    std::vector<ui64> TupleIntVals; // Packed value of all fixed length values of table tuple.  Keys columns should be packed first.
    std::vector<ui32> TupleStrSizes; // Sizes of all packed strings
    std::vector<char*> TupleStrings; // All values of tuple strings
    std::vector<TType*>  ColumnTypes; // Types of all columns
    std::vector<std::shared_ptr<TValuePacker>> Packers; // Packers for composite data types
    const THolderFactory& HolderFactory; // To use during unpacking
    std::vector<TColumnDataPackInfo> ColumnsPackInfo; // Information about columns packing
    std::unique_ptr<GraceJoin::TTable> TablePtr;  // Table to pack data
    std::vector<NUdf::TUnboxedValue> TupleHolder; // Storage for tuple data
    std::vector<NUdf::TUnboxedValue*> TuplePtrs; // Storage for tuple data pointers to use in FetchValues
    std::vector<std::string> TupleStringHolder; // Storage for complex tuple data types serialized to strings
    std::vector<NUdf::TUnboxedValue> IColumnsHolder; // Storage for interface-based types (IHash, IEquate)
    GraceJoin::TupleData JoinTupleData; // TupleData to get join results
    ui64 TotalColumnsNum = 0; // Total number of columns to pack
    ui64 TotalIntColumnsNum = 0; // Total number of int columns
    ui64 TotalStrColumnsNum = 0; // Total number of string columns
    ui64 TotalIColumnsNum = 0; // Total number of interface-based columns
    ui64 KeyIntColumnsNum = 0;  // Total number of key int columns in original table
    ui64 PackedKeyIntColumnsNum = 0; // Length of ui64 array containing data of all key int columns after packing
    ui64 KeyStrColumnsNum = 0; // Total number of key string columns
    ui64 KeyIColumnsNum = 0; // Total number of interface-based columns
    ui64 DataIntColumnsNum = TotalIntColumnsNum - KeyIntColumnsNum;
    ui64 PackedDataIntColumnsNum = 0; // Length of ui64 array containing data of all non-key int columns after packing
    ui64 DataStrColumnsNum = TotalStrColumnsNum - KeyStrColumnsNum;
    ui64 DataIColumnsNum = TotalIColumnsNum - KeyIColumnsNum;
    std::vector<GraceJoin::TColTypeInterface> ColumnInterfaces;
    bool IsAny; // Flag to support any join attribute
    inline void Pack() ; // Packs new tuple from TupleHolder and TuplePtrs to TupleIntVals, TupleStrSizes, TupleStrings
    inline void UnPack(); // Unpacks packed values from TupleIntVals, TupleStrSizes, TupleStrings into TupleHolder and TuplePtrs
    TGraceJoinPacker(const std::vector<TType*>& columnTypes, const std::vector<ui32>& keyColumns, const THolderFactory& holderFactory, bool isAny);
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

    if (type->GetKind() == TType::EKind::Pg ) {

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

    const NYql::NUdf::TDataTypeInfo& ti =  GetDataTypeInfo(dataType);
    res.Name = ti.Name;

    switch (dataType){
        case NUdf::EDataSlot::Bool:
            res.Bytes = sizeof(bool); break;
        case NUdf::EDataSlot::Int8:
            res.Bytes = sizeof(i8); break;
        case NUdf::EDataSlot::Uint8:
            res.Bytes = sizeof(ui8); break;
        case NUdf::EDataSlot::Int16:
            res.Bytes = sizeof(i16); break;
        case NUdf::EDataSlot::Uint16:
            res.Bytes = sizeof(ui16); break;
        case NUdf::EDataSlot::Int32:
            res.Bytes = sizeof(i32); break;
        case NUdf::EDataSlot::Uint32:
            res.Bytes = sizeof(ui32); break;
        case NUdf::EDataSlot::Int64:
            res.Bytes = sizeof(i64); break;
        case NUdf::EDataSlot::Uint64:
            res.Bytes = sizeof(ui64); break;
        case NUdf::EDataSlot::Float:
            res.Bytes = sizeof(float); break;
        case NUdf::EDataSlot::Double:
            res.Bytes = sizeof(double); break;
        case NUdf::EDataSlot::Date:
            res.Bytes = sizeof(ui16); break;
        case NUdf::EDataSlot::Datetime:
            res.Bytes = sizeof(ui32); break;
        case NUdf::EDataSlot::Timestamp:
            res.Bytes = sizeof(ui64); break;
        case NUdf::EDataSlot::Interval:
            res.Bytes = sizeof(i64); break;
        case NUdf::EDataSlot::TzDate:
            res.Bytes = 4; break;
        case NUdf::EDataSlot::TzDatetime:
            res.Bytes = 6; break;
        case NUdf::EDataSlot::TzTimestamp:
            res.Bytes = 10; break;
        case NUdf::EDataSlot::Decimal:
            res.Bytes = 16; break;
        case NUdf::EDataSlot::Date32:
            res.Bytes = 4; break;
        case NUdf::EDataSlot::Datetime64:
            res.Bytes = 8; break;
        case NUdf::EDataSlot::Timestamp64:
            res.Bytes = 8; break;
        case NUdf::EDataSlot::Interval64:
            res.Bytes = 8; break;
        case NUdf::EDataSlot::Uuid:
        case NUdf::EDataSlot::DyNumber:
        case NUdf::EDataSlot::JsonDocument:
        case NUdf::EDataSlot::String:
        case NUdf::EDataSlot::Utf8:
        case NUdf::EDataSlot::Yson:
        case NUdf::EDataSlot::Json:
            res.IsString = true; break;
        default:
        {
            MKQL_ENSURE(false, "Unknown data type.");
            res.IsString = true;
        }
    }

    return res;
}

void TGraceJoinPacker::Pack()  {

    TuplesPacked++;
    TuplesBatchPacked++;
    std::fill(TupleIntVals.begin(), TupleIntVals.end(), 0);

    for (ui64 i = 0; i < ColumnsPackInfo.size(); i++) {

        const TColumnDataPackInfo &pi = ColumnsPackInfo[i];
        ui32 offset = pi.Offset;

        NYql::NUdf::TUnboxedValue value = *TuplePtrs[pi.ColumnIdx];
        if (!value) { // Null value
            ui64 currNullsIdx = (i + 1) / (sizeof(ui64) * 8);
            ui64 remShift = ( (i + 1) - currNullsIdx * (sizeof(ui64) * 8) );
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
            if (pi.IsIType ) { // Interface-based type
                IColumnsHolder[offset] = value;
            } else {
                TStringBuf strBuf = Packers[pi.ColumnIdx]->Pack(value);
                TupleStringHolder[i] = strBuf;
                TupleStrings[offset] = TupleStringHolder[i].data();
                TupleStrSizes[offset] = TupleStringHolder[i].size();
            }
            continue;
        }

        char *buffPtr = reinterpret_cast<char *> (TupleIntVals.data()) + offset;
        switch (pi.DataType)
        {
        case NUdf::EDataSlot::Bool:
            WriteUnaligned<bool>(buffPtr, value.Get<bool>()); break;
        case NUdf::EDataSlot::Int8:
            WriteUnaligned<i8>(buffPtr, value.Get<i8>()); break;
        case NUdf::EDataSlot::Uint8:
            WriteUnaligned<ui8>(buffPtr, value.Get<ui8>()); break;
        case NUdf::EDataSlot::Int16:
            WriteUnaligned<i16>(buffPtr, value.Get<i16>()); break;
        case NUdf::EDataSlot::Uint16:
            WriteUnaligned<ui16>(buffPtr, value.Get<ui16>()); break;
        case NUdf::EDataSlot::Int32:
            WriteUnaligned<i32>(buffPtr,  value.Get<i32>()); break;
        case NUdf::EDataSlot::Uint32:
            WriteUnaligned<ui32>(buffPtr, value.Get<ui32>()); break;
        case NUdf::EDataSlot::Int64:
            WriteUnaligned<i64>(buffPtr, value.Get<i64>()); break;
        case NUdf::EDataSlot::Uint64:
            WriteUnaligned<ui64>(buffPtr, value.Get<ui64>()); break;
        case NUdf::EDataSlot::Float:
            WriteUnaligned<float>(buffPtr, value.Get<float>()); break;
        case NUdf::EDataSlot::Double:
            WriteUnaligned<double>(buffPtr, value.Get<double>()); break;
        case NUdf::EDataSlot::Date:
            WriteUnaligned<ui16>(buffPtr, value.Get<ui16>()); break;
        case NUdf::EDataSlot::Datetime:
            WriteUnaligned<ui32>(buffPtr, value.Get<ui32>()); break;
        case NUdf::EDataSlot::Timestamp:
            WriteUnaligned<ui64>(buffPtr, value.Get<ui64>()); break;
        case NUdf::EDataSlot::Interval:
            WriteUnaligned<i64>(buffPtr, value.Get<i64>()); break;
        case NUdf::EDataSlot::Date32:
            WriteUnaligned<i32>(buffPtr, value.Get<i32>()); break;
        case NUdf::EDataSlot::Datetime64:
            WriteUnaligned<i64>(buffPtr, value.Get<i64>()); break;
        case NUdf::EDataSlot::Timestamp64:
            WriteUnaligned<i64>(buffPtr, value.Get<i64>()); break;
        case NUdf::EDataSlot::Interval64:
            WriteUnaligned<i64>(buffPtr, value.Get<i64>()); break;

        case NUdf::EDataSlot::TzDate:
        {
            WriteUnaligned<ui16>(buffPtr, value.Get<ui16>());
            WriteUnaligned<ui16>(buffPtr + sizeof(ui16), value.GetTimezoneId());
            break;
        }
        case NUdf::EDataSlot::TzDatetime:
        {
            WriteUnaligned<ui32>(buffPtr, value.Get<ui32>());
            WriteUnaligned<ui16>(buffPtr + sizeof(ui32), value.GetTimezoneId());
            break;
        }
        case NUdf::EDataSlot::TzTimestamp:
        {
            WriteUnaligned<ui64>(buffPtr, value.Get<ui64>());
            WriteUnaligned<ui16>(buffPtr + sizeof(ui64), value.GetTimezoneId());
            break;
        }
        case NUdf::EDataSlot::Decimal:
        {
            NYql::NDecimal::Serialize(value.GetInt128(), buffPtr);
            break;
        }
        default:
        {

            auto str = TuplePtrs[pi.ColumnIdx]->AsStringRef();
            TupleStrings[offset] = str.Data();
            TupleStrSizes[offset] = str.Size();
        }

        }
    }
}

void TGraceJoinPacker::UnPack()  {
    TuplesUnpacked++;
    for (ui64 i = 0; i < ColumnsPackInfo.size(); i++) {
        const TColumnDataPackInfo &pi = ColumnsPackInfo[i];
        ui32 offset = pi.Offset;
        NYql::NUdf::TUnboxedValue & value = *TuplePtrs[pi.ColumnIdx];
        if (JoinTupleData.AllNulls) {
            value = NYql::NUdf::TUnboxedValue();
            continue;
        }
        ui64 currNullsIdx = (i + 1) / (sizeof(ui64) * 8);
        ui64 remShift = ( (i + 1) - currNullsIdx * (sizeof(ui64) * 8) );
        ui64 bitMask = ui64(0x1) << remShift;
        if ( TupleIntVals[currNullsIdx] & bitMask ) {
            value = NYql::NUdf::TUnboxedValue();
            continue;
        }

        TType * type = pi.MKQLType;

        TType * colType;
        if (type->IsOptional()) {
            colType = AS_TYPE(TOptionalType, type)->GetItemType();
        } else {
            colType = type;
        }

        if (colType->GetKind() != TType::EKind::Data) {
            if (colType->GetKind() == TType::EKind::Pg) {
                if ( pi.IsIType ) { // Interface-based type
                    value = IColumnsHolder[offset];
                    continue;
                }
            }
            value = Packers[pi.ColumnIdx]->Unpack(TStringBuf(TupleStrings[offset], TupleStrSizes[offset]), HolderFactory);
            continue;
        }

        char *buffPtr = reinterpret_cast<char *> (TupleIntVals.data()) + offset;
        switch (pi.DataType)
        {
        case NUdf::EDataSlot::Bool:
            value = NUdf::TUnboxedValuePod(ReadUnaligned<bool>(buffPtr)); break;
        case NUdf::EDataSlot::Int8:
            value = NUdf::TUnboxedValuePod(ReadUnaligned<i8>(buffPtr)); break;
        case NUdf::EDataSlot::Uint8:
            value = NUdf::TUnboxedValuePod(ReadUnaligned<ui8>(buffPtr)); break;
        case NUdf::EDataSlot::Int16:
            value = NUdf::TUnboxedValuePod(ReadUnaligned<i16>(buffPtr)); break;
        case NUdf::EDataSlot::Uint16:
            value = NUdf::TUnboxedValuePod(ReadUnaligned<ui16>(buffPtr)); break;
        case NUdf::EDataSlot::Int32:
            value = NUdf::TUnboxedValuePod(ReadUnaligned<i32>(buffPtr)); break;
        case NUdf::EDataSlot::Uint32:
            value = NUdf::TUnboxedValuePod(ReadUnaligned<ui32>(buffPtr)); break;
        case NUdf::EDataSlot::Int64:
            value = NUdf::TUnboxedValuePod(ReadUnaligned<i64>(buffPtr)); break;
        case NUdf::EDataSlot::Uint64:
            value = NUdf::TUnboxedValuePod(ReadUnaligned<ui64>(buffPtr)); break;
        case NUdf::EDataSlot::Float:
            value = NUdf::TUnboxedValuePod(ReadUnaligned<float>(buffPtr)); break;
        case NUdf::EDataSlot::Double:
            value = NUdf::TUnboxedValuePod(ReadUnaligned<double>(buffPtr)); break;
        case NUdf::EDataSlot::Date:
            value = NUdf::TUnboxedValuePod(ReadUnaligned<ui16>(buffPtr)); break;
        case NUdf::EDataSlot::Datetime:
            value = NUdf::TUnboxedValuePod(ReadUnaligned<ui32>(buffPtr)); break;
        case NUdf::EDataSlot::Timestamp:
            value = NUdf::TUnboxedValuePod(ReadUnaligned<ui64>(buffPtr)); break;
        case NUdf::EDataSlot::Interval:
            value = NUdf::TUnboxedValuePod(ReadUnaligned<i64>(buffPtr)); break;
        case NUdf::EDataSlot::Date32:
            value = NUdf::TUnboxedValuePod(ReadUnaligned<i32>(buffPtr)); break;
        case NUdf::EDataSlot::Datetime64:
            value = NUdf::TUnboxedValuePod(ReadUnaligned<i64>(buffPtr)); break;
        case NUdf::EDataSlot::Timestamp64:
            value = NUdf::TUnboxedValuePod(ReadUnaligned<i64>(buffPtr)); break;
        case NUdf::EDataSlot::Interval64:
            value = NUdf::TUnboxedValuePod(ReadUnaligned<i64>(buffPtr)); break;
        case NUdf::EDataSlot::TzDate:
        {
            value = NUdf::TUnboxedValuePod(ReadUnaligned<ui16>(buffPtr));
            value.SetTimezoneId(ReadUnaligned<ui16>(buffPtr + sizeof(ui16))) ;
            break;
        }
        case NUdf::EDataSlot::TzDatetime:
        {
            value = NUdf::TUnboxedValuePod(ReadUnaligned<ui32>(buffPtr));
            value.SetTimezoneId(ReadUnaligned<ui16>(buffPtr + sizeof(ui32)));
            break;
        }
        case NUdf::EDataSlot::TzTimestamp:
        {
            value = NUdf::TUnboxedValuePod(ReadUnaligned<ui64>(buffPtr));
            value.SetTimezoneId(ReadUnaligned<ui16>(buffPtr + sizeof(ui64))) ;
            break;
        }
        case NUdf::EDataSlot::Decimal:
        {
            const auto des = NYql::NDecimal::Deserialize(buffPtr, sizeof(NYql::NDecimal::TInt128));
            MKQL_ENSURE(!NYql::NDecimal::IsError(des.first), "Bad packed data: invalid decimal.");
            value = NUdf::TUnboxedValuePod(des.first);
            break;
        }
        default:
        {
            value = MakeString(NUdf::TStringRef(TupleStrings[offset], TupleStrSizes[offset]));
        }

        }

    }

}


TGraceJoinPacker::TGraceJoinPacker(const std::vector<TType *> & columnTypes, const std::vector<ui32>& keyColumns, const THolderFactory& holderFactory, bool isAny) :
                                    ColumnTypes(columnTypes)
                                    , HolderFactory(holderFactory)
                                    , IsAny(isAny) {

    ui64 nColumns = ColumnTypes.size();
    ui64 nKeyColumns = keyColumns.size();

    for (ui32 i = 0; i < keyColumns.size(); i++ ) {
        auto colType = columnTypes[keyColumns[i]];
        auto packInfo = GetPackInfo(colType);
        packInfo.ColumnIdx = keyColumns[i];
        packInfo.IsKeyColumn = true;
        ColumnsPackInfo.push_back(packInfo);
    }



    for ( ui32 i = 0; i < columnTypes.size(); i++  ) {

        auto colType = columnTypes[i];
        auto packInfo = GetPackInfo(colType);
        packInfo.ColumnIdx = i;


        ui32 keyColNums = std::count_if(keyColumns.begin(), keyColumns.end(), [&](ui32 k) {return k == i;});

        Packers.push_back(std::make_shared<TValuePacker>(true,colType));
        if (keyColNums == 0) {
            ColumnsPackInfo.push_back(packInfo);
        }
     }


    nColumns = ColumnsPackInfo.size();

    ui64 totalIntColumnsNum = std::count_if(ColumnsPackInfo.begin(), ColumnsPackInfo.end(), [](TColumnDataPackInfo a) { return !a.IsString && !a.IsPgType; });
    ui64 totalIColumnsNum = std::count_if(ColumnsPackInfo.begin(), ColumnsPackInfo.end(), [](TColumnDataPackInfo a) { return a.IsIType; });
    ui64 totalStrColumnsNum = nColumns - totalIntColumnsNum - totalIColumnsNum;

    ui64 keyIntColumnsNum = std::count_if(ColumnsPackInfo.begin(), ColumnsPackInfo.end(), [](TColumnDataPackInfo a) { return (a.IsKeyColumn && !a.IsString && !a.IsPgType);});
    ui64 keyIColumnsNum = std::count_if(ColumnsPackInfo.begin(), ColumnsPackInfo.end(), [](TColumnDataPackInfo a) { return (a.IsKeyColumn && a.IsIType);});
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

    NullsBitmapSize = ( (nColumns + 1)/ (8 * sizeof(ui64)) + 1) ;

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

    ui32 currIntOffset = NullsBitmapSize * sizeof(ui64) ;
    ui32 currStrOffset = 0;
    ui32 currIOffset = 0;
    std::vector<GraceJoin::TColTypeInterface> ctiv;

    bool prevKeyColumn = false;

    ui32 keyIntOffset = currIntOffset;

    for( auto & p: ColumnsPackInfo ) {
        if ( !p.IsString && !p.IsIType ) {
            if (prevKeyColumn && !p.IsKeyColumn) {
                currIntOffset = ( (currIntOffset + sizeof(ui64) - 1) / sizeof(ui64) ) * sizeof(ui64);

            }
            prevKeyColumn = p.IsKeyColumn;
            p.Offset = currIntOffset;
            currIntOffset += p.Bytes;
            if (p.IsKeyColumn) {
                keyIntOffset = currIntOffset;
            }

        } else if ( p.IsString ) {
            p.Offset = currStrOffset;
            currStrOffset++;
        } else if (p.IsIType) {
            p.Offset = currIOffset;
            currIOffset++;
            GraceJoin::TColTypeInterface cti{ MakeHashImpl(p.MKQLType), MakeEquateImpl(p.MKQLType), std::make_shared<TValuePacker>(true, p.MKQLType) , HolderFactory  };
            ColumnInterfaces.push_back(cti);
        }
    }

    PackedKeyIntColumnsNum =  (keyIntOffset + sizeof(ui64) - 1 ) / sizeof(ui64) - NullsBitmapSize;
    PackedDataIntColumnsNum = (currIntOffset + sizeof(ui64) - 1) / sizeof(ui64) - PackedKeyIntColumnsNum - NullsBitmapSize;

    GraceJoin::TColTypeInterface * cti_p = nullptr;

    if (TotalIColumnsNum > 0 ) {
        cti_p = ColumnInterfaces.data();
    }

    TablePtr = std::make_unique<GraceJoin::TTable>(
        PackedKeyIntColumnsNum, KeyStrColumnsNum, PackedDataIntColumnsNum,
        DataStrColumnsNum, KeyIColumnsNum, DataIColumnsNum, NullsBitmapSize, cti_p, IsAny );

}

class TGraceJoinSpillingSupportState : public TComputationValue<TGraceJoinSpillingSupportState> {
    using TBase = TComputationValue<TGraceJoinSpillingSupportState>;
    enum class EOperatingMode {
        InMemory,
        Spilling,
        ProcessSpilled
    };
public:

    TGraceJoinSpillingSupportState(TMemoryUsageInfo* memInfo,
        IComputationWideFlowNode* flowLeft, IComputationWideFlowNode* flowRight,
        EJoinKind joinKind,  EAnyJoinSettings anyJoinSettings, const std::vector<ui32>& leftKeyColumns, const std::vector<ui32>& rightKeyColumns,
        const std::vector<ui32>& leftRenames, const std::vector<ui32>& rightRenames,
        const std::vector<TType*>& leftColumnsTypes, const std::vector<TType*>& rightColumnsTypes, const THolderFactory & holderFactory,
        const bool isSelfJoin)
    :  TBase(memInfo)
    ,   FlowLeft(flowLeft)
    ,   FlowRight(flowRight)
    ,   JoinKind(joinKind)
    ,   LeftKeyColumns(leftKeyColumns)
    ,   RightKeyColumns(rightKeyColumns)
    ,   LeftRenames(leftRenames)
    ,   RightRenames(rightRenames)
    ,   LeftPacker(std::make_unique<TGraceJoinPacker>(leftColumnsTypes, leftKeyColumns, holderFactory, (anyJoinSettings == EAnyJoinSettings::Left || anyJoinSettings == EAnyJoinSettings::Both)))
    ,   RightPacker(std::make_unique<TGraceJoinPacker>(rightColumnsTypes, rightKeyColumns, holderFactory, (anyJoinSettings == EAnyJoinSettings::Right || anyJoinSettings == EAnyJoinSettings::Both)))
    ,   JoinedTablePtr(std::make_unique<GraceJoin::TTable>())
    ,   JoinCompleted(std::make_unique<bool>(false))
    ,   PartialJoinCompleted(std::make_unique<bool>(false))
    ,   HaveMoreLeftRows(std::make_unique<bool>(true))
    ,   HaveMoreRightRows(std::make_unique<bool>(true))
    ,   JoinedTuple(std::make_unique<std::vector<NUdf::TUnboxedValue*>>() )
    ,   IsSelfJoin_(isSelfJoin)
    ,   SelfJoinSameKeys_(isSelfJoin && (leftKeyColumns == rightKeyColumns))
    {
        if (JoinKind == EJoinKind::Full || JoinKind == EJoinKind::Exclusion || IsSelfJoin_) {
            LeftPacker->BatchSize = std::numeric_limits<ui64>::max();
            RightPacker->BatchSize = std::numeric_limits<ui64>::max();
        }
    }

    EFetchResult FetchValues(TComputationContext& ctx, NUdf::TUnboxedValue*const* output) {
        while (true) {
            switch(GetMode()) {
                case EOperatingMode::InMemory: {
                    auto r = DoCalculateInMemory(ctx, output);
                    if (GetMode() == EOperatingMode::InMemory) {
                        return r;
                    }
                    break;
                }
                case EOperatingMode::Spilling: {
                    DoCalculateWithSpilling(ctx);
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
        Y_UNREACHABLE();
    }

private:
    EOperatingMode GetMode() const {
        return Mode;
    }

    bool HasMemoryForProcessing() const {
        return !TlsAllocState->IsMemoryYellowZoneEnabled();
    }

    bool IsSwitchToSpillingModeCondition() const {
        return false;
        // TODO: YQL-18033
        // return !HasMemoryForProcessing();
    }


    void SwitchMode(EOperatingMode mode, TComputationContext& ctx) {
        switch(mode) {
            case EOperatingMode::InMemory: {
                MKQL_ENSURE(false, "Internal logic error");
                break;
            }
            case EOperatingMode::Spilling: {
                MKQL_ENSURE(EOperatingMode::InMemory == Mode, "Internal logic error");
                auto spiller = ctx.SpillerFactory->CreateSpiller();
                RightPacker->TablePtr->InitializeBucketSpillers(spiller);
                LeftPacker->TablePtr->InitializeBucketSpillers(spiller);
                break;
            }
            case EOperatingMode::ProcessSpilled: {
                MKQL_ENSURE(EOperatingMode::Spilling == Mode, "Internal logic error");
                break;
            }

        }
        Mode = mode;
    }

    bool FetchAndPackData(TComputationContext& ctx) {
        const NKikimr::NMiniKQL::EFetchResult resultLeft = FlowLeft->FetchValues(ctx, LeftPacker->TuplePtrs.data());
        NKikimr::NMiniKQL::EFetchResult resultRight;

        if (IsSelfJoin_) {
            resultRight = resultLeft;
            if (!SelfJoinSameKeys_) {
                std::copy_n(LeftPacker->TupleHolder.begin(), LeftPacker->TotalColumnsNum, RightPacker->TupleHolder.begin());
            }
        } else {
            resultRight = FlowRight->FetchValues(ctx, RightPacker->TuplePtrs.data());
        }

        if (resultLeft == EFetchResult::One) {
            if (LeftPacker->TuplesPacked == 0) {
                LeftPacker->StartTime = std::chrono::system_clock::now();
            }
            LeftPacker->Pack();
            LeftPacker->TablePtr->AddTuple(LeftPacker->TupleIntVals.data(), LeftPacker->TupleStrings.data(), LeftPacker->TupleStrSizes.data(), LeftPacker->IColumnsHolder.data());
        }

        if (resultRight == EFetchResult::One) {
            if (RightPacker->TuplesPacked == 0) {
                RightPacker->StartTime = std::chrono::system_clock::now();
            }

            if ( !SelfJoinSameKeys_ ) {
                RightPacker->Pack();
                RightPacker->TablePtr->AddTuple(RightPacker->TupleIntVals.data(), RightPacker->TupleStrings.data(), RightPacker->TupleStrSizes.data(), RightPacker->IColumnsHolder.data());
            }
        }

        if (resultLeft == EFetchResult::Finish ) {
            *HaveMoreLeftRows = false;
        }


        if (resultRight == EFetchResult::Finish ) {
            *HaveMoreRightRows = false;
        }

        if ((resultLeft == EFetchResult::Yield && (!*HaveMoreRightRows || resultRight == EFetchResult::Yield)) ||
            (resultRight == EFetchResult::Yield && !*HaveMoreLeftRows))
        {
            return true;
        }
        return false;
    }

    void UnpackJoinedData(NUdf::TUnboxedValue*const* output) {
        LeftPacker->UnPack();
        RightPacker->UnPack();

        auto &valsLeft = LeftPacker->TupleHolder;
        auto &valsRight = RightPacker->TupleHolder;

        for (size_t i = 0; i < LeftRenames.size() / 2; i++) {
            auto & valPtr = output[LeftRenames[2 * i + 1]];
            if ( valPtr ) {
                *valPtr = valsLeft[LeftRenames[2 * i]];
            }
        }

        for (size_t i = 0; i < RightRenames.size() / 2; i++) {
            auto & valPtr = output[RightRenames[2 * i + 1]];
            if ( valPtr ) {
                *valPtr = valsRight[RightRenames[2 * i]];
            }
        }
    }

    EFetchResult DoCalculateInMemory(TComputationContext& ctx, NUdf::TUnboxedValue*const* output) {
        // Collecting data for join and perform join (batch or full)
        while (!*JoinCompleted ) {

            if ( *PartialJoinCompleted) {
                // Returns join results (batch or full)
                while (JoinedTablePtr->NextJoinedData(LeftPacker->JoinTupleData, RightPacker->JoinTupleData)) {
                    UnpackJoinedData(output);
                    return EFetchResult::One;
                }

                // Resets batch state for batch join
                if (!*HaveMoreRightRows) {
                    *PartialJoinCompleted = false;
                    LeftPacker->TuplesBatchPacked = 0;
                    LeftPacker->TablePtr->Clear(); // Clear table content, ready to collect data for next batch
                    JoinedTablePtr->Clear();
                    JoinedTablePtr->ResetIterator();
                }


                if (!*HaveMoreLeftRows ) {
                    *PartialJoinCompleted = false;
                    RightPacker->TuplesBatchPacked = 0;
                    RightPacker->TablePtr->Clear(); // Clear table content, ready to collect data for next batch
                    JoinedTablePtr->Clear();
                    JoinedTablePtr->ResetIterator();
                }
            }

            if (!*HaveMoreRightRows && !*HaveMoreLeftRows) {
                *JoinCompleted = true;
                break;
            }

            bool isYield = FetchAndPackData(ctx);
            if (ctx.SpillerFactory && IsSwitchToSpillingModeCondition()) {
                SwitchMode(EOperatingMode::Spilling, ctx);
                return EFetchResult::Yield;
            }
            if (isYield) return EFetchResult::Yield;

            if (!*HaveMoreRightRows && !*PartialJoinCompleted && LeftPacker->TuplesBatchPacked >= LeftPacker->BatchSize ) {
                *PartialJoinCompleted = true;
                JoinedTablePtr->Join(*LeftPacker->TablePtr, *RightPacker->TablePtr, JoinKind, *HaveMoreLeftRows, *HaveMoreRightRows);
                JoinedTablePtr->ResetIterator();
            }

            if (!*HaveMoreLeftRows && !*PartialJoinCompleted && RightPacker->TuplesBatchPacked >= RightPacker->BatchSize ) {
                *PartialJoinCompleted = true;
                JoinedTablePtr->Join(*LeftPacker->TablePtr, *RightPacker->TablePtr, JoinKind, *HaveMoreLeftRows, *HaveMoreRightRows);
                JoinedTablePtr->ResetIterator();

            }

            if (!*HaveMoreRightRows && !*HaveMoreLeftRows && !*PartialJoinCompleted) {
                *PartialJoinCompleted = true;
                LeftPacker->StartTime = std::chrono::system_clock::now();
                RightPacker->StartTime = std::chrono::system_clock::now();
                if ( SelfJoinSameKeys_ ) {
                    JoinedTablePtr->Join(*LeftPacker->TablePtr, *LeftPacker->TablePtr, JoinKind, *HaveMoreLeftRows, *HaveMoreRightRows);
                } else {
                    JoinedTablePtr->Join(*LeftPacker->TablePtr, *RightPacker->TablePtr, JoinKind, *HaveMoreLeftRows, *HaveMoreRightRows);
                }
                JoinedTablePtr->ResetIterator();
                LeftPacker->EndTime = std::chrono::system_clock::now();
                RightPacker->EndTime = std::chrono::system_clock::now();
            }

        }

        return EFetchResult::Finish;
    }

    bool TryToReduceMemoryAndWait() {
        bool isWaitingLeftForReduce = LeftPacker->TablePtr->TryToReduceMemoryAndWait();
        bool isWaitingRightForReduce = RightPacker->TablePtr->TryToReduceMemoryAndWait();

        return isWaitingLeftForReduce || isWaitingRightForReduce;
    }

    void UpdateSpilling() {
        LeftPacker->TablePtr->UpdateSpilling();
        RightPacker->TablePtr->UpdateSpilling();
    }

    bool HasRunningAsyncOperation() const {
        return LeftPacker->TablePtr->HasRunningAsyncIoOperation() || RightPacker->TablePtr->HasRunningAsyncIoOperation();
    }

    bool IsProcessingFinished() {
        return LeftPacker->TablePtr->IsProcessingFinished() || RightPacker->TablePtr->IsProcessingFinished();
    }

void DoCalculateWithSpilling(TComputationContext& ctx) {
    UpdateSpilling();

    if (!HasMemoryForProcessing()) {
        bool isWaitingForReduce = TryToReduceMemoryAndWait();
        if (isWaitingForReduce) return;
    }

    while (*HaveMoreLeftRows || *HaveMoreRightRows) {
        bool isYield = FetchAndPackData(ctx);
        if (isYield) return;
    }

    if (!*HaveMoreLeftRows && !*HaveMoreRightRows) {
        UpdateSpilling();
        if (HasRunningAsyncOperation() || !IsProcessingFinished()) return;
        if (!IsSpillingFinalized) {
            LeftPacker->TablePtr->FinalizeSpilling();
            RightPacker->TablePtr->FinalizeSpilling();
            IsSpillingFinalized = true;

            if (HasRunningAsyncOperation()) return;
        }
        SwitchMode(EOperatingMode::ProcessSpilled, ctx);
        return;
    }
}

EFetchResult ProcessSpilledData(TComputationContext&, NUdf::TUnboxedValue*const* output) {
    while (NextBucketToJoin != GraceJoin::NumberOfBuckets) {
        UpdateSpilling();

        if (HasRunningAsyncOperation()) return EFetchResult::Yield;

        if (!LeftPacker->TablePtr->IsBucketInMemory(NextBucketToJoin)) {
            LeftPacker->TablePtr->StartLoadingBucket(NextBucketToJoin);
        }

        if (!RightPacker->TablePtr->IsBucketInMemory(NextBucketToJoin)) {
            RightPacker->TablePtr->StartLoadingBucket(NextBucketToJoin);
        } 

        if (LeftPacker->TablePtr->IsBucketInMemory(NextBucketToJoin) && RightPacker->TablePtr->IsBucketInMemory(NextBucketToJoin)) {
            if (*PartialJoinCompleted) {
                while (JoinedTablePtr->NextJoinedData(LeftPacker->JoinTupleData, RightPacker->JoinTupleData)) {
                    UnpackJoinedData(output);

                    return EFetchResult::One;
                }

                LeftPacker->TuplesBatchPacked = 0;
                LeftPacker->TablePtr->ClearBucket(NextBucketToJoin); // Clear content of returned bucket

                RightPacker->TuplesBatchPacked = 0;
                RightPacker->TablePtr->ClearBucket(NextBucketToJoin); // Clear content of returned bucket

                JoinedTablePtr->Clear();
                JoinedTablePtr->ResetIterator();
                *PartialJoinCompleted = false;

                NextBucketToJoin++;
            } else {
                LeftPacker->TablePtr->PrepareBucket(NextBucketToJoin);
                RightPacker->TablePtr->PrepareBucket(NextBucketToJoin);
                *PartialJoinCompleted = true;
                LeftPacker->StartTime = std::chrono::system_clock::now();
                RightPacker->StartTime = std::chrono::system_clock::now();
                if ( SelfJoinSameKeys_ ) {
                    JoinedTablePtr->Join(*LeftPacker->TablePtr, *LeftPacker->TablePtr, JoinKind, *HaveMoreLeftRows, *HaveMoreRightRows, NextBucketToJoin, NextBucketToJoin+1);
                } else {
                    JoinedTablePtr->Join(*LeftPacker->TablePtr, *RightPacker->TablePtr, JoinKind, *HaveMoreLeftRows, *HaveMoreRightRows, NextBucketToJoin, NextBucketToJoin+1);
                }
                
                JoinedTablePtr->ResetIterator();
                LeftPacker->EndTime = std::chrono::system_clock::now();
                RightPacker->EndTime = std::chrono::system_clock::now();
            }

        }
    }
    return EFetchResult::Finish;
}

private:
    EOperatingMode Mode = EOperatingMode::InMemory;

    IComputationWideFlowNode* const FlowLeft;
    IComputationWideFlowNode* const FlowRight;

    const EJoinKind JoinKind;
    const std::vector<ui32> LeftKeyColumns;
    const std::vector<ui32> RightKeyColumns;
    const std::vector<ui32> LeftRenames;
    const std::vector<ui32> RightRenames;
    const std::vector<TType *> LeftColumnsTypes;
    const std::vector<TType *> RightColumnsTypes;
    const std::unique_ptr<TGraceJoinPacker> LeftPacker;
    const std::unique_ptr<TGraceJoinPacker> RightPacker;
    const std::unique_ptr<GraceJoin::TTable> JoinedTablePtr;
    const std::unique_ptr<bool> JoinCompleted;
    const std::unique_ptr<bool> PartialJoinCompleted;
    const std::unique_ptr<bool> HaveMoreLeftRows;
    const std::unique_ptr<bool> HaveMoreRightRows;
    const std::unique_ptr<std::vector<NUdf::TUnboxedValue*>> JoinedTuple;
    const bool IsSelfJoin_;
    const bool SelfJoinSameKeys_;

    bool IsSpillingFinalized = false;

    ui32 NextBucketToJoin = 0;
};

class TGraceJoinState : public TComputationValue<TGraceJoinState> {
using TBase = TComputationValue<TGraceJoinState>;
public:
    EFetchResult FetchValues(TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const;

    TGraceJoinState(TMemoryUsageInfo* memInfo,
        IComputationWideFlowNode* flowLeft, IComputationWideFlowNode* flowRight,
        EJoinKind joinKind,  EAnyJoinSettings anyJoinSettings, const std::vector<ui32>& leftKeyColumns, const std::vector<ui32>& rightKeyColumns,
        const std::vector<ui32>& leftRenames, const std::vector<ui32>& rightRenames,
        const std::vector<TType*>& leftColumnsTypes, const std::vector<TType*>& rightColumnsTypes, const THolderFactory & holderFactory,
        const bool isSelfJoin)
    :  TBase(memInfo)
    ,   FlowLeft(flowLeft)
    ,   FlowRight(flowRight)
    ,   JoinKind(joinKind)
    ,   LeftKeyColumns(leftKeyColumns)
    ,   RightKeyColumns(rightKeyColumns)
    ,   LeftRenames(leftRenames)
    ,   RightRenames(rightRenames)    ,   LeftPacker(std::make_unique<TGraceJoinPacker>(leftColumnsTypes, leftKeyColumns, holderFactory, (anyJoinSettings == EAnyJoinSettings::Left || anyJoinSettings == EAnyJoinSettings::Both)))
    ,   RightPacker(std::make_unique<TGraceJoinPacker>(rightColumnsTypes, rightKeyColumns, holderFactory, (anyJoinSettings == EAnyJoinSettings::Right || anyJoinSettings == EAnyJoinSettings::Both)))
    ,   JoinedTablePtr(std::make_unique<GraceJoin::TTable>())
    ,   JoinCompleted(std::make_unique<bool>(false))
    ,   PartialJoinCompleted(std::make_unique<bool>(false))
    ,   HaveMoreLeftRows(std::make_unique<bool>(true))
    ,   HaveMoreRightRows(std::make_unique<bool>(true))
    ,   JoinedTuple(std::make_unique<std::vector<NUdf::TUnboxedValue*>>() )
    ,   IsSelfJoin_(isSelfJoin)
    ,   SelfJoinSameKeys_(isSelfJoin && (leftKeyColumns == rightKeyColumns))
    {
        if (JoinKind == EJoinKind::Full || JoinKind == EJoinKind::Exclusion || IsSelfJoin_) {
            LeftPacker->BatchSize = std::numeric_limits<ui64>::max();
            RightPacker->BatchSize = std::numeric_limits<ui64>::max();
        }
    }
private:
    IComputationWideFlowNode* const FlowLeft;
    IComputationWideFlowNode* const FlowRight;

    const EJoinKind JoinKind;
    const std::vector<ui32> LeftKeyColumns;
    const std::vector<ui32> RightKeyColumns;
    const std::vector<ui32> LeftRenames;
    const std::vector<ui32> RightRenames;
    const std::vector<TType *> LeftColumnsTypes;
    const std::vector<TType *> RightColumnsTypes;
    const std::unique_ptr<TGraceJoinPacker> LeftPacker;
    const std::unique_ptr<TGraceJoinPacker> RightPacker;
    const std::unique_ptr<GraceJoin::TTable> JoinedTablePtr;
    const std::unique_ptr<bool> JoinCompleted;
    const std::unique_ptr<bool> PartialJoinCompleted;
    const std::unique_ptr<bool> HaveMoreLeftRows;
    const std::unique_ptr<bool> HaveMoreRightRows;
    const std::unique_ptr<std::vector<NUdf::TUnboxedValue*>> JoinedTuple;
    const bool IsSelfJoin_;
    const bool SelfJoinSameKeys_;
};

class TGraceJoinWrapper : public TStatefulWideFlowCodegeneratorNode<TGraceJoinWrapper> {
    using TBaseComputation = TStatefulWideFlowCodegeneratorNode<TGraceJoinWrapper>;

    public:
        TGraceJoinWrapper(TComputationMutables& mutables, IComputationWideFlowNode* flowLeft, IComputationWideFlowNode* flowRight,
        EJoinKind joinKind, EAnyJoinSettings anyJoinSettings,  std::vector<ui32>&& leftKeyColumns, std::vector<ui32>&& rightKeyColumns,
        std::vector<ui32>&& leftRenames, std::vector<ui32>&& rightRenames,
        std::vector<TType*>&& leftColumnsTypes, std::vector<TType*>&& rightColumnsTypes,
        std::vector<EValueRepresentation>&& outputRepresentations, bool isSelfJoin)
            : TBaseComputation(mutables, nullptr, EValueRepresentation::Boxed)
            , FlowLeft(flowLeft)
            , FlowRight(flowRight)
            , JoinKind(joinKind)
            , AnyJoinSettings_(anyJoinSettings)
            , LeftKeyColumns(std::move(leftKeyColumns))
            , RightKeyColumns(std::move(rightKeyColumns))
            , LeftRenames(std::move(leftRenames))
            , RightRenames(std::move(rightRenames))
            , LeftColumnsTypes(std::move(leftColumnsTypes))
            , RightColumnsTypes(std::move(rightColumnsTypes))
            , OutputRepresentations(std::move(outputRepresentations))
            , IsSelfJoin_(isSelfJoin)
        {}

        EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue*const* output)  const {
            if (!state.HasValue()) {
                if (!ctx.ExecuteLLVM) {
                    MakeSpillingSupportState(ctx, state);
                } else {
                    MakeState(ctx, state);
                }
            }
            
            if (ctx.ExecuteLLVM) {
                return static_cast<TGraceJoinState*>(state.AsBoxed().Get())->FetchValues(ctx, output);
            }

            return static_cast<TGraceJoinSpillingSupportState*>(state.AsBoxed().Get())->FetchValues(ctx, output);
        }
#ifndef MKQL_DISABLE_CODEGEN
    ICodegeneratorInlineWideNode::TGenerateResult DoGenGetValues(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto valueType = Type::getInt128Ty(context);
        const auto indexType = Type::getInt32Ty(context);

        const auto arrayType = ArrayType::get(valueType, OutputRepresentations.size());
        const auto fieldsType = ArrayType::get(PointerType::getUnqual(valueType), OutputRepresentations.size());

        const auto atTop = &ctx.Func->getEntryBlock().back();

        const auto values = new AllocaInst(arrayType, 0U, "values", atTop);
        const auto fields = new AllocaInst(fieldsType, 0U, "fields", atTop);

        ICodegeneratorInlineWideNode::TGettersList getters(OutputRepresentations.size());

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

        BranchInst::Create(main, make, HasValue(statePtr, block), block);
        block = make;

        const auto ptrType = PointerType::getUnqual(StructType::get(context));
        const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block);
        const auto makeFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TGraceJoinWrapper::MakeState));
        const auto makeType = FunctionType::get(Type::getVoidTy(context), {self->getType(), ctx.Ctx->getType(), statePtr->getType()}, false);
        const auto makeFuncPtr = CastInst::Create(Instruction::IntToPtr, makeFunc, PointerType::getUnqual(makeType), "function", block);
        CallInst::Create(makeType, makeFuncPtr, {self, ctx.Ctx, statePtr}, "", block);
        BranchInst::Create(main, block);

        block = main;

        for (ui32 i = 0U; i < OutputRepresentations.size(); ++i) {
            ValueCleanup(OutputRepresentations[i], pointers[i], ctx, block);
        }

        new StoreInst(initV, values, block);

        const auto state = new LoadInst(valueType, statePtr, "state", block);
        const auto half = CastInst::Create(Instruction::Trunc, state, Type::getInt64Ty(context), "half", block);
        const auto stateArg = CastInst::Create(Instruction::IntToPtr, half, statePtrType, "state_arg", block);

        const auto func = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TGraceJoinState::FetchValues));
        const auto funcType = FunctionType::get(Type::getInt32Ty(context), { statePtrType, ctx.Ctx->getType(), fields->getType() }, false);
        const auto funcPtr = CastInst::Create(Instruction::IntToPtr, func, PointerType::getUnqual(funcType), "fetch_func", block);
        const auto result = CallInst::Create(funcType, funcPtr, { stateArg, ctx.Ctx, fields }, "fetch", block);

        for (ui32 i = 0U; i < OutputRepresentations.size(); ++i) {
            ValueRelease(OutputRepresentations[i], pointers[i], ctx, block);
        }

        return {result, std::move(getters)};
    }
#endif
    private:
        void RegisterDependencies() const final {
            FlowDependsOnBoth(FlowLeft, FlowRight);
        }

        void MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state) const {
            state = ctx.HolderFactory.Create<TGraceJoinState>(
                FlowLeft, FlowRight, JoinKind, AnyJoinSettings_, LeftKeyColumns, RightKeyColumns,
                LeftRenames, RightRenames, LeftColumnsTypes, RightColumnsTypes,
                ctx.HolderFactory, IsSelfJoin_);
        }

        void MakeSpillingSupportState(TComputationContext& ctx, NUdf::TUnboxedValue& state) const {
            state = ctx.HolderFactory.Create<TGraceJoinSpillingSupportState>(
                FlowLeft, FlowRight, JoinKind, AnyJoinSettings_, LeftKeyColumns, RightKeyColumns,
                LeftRenames, RightRenames, LeftColumnsTypes, RightColumnsTypes,
                ctx.HolderFactory, IsSelfJoin_);
        }

        IComputationWideFlowNode *const  FlowLeft;
        IComputationWideFlowNode *const  FlowRight;
        const EJoinKind JoinKind;
        const EAnyJoinSettings AnyJoinSettings_;
        const std::vector<ui32> LeftKeyColumns;
        const std::vector<ui32> RightKeyColumns;
        const std::vector<ui32> LeftRenames;
        const std::vector<ui32> RightRenames;
        const std::vector<TType *> LeftColumnsTypes;
        const std::vector<TType *> RightColumnsTypes;
        const std::vector<EValueRepresentation> OutputRepresentations;
        const bool IsSelfJoin_;
};

EFetchResult TGraceJoinState::FetchValues(TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {

            // Collecting data for join and perform join (batch or full)
            while (!*JoinCompleted ) {

                if ( *PartialJoinCompleted) {

                    // Returns join results (batch or full)

                    while (JoinedTablePtr->NextJoinedData(LeftPacker->JoinTupleData, RightPacker->JoinTupleData)) {

                        LeftPacker->UnPack();
                        RightPacker->UnPack();

                        auto &valsLeft = LeftPacker->TupleHolder;
                        auto &valsRight = RightPacker->TupleHolder;


                        for (size_t i = 0; i < LeftRenames.size() / 2; i++)
                        {
                            auto & valPtr = output[LeftRenames[2 * i + 1]];
                            if ( valPtr ) {
                                *valPtr = valsLeft[LeftRenames[2 * i]];
                            }
                        }

                        for (size_t i = 0; i < RightRenames.size() / 2; i++)
                        {
                            auto & valPtr = output[RightRenames[2 * i + 1]];
                            if ( valPtr ) {
                                *valPtr = valsRight[RightRenames[2 * i]];
                            }
                        }

                        return EFetchResult::One;

                    }

                    // Resets batch state for batch join
                    if (!*HaveMoreRightRows) {
                        *PartialJoinCompleted = false;
                        LeftPacker->TuplesBatchPacked = 0;
                        LeftPacker->TablePtr->Clear(); // Clear table content, ready to collect data for next batch
                        JoinedTablePtr->Clear();
                        JoinedTablePtr->ResetIterator();
                    }


                    if (!*HaveMoreLeftRows ) {
                        *PartialJoinCompleted = false;
                        RightPacker->TuplesBatchPacked = 0;
                        RightPacker->TablePtr->Clear(); // Clear table content, ready to collect data for next batch
                        JoinedTablePtr->Clear();
                        JoinedTablePtr->ResetIterator();
                    }

                }

                if (!*HaveMoreRightRows && !*HaveMoreLeftRows) {
                    *JoinCompleted = true;
                    break;
                }


                const NKikimr::NMiniKQL::EFetchResult resultLeft = FlowLeft->FetchValues(ctx, LeftPacker->TuplePtrs.data());

                NKikimr::NMiniKQL::EFetchResult resultRight;

                if (IsSelfJoin_) {
                    resultRight = resultLeft;
                    if (!SelfJoinSameKeys_) {
                        std::copy_n(LeftPacker->TupleHolder.begin(), LeftPacker->TotalColumnsNum, RightPacker->TupleHolder.begin());
                    }
                } else {
                    resultRight = FlowRight->FetchValues(ctx, RightPacker->TuplePtrs.data());
                }

                if (resultLeft == EFetchResult::One) {
                    if (LeftPacker->TuplesPacked == 0) {
                        LeftPacker->StartTime = std::chrono::system_clock::now();
                    }
                    LeftPacker->Pack();
                    LeftPacker->TablePtr->AddTuple(LeftPacker->TupleIntVals.data(), LeftPacker->TupleStrings.data(), LeftPacker->TupleStrSizes.data(), LeftPacker->IColumnsHolder.data());
                }

                if (resultRight == EFetchResult::One) {
                    if (RightPacker->TuplesPacked == 0) {
                        RightPacker->StartTime = std::chrono::system_clock::now();
                    }

                    if ( !SelfJoinSameKeys_ ) {
                        RightPacker->Pack();
                        RightPacker->TablePtr->AddTuple(RightPacker->TupleIntVals.data(), RightPacker->TupleStrings.data(), RightPacker->TupleStrSizes.data(), RightPacker->IColumnsHolder.data());
                    }
                }

                if (resultLeft == EFetchResult::Finish ) {
                    *HaveMoreLeftRows = false;
                }


                if (resultRight == EFetchResult::Finish ) {
                    *HaveMoreRightRows = false;
                }

                if ((resultLeft == EFetchResult::Yield && (!*HaveMoreRightRows || resultRight == EFetchResult::Yield)) ||
                    (resultRight == EFetchResult::Yield && !*HaveMoreLeftRows))
                {
                    return EFetchResult::Yield;
                }

                if (!*HaveMoreRightRows && !*PartialJoinCompleted && LeftPacker->TuplesBatchPacked >= LeftPacker->BatchSize ) {
                    *PartialJoinCompleted = true;
                    JoinedTablePtr->Join(*LeftPacker->TablePtr, *RightPacker->TablePtr, JoinKind, *HaveMoreLeftRows, *HaveMoreRightRows);
                    JoinedTablePtr->ResetIterator();

                }


                if (!*HaveMoreLeftRows && !*PartialJoinCompleted && RightPacker->TuplesBatchPacked >= RightPacker->BatchSize ) {
                    *PartialJoinCompleted = true;
                    JoinedTablePtr->Join(*LeftPacker->TablePtr, *RightPacker->TablePtr, JoinKind, *HaveMoreLeftRows, *HaveMoreRightRows);
                    JoinedTablePtr->ResetIterator();

                }

                if (!*HaveMoreRightRows && !*HaveMoreLeftRows && !*PartialJoinCompleted) {
                    *PartialJoinCompleted = true;
                    LeftPacker->StartTime = std::chrono::system_clock::now();
                    RightPacker->StartTime = std::chrono::system_clock::now();
                    if ( SelfJoinSameKeys_ ) {
                        JoinedTablePtr->Join(*LeftPacker->TablePtr, *LeftPacker->TablePtr, JoinKind, *HaveMoreLeftRows, *HaveMoreRightRows);
                    } else {
                        JoinedTablePtr->Join(*LeftPacker->TablePtr, *RightPacker->TablePtr, JoinKind, *HaveMoreLeftRows, *HaveMoreRightRows);
                    }
                    JoinedTablePtr->ResetIterator();
                    LeftPacker->EndTime = std::chrono::system_clock::now();
                    RightPacker->EndTime = std::chrono::system_clock::now();

                }

            }

            return EFetchResult::Finish;
}


}

IComputationNode* WrapGraceJoin(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 8, "Expected 8 args");

    const auto leftFlowNode = callable.GetInput(0);
    const auto rightFlowNode = callable.GetInput(1);
    const auto leftFlowComponents = GetWideComponents(AS_TYPE(TFlowType, leftFlowNode));
    const auto rightFlowComponents = GetWideComponents(AS_TYPE(TFlowType, rightFlowNode));
    const auto joinKindNode = callable.GetInput(2);
    const auto leftKeyColumnsNode = AS_VALUE(TTupleLiteral, callable.GetInput(3));
    const auto rightKeyColumnsNode = AS_VALUE(TTupleLiteral, callable.GetInput(4));
    const auto leftRenamesNode = AS_VALUE(TTupleLiteral, callable.GetInput(5));
    const auto rightRenamesNode = AS_VALUE(TTupleLiteral, callable.GetInput(6));
    const ui32 rawJoinKind = AS_VALUE(TDataLiteral, joinKindNode)->AsValue().Get<ui32>();

    const EAnyJoinSettings anyJoinSettings = GetAnyJoinSettings(AS_VALUE(TDataLiteral, callable.GetInput(7))->AsValue().Get<ui32>());

    const auto flowLeft = dynamic_cast<IComputationWideFlowNode*> (LocateNode(ctx.NodeLocator, callable, 0));
    const auto flowRight = dynamic_cast<IComputationWideFlowNode*> (LocateNode(ctx.NodeLocator, callable, 1));

    const auto outputFlowComponents = GetWideComponents(AS_TYPE(TFlowType, callable.GetType()->GetReturnType()));
    std::vector<EValueRepresentation> outputRepresentations;
    outputRepresentations.reserve(outputFlowComponents.size());
    for (ui32 i = 0U; i < outputFlowComponents.size(); ++i) {
        outputRepresentations.emplace_back(GetValueRepresentation(outputFlowComponents[i]));
    }

    std::vector<ui32> leftKeyColumns, leftRenames, rightKeyColumns, rightRenames;
    std::vector<TType*> leftColumnsTypes(leftFlowComponents.begin(), leftFlowComponents.end());
    std::vector<TType*> rightColumnsTypes(rightFlowComponents.begin(), rightFlowComponents.end());

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

    rightRenames.reserve(rightRenamesNode->GetValuesCount());
    for (ui32 i = 0; i < rightRenamesNode->GetValuesCount(); ++i) {
        rightRenames.emplace_back(AS_VALUE(TDataLiteral, rightRenamesNode->GetValue(i))->AsValue().Get<ui32>());
    }

    return new TGraceJoinWrapper(
        ctx.Mutables, flowLeft, flowRight, GetJoinKind(rawJoinKind), anyJoinSettings,
        std::move(leftKeyColumns), std::move(rightKeyColumns), std::move(leftRenames), std::move(rightRenames),
        std::move(leftColumnsTypes), std::move(rightColumnsTypes), std::move(outputRepresentations), false);

}

IComputationNode* WrapGraceSelfJoin(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 7, "Expected 7 args");

    const auto leftFlowNode = callable.GetInput(0);
    const auto leftFlowComponents = GetWideComponents(AS_TYPE(TFlowType, leftFlowNode));
    const auto joinKindNode = callable.GetInput(1);
    const auto leftKeyColumnsNode = AS_VALUE(TTupleLiteral, callable.GetInput(2));
    const auto rightKeyColumnsNode = AS_VALUE(TTupleLiteral, callable.GetInput(3));
    const auto leftRenamesNode = AS_VALUE(TTupleLiteral, callable.GetInput(4));
    const auto rightRenamesNode = AS_VALUE(TTupleLiteral, callable.GetInput(5));
    const ui32 rawJoinKind = AS_VALUE(TDataLiteral, joinKindNode)->AsValue().Get<ui32>();

    const EAnyJoinSettings anyJoinSettings = GetAnyJoinSettings(AS_VALUE(TDataLiteral, callable.GetInput(6))->AsValue().Get<ui32>());

    const auto flowLeft = dynamic_cast<IComputationWideFlowNode*> (LocateNode(ctx.NodeLocator, callable, 0));

    const auto outputFlowComponents = GetWideComponents(AS_TYPE(TFlowType, callable.GetType()->GetReturnType()));
    std::vector<EValueRepresentation> outputRepresentations;
    outputRepresentations.reserve(outputFlowComponents.size());
    for (ui32 i = 0U; i < outputFlowComponents.size(); ++i) {
        outputRepresentations.emplace_back(GetValueRepresentation(outputFlowComponents[i]));
    }

    std::vector<ui32> leftKeyColumns, leftRenames, rightKeyColumns, rightRenames;
    std::vector<TType*> leftColumnsTypes(leftFlowComponents.begin(), leftFlowComponents.end());
    std::vector<TType*> rightColumnsTypes{leftColumnsTypes};

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

    MKQL_ENSURE(leftKeyColumns.size() == rightKeyColumns.size(), "Number of key columns for self join should be equal");

//    MKQL_ENSURE(leftKeyColumns == rightKeyColumns, "Key columns for self join should be equal");

    rightRenames.reserve(rightRenamesNode->GetValuesCount());
    for (ui32 i = 0; i < rightRenamesNode->GetValuesCount(); ++i) {
        rightRenames.emplace_back(AS_VALUE(TDataLiteral, rightRenamesNode->GetValue(i))->AsValue().Get<ui32>());
    }

    return new TGraceJoinWrapper(
        ctx.Mutables, flowLeft, nullptr, GetJoinKind(rawJoinKind), anyJoinSettings,
        std::move(leftKeyColumns), std::move(rightKeyColumns), std::move(leftRenames), std::move(rightRenames),
        std::move(leftColumnsTypes), std::move(rightColumnsTypes), std::move(outputRepresentations), true);


}


}

}

