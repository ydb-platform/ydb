#include "mkql_grace_join.h"
#include "mkql_grace_join_imp.h"

#include <ydb/library/yql/public/udf/udf_data_type.h>
#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/public/decimal/yql_decimal_serialize.h>
#include <ydb/library/yql/minikql/computation/mkql_custom_list.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/comp_nodes/mkql_factories.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_pack.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>

#include <ydb/library/yql/utils/log/log.h>

#include <chrono>

namespace NKikimr {
namespace NMiniKQL {

namespace {


struct TColumnDataPackInfo {
    ui32 ColumnIdx = 0; // Column index in tuple
    ui32 Bytes = 0; // Size in bytes for fixed size values
    TType* MKQLType; // Data type of the column in term of compute nodes data flows
    NUdf::EDataSlot DataType = NUdf::EDataSlot::Uint32; // Data type of the column for standard types (TDataType)
    bool IsKeyColumn = false; // True if this columns is key for join
    bool IsString = false; // True if value is string
};

struct TGraceJoinPacker {
    ui64 NullsBitmapSize = 0; // Number of ui64 values for nulls bitmap
    std::vector<ui64> TupleIntVals; // Packed value of all fixed length values of table tuple.  Keys columns should be packed first.
    std::vector<ui32> TupleStrSizes; // Sizes of all packed strings
    std::vector<char*> TupleStrings; // All values of tuple strings
    std::vector<ui32> Offsets; // Offsets of table column values in bytes
    std::vector<ui32> PackedIdx; // Indexes of initial columns after packing
    std::vector<TType*>  ColumnTypes; // Types of all columns
    std::vector<TValuePacker> Packers; // Packers for composite data types
    const THolderFactory& HolderFactory; // To use during unpacking
    std::vector<TColumnDataPackInfo> ColumnsPackInfo; // Information about columns packing
    std::unique_ptr<GraceJoin::TTable> TablePtr;  // Table to pack data
    std::vector<NUdf::TUnboxedValue> TupleHolder; // Storage for tuple data
    std::vector<NUdf::TUnboxedValue*> TuplePtrs; // Storage for tuple data pointers to use in FetchValues
    std::vector<std::string> TupleStringHolder; // Storage for complex tuple data types serialized to strings
    GraceJoin::TupleData JoinTupleData; // TupleData to get join results
    ui64 TotalColumnsNum = 0; // Total number of columns to pack
    ui64 TotalIntColumnsNum = 0; // Total number of int columns
    ui64 TotalStrColumnsNum = 0; // Total number of string columns
    ui64 KeyIntColumnsNum = 0;  // Total number of key int columns
    ui64 KeyStrColumnsNum = 0; // Total number of key string columns
    ui64 DataIntColumnsNum = TotalIntColumnsNum - KeyIntColumnsNum;
    ui64 DataStrColumnsNum = TotalStrColumnsNum - KeyStrColumnsNum;
    inline void Pack() ; // Packs new tuple from TupleHolder and TuplePtrs to TupleIntVals, TupleStrSizes, TupleStrings
    inline void UnPack(); // Unpacks packed values from TupleIntVals, TupleStrSizes, TupleStrings into TupleHolder and TuplePtrs 
    TGraceJoinPacker(const std::vector<TType*>& columnTypes, const std::vector<ui32>& keyColumns, const THolderFactory& holderFactory);
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

     if (colType->GetKind() != TType::EKind::Data) {
        res.IsString = true;
        res.DataType = NUdf::EDataSlot::String;
        return;
    }
        
    colTypeId = AS_TYPE(TDataType, colType)->GetSchemeType();

    NUdf::EDataSlot dataType = NUdf::GetDataSlot(colTypeId);
    res.DataType = dataType;

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
            res.Bytes = sizeof(i16); break;
        case NUdf::EDataSlot::Int32:
            res.Bytes = sizeof(i32); break;
        case NUdf::EDataSlot::Uint32:
            res.Bytes = sizeof(ui32); break;
        case NUdf::EDataSlot::Int64:
            res.Bytes = sizeof(i64); break;
        case NUdf::EDataSlot::Uint64:
            res.Bytes = sizeof(ui64); break;
        case NUdf::EDataSlot::Date:
            res.Bytes = sizeof(ui64); break;
        case NUdf::EDataSlot::Datetime:
            res.Bytes = sizeof(ui32); break;
        case NUdf::EDataSlot::Timestamp:
            res.Bytes = sizeof(ui64); break;
        case NUdf::EDataSlot::Interval:
            res.Bytes = sizeof(i64); break;
        case NUdf::EDataSlot::Uuid:
            res.IsString = true; break;
        case NUdf::EDataSlot::TzDate:
            res.Bytes = 4; break;
        case NUdf::EDataSlot::TzDatetime:
            res.Bytes = 6; break;
        case NUdf::EDataSlot::TzTimestamp:
            res.Bytes = 10; break;
        case NUdf::EDataSlot::Decimal:
            res.Bytes = 16; break;
        default:
        {
            res.IsString = true;
        }
    }

    return res;
}

void TGraceJoinPacker::Pack()  {

    for (ui64 i = 0; i < NullsBitmapSize; ++i) {
        TupleIntVals[i] = 0;  // Clearing nulls bit array. Bit 1 means particular column contains null value
    }

    for (ui64 i = 0; i < ColumnsPackInfo.size(); i++) {

        const TColumnDataPackInfo &pi = ColumnsPackInfo[i];
        ui32 offset = Offsets[i];
        NYql::NUdf::TUnboxedValue value = *TuplePtrs[i];
        if (!value) { // Null value
            ui64 currNullsIdx = PackedIdx[i] / (sizeof(ui64) * 8);
            ui64 remShift = ( PackedIdx[i] - currNullsIdx * (sizeof(ui64) * 8) );
            ui64 bitMask = (0x1) << remShift;
            TupleIntVals[currNullsIdx] |= bitMask;
            continue;
        }
        TType* type = ColumnTypes[i];

        TType* colType;
        if (type->IsOptional()) {
            colType = AS_TYPE(TOptionalType, type)->GetItemType();
        } else {
            colType = type;
        }

        if (colType->GetKind() != TType::EKind::Data) {
            TStringBuf strBuf = Packers[i].Pack(value);
            TupleStringHolder[i] = strBuf;
            TupleStrings[offset] = TupleStringHolder[i].data();
            TupleStrSizes[offset] = TupleStringHolder[i].size();
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
            *(reinterpret_cast<i32*> (buffPtr)) = value.Get<i32>(); break;
        case NUdf::EDataSlot::Uint32:
            WriteUnaligned<ui32>(buffPtr, value.Get<ui32>()); break; 
        case NUdf::EDataSlot::Int64:
            WriteUnaligned<i64>(buffPtr, value.Get<i64>()); break; 
        case NUdf::EDataSlot::Uint64:
            WriteUnaligned<ui64>(buffPtr, value.Get<ui64>()); break; 
            *(reinterpret_cast<ui64*> (buffPtr)) = value.Get<ui64>(); break;
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
        case NUdf::EDataSlot::Uuid:
        {
            auto str = TuplePtrs[i]->AsStringRef();
            TupleStrings[offset] = str.Data();
            TupleStrSizes[offset] = str.Size();
        }
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
            WriteUnaligned<ui32>(buffPtr, value.Get<ui64>());
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
            auto str = TuplePtrs[i]->AsStringRef();
            TupleStrings[offset] = str.Data();
            TupleStrSizes[offset] = str.Size();
        }
        
        }
    }
}

void TGraceJoinPacker::UnPack()  {
    for (ui64 i = 0; i < ColumnsPackInfo.size(); i++) {
        const TColumnDataPackInfo &pi = ColumnsPackInfo[i];
        ui32 offset = Offsets[i];
        NYql::NUdf::TUnboxedValue & value = *TuplePtrs[i];
        if (JoinTupleData.AllNulls) {
            value = NYql::NUdf::TUnboxedValue();
            continue;
        }
        ui64 currNullsIdx = PackedIdx[i] / (sizeof(ui64) * 8);
        ui64 remShift = ( PackedIdx[i] - currNullsIdx * (sizeof(ui64) * 8) );
        ui64 bitMask = (0x1) << remShift;
        if ( TupleIntVals[currNullsIdx] & bitMask ) {
            value = NYql::NUdf::TUnboxedValue();
            continue;
        }

        TType * type = ColumnTypes[i];

        TType * colType;
        if (type->IsOptional()) {
            colType = AS_TYPE(TOptionalType, type)->GetItemType();
        } else {
            colType = type;
        }

        if (colType->GetKind() != TType::EKind::Data) {
            value = Packers[i].Unpack(TStringBuf(TupleStrings[offset], TupleStrSizes[offset]), HolderFactory);
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
        case NUdf::EDataSlot::Uuid:
        {
            value = MakeString(NUdf::TStringRef(TupleStrings[offset], TupleStrSizes[offset]));
        }
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


TGraceJoinPacker::TGraceJoinPacker(const std::vector<TType *> & columnTypes, const std::vector<ui32>& keyColumns, const THolderFactory& holderFactory) : 
                                    ColumnTypes(columnTypes)
                                    , HolderFactory(holderFactory) {

    ui64 nColumns = ColumnTypes.size();
    ui64 nKeyColumns = keyColumns.size();

    std::vector<TColumnDataPackInfo> allColumnsPackInfo;

    for ( ui32 i = 0; i < columnTypes.size(); i++  ) {

        auto colType = columnTypes[i];
        auto packInfo = GetPackInfo(colType);
        packInfo.ColumnIdx = i;
        allColumnsPackInfo.push_back(packInfo);
        Packers.push_back(TValuePacker(true,colType));
    }

    for ( auto const & keyColIdx: keyColumns ) {
        allColumnsPackInfo[keyColIdx].IsKeyColumn = true;
    }


    ColumnsPackInfo = allColumnsPackInfo;

    ui64 totalIntColumnsNum = std::count_if(ColumnsPackInfo.begin(), ColumnsPackInfo.end(), [](TColumnDataPackInfo a) { return !a.IsString; });
    ui64 totalStrColumnsNum = nColumns - totalIntColumnsNum;

    ui64 keyIntColumnsNum = std::count_if(ColumnsPackInfo.begin(), ColumnsPackInfo.end(), [](TColumnDataPackInfo a) { return (a.IsKeyColumn && !a.IsString );});
    ui64 keyStrColumnsNum = nKeyColumns - keyIntColumnsNum;
    ui64 dataIntColumnsNum = totalIntColumnsNum - keyIntColumnsNum;
    ui64 dataStrColumnsNum = totalStrColumnsNum - keyStrColumnsNum;

    TotalColumnsNum = nColumns;
    TotalIntColumnsNum = totalIntColumnsNum;
    TotalStrColumnsNum = totalStrColumnsNum;
    KeyIntColumnsNum = keyIntColumnsNum;
    KeyStrColumnsNum = keyStrColumnsNum;
    DataIntColumnsNum = TotalIntColumnsNum - KeyIntColumnsNum;
    DataStrColumnsNum = TotalStrColumnsNum - KeyStrColumnsNum;

    TupleIntVals.resize(2 * totalIntColumnsNum );
    TupleStrings.resize(totalStrColumnsNum);
    TupleStrSizes.resize(totalStrColumnsNum);

    JoinTupleData.IntColumns = TupleIntVals.data();
    JoinTupleData.StrColumns = TupleStrings.data();
    JoinTupleData.StrSizes = TupleStrSizes.data();

    std::sort(allColumnsPackInfo.begin(), allColumnsPackInfo.end(), [](TColumnDataPackInfo & a, TColumnDataPackInfo & b)
        {
            if (a.IsKeyColumn && !b.IsKeyColumn) return true;
            if (a.Bytes > b.Bytes) return true;
            if (a.ColumnIdx > b.ColumnIdx ) return true;
            return false;
        });

    Offsets.resize(nColumns);
    PackedIdx.resize(nColumns);
    TupleHolder.resize(nColumns);
    TupleStringHolder.resize(nColumns);

    std::transform(TupleHolder.begin(), TupleHolder.end(), std::back_inserter(TuplePtrs), [](NUdf::TUnboxedValue& v) { return std::addressof(v); });

    NullsBitmapSize = (nColumns / (8 * sizeof(ui64)) + 1) ;
    ui32 currIntOffset = NullsBitmapSize * sizeof(ui64) ;
    ui32 currStrOffset = 0;
    ui32 currIdx = 0;

    for( auto const & p: allColumnsPackInfo ) {
        if ( !p.IsString ) {
            Offsets[p.ColumnIdx] = currIntOffset;
            currIntOffset += p.Bytes;
        } else {
            Offsets[p.ColumnIdx] = currStrOffset;
            currStrOffset++;
        }
        PackedIdx[p.ColumnIdx] = currIdx;
        currIdx++; 
    }

    TablePtr = std::make_unique<GraceJoin::TTable>(keyIntColumnsNum, keyStrColumnsNum, dataIntColumnsNum, dataStrColumnsNum);

}

struct GraceJoinState {
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
        const std::unique_ptr<std::vector<NUdf::TUnboxedValue*>> JoinedTuple;
        EFetchResult DoCalculateWrapper( TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const; 

        GraceJoinState(  
            IComputationWideFlowNode* flowLeft, IComputationWideFlowNode* flowRight,
            EJoinKind joinKind,  const std::vector<ui32>& leftKeyColumns, const std::vector<ui32>& rightKeyColumns,
            const std::vector<ui32>& leftRenames, const std::vector<ui32>& rightRenames,
            const std::vector<TType*>& leftColumnsTypes, const std::vector<TType*>& rightColumnsTypes, const THolderFactory & holderFactory) :
            LeftPacker(std::make_unique<TGraceJoinPacker>(leftColumnsTypes, leftKeyColumns, holderFactory))
        ,   RightPacker(std::make_unique<TGraceJoinPacker>(rightColumnsTypes, rightKeyColumns, holderFactory))
        ,   JoinedTablePtr(std::make_unique<GraceJoin::TTable>())
        ,   JoinCompleted(std::make_unique<bool>(false))
        ,   JoinedTuple(std::make_unique<std::vector<NUdf::TUnboxedValue*>>() )
        ,   FlowLeft(flowLeft)
        ,   FlowRight(flowRight)
        ,   JoinKind(joinKind)
        ,   LeftKeyColumns(leftKeyColumns)
        ,   RightKeyColumns(rightKeyColumns)
        ,   LeftRenames(leftRenames)
        ,   RightRenames(rightRenames) {}
};


class TState : public TComputationValue<TState> {
using TBase = TComputationValue<TState>;
public:
    TState(TMemoryUsageInfo* memInfo, std::unique_ptr<GraceJoinState>&& state)
        : TBase(memInfo), State(std::move(state))
    {
    }
    std::unique_ptr<GraceJoinState> State;
};


class TGraceJoinWrapper : public TStatefulWideFlowCodegeneratorNode<TGraceJoinWrapper> {
    using TBaseComputation = TStatefulWideFlowCodegeneratorNode<TGraceJoinWrapper>;

    public:
        TGraceJoinWrapper(TComputationMutables& mutables, IComputationWideFlowNode* flowLeft, IComputationWideFlowNode* flowRight,
        EJoinKind joinKind,  std::vector<ui32>&& leftKeyColumns, std::vector<ui32>&& rightKeyColumns,
        std::vector<ui32>&& leftRenames, std::vector<ui32>&& rightRenames,
        std::vector<TType*>&& leftColumnsTypes, std::vector<TType*>&& rightColumnsTypes )
        : TBaseComputation(mutables, nullptr, EValueRepresentation::Boxed)  
        , FlowLeft(flowLeft)
        , FlowRight(flowRight)
        , JoinKind(joinKind)
        , LeftKeyColumns(leftKeyColumns)
        , RightKeyColumns(rightKeyColumns)
        , LeftRenames(leftRenames)
        , RightRenames(rightRenames)
        , LeftColumnsTypes(leftColumnsTypes)
        , RightColumnsTypes(rightColumnsTypes) { }

        EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue*const* output)  const {

            if (!state.HasValue()) {
                MakeState(ctx, state);
            }
        
            const auto statePtr = static_cast<TState*>(state.AsBoxed().Get());
            return statePtr->State->DoCalculateWrapper( ctx, output);

        }


#ifndef MKQL_DISABLE_CODEGEN
    ICodegeneratorInlineWideNode::TGenerateResult DoGenGetValues(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen->GetContext();

        const auto valueType = Type::getInt128Ty(context);
        const auto ptrValueType = PointerType::getUnqual(valueType);
        const auto structPtrType = PointerType::getUnqual(StructType::get(context));
        const auto contextType = GetCompContextType(context);
        const auto statusType = Type::getInt32Ty(context);

        const auto stateType = StructType::get(context, {
            structPtrType              // vtbl
        });

        const auto statePtrType = PointerType::getUnqual(stateType);

        ui64 outputSize = LeftRenames.size() + RightRenames.size();
        const auto keys = new AllocaInst(ArrayType::get(valueType, outputSize), 0U, "keys", &ctx.Func->getEntryBlock().back());

        const auto make = BasicBlock::Create(context, "make", ctx.Func);
        const auto main = BasicBlock::Create(context, "main", ctx.Func);
        const auto more = BasicBlock::Create(context, "more", ctx.Func);

        BranchInst::Create(main, make, HasValue(statePtr, block), block);
        block = make;

        const auto ptrType = PointerType::getUnqual(StructType::get(context));
        const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block);
        const auto makeFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TGraceJoinWrapper::MakeState));
        const auto makeType = FunctionType::get(Type::getVoidTy(context), {self->getType(), ctx.Ctx->getType(), statePtr->getType()}, false);
        const auto makeFuncPtr = CastInst::Create(Instruction::IntToPtr, makeFunc, PointerType::getUnqual(makeType), "function", block);
        CallInst::Create(makeFuncPtr, {self, ctx.Ctx, statePtr}, "", block);
        BranchInst::Create(main, block);

        block = main;

        const auto state = new LoadInst(statePtr, "state", block);
        const auto half = CastInst::Create(Instruction::Trunc, state, Type::getInt64Ty(context), "half", block);
        const auto stateArg = CastInst::Create(Instruction::IntToPtr, half, statePtrType, "state_arg", block);
        BranchInst::Create(more, block);

        block = more;

        const auto over = BasicBlock::Create(context, "over", ctx.Func);
        const auto result = PHINode::Create(statusType, 3U, "result", over);

        ICodegeneratorInlineWideNode::TGettersList getters;
        getters.reserve(outputSize);
/*
        std::transform(Nodes.FinishResultNodes.cbegin(), Nodes.FinishResultNodes.cend(), std::back_inserter(getters), [&](IComputationNode* node) {
            return [node](const TCodegenContext& ctx, BasicBlock*& block){ return GetNodeValue(node, ctx, block); };
        });
*/
        return {result, std::move(getters)};
    }
#endif

    private:
        void RegisterDependencies() const final {
            this->DependsOn(FlowLeft, FlowRight);
        }


        void MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state) const {
            state = ctx.HolderFactory.Create<TState>(std::make_unique<GraceJoinState>(
                FlowLeft, FlowRight, JoinKind,  LeftKeyColumns, RightKeyColumns, 
                LeftRenames, RightRenames, LeftColumnsTypes, RightColumnsTypes, 
                ctx.HolderFactory));
        }

        IComputationWideFlowNode*  FlowLeft;
        IComputationWideFlowNode*  FlowRight;
        EJoinKind JoinKind;
        std::vector<ui32> LeftKeyColumns;
        std::vector<ui32> RightKeyColumns;
        std::vector<ui32> LeftRenames;
        std::vector<ui32> RightRenames;
        std::vector<TType *> LeftColumnsTypes;
        std::vector<TType *> RightColumnsTypes;

};


EFetchResult GraceJoinState::DoCalculateWrapper(TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {


             while (!*JoinCompleted) {

                const NKikimr::NMiniKQL::EFetchResult resultLeft = FlowLeft->FetchValues(ctx, LeftPacker->TuplePtrs.data());
                const NKikimr::NMiniKQL::EFetchResult resultRight = FlowRight->FetchValues(ctx, RightPacker->TuplePtrs.data());

                if (resultLeft == EFetchResult::One) {
                    LeftPacker->Pack();
                    LeftPacker->TablePtr->AddTuple(LeftPacker->TupleIntVals.data(), LeftPacker->TupleStrings.data(), LeftPacker->TupleStrSizes.data());
                }

                if (resultRight == EFetchResult::One) {
                    RightPacker->Pack();
                    RightPacker->TablePtr->AddTuple(RightPacker->TupleIntVals.data(), RightPacker->TupleStrings.data(), RightPacker->TupleStrSizes.data());
                }

                if (resultLeft == EFetchResult::Yield || resultRight == EFetchResult::Yield) {
                    return EFetchResult::Yield;
                }

                if (resultRight == EFetchResult::Finish && resultLeft == EFetchResult::Finish && !*JoinCompleted) {
                    *JoinCompleted = true;
                    JoinedTablePtr->Join(*LeftPacker->TablePtr, *RightPacker->TablePtr, JoinKind);
                    JoinedTablePtr->ResetIterator();
                }
            }

            JoinedTuple->resize((LeftRenames.size() + RightRenames.size()) / 2);
            while (JoinedTablePtr->NextJoinedData(LeftPacker->JoinTupleData, RightPacker->JoinTupleData)) {

                LeftPacker->UnPack();
                RightPacker->UnPack();

                auto &valsLeft = LeftPacker->TupleHolder;
                auto &valsRight = RightPacker->TupleHolder;

                for (ui32 i = 0; i < LeftRenames.size() / 2; i++)
                {
                    *output[LeftRenames[2 * i + 1]] = valsLeft[LeftRenames[2 * i]];
                }

                for (ui32 i = 0; i < RightRenames.size() / 2; i++)
                {
                    *output[RightRenames[2 * i + 1]] = valsRight[RightRenames[2 * i]];
                }

                return EFetchResult::One;
            }

            return EFetchResult::Finish;
}


}

IComputationNode* WrapGraceJoin(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 7, "Expected 7 args");

    const auto leftFlowNode = callable.GetInput(0);
    const auto rightFlowNode = callable.GetInput(1);
    const auto leftFlowTupleType = AS_TYPE(TFlowType, leftFlowNode)->GetItemType();
    const auto rightFlowTupleType = AS_TYPE(TFlowType, rightFlowNode)->GetItemType();
    const auto joinKindNode = callable.GetInput(2);
    const auto leftKeyColumnsNode = AS_VALUE(TTupleLiteral, callable.GetInput(3));
    const auto rightKeyColumnsNode = AS_VALUE(TTupleLiteral, callable.GetInput(4));
    const auto leftRenamesNode = AS_VALUE(TTupleLiteral, callable.GetInput(5));
    const auto rightRenamesNode = AS_VALUE(TTupleLiteral, callable.GetInput(6));
    const ui32 rawJoinKind = AS_VALUE(TDataLiteral, joinKindNode)->AsValue().Get<ui32>();

    const auto flowLeft = dynamic_cast<IComputationWideFlowNode*> (LocateNode(ctx.NodeLocator, callable, 0));
    const auto flowRight = dynamic_cast<IComputationWideFlowNode*> (LocateNode(ctx.NodeLocator, callable, 1));


    std::vector<ui32> leftKeyColumns, leftRenames, rightKeyColumns, rightRenames;
    std::vector<TType *> leftColumnsTypes, rightColumnsTypes;
 
    leftColumnsTypes.resize(AS_TYPE(TTupleType, leftFlowTupleType)->GetElementsCount());
    rightColumnsTypes.resize(AS_TYPE(TTupleType, rightFlowTupleType)->GetElementsCount());
 
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
    
    for (ui32 i = 0; i < leftColumnsTypes.size(); ++i) {
        leftColumnsTypes[i] = AS_TYPE(TTupleType, leftFlowTupleType)->GetElementType(i);
    }


    for (ui32 i = 0; i < rightColumnsTypes.size(); ++i) {
        rightColumnsTypes[i] = AS_TYPE(TTupleType, rightFlowTupleType)->GetElementType(i);
    }

    return new TGraceJoinWrapper(
        ctx.Mutables, flowLeft, flowRight, GetJoinKind(rawJoinKind), 
        std::move(leftKeyColumns), std::move(rightKeyColumns), std::move(leftRenames), std::move(rightRenames), 
        std::move(leftColumnsTypes), std::move(rightColumnsTypes));
 
}

}

}

