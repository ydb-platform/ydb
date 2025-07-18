#include "mkql_block_impl.h"
#include "mkql_block_builder.h"
#include "mkql_block_reader.h"

#include <yql/essentials/minikql/arrow/mkql_functions.h>
#include <yql/essentials/minikql/computation/mkql_datum_validate.h>
#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/arrow/arrow_util.h>
#include <yql/essentials/minikql/arrow/mkql_bit_utils.h>
#include <yql/essentials/minikql/mkql_type_helper.h>
#include <yql/essentials/public/udf/arrow/args_dechunker.h>

#include <yql/essentials/parser/pg_wrapper/interface/arrow.h>

#include <arrow/compute/exec_internal.h>

extern "C" uint64_t GetBlockCount(const NYql::NUdf::TUnboxedValuePod data) {
    return NKikimr::NMiniKQL::TArrowBlock::From(data).GetDatum().scalar_as<arrow::UInt64Scalar>().value;
}

extern "C" uint64_t GetBitmapPopCountCount(const NYql::NUdf::TUnboxedValuePod data) {
    const auto& arr = NKikimr::NMiniKQL::TArrowBlock::From(data).GetDatum().array();
    const size_t len = (size_t)arr->length;
    MKQL_ENSURE(arr->GetNullCount() == 0, "Bitmap block should not have nulls");
    const ui8* src = arr->GetValues<ui8>(1);
    return NKikimr::NMiniKQL::GetSparseBitmapPopCount(src, len);
}

extern "C" uint8_t GetBitmapScalarValue(const NYql::NUdf::TUnboxedValuePod data) {
    return NKikimr::NMiniKQL::TArrowBlock::From(data).GetDatum().scalar_as<arrow::UInt8Scalar>().value;
}

namespace NKikimr::NMiniKQL {

namespace {

// TODO(YQL): This must be rewrited via traits dispatcher.
template<typename T>
arrow::Datum DoConvertScalar(TType* type, const T& value, arrow::MemoryPool& pool) {
    std::shared_ptr<arrow::DataType> arrowType;
    MKQL_ENSURE(ConvertArrowType(type, arrowType), "Unsupported type of scalar " << *type);
    if (!value) {
        return arrow::MakeNullScalar(arrowType);
    }
    auto needWrapWithExternalOptional = NeedWrapWithExternalOptional(type);

    if (type->IsOptional()) {
        type = AS_TYPE(TOptionalType, type)->GetItemType();
    }

    if (needWrapWithExternalOptional) {
        std::vector<std::shared_ptr<arrow::Scalar>> arrowValue;
        arrowValue.emplace_back(DoConvertScalar(type, value.GetOptionalValue(), pool).scalar());
        return arrow::Datum(std::make_shared<arrow::StructScalar>(arrowValue, arrowType));
    }

    if (type->IsStruct()) {
        auto structType = AS_TYPE(TStructType, type);
        std::vector<std::shared_ptr<arrow::Scalar>> arrowValue;
        for (ui32 i = 0; i < structType->GetMembersCount(); ++i) {
            arrowValue.emplace_back(DoConvertScalar(structType->GetMemberType(i), value.GetElement(i), pool).scalar());
        }

        return arrow::Datum(std::make_shared<arrow::StructScalar>(arrowValue, arrowType));
    }

    if (type->IsTuple()) {
        auto tupleType = AS_TYPE(TTupleType, type);
        std::vector<std::shared_ptr<arrow::Scalar>> arrowValue;
        for (ui32 i = 0; i < tupleType->GetElementsCount(); ++i) {
            arrowValue.emplace_back(DoConvertScalar(tupleType->GetElementType(i), value.GetElement(i), pool).scalar());
        }

        return arrow::Datum(std::make_shared<arrow::StructScalar>(arrowValue, arrowType));
    }

    if (type->IsData()) {
        auto slot = *AS_TYPE(TDataType, type)->GetDataSlot();
        switch (slot) {
        case NUdf::EDataSlot::Int8:
            return arrow::Datum(static_cast<int8_t>(value.template Get<i8>()));
        case NUdf::EDataSlot::Bool:
        case NUdf::EDataSlot::Uint8:
            return arrow::Datum(static_cast<uint8_t>(value.template Get<ui8>()));
        case NUdf::EDataSlot::Int16:
            return arrow::Datum(static_cast<int16_t>(value.template Get<i16>()));
        case NUdf::EDataSlot::Uint16:
        case NUdf::EDataSlot::Date:
            return arrow::Datum(static_cast<uint16_t>(value.template Get<ui16>()));
        case NUdf::EDataSlot::Int32:
        case NUdf::EDataSlot::Date32:
            return arrow::Datum(static_cast<int32_t>(value.template Get<i32>()));
        case NUdf::EDataSlot::Uint32:
        case NUdf::EDataSlot::Datetime:
            return arrow::Datum(static_cast<uint32_t>(value.template Get<ui32>()));
        case NUdf::EDataSlot::Int64:
        case NUdf::EDataSlot::Interval:
        case NUdf::EDataSlot::Interval64:
        case NUdf::EDataSlot::Datetime64:
        case NUdf::EDataSlot::Timestamp64:
            return arrow::Datum(static_cast<int64_t>(value.template Get<i64>()));
        case NUdf::EDataSlot::Uint64:
        case NUdf::EDataSlot::Timestamp:
            return arrow::Datum(static_cast<uint64_t>(value.template Get<ui64>()));
        case NUdf::EDataSlot::Float:
            return arrow::Datum(static_cast<float>(value.template Get<float>()));
        case NUdf::EDataSlot::Double:
            return arrow::Datum(static_cast<double>(value.template Get<double>()));
        case NUdf::EDataSlot::String:
        case NUdf::EDataSlot::Utf8:
        case NUdf::EDataSlot::Yson:
        case NUdf::EDataSlot::Json:
        case NUdf::EDataSlot::JsonDocument: {
            const auto& str = value.AsStringRef();
            std::shared_ptr<arrow::Buffer> buffer(ARROW_RESULT(arrow::AllocateBuffer(str.Size(), &pool)));
            std::memcpy(buffer->mutable_data(), str.Data(), str.Size());
            std::shared_ptr<arrow::Scalar> scalar;
            if (slot == NUdf::EDataSlot::String || slot == NUdf::EDataSlot::Yson || slot == NUdf::EDataSlot::JsonDocument) {
                scalar = std::make_shared<arrow::BinaryScalar>(buffer, arrow::binary());
            } else {
                // NOTE: Do not use |arrow::BinaryScalar| for utf8 and json types directly.
                // This is necessary so that the type of the scalar is clearly preserved at runtime.
                scalar = std::make_shared<arrow::StringScalar>(buffer);
            }
            return arrow::Datum(scalar);
        }
        case NUdf::EDataSlot::TzDate: {
            auto items = arrow::StructScalar::ValueType{
                std::make_shared<arrow::UInt16Scalar>(value.template Get<ui16>()),
                std::make_shared<arrow::UInt16Scalar>(value.GetTimezoneId())
            };

            return arrow::Datum(std::make_shared<arrow::StructScalar>(items, MakeTzDateArrowType<NUdf::EDataSlot::TzDate>()));
        }
        case NUdf::EDataSlot::TzDatetime: {
            auto items = arrow::StructScalar::ValueType{
                std::make_shared<arrow::UInt32Scalar>(value.template Get<ui32>()),
                std::make_shared<arrow::UInt16Scalar>(value.GetTimezoneId())
            };

            return arrow::Datum(std::make_shared<arrow::StructScalar>(items, MakeTzDateArrowType<NUdf::EDataSlot::TzDatetime>()));
        }
        case NUdf::EDataSlot::TzTimestamp: {
            auto items = arrow::StructScalar::ValueType{
                std::make_shared<arrow::UInt64Scalar>(value.template Get<ui64>()),
                std::make_shared<arrow::UInt16Scalar>(value.GetTimezoneId())
            };

            return arrow::Datum(std::make_shared<arrow::StructScalar>(items, MakeTzDateArrowType<NUdf::EDataSlot::TzTimestamp>()));
        }
        case NUdf::EDataSlot::TzDate32: {
            auto items = arrow::StructScalar::ValueType{
                std::make_shared<arrow::Int32Scalar>(value.template Get<i32>()),
                std::make_shared<arrow::UInt16Scalar>(value.GetTimezoneId())
            };

            return arrow::Datum(std::make_shared<arrow::StructScalar>(items, MakeTzDateArrowType<NUdf::EDataSlot::TzDate32>()));
        }
        case NUdf::EDataSlot::TzDatetime64: {
            auto items = arrow::StructScalar::ValueType{
                std::make_shared<arrow::Int64Scalar>(value.template Get<i64>()),
                std::make_shared<arrow::UInt16Scalar>(value.GetTimezoneId())
            };

            return arrow::Datum(std::make_shared<arrow::StructScalar>(items, MakeTzDateArrowType<NUdf::EDataSlot::TzDatetime64>()));
        }
        case NUdf::EDataSlot::TzTimestamp64: {
            auto items = arrow::StructScalar::ValueType{
                std::make_shared<arrow::Int64Scalar>(value.template Get<i64>()),
                std::make_shared<arrow::UInt16Scalar>(value.GetTimezoneId())
            };

            return arrow::Datum(std::make_shared<arrow::StructScalar>(items, MakeTzDateArrowType<NUdf::EDataSlot::TzTimestamp64>()));
        }
        case NUdf::EDataSlot::Decimal: {
            std::shared_ptr<arrow::Buffer> buffer(ARROW_RESULT(arrow::AllocateBuffer(16, &pool)));
            *reinterpret_cast<NYql::NDecimal::TInt128*>(buffer->mutable_data()) = value.GetInt128();
            return arrow::Datum(std::make_shared<TPrimitiveDataType<NYql::NDecimal::TInt128>::TScalarResult>(buffer));
        }
        default:
            MKQL_ENSURE(false, "Unsupported data slot " << slot);
        }
    }

    if (type->IsPg()) {
        return NYql::MakePgScalar(AS_TYPE(TPgType, type), value, pool);
    }

    MKQL_ENSURE(false, "Unsupported type " << *type);
}

} // namespace

arrow::Datum ConvertScalar(TType* type, const NUdf::TUnboxedValuePod& value, arrow::MemoryPool& pool) {
    return DoConvertScalar(type, value, pool);
}

arrow::Datum ConvertScalar(TType* type, const NUdf::TBlockItem& value, arrow::MemoryPool& pool) {
    return DoConvertScalar(type, value, pool);
}

arrow::Datum MakeArrayFromScalar(const arrow::Scalar& scalar, size_t len, TType* type, arrow::MemoryPool& pool) {
    MKQL_ENSURE(len > 0, "Invalid block size");
    auto reader = MakeBlockReader(TTypeInfoHelper(), type);
    auto builder = MakeArrayBuilder(TTypeInfoHelper(), type, pool, len, nullptr);

    auto scalarItem = reader->GetScalarItem(scalar);
    builder->Add(scalarItem, len);

    return builder->Build(true);
}

arrow::ValueDescr ToValueDescr(TType* type) {
    arrow::ValueDescr ret;
    MKQL_ENSURE(ConvertInputArrowType(type, ret), "can't get arrow type");
    return ret;
}

std::vector<arrow::ValueDescr> ToValueDescr(const TVector<TType*>& types) {
    std::vector<arrow::ValueDescr> res;
    res.reserve(types.size());
    for (const auto& type : types) {
        if (type) {
            res.emplace_back(ToValueDescr(type));
        } else {
            res.emplace_back();
        }
    }

    return res;
}

std::vector<arrow::compute::InputType> ConvertToInputTypes(const TVector<TType*>& argTypes) {
    std::vector<arrow::compute::InputType> result;
    result.reserve(argTypes.size());
    for (auto& type : argTypes) {
        result.emplace_back(ToValueDescr(type));
    }
    return result;
}

arrow::compute::OutputType ConvertToOutputType(TType* output) {
    return arrow::compute::OutputType(ToValueDescr(output));
}

NUdf::TUnboxedValuePod MakeBlockCount(const THolderFactory& holderFactory, const uint64_t count) {
    return holderFactory.CreateArrowBlock(arrow::Datum(count));
}

TBlockFuncNode::TBlockFuncNode(TComputationMutables& mutables, NYql::NUdf::EValidateDatumMode validateDatumMode, TStringBuf name, TComputationNodePtrVector&& argsNodes,
                               const TVector<TType*>& argsTypes, TType* outputType, const arrow::compute::ScalarKernel& kernel,
                               std::shared_ptr<arrow::compute::ScalarKernel> kernelHolder,
                               const arrow::compute::FunctionOptions* functionOptions)
    : TMutableComputationNode(mutables)
    , ValidateDatumMode_(validateDatumMode)
    , StateIndex_(mutables.CurValueIndex++)
    , ArgsNodes_(std::move(argsNodes))
    , ArgsValuesDescr_(ToValueDescr(argsTypes))
    , OutValueDescr_(ToValueDescr(outputType))
    , Kernel_(kernel)
    , KernelHolder_(std::move(kernelHolder))
    , Options_(functionOptions)
    , ScalarOutput_(GetResultShape(argsTypes) == TBlockType::EShape::Scalar)
    , Name_(name.starts_with("Block") ? name.substr(5) : name)
{
}

NUdf::TUnboxedValuePod TBlockFuncNode::DoCalculate(TComputationContext& ctx) const {
    auto& state = GetState(ctx);

    std::vector<arrow::Datum> argDatums;
    for (ui32 i = 0; i < ArgsNodes_.size(); ++i) {
        const auto& value = ArgsNodes_[i]->GetValue(ctx);
        argDatums.emplace_back(TArrowBlock::From(value).GetDatum());
        ValidateDatum(argDatums.back(), ArgsValuesDescr_[i], ValidateDatumMode_);
    }

    if (ScalarOutput_) {
        auto executor = arrow::compute::detail::KernelExecutor::MakeScalar();
        ARROW_OK(executor->Init(&state.KernelContext, { &Kernel_, ArgsValuesDescr_, Options_ }));

        auto listener = std::make_shared<arrow::compute::detail::DatumAccumulator>();
        ARROW_OK(executor->Execute(argDatums, listener.get()));
        auto output = executor->WrapResults(argDatums, listener->values());
        return ctx.HolderFactory.CreateArrowBlock(std::move(output));
    }

    NYql::NUdf::TArgsDechunker dechunker(std::move(argDatums));
    std::vector<arrow::Datum> chunk;
    TVector<std::shared_ptr<arrow::ArrayData>> arrays;

    while (dechunker.Next(chunk)) {
        auto executor = arrow::compute::detail::KernelExecutor::MakeScalar();
        ARROW_OK(executor->Init(&state.KernelContext, { &Kernel_, ArgsValuesDescr_, Options_ }));

        arrow::compute::detail::DatumAccumulator listener;
        ARROW_OK(executor->Execute(chunk, &listener));
        auto output = executor->WrapResults(chunk, listener.values());

        ForEachArrayData(output, [&](const auto& arr) { arrays.push_back(arr); });
    }
    auto resultArray = MakeArray(arrays);
    ValidateDatum(resultArray, OutValueDescr_, ValidateDatumMode_);
    return ctx.HolderFactory.CreateArrowBlock(std::move(resultArray));
}


void TBlockFuncNode::RegisterDependencies() const {
    for (const auto& arg : ArgsNodes_) {
        DependsOn(arg);
    }
}

TBlockFuncNode::TState& TBlockFuncNode::GetState(TComputationContext& ctx) const {
    auto& result = ctx.MutableValues[StateIndex_];
    if (!result.HasValue()) {
        result = ctx.HolderFactory.Create<TState>(Options_, Kernel_, ArgsValuesDescr_, ctx);
    }

    return *static_cast<TState*>(result.AsBoxed().Get());
}

std::unique_ptr<IArrowKernelComputationNode> TBlockFuncNode::PrepareArrowKernelComputationNode(TComputationContext&) const {
    return std::make_unique<TArrowNode>(this);
}

TBlockFuncNode::TArrowNode::TArrowNode(const TBlockFuncNode* parent)
    : Parent_(parent)
{}

TStringBuf TBlockFuncNode::TArrowNode::GetKernelName() const {
    return Parent_->Name_;
}

const arrow::compute::ScalarKernel& TBlockFuncNode::TArrowNode::GetArrowKernel() const {
    return Parent_->Kernel_;
}

const std::vector<arrow::ValueDescr>& TBlockFuncNode::TArrowNode::GetArgsDesc() const {
    return Parent_->ArgsValuesDescr_;
}

const IComputationNode* TBlockFuncNode::TArrowNode::GetArgument(ui32 index) const {
    MKQL_ENSURE(index < Parent_->ArgsNodes_.size(), "Wrong index");
    return Parent_->ArgsNodes_[index];
}

TBlockState::TBlockState(TMemoryUsageInfo* memInfo, size_t width, i64 blockLengthIndex)
    : TBase(memInfo), Values(width), Deques(width), Arrays(width)
    , BlockLengthIndex(blockLengthIndex == LAST_COLUMN_MARKER ? width - 1 : blockLengthIndex)
{
    MKQL_ENSURE(blockLengthIndex == LAST_COLUMN_MARKER || (0 <= blockLengthIndex && size_t(blockLengthIndex) < width), "Bad blockLengthIndex");
    Pointer = Values.data();
}

void TBlockState::ClearValues() {
    Values.assign(Values.size(), NUdf::TUnboxedValuePod());
}

void TBlockState::FillArrays() {
    MKQL_ENSURE(Count == 0, "All existing arrays have to be processed");
    auto& counterDatum = TArrowBlock::From(Values[BlockLengthIndex]).GetDatum();
    MKQL_ENSURE(counterDatum.is_scalar(), "Unexpected block length type (expecting scalar)");
    Count = counterDatum.scalar_as<arrow::UInt64Scalar>().value;
    if (!Count)
        return;

    for (size_t i = 0U; i < Deques.size(); ++i) {
        if (i == BlockLengthIndex) {
            continue;
        }

        Deques[i].clear();
        if (const auto& value = Values[i]) {
            const auto& datum = TArrowBlock::From(value).GetDatum();
            if (datum.is_scalar()) {
                return;
            }
            MKQL_ENSURE(datum.is_arraylike(), "Unexpected block type (expecting array or chunked array)");
            ForEachArrayData(datum, [this, i](const auto& arrayData) {
                Deques[i].push_back(arrayData);
            });
        }
    }
}

ui64 TBlockState::Slice() {
    auto sliceSize = Count;
    for (size_t i = 0; i < Deques.size(); ++i) {
        const auto& arr = Deques[i];
        if (arr.empty())
            continue;

        Y_ABORT_UNLESS(ui64(arr.front()->length) <= Count);
        MKQL_ENSURE(ui64(arr.front()->length) <= Count, "Unexpected array length at column #" << i);
        sliceSize = std::min<ui64>(sliceSize, arr.front()->length);
    }

    for (size_t i = 0; i < Arrays.size(); ++i) {
        auto& arr = Deques[i];
        if (arr.empty())
            continue;
        if (auto& array = arr.front(); ui64(array->length) == sliceSize) {
            Arrays[i] = std::move(array);
            Deques[i].pop_front();
        } else
            Arrays[i] = Chop(array, sliceSize);
    }

    Count -= sliceSize;
    return sliceSize;
}

NUdf::TUnboxedValuePod TBlockState::Get(const ui64 sliceSize, const THolderFactory& holderFactory, const size_t idx) const {
    if (idx == BlockLengthIndex)
        return holderFactory.CreateArrowBlock(arrow::Datum(std::make_shared<arrow::UInt64Scalar>(sliceSize)));

    if (auto array = Arrays[idx])
        return holderFactory.CreateArrowBlock(std::move(array));
    else
        return Values[idx];
}

}
