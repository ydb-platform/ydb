#include "mkql_block_agg_sum.h"
#include "mkql_block_agg_state_helper.h"

#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_node_printer.h>

#include <ydb/library/yql/minikql/computation/mkql_block_builder.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/arrow/arrow_defs.h>
#include <ydb/library/yql/minikql/arrow/arrow_util.h>
#include <ydb/library/yql/minikql/arrow/mkql_bit_utils.h>

#include <arrow/scalar.h>
#include <arrow/array/builder_primitive.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

template<bool IsNullable, typename TSum>
struct TSumState;

template<typename TSum>
struct TSumState<true, TSum> {
    typename TPrimitiveDataType<TSum>::TArithmetic Sum_ = 0;
    ui8 IsValid_ = 0;
};

template<typename TSum>
struct TSumState<false, TSum> {
    typename TPrimitiveDataType<TSum>::TArithmetic Sum_ = 0;
};

template<typename TOut>
struct TAvgState {
    typename TPrimitiveDataType<TOut>::TArithmetic Sum_ = 0;
    ui64 Count_ = 0;
};

template <bool IsNullable, typename TSum>
class TSumColumnBuilder : public IAggColumnBuilder {
public:
    using TStateType = TSumState<IsNullable, TSum>;

    TSumColumnBuilder(ui64 size, TType* dataType, TComputationContext& ctx)
        : Builder_(TTypeInfoHelper(), dataType, ctx.ArrowMemoryPool, size)
        , Ctx_(ctx)
    {
    }

    void Add(const void* state) final {
        auto typedState = MakeStateWrapper<TStateType>(state);
        if constexpr (IsNullable) {
            if (!typedState->IsValid_) {
                Builder_.Add(TBlockItem());
                return;
            }
        }
        Builder_.Add(TBlockItem(TSum(typedState->Sum_)));
    }

    NUdf::TUnboxedValue Build() final {
        return Ctx_.HolderFactory.CreateArrowBlock(Builder_.Build(true));
    }

private:
    NYql::NUdf::TFixedSizeArrayBuilder<TSum, IsNullable> Builder_;
    TComputationContext& Ctx_;
};

template<typename TOut>
class TAvgStateColumnBuilder : public IAggColumnBuilder {
public:
    TAvgStateColumnBuilder(ui64 size, TType* outputType, TComputationContext& ctx)
        : Ctx_(ctx)
        , Builder_(MakeArrayBuilder(TTypeInfoHelper(), outputType, ctx.ArrowMemoryPool, size, &ctx.Builder->GetPgBuilder()))
{
    }

    void Add(const void* state) final {
        auto typedState = MakeStateWrapper<TAvgState<TOut>>(state);
        auto tupleBuilder = static_cast<NUdf::TTupleArrayBuilder<true>*>(Builder_.get());
        if (typedState->Count_) {
            TBlockItem tupleItems[] = { TBlockItem(TOut(typedState->Sum_)), TBlockItem(typedState->Count_)} ;
            tupleBuilder->Add(TBlockItem(tupleItems));
        } else {
            tupleBuilder->Add(TBlockItem());
        }
    }

    NUdf::TUnboxedValue Build() final {
        return Ctx_.HolderFactory.CreateArrowBlock(Builder_->Build(true));
    }

private:
    TComputationContext& Ctx_;
    const std::unique_ptr<IArrayBuilder> Builder_;
};

template<typename TOut>
class TAvgResultColumnBuilder : public IAggColumnBuilder {
public:
    TAvgResultColumnBuilder(ui64 size, TComputationContext& ctx)
        : Ctx_(ctx)
        , Builder_(TTypeInfoHelper(), arrow::TypeTraits<typename TPrimitiveDataType<TOut>::TResult>::type_singleton(), ctx.ArrowMemoryPool, size)
    {
    }

    void Add(const void* state) final {
        auto typedState = MakeStateWrapper<TAvgState<TOut>>(state);
        if (typedState->Count_) {
            Builder_.Add(TBlockItem(TOut(typedState->Sum_ / typedState->Count_)));
        } else {
            Builder_.Add(TBlockItem());
        }
    }

    NUdf::TUnboxedValue Build() final {
        return Ctx_.HolderFactory.CreateArrowBlock(Builder_.Build(true));
    }

private:
    TComputationContext& Ctx_;
    NYql::NUdf::TFixedSizeArrayBuilder<TOut, /*Nullable=*/true> Builder_;
};

template <typename TTag, bool IsNullable, bool IsScalar, typename TIn, typename TSum>
class TSumBlockAggregator;

template <typename TTag, typename TIn, typename TOut>
class TAvgBlockAggregator;

template <bool IsNullable, bool IsScalar, typename TIn, typename TSum>
class TSumBlockAggregator<TCombineAllTag, IsNullable, IsScalar, TIn, TSum> : public TCombineAllTag::TBase {
public:
    using TBase = TCombineAllTag::TBase;
    using TStateType = TSumState<IsNullable, TSum>;
    using TInScalar = typename TPrimitiveDataType<TIn>::TScalarResult;

    TSumBlockAggregator(std::optional<ui32> filterColumn, ui32 argColumn, TType* dataType, TComputationContext& ctx)
        : TBase(sizeof(TStateType), filterColumn, ctx)
        , ArgColumn_(argColumn)
    {
        Y_UNUSED(dataType);
    }

    void InitState(void* state) final {
        TStateType st;
        memcpy(state, (void*)&st, sizeof(st));
    }

    void DestroyState(void* state) noexcept final {
        static_assert(std::is_trivially_destructible<TStateType>::value);
        Y_UNUSED(state);
    }

    void AddMany(void* state, const NUdf::TUnboxedValue* columns, ui64 batchLength, std::optional<ui64> filtered) final {
        auto typedState = MakeStateWrapper<TStateType>(state);
        const auto& datum = TArrowBlock::From(columns[ArgColumn_]).GetDatum();
        if constexpr (IsScalar) {
            Y_ENSURE(datum.is_scalar());
            if constexpr (IsNullable) {
                if (datum.scalar()->is_valid) {
                    typedState->Sum_ += (filtered ? *filtered : batchLength) * Cast(datum.scalar_as<TInScalar>().value);
                    typedState->IsValid_ = 1;
                }
            } else {
                typedState->Sum_ += (filtered ? *filtered : batchLength) * Cast(datum.scalar_as<TInScalar>().value);
            }
        } else {
            const auto& array = datum.array();
            auto ptr = array->GetValues<TIn>(1);
            auto len = array->length;
            auto nullCount = IsNullable ? array->GetNullCount() : 0;
            auto count = len - nullCount;
            if (!count) {
                return;
            }

            if (!filtered) {
                if constexpr (IsNullable) {
                    typedState->IsValid_ = 1;
                }
                auto sum = typedState->Sum_;
                if (IsNullable && nullCount != 0) {
                    auto nullBitmapPtr = array->GetValues<uint8_t>(0, 0);
                    for (int64_t i = 0; i < len; ++i) {
                        ui64 fullIndex = i + array->offset;
                        ui8 notNull = (nullBitmapPtr[fullIndex >> 3] >> (fullIndex & 0x07)) & 1;
                        sum += SelectArg<TIn>(notNull, ptr[i], 0);
                    }
                } else {
                    for (int64_t i = 0; i < len; ++i) {
                        sum += ptr[i];
                    }
                }

                typedState->Sum_ = sum;
            } else {
                const auto& filterDatum = TArrowBlock::From(columns[*FilterColumn_]).GetDatum();
                const auto& filterArray = filterDatum.array();
                MKQL_ENSURE(filterArray->GetNullCount() == 0, "Expected non-nullable bool column");
                const ui8* filterBitmap = filterArray->template GetValues<uint8_t>(1);
                auto sum = typedState->Sum_;
                if (IsNullable && nullCount != 0) {
                    ui64 count = 0;
                    auto nullBitmapPtr = array->template GetValues<uint8_t>(0, 0);
                    for (int64_t i = 0; i < len; ++i) {
                        ui64 fullIndex = i + array->offset;
                        ui8 notNullAndFiltered = ((nullBitmapPtr[fullIndex >> 3] >> (fullIndex & 0x07)) & 1) & filterBitmap[i];
                        sum += SelectArg<TIn>(notNullAndFiltered, ptr[i], 0);
                        count += notNullAndFiltered;
                    }

                    if constexpr (IsNullable) {
                        typedState->IsValid_ |= count ? 1 : 0;
                    }
                } else {
                    for (int64_t i = 0; i < len; ++i) {
                        sum += SelectArg<TIn>(filterBitmap[i], ptr[i], 0);
                    }
                    if constexpr (IsNullable) {
                        typedState->IsValid_ = 1;
                    }
                }

                typedState->Sum_ = sum;
            }
        }
    }

    NUdf::TUnboxedValue FinishOne(const void* state) final {
        auto typedState = MakeStateWrapper<TStateType>(state);
        if constexpr (IsNullable) {
            if (!typedState->IsValid_) {
                return NUdf::TUnboxedValuePod();
            }
        }
        return NUdf::TUnboxedValuePod(TSum(typedState->Sum_));
    }

private:
    const ui32 ArgColumn_;
};

template <bool IsNullable, bool IsScalar, typename TIn, typename TSum>
void PushValueToState(TSumState<IsNullable, TSum>* typedState, const arrow::Datum& datum, ui64 row) {
    using TInScalar = typename TPrimitiveDataType<TIn>::TScalarResult;
    if constexpr (IsScalar) {
        Y_ENSURE(datum.is_scalar());
        if constexpr (IsNullable) {
            if (datum.scalar()->is_valid) {
                typedState->Sum_ += Cast(datum.scalar_as<TInScalar>().value);
                typedState->IsValid_ = 1;
            }
        } else {
            typedState->Sum_ += Cast(datum.scalar_as<TInScalar>().value);
        }
    } else {
        const auto& array = datum.array();
        auto ptr = array->GetValues<TIn>(1);
        if constexpr (IsNullable) {
            if (array->GetNullCount() == 0) {
                typedState->IsValid_ = 1;
                typedState->Sum_ += ptr[row];
            } else {
                auto nullBitmapPtr = array->GetValues<uint8_t>(0, 0);
                ui64 fullIndex = row + array->offset;
                ui8 notNull = (nullBitmapPtr[fullIndex >> 3] >> (fullIndex & 0x07)) & 1;
                typedState->Sum_ += SelectArg<TIn>(notNull, ptr[row], 0);
                typedState->IsValid_ |= notNull;
            }
        } else {
            typedState->Sum_ += ptr[row];
        }
    }
}

template <bool IsNullable, bool IsScalar, typename TIn, typename TSum>
class TSumBlockAggregator<TCombineKeysTag, IsNullable, IsScalar, TIn, TSum> : public TCombineKeysTag::TBase {
public:
    using TBase = TCombineKeysTag::TBase;
    using TStateType = TSumState<IsNullable, TSum>;

    TSumBlockAggregator(std::optional<ui32> filterColumn, ui32 argColumn, TType* dataType, TComputationContext& ctx)
        : TBase(sizeof(TStateType), filterColumn, ctx)
        , ArgColumn_(argColumn)
        , DataType_(dataType)
    {
    }

    void InitKey(void* state, ui64 batchNum, const NUdf::TUnboxedValue* columns, ui64 row) final {
        TStateType st;
        memcpy(state, (void*)&st, sizeof(st));
        UpdateKey(state, batchNum, columns, row);
    }

    void DestroyState(void* state) noexcept final {
        static_assert(std::is_trivially_destructible<TStateType>::value);
        Y_UNUSED(state);
    }

    void UpdateKey(void* state, ui64 batchNum, const NUdf::TUnboxedValue* columns, ui64 row) final {
        Y_UNUSED(batchNum);
        auto typedState = MakeStateWrapper<TStateType>(state);
        const auto& datum = TArrowBlock::From(columns[ArgColumn_]).GetDatum();
        PushValueToState<IsNullable, IsScalar, TIn, TSum>(typedState.Get(), datum, row);
    }

    std::unique_ptr<IAggColumnBuilder> MakeStateBuilder(ui64 size) final {
        return std::make_unique<TSumColumnBuilder<IsNullable, TSum>>(size, DataType_, Ctx_);
    }

private:
    const ui32 ArgColumn_;
    TType* const DataType_;
};

template <bool IsNullable, bool IsScalar, typename TIn, typename TSum>
class TSumBlockAggregator<TFinalizeKeysTag, IsNullable, IsScalar, TIn, TSum> : public TFinalizeKeysTag::TBase {
public:
    using TBase = TFinalizeKeysTag::TBase;
    using TStateType = TSumState<IsNullable, TSum>;

    TSumBlockAggregator(std::optional<ui32> filterColumn, ui32 argColumn, TType* dataType, TComputationContext& ctx)
        : TBase(sizeof(TStateType), filterColumn, ctx)
        , ArgColumn_(argColumn)
        , DataType_(dataType)
    {
    }

    void LoadState(void* state, ui64 batchNum, const NUdf::TUnboxedValue* columns, ui64 row) final {
        TStateType st;
        memcpy(state, (void*)&st, sizeof(st));
        UpdateState(state, batchNum, columns, row);
    }

    void DestroyState(void* state) noexcept final {
        static_assert(std::is_trivially_destructible<TStateType>::value);
        Y_UNUSED(state);
    }

    void UpdateState(void* state, ui64 batchNum, const NUdf::TUnboxedValue* columns, ui64 row) final {
        Y_UNUSED(batchNum);
        auto typedState = MakeStateWrapper<TStateType>(state);
        const auto& datum = TArrowBlock::From(columns[ArgColumn_]).GetDatum();
        PushValueToState<IsNullable, IsScalar, TIn, TSum>(typedState.Get(), datum, row);
    }

    std::unique_ptr<IAggColumnBuilder> MakeResultBuilder(ui64 size) final {
        return std::make_unique<TSumColumnBuilder<IsNullable, TSum>>(size, DataType_, Ctx_);
    }

private:
    const ui32 ArgColumn_;
    TType* const DataType_;
};

template<typename TIn, typename TOut>
class TAvgBlockAggregator<TCombineAllTag, TIn, TOut> : public TCombineAllTag::TBase {
public:
    using TBase = TCombineAllTag::TBase;
    using TInScalar = typename TPrimitiveDataType<TIn>::TScalarResult;

    TAvgBlockAggregator(std::optional<ui32> filterColumn, ui32 argColumn, TType* outputType, TComputationContext& ctx)
        : TBase(sizeof(TAvgState<TOut>), filterColumn, ctx)
        , ArgColumn_(argColumn)
    {
        Y_UNUSED(outputType);
    }

    void InitState(void* state) final {
        TAvgState<TOut> st;
        memcpy(state, (void*)&st, sizeof(st));
    }

    void DestroyState(void* state) noexcept final {
        static_assert(std::is_trivially_destructible<TAvgState<TOut>>::value);
        Y_UNUSED(state);
    }

    void AddMany(void* state, const NUdf::TUnboxedValue* columns, ui64 batchLength, std::optional<ui64> filtered) final {
        auto typedState = MakeStateWrapper<TAvgState<TOut>>(state);
        const auto& datum = TArrowBlock::From(columns[ArgColumn_]).GetDatum();
        if (datum.is_scalar()) {
            if (datum.scalar()->is_valid) {
                typedState->Sum_ += (filtered ? *filtered : batchLength) * Cast(datum.scalar_as<TInScalar>().value);
                typedState->Count_ += batchLength;
            }
        } else {
            const auto& array = datum.array();
            auto ptr = array->GetValues<TIn>(1);
            auto len = array->length;
            auto count = len - array->GetNullCount();
            if (!count) {
                return;
            }

            if (!filtered) {
                typedState->Count_ += count;
                auto sum = typedState->Sum_;
                if (array->GetNullCount() == 0) {
                    for (int64_t i = 0; i < len; ++i) {
                        sum += ptr[i];
                    }
                } else {
                    auto nullBitmapPtr = array->GetValues<uint8_t>(0, 0);
                    for (int64_t i = 0; i < len; ++i) {
                        ui64 fullIndex = i + array->offset;
                        // bit 1 -> mask 0xFF..FF, bit 0 -> mask 0x00..00
                        ui8 notNull = (nullBitmapPtr[fullIndex >> 3] >> (fullIndex & 0x07)) & 1;
                        sum += SelectArg<TIn>(notNull, ptr[i], 0);
                    }
                }

                typedState->Sum_ = sum;
            } else {
                const auto& filterDatum = TArrowBlock::From(columns[*FilterColumn_]).GetDatum();
                const auto& filterArray = filterDatum.array();
                MKQL_ENSURE(filterArray->GetNullCount() == 0, "Expected non-nullable bool column");
                const ui8* filterBitmap = filterArray->template GetValues<uint8_t>(1);

                auto sum = typedState->Sum_;
                ui64 count = typedState->Count_;
                if (array->GetNullCount() == 0) {
                    for (int64_t i = 0; i < len; ++i) {
                        ui8 filtered = filterBitmap[i];
                        sum += SelectArg<TIn>(filterBitmap[i], ptr[i], 0);
                        count += filtered;
                    }
                } else {
                    auto nullBitmapPtr = array->GetValues<uint8_t>(0, 0);
                    for (int64_t i = 0; i < len; ++i) {
                        ui64 fullIndex = i + array->offset;
                        ui8 notNullAndFiltered = ((nullBitmapPtr[fullIndex >> 3] >> (fullIndex & 0x07)) & 1) & filterBitmap[i];
                        sum += SelectArg<TIn>(notNullAndFiltered, ptr[i], 0);
                        count += notNullAndFiltered;
                    }
                }

                typedState->Sum_ = sum;
                typedState->Count_ = count;
            }
        }
    }

    NUdf::TUnboxedValue FinishOne(const void* state) final {
        auto typedState = MakeStateWrapper<TAvgState<TOut>>(state);
        if (!typedState->Count_) {
            return NUdf::TUnboxedValuePod();
        }

        NUdf::TUnboxedValue* items;
        auto arr = Ctx_.HolderFactory.CreateDirectArrayHolder(2, items);
        items[0] = NUdf::TUnboxedValuePod(TOut(typedState->Sum_));
        items[1] = NUdf::TUnboxedValuePod(typedState->Count_);
        return arr;
    }

private:
    ui32 ArgColumn_;
};

template <typename TIn, typename TOut>
class TAvgBlockAggregator<TCombineKeysTag, TIn, TOut> : public TCombineKeysTag::TBase {
public:
    using TBase = TCombineKeysTag::TBase;
    using TInScalar = typename TPrimitiveDataType<TIn>::TScalarResult;

    TAvgBlockAggregator(std::optional<ui32> filterColumn, ui32 argColumn, TType* outputType, TComputationContext& ctx)
        : TBase(sizeof(TAvgState<TOut>), filterColumn, ctx)
        , ArgColumn_(argColumn)
        , OutputType_(outputType)
    {
    }

    void InitKey(void* state, ui64 batchNum, const NUdf::TUnboxedValue* columns, ui64 row) final {
        TAvgState<TOut> st;
        memcpy(state, (void*)&st, sizeof(st));
        UpdateKey(state, batchNum, columns, row);
    }

    void DestroyState(void* state) noexcept final {
        static_assert(std::is_trivially_destructible<TAvgState<TOut>>::value);
        Y_UNUSED(state);
    }

    void UpdateKey(void* state, ui64 batchNum, const NUdf::TUnboxedValue* columns, ui64 row) final {
        Y_UNUSED(batchNum);
        auto typedState = MakeStateWrapper<TAvgState<TOut>>(state);
        const auto& datum = TArrowBlock::From(columns[ArgColumn_]).GetDatum();
        if (datum.is_scalar()) {
            if (datum.scalar()->is_valid) {
                typedState->Sum_ += Cast(datum.scalar_as<TInScalar>().value);
                typedState->Count_ += 1;
            }
        } else {
            const auto& array = datum.array();
            auto ptr = array->GetValues<TIn>(1);
            if (array->GetNullCount() == 0) {
                typedState->Sum_ += ptr[row];
                typedState->Count_ += 1;
            } else {
                auto nullBitmapPtr = array->GetValues<uint8_t>(0, 0);
                ui64 fullIndex = row + array->offset;
                ui8 notNull = (nullBitmapPtr[fullIndex >> 3] >> (fullIndex & 0x07)) & 1;
                typedState->Sum_ += SelectArg<TIn>(notNull, ptr[row], 0);
                typedState->Count_ += notNull;
            }
        }
    }

    std::unique_ptr<IAggColumnBuilder> MakeStateBuilder(ui64 size) final {
        return std::make_unique<TAvgStateColumnBuilder<TOut>>(size, OutputType_, Ctx_);
    }

private:
    const ui32 ArgColumn_;
    TType* const OutputType_;
};

template<typename TOut>
class TAvgBlockAggregatorOverState : public TFinalizeKeysTag::TBase {
public:
    using TBase = TFinalizeKeysTag::TBase;
    using TInScalar = typename TPrimitiveDataType<TOut>::TScalarResult;

    TAvgBlockAggregatorOverState(ui32 argColumn, TComputationContext& ctx)
        : TBase(sizeof(TAvgState<TOut>), {}, ctx)
        , ArgColumn_(argColumn)
    {
    }

    void LoadState(void* state, ui64 batchNum, const NUdf::TUnboxedValue* columns, ui64 row) final {
        TAvgState<TOut> st;
        memcpy(state, (void*)&st, sizeof(st));
        UpdateState(state, batchNum, columns, row);
    }

    void DestroyState(void* state) noexcept final {
        static_assert(std::is_trivially_destructible<TAvgState<TOut>>::value);
        Y_UNUSED(state);
    }

    void UpdateState(void* state, ui64 batchNum, const NUdf::TUnboxedValue* columns, ui64 row) final {
        Y_UNUSED(batchNum);
        auto typedState = MakeStateWrapper<TAvgState<TOut>>(state);
        const auto& datum = TArrowBlock::From(columns[ArgColumn_]).GetDatum();
        if (datum.is_scalar()) {
            if (datum.scalar()->is_valid) {
                const auto& structScalar = arrow::internal::checked_cast<const arrow::StructScalar&>(*datum.scalar());

                typedState->Sum_ += Cast(arrow::internal::checked_cast<const TInScalar&>(*structScalar.value[0]).value);
                typedState->Count_ += arrow::internal::checked_cast<const arrow::UInt64Scalar&>(*structScalar.value[1]).value;
            }
        } else {
            const auto& array = datum.array();
            auto sumPtr = array->child_data[0]->GetValues<TOut>(1);
            auto countPtr = array->child_data[1]->GetValues<ui64>(1);
            if (array->GetNullCount() == 0) {
                typedState->Sum_ += sumPtr[row];
                typedState->Count_ += countPtr[row];
            } else {
                auto nullBitmapPtr = array->GetValues<uint8_t>(0, 0);
                ui64 fullIndex = row + array->offset;
                // bit 1 -> mask 0xFF..FF, bit 0 -> mask 0x00..00
                auto bit = (nullBitmapPtr[fullIndex >> 3] >> (fullIndex & 0x07)) & 1;
                ui64 mask = -ui64(bit);
                typedState->Sum_ += sumPtr[row] * bit;
                typedState->Count_ += mask & countPtr[row];
            }
        }
    }

    std::unique_ptr<IAggColumnBuilder> MakeResultBuilder(ui64 size) final {
        return std::make_unique<TAvgResultColumnBuilder<TOut>>(size, Ctx_);
    }

private:
    const ui32 ArgColumn_;
};

template <typename TTag, bool IsNullable, bool IsScalar, typename TIn, typename TSum>
class TPreparedSumBlockAggregator : public TTag::TPreparedAggregator {
public:
    using TBase = typename TTag::TPreparedAggregator;
    using TStateType = TSumState<IsNullable, TSum>;

    TPreparedSumBlockAggregator(std::optional<ui32> filterColumn, ui32 argColumn, TType* dataType)
        : TBase(sizeof(TStateType))
        , FilterColumn_(filterColumn)
        , ArgColumn_(argColumn)
        , DataType_(dataType)
    {}

    std::unique_ptr<typename TTag::TAggregator> Make(TComputationContext& ctx) const final {
        return std::make_unique<TSumBlockAggregator<TTag, IsNullable, IsScalar, TIn, TSum>>(FilterColumn_, ArgColumn_, DataType_, ctx);
    }

private:
    const std::optional<ui32> FilterColumn_;
    const ui32 ArgColumn_;
    TType* const DataType_;
};

template<typename TTag, typename TIn, typename TSum>
std::unique_ptr<typename TTag::TPreparedAggregator> PrepareSumFixed(TType* type, bool isOptional, bool isScalar, std::optional<ui32> filterColumn, ui32 argColumn) {
    if (isScalar) {
        if (isOptional) {
            return std::make_unique<TPreparedSumBlockAggregator<TTag, true, true, TIn, TSum>>(filterColumn, argColumn, type);
        }
        return std::make_unique<TPreparedSumBlockAggregator<TTag, false, true, TIn, TSum>>(filterColumn, argColumn, type);
    }
    if (isOptional) {
        return std::make_unique<TPreparedSumBlockAggregator<TTag, true, false, TIn, TSum>>(filterColumn, argColumn, type);
    }
    return std::make_unique<TPreparedSumBlockAggregator<TTag, false, false, TIn, TSum>>(filterColumn, argColumn, type);
}

template <typename TTag>
std::unique_ptr<typename TTag::TPreparedAggregator> PrepareSum(TTupleType* tupleType, std::optional<ui32> filterColumn, ui32 argColumn, const TTypeEnvironment& env) {
    auto blockType = AS_TYPE(TBlockType, tupleType->GetElementType(argColumn));
    auto argType = blockType->GetItemType();
    bool isOptional;
    auto dataType = UnpackOptionalData(argType, isOptional);
    bool isScalar = blockType->GetShape() == TBlockType::EShape::Scalar;


    TType* sumRetType = nullptr;
    const auto& typeInfo = NYql::NUdf::GetDataTypeInfo(*dataType->GetDataSlot());
    if (typeInfo.Features & NYql::NUdf::EDataTypeFeatures::SignedIntegralType) {
        sumRetType = TDataType::Create(NUdf::TDataType<i64>::Id, env);
    } else if (typeInfo.Features & NYql::NUdf::EDataTypeFeatures::UnsignedIntegralType) {
        sumRetType = TDataType::Create(NUdf::TDataType<ui64>::Id, env);
    } else if (*dataType->GetDataSlot() == NUdf::EDataSlot::Decimal
        || *dataType->GetDataSlot() == NUdf::EDataSlot::Interval) {
        sumRetType = TDataType::Create(NUdf::TDataType<NUdf::TDecimal>::Id, env);
    } else {
        Y_ENSURE(typeInfo.Features & NYql::NUdf::EDataTypeFeatures::FloatType);
        sumRetType = dataType;
    }
    sumRetType = TOptionalType::Create(sumRetType, env);


    switch (*dataType->GetDataSlot()) {
    case NUdf::EDataSlot::Int8:
        return PrepareSumFixed<TTag, i8, i64>(sumRetType, isOptional, isScalar, filterColumn, argColumn);
    case NUdf::EDataSlot::Uint8:
        return PrepareSumFixed<TTag, ui8, ui64>(sumRetType, isOptional, isScalar, filterColumn, argColumn);
    case NUdf::EDataSlot::Int16:
        return PrepareSumFixed<TTag, i16, i64>(sumRetType, isOptional, isScalar, filterColumn, argColumn);
    case NUdf::EDataSlot::Uint16:
        return PrepareSumFixed<TTag, ui16, ui64>(sumRetType, isOptional, isScalar, filterColumn, argColumn);
    case NUdf::EDataSlot::Int32:
        return PrepareSumFixed<TTag, i32, i64>(sumRetType, isOptional, isScalar, filterColumn, argColumn);
    case NUdf::EDataSlot::Uint32:
        return PrepareSumFixed<TTag, ui32, ui64>(sumRetType, isOptional, isScalar, filterColumn, argColumn);
    case NUdf::EDataSlot::Int64:
        return PrepareSumFixed<TTag, i64, i64>(sumRetType, isOptional, isScalar, filterColumn, argColumn);
    case NUdf::EDataSlot::Uint64:
        return PrepareSumFixed<TTag, ui64, ui64>(sumRetType, isOptional, isScalar, filterColumn, argColumn);
    case NUdf::EDataSlot::Float:
        return PrepareSumFixed<TTag, float, float>(sumRetType, isOptional, isScalar, filterColumn, argColumn);
    case NUdf::EDataSlot::Double:
        return PrepareSumFixed<TTag, double, double>(sumRetType, isOptional, isScalar, filterColumn, argColumn);
    case NUdf::EDataSlot::Interval:
        return PrepareSumFixed<TTag, i64, NYql::NDecimal::TInt128>(sumRetType, isOptional, isScalar, filterColumn, argColumn);
    case NUdf::EDataSlot::Decimal:
        return PrepareSumFixed<TTag, NYql::NDecimal::TInt128, NYql::NDecimal::TInt128>(sumRetType, isOptional, isScalar, filterColumn, argColumn);
    default:
        throw yexception() << "Unsupported SUM input type";
    }
}

class TBlockSumFactory : public IBlockAggregatorFactory {
public:
    std::unique_ptr<TCombineAllTag::TPreparedAggregator> PrepareCombineAll(
        TTupleType* tupleType,
        std::optional<ui32> filterColumn,
        const std::vector<ui32>& argsColumns,
        const TTypeEnvironment& env) const final
    {
        return PrepareSum<TCombineAllTag>(tupleType, filterColumn, argsColumns[0], env);
    }

    std::unique_ptr<TCombineKeysTag::TPreparedAggregator> PrepareCombineKeys(
        TTupleType* tupleType,
        const std::vector<ui32>& argsColumns,
        const TTypeEnvironment& env) const final
    {
        return PrepareSum<TCombineKeysTag>(tupleType, std::optional<ui32>(), argsColumns[0], env);
    }

    std::unique_ptr<TFinalizeKeysTag::TPreparedAggregator> PrepareFinalizeKeys(
        TTupleType* tupleType,
        const std::vector<ui32>& argsColumns,
        const TTypeEnvironment& env,
        TType* returnType) const final
    {
        Y_UNUSED(returnType);
        return PrepareSum<TFinalizeKeysTag>(tupleType, std::optional<ui32>(), argsColumns[0], env);
    }
};

template <typename TTag, typename TIn, typename TOut>
class TPreparedAvgBlockAggregator : public TTag::TPreparedAggregator {
public:
    using TBase = typename TTag::TPreparedAggregator;

    TPreparedAvgBlockAggregator(std::optional<ui32> filterColumn, ui32 argColumn, TType* outputType)
        : TBase(sizeof(TAvgState<TOut>))
        , FilterColumn_(filterColumn)
        , ArgColumn_(argColumn)
        , OutputType_(outputType)
    {}

    std::unique_ptr<typename TTag::TAggregator> Make(TComputationContext& ctx) const final {
        return std::make_unique<TAvgBlockAggregator<TTag, TIn, TOut>>(FilterColumn_, ArgColumn_, OutputType_, ctx);
    }

private:
    const std::optional<ui32> FilterColumn_;
    const ui32 ArgColumn_;
    TType* const OutputType_;
};

template<typename TOut>
class TPreparedAvgBlockAggregatorOverState : public TFinalizeKeysTag::TPreparedAggregator {
public:
    using TBase = TFinalizeKeysTag::TPreparedAggregator;

    TPreparedAvgBlockAggregatorOverState(ui32 argColumn)
        : TBase(sizeof(TAvgState<TOut>))
        , ArgColumn_(argColumn)
    {}

    std::unique_ptr<typename TFinalizeKeysTag::TAggregator> Make(TComputationContext& ctx) const final {
        return std::make_unique<TAvgBlockAggregatorOverState<TOut>>(ArgColumn_, ctx);
    }

private:
    const ui32 ArgColumn_;
};

template <typename TTag>
std::unique_ptr<typename TTag::TPreparedAggregator> PrepareAvg(TTupleType* tupleType, std::optional<ui32> filterColumn, ui32 argColumn, const TTypeEnvironment& env);

template <typename TTag>
std::unique_ptr<typename TTag::TPreparedAggregator> PrepareAvgOverInput(TTupleType* tupleType, std::optional<ui32> filterColumn, ui32 argColumn, const TTypeEnvironment& env) {
    auto doubleType = TDataType::Create(NUdf::TDataType<double>::Id, env);
    auto ui64Type = TDataType::Create(NUdf::TDataType<ui64>::Id, env);
    auto decimalType = TDataType::Create(NUdf::TDataType<NUdf::TDecimal>::Id, env);
    TVector<TType*> tupleElements = { doubleType, ui64Type };
    auto avgRetType = TOptionalType::Create(TTupleType::Create(2, tupleElements.data(), env), env);

    TVector<TType*> tupleDecimalElements = { decimalType, ui64Type };
    auto avgRetDecimalType = TOptionalType::Create(TTupleType::Create(2, tupleDecimalElements.data(), env), env);

    auto argType = AS_TYPE(TBlockType, tupleType->GetElementType(argColumn))->GetItemType();
    bool isOptional;
    auto dataType = UnpackOptionalData(argType, isOptional);
    switch (*dataType->GetDataSlot()) {
    case NUdf::EDataSlot::Int8:
        return std::make_unique<TPreparedAvgBlockAggregator<TTag, i8, double>>(filterColumn, argColumn, avgRetType);
    case NUdf::EDataSlot::Uint8:
    case NUdf::EDataSlot::Bool:
        return std::make_unique<TPreparedAvgBlockAggregator<TTag, ui8, double>>(filterColumn, argColumn, avgRetType);
    case NUdf::EDataSlot::Int16:
        return std::make_unique<TPreparedAvgBlockAggregator<TTag, i16, double>>(filterColumn, argColumn, avgRetType);
    case NUdf::EDataSlot::Uint16:
        return std::make_unique<TPreparedAvgBlockAggregator<TTag, ui16, double>>(filterColumn, argColumn, avgRetType);
    case NUdf::EDataSlot::Int32:
        return std::make_unique<TPreparedAvgBlockAggregator<TTag, i32, double>>(filterColumn, argColumn, avgRetType);
    case NUdf::EDataSlot::Uint32:
        return std::make_unique<TPreparedAvgBlockAggregator<TTag, ui32, double>>(filterColumn, argColumn, avgRetType);
    case NUdf::EDataSlot::Int64:
        return std::make_unique<TPreparedAvgBlockAggregator<TTag, i64, double>>(filterColumn, argColumn, avgRetType);
    case NUdf::EDataSlot::Uint64:
        return std::make_unique<TPreparedAvgBlockAggregator<TTag, ui64, double>>(filterColumn, argColumn, avgRetType);
    case NUdf::EDataSlot::Float:
        return std::make_unique<TPreparedAvgBlockAggregator<TTag, float, double>>(filterColumn, argColumn, avgRetType);
    case NUdf::EDataSlot::Double:
        return std::make_unique<TPreparedAvgBlockAggregator<TTag, double, double>>(filterColumn, argColumn, avgRetType);
    case NUdf::EDataSlot::Interval:
        return std::make_unique<TPreparedAvgBlockAggregator<TTag, i64, NYql::NDecimal::TInt128>>(filterColumn, argColumn, avgRetDecimalType);
    case NUdf::EDataSlot::Decimal:
        return std::make_unique<TPreparedAvgBlockAggregator<TTag, NYql::NDecimal::TInt128, NYql::NDecimal::TInt128>>(filterColumn, argColumn, avgRetDecimalType);
    default:
        throw yexception() << "Unsupported AVG input type";
    }
}

template <>
std::unique_ptr<typename TCombineAllTag::TPreparedAggregator> PrepareAvg<TCombineAllTag>(TTupleType* tupleType, std::optional<ui32> filterColumn, ui32 argColumn, const TTypeEnvironment& env) {
    return PrepareAvgOverInput<TCombineAllTag>(tupleType, filterColumn, argColumn, env);
}

template <>
std::unique_ptr<typename TCombineKeysTag::TPreparedAggregator> PrepareAvg<TCombineKeysTag>(TTupleType* tupleType, std::optional<ui32> filterColumn, ui32 argColumn, const TTypeEnvironment& env) {
    return PrepareAvgOverInput<TCombineKeysTag>(tupleType, filterColumn, argColumn, env);
}

template <>
std::unique_ptr<typename TFinalizeKeysTag::TPreparedAggregator> PrepareAvg<TFinalizeKeysTag>(TTupleType* tupleType, std::optional<ui32> filterColumn, ui32 argColumn, const TTypeEnvironment& env) {
    Y_UNUSED(filterColumn);
    Y_UNUSED(env);

    auto argType = AS_TYPE(TBlockType, tupleType->GetElementType(argColumn))->GetItemType();
    bool isOptional;
    auto aggTupleType = UnpackOptional(argType, isOptional);
    MKQL_ENSURE(aggTupleType->IsTuple(),
        "Expected tuple or optional of tuple, actual: " << PrintNode(argType, true));
    auto dataType = UnpackOptionalData(AS_TYPE(TTupleType, aggTupleType)->GetElementType(0), isOptional);

    switch (*dataType->GetDataSlot()) {
    case NUdf::EDataSlot::Decimal:
        return std::make_unique<TPreparedAvgBlockAggregatorOverState<NYql::NDecimal::TInt128>>(argColumn);
    case NUdf::EDataSlot::Double:
        return std::make_unique<TPreparedAvgBlockAggregatorOverState<double>>(argColumn);
    default:
        throw yexception() << "Unsupported Finalize input type";
    }
}

class TBlockAvgFactory : public IBlockAggregatorFactory {
public:
    std::unique_ptr<TCombineAllTag::TPreparedAggregator> PrepareCombineAll(
        TTupleType* tupleType,
        std::optional<ui32> filterColumn,
        const std::vector<ui32>& argsColumns,
        const TTypeEnvironment& env) const final {
        return PrepareAvg<TCombineAllTag>(tupleType, filterColumn, argsColumns[0], env);
    }

    std::unique_ptr<TCombineKeysTag::TPreparedAggregator> PrepareCombineKeys(
        TTupleType* tupleType,
        const std::vector<ui32>& argsColumns,
        const TTypeEnvironment& env) const final {
        return PrepareAvg<TCombineKeysTag>(tupleType, std::optional<ui32>(), argsColumns[0], env);
    }

    std::unique_ptr<TFinalizeKeysTag::TPreparedAggregator> PrepareFinalizeKeys(
        TTupleType* tupleType,
        const std::vector<ui32>& argsColumns,
        const TTypeEnvironment& env,
        TType* returnType) const final {
        Y_UNUSED(returnType);
        return PrepareAvg<TFinalizeKeysTag>(tupleType, std::optional<ui32>(), argsColumns[0], env);
    }
};

}

std::unique_ptr<IBlockAggregatorFactory> MakeBlockSumFactory() {
    return std::make_unique<TBlockSumFactory>();
}

std::unique_ptr<IBlockAggregatorFactory> MakeBlockAvgFactory() {
    return std::make_unique<TBlockAvgFactory>();
}

}
}
