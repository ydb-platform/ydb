#include "mkql_block_agg_minmax.h"
#include "mkql_block_agg_state_helper.h"

#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>

#include <ydb/library/yql/minikql/computation/mkql_block_builder.h>
#include <ydb/library/yql/minikql/computation/mkql_block_reader.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>

#include <ydb/library/yql/minikql/arrow/arrow_defs.h>
#include <ydb/library/yql/minikql/arrow/arrow_util.h>
#include <ydb/library/yql/minikql/arrow/mkql_bit_utils.h>

#include <ydb/library/yql/public/udf/arrow/block_item_comparator.h>

#include <arrow/scalar.h>
#include <arrow/array/builder_primitive.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

template<typename T>
inline bool AggLess(T a, T b) {
    if constexpr (std::is_floating_point<T>::value) {
        if (std::isunordered(a, b)) {
            // biggest fp value in agg ordering is NaN
            return std::isnan(a) < std::isnan(b);
        }
    }
    return a < b;
}

template <bool IsMin, typename T>
inline T UpdateMinMax(T x, T y) {
    if constexpr (IsMin) {
        return AggLess(x, y) ? x : y;
    } else {
        return AggLess(y, x) ? x : y;
    }
}

template<bool IsMin, typename T>
inline void UpdateMinMax(TMaybe<T>& state, bool& stateUpdated, T value) {
    if constexpr (IsMin) {
        if (!state || AggLess(value, *state)) {
            state = value;
            stateUpdated = true;
        }
    } else {
        if (!state || AggLess(*state, value)) {
            state = value;
            stateUpdated = true;
        }
    }
}

template<bool IsMin>
inline void UpdateMinMax(NYql::NUdf::IBlockItemComparator& comparator, TBlockItem& state, bool& stateUpdated, TBlockItem value) {
    if constexpr (IsMin) {
        if (!state || comparator.Less(value, state)) {
            state = value;
            stateUpdated = true;
        }
    } else {
        if (!state || comparator.Less(state, value)) {
            state = value;
            stateUpdated = true;
        }
    }
}

template<typename TTag, typename TString, bool IsMin>
class TMinMaxBlockStringAggregator;

template<typename TTag, bool IsNullable, bool IsScalar, typename TIn, bool IsMin>
class TMinMaxBlockFixedAggregator;

template<typename TTag, bool IsMin>
class TMinMaxBlockGenericAggregator;

template <bool IsNullable, typename TIn, bool IsMin>
struct TState;

template<typename TIn, bool IsMin>
constexpr TIn InitialStateValue() {
    if constexpr (std::is_floating_point<TIn>::value) {
        static_assert(std::numeric_limits<TIn>::has_infinity && std::numeric_limits<TIn>::has_quiet_NaN);
        if constexpr (IsMin) {
            // biggest fp value in agg ordering is NaN
            return std::numeric_limits<TIn>::quiet_NaN();
        } else {
            return -std::numeric_limits<TIn>::infinity();
        }
    } else if constexpr (std::is_same_v<TIn, NYql::NDecimal::TInt128>) {
        if constexpr (IsMin) {
            return NYql::NDecimal::Nan(); 
        } else {
            return -NYql::NDecimal::Inf();
        }
    } else if constexpr (std::is_arithmetic<TIn>::value) {
        if constexpr (IsMin) {
            return std::numeric_limits<TIn>::max();
        } else {
            return std::numeric_limits<TIn>::min();
        }
    } else {
        static_assert(std::is_arithmetic<TIn>::value);
    }
}

template <typename TIn, bool IsMin>
struct TState<true, TIn, IsMin> {
    TIn Value = InitialStateValue<TIn, IsMin>();
    ui8 IsValid = 0;
};

template <typename TIn, bool IsMin>
struct TState<false, TIn, IsMin> {
    TIn Value = InitialStateValue<TIn, IsMin>();
};

using TGenericState = NUdf::TUnboxedValuePod;

template <bool IsNullable, typename TIn, bool IsMin>
class TColumnBuilder : public IAggColumnBuilder {
    using TBuilder = typename NYql::NUdf::TFixedSizeArrayBuilder<TIn, IsNullable>;
    using TStateType = TState<IsNullable, TIn, IsMin>;
public:
    TColumnBuilder(ui64 size, TType* type, TComputationContext& ctx)
        : Builder_(TTypeInfoHelper(), type, ctx.ArrowMemoryPool, size)
        , Ctx_(ctx)
    {
    }

    void Add(const void* state) final {
        auto typedState = MakeStateWrapper<TStateType>(state);
        if constexpr (IsNullable) {
            if (!typedState->IsValid) {
                Builder_.Add(TBlockItem());
                return;
            }
        }
        Builder_.Add(TBlockItem(typedState->Value));
    }

    NUdf::TUnboxedValue Build() final {
        return Ctx_.HolderFactory.CreateArrowBlock(Builder_.Build(true));
    }

private:
    TBuilder Builder_;
    TComputationContext& Ctx_;
};

class TGenericColumnBuilder : public IAggColumnBuilder {
public:
    TGenericColumnBuilder(ui64 size, TType* columnType, TComputationContext& ctx)
        : Builder_(MakeArrayBuilder(TTypeInfoHelper(), columnType, ctx.ArrowMemoryPool, size, &ctx.Builder->GetPgBuilder()))
        , Ctx_(ctx)
    {
    }

    void Add(const void* state) final {
        Builder_->Add(*static_cast<const TGenericState*>(state));
    }

    NUdf::TUnboxedValue Build() final {
        return Ctx_.HolderFactory.CreateArrowBlock(Builder_->Build(true));
    }

private:
    const std::unique_ptr<IArrayBuilder> Builder_;
    TComputationContext& Ctx_;
};

template <bool IsMin>
void PushValueToState(TGenericState* typedState, const arrow::Datum& datum, ui64 row, IBlockReader& reader,
    IBlockItemConverter& converter, NYql::NUdf::IBlockItemComparator& comparator, TComputationContext& ctx)
{
    TBlockItem stateItem;
    bool stateChanged = false;
    if (datum.is_scalar()) {
        if (datum.scalar()->is_valid) {
            stateItem = reader.GetScalarItem(*datum.scalar());
            stateChanged = true;
        }
    } else {
        if (*typedState) {
            stateItem = converter.MakeItem(*typedState);
        }

        const auto& array = datum.array();

        TBlockItem curr = reader.GetItem(*array, row);
        if (curr) {
            UpdateMinMax<IsMin>(comparator, stateItem, stateChanged, curr);
        }
    }

    if (stateChanged) {
        typedState->DeleteUnreferenced();
        *typedState = converter.MakeValue(stateItem, ctx.HolderFactory);
    }
}

template<bool IsMin>
class TMinMaxBlockGenericAggregator<TCombineAllTag, IsMin> : public TCombineAllTag::TBase {
public:
    using TBase = TCombineAllTag::TBase;

    TMinMaxBlockGenericAggregator(TType* type, std::optional<ui32> filterColumn, ui32 argColumn, TComputationContext& ctx)
        : TBase(sizeof(TGenericState), filterColumn, ctx)
        , ArgColumn_(argColumn)
        , ReaderOne_(MakeBlockReader(TTypeInfoHelper(), type))
        , ReaderTwo_(MakeBlockReader(TTypeInfoHelper(), type))
        , Converter_(MakeBlockItemConverter(TTypeInfoHelper(), type, ctx.Builder->GetPgBuilder()))
        , Compare_(TBlockTypeHelper().MakeComparator(type))
    {
    }

    void InitState(void* state) final {
        new(state) TGenericState();
    }

    void DestroyState(void* state) noexcept final {
        auto typedState = static_cast<TGenericState*>(state);
        typedState->DeleteUnreferenced();
        *typedState = TGenericState();
    }

    void AddMany(void* state, const NUdf::TUnboxedValue* columns, ui64 batchLength, std::optional<ui64> filtered) final {
        TGenericState& typedState = *static_cast<TGenericState*>(state);
        Y_UNUSED(batchLength);
        const auto& datum = TArrowBlock::From(columns[ArgColumn_]).GetDatum();

        IBlockReader* currReader = ReaderOne_.get();
        IBlockReader* stateReader = ReaderTwo_.get();

        TBlockItem stateItem;
        bool stateChanged = false;
        if (datum.is_scalar()) {
            if (datum.scalar()->is_valid) {
                stateItem = currReader->GetScalarItem(*datum.scalar());
                stateChanged = true;
            }
        } else {
            if (typedState) {
                stateItem = Converter_->MakeItem(typedState);
            }

            const auto& array = datum.array();
            auto len = array->length;

            const ui8* filterBitmap = nullptr;
            if (filtered) {
                const auto& filterDatum = TArrowBlock::From(columns[*FilterColumn_]).GetDatum();
                const auto& filterArray = filterDatum.array();
                MKQL_ENSURE(filterArray->GetNullCount() == 0, "Expected non-nullable bool column");
                filterBitmap = filterArray->template GetValues<uint8_t>(1);
            }
            auto& comparator = *Compare_;
            for (auto i = 0; i < len; ++i) {
                TBlockItem curr = currReader->GetItem(*array, i);
                if (curr && (!filterBitmap || filterBitmap[i])) {
                    bool changed = false;
                    UpdateMinMax<IsMin>(comparator, stateItem, changed, curr);
                    if (changed) {
                        std::swap(currReader, stateReader);
                        stateChanged = true;
                    }
                }
            }
        }

        if (stateChanged) {
            typedState.DeleteUnreferenced();
            typedState = Converter_->MakeValue(stateItem, Ctx_.HolderFactory);
        }
    }

    NUdf::TUnboxedValue FinishOne(const void *state) final {
        auto typedState = *static_cast<const TGenericState *>(state);
        return typedState;
    }

private:
    const ui32 ArgColumn_;
    const std::unique_ptr<IBlockReader> ReaderOne_;
    const std::unique_ptr<IBlockReader> ReaderTwo_;
    const std::unique_ptr<IBlockItemConverter> Converter_;
    const NYql::NUdf::IBlockItemComparator::TPtr Compare_;
};

template<bool IsMin>
class TMinMaxBlockGenericAggregator<TCombineKeysTag, IsMin> : public TCombineKeysTag::TBase {
public:
    using TBase = TCombineKeysTag::TBase;

    TMinMaxBlockGenericAggregator(TType* type, std::optional<ui32> filterColumn, ui32 argColumn, TComputationContext& ctx)
        : TBase(sizeof(TGenericState), filterColumn, ctx)
        , ArgColumn_(argColumn)
        , Type_(type)
        , Reader_(MakeBlockReader(TTypeInfoHelper(), type))
        , Converter_(MakeBlockItemConverter(TTypeInfoHelper(), type, ctx.Builder->GetPgBuilder()))
        , Compare_(TBlockTypeHelper().MakeComparator(type))
    {
    }

    void InitKey(void* state, ui64 batchNum, const NUdf::TUnboxedValue* columns, ui64 row) final {
        new(state) TGenericState();
        UpdateKey(state, batchNum, columns, row);
    }

    void DestroyState(void* state) noexcept final {
        auto typedState = static_cast<TGenericState*>(state);
        typedState->DeleteUnreferenced();
        *typedState = TGenericState();
    }

    void UpdateKey(void* state, ui64 batchNum, const NUdf::TUnboxedValue* columns, ui64 row) final {
        Y_UNUSED(batchNum);
        auto typedState = static_cast<TGenericState*>(state);
        const auto& datum = TArrowBlock::From(columns[ArgColumn_]).GetDatum();
        PushValueToState<IsMin>(typedState, datum, row, *Reader_, *Converter_, *Compare_, Ctx_);
    }

    std::unique_ptr<IAggColumnBuilder> MakeStateBuilder(ui64 size) final {
        return std::make_unique<TGenericColumnBuilder>(size, Type_, Ctx_);
    }

private:
    const ui32 ArgColumn_;
    TType* const Type_;
    const std::unique_ptr<IBlockReader> Reader_;
    const std::unique_ptr<IBlockItemConverter> Converter_;
    const NYql::NUdf::IBlockItemComparator::TPtr Compare_;
};

template<bool IsMin>
class TMinMaxBlockGenericAggregator<TFinalizeKeysTag, IsMin> : public TFinalizeKeysTag::TBase {
public:
    using TBase = TFinalizeKeysTag::TBase;

    TMinMaxBlockGenericAggregator(TType* type, std::optional<ui32> filterColumn, ui32 argColumn, TComputationContext& ctx)
        : TBase(sizeof(TGenericState), filterColumn, ctx)
        , ArgColumn_(argColumn)
        , Type_(type)
        , Reader_(MakeBlockReader(TTypeInfoHelper(), type))
        , Converter_(MakeBlockItemConverter(TTypeInfoHelper(), type, ctx.Builder->GetPgBuilder()))
        , Compare_(TBlockTypeHelper().MakeComparator(type))
    {
    }

    void LoadState(void* state, ui64 batchNum, const NUdf::TUnboxedValue* columns, ui64 row) final {
        new(state) TGenericState();
        UpdateState(state, batchNum, columns, row);
    }

    void DestroyState(void* state) noexcept final {
        auto typedState = static_cast<TGenericState*>(state);
        typedState->DeleteUnreferenced();
        *typedState = TGenericState();
    }

    void UpdateState(void* state, ui64 batchNum, const NUdf::TUnboxedValue* columns, ui64 row) final {
        Y_UNUSED(batchNum);
        auto typedState = static_cast<TGenericState*>(state);
        const auto& datum = TArrowBlock::From(columns[ArgColumn_]).GetDatum();
        PushValueToState<IsMin>(typedState, datum, row, *Reader_, *Converter_, *Compare_, Ctx_);
    }

    std::unique_ptr<IAggColumnBuilder> MakeResultBuilder(ui64 size) final {
        return std::make_unique<TGenericColumnBuilder>(size, Type_, Ctx_);
    }

private:
    const ui32 ArgColumn_;
    TType* const Type_;
    const std::unique_ptr<IBlockReader> Reader_;
    const std::unique_ptr<IBlockItemConverter> Converter_;
    const NYql::NUdf::IBlockItemComparator::TPtr Compare_;
};

template <typename TStringType, bool IsMin>
void PushValueToState(TGenericState* typedState, const arrow::Datum& datum, ui64 row) {
    using TOffset = typename TPrimitiveDataType<TStringType>::TResult::offset_type;;

    TMaybe<NUdf::TStringRef> currentState;
    if (*typedState) {
        currentState = typedState->AsStringRef();
    }

    bool stateUpdated = false;
    if (datum.is_scalar()) {
        if (datum.scalar()->is_valid) {
            auto buffer = arrow::internal::checked_cast<const arrow::BaseBinaryScalar&>(*datum.scalar()).value;
            const char* data = reinterpret_cast<const char*>(buffer->data());
            auto value = NUdf::TStringRef(data, buffer->size());
            UpdateMinMax<IsMin>(currentState, stateUpdated, value);
        }
    } else {
        const auto& array = datum.array();

        const TOffset* offsets = array->GetValues<TOffset>(1);
        const char* data = array->GetValues<char>(2, 0);

        if (array->GetNullCount() == 0) {
            auto value = NUdf::TStringRef(data + offsets[row], offsets[row + 1] - offsets[row]);
            UpdateMinMax<IsMin>(currentState, stateUpdated, value);
        } else {
            auto nullBitmapPtr = array->GetValues<uint8_t>(0, 0);
            ui64 fullIndex = row + array->offset;
            if ((nullBitmapPtr[fullIndex >> 3] >> (fullIndex & 0x07)) & 1) {
                auto value = NUdf::TStringRef(data + offsets[row], offsets[row + 1] - offsets[row]);
                UpdateMinMax<IsMin>(currentState, stateUpdated, value);
            }
        }
    }

    if (stateUpdated) {
        auto newState = MakeString(*currentState);
        typedState->DeleteUnreferenced();
        *typedState = std::move(newState);
    }
}

template<typename TStringType, bool IsMin>
class TMinMaxBlockStringAggregator<TCombineAllTag, TStringType, IsMin> : public TCombineAllTag::TBase {
public:
    using TBase = TCombineAllTag::TBase;
    using TOffset = typename TPrimitiveDataType<TStringType>::TResult::offset_type;

    TMinMaxBlockStringAggregator(TType* type, std::optional<ui32> filterColumn, ui32 argColumn, TComputationContext& ctx)
        : TBase(sizeof(TGenericState), filterColumn, ctx)
        , ArgColumn_(argColumn)
    {
        Y_UNUSED(type);
    }

    void InitState(void* state) final {
        new(state) TGenericState();
    }

    void DestroyState(void* state) noexcept final {
        auto typedState = static_cast<TGenericState*>(state);
        typedState->DeleteUnreferenced();
        *typedState = TGenericState();
    }

    void AddMany(void* state, const NUdf::TUnboxedValue* columns, ui64 batchLength, std::optional<ui64> filtered) final {
        TGenericState& typedState = *static_cast<TGenericState*>(state);
        Y_UNUSED(batchLength);
        const auto& datum = TArrowBlock::From(columns[ArgColumn_]).GetDatum();

        TMaybe<NUdf::TStringRef> currentState;
        if (typedState) {
            currentState = typedState.AsStringRef();
        }
        bool stateUpdated = false;
        if (datum.is_scalar()) {
            if (datum.scalar()->is_valid) {
                auto buffer = arrow::internal::checked_cast<const arrow::BaseBinaryScalar&>(*datum.scalar()).value;
                const char* data = reinterpret_cast<const char*>(buffer->data());
                auto value = NUdf::TStringRef(data, buffer->size());
                UpdateMinMax<IsMin>(currentState, stateUpdated, value);
            }
        } else {
            const auto& array = datum.array();
            auto len = array->length;
            auto count = len - array->GetNullCount();
            if (!count) {
                return;
            }

            const TOffset* offsets = array->GetValues<TOffset>(1);
            const char* data = array->GetValues<char>(2, 0);
            if (!filtered) {
                if (array->GetNullCount() == 0) {
                    for (int64_t i = 0; i < len; ++i) {
                        NUdf::TStringRef value(data + offsets[i], offsets[i + 1] - offsets[i]);
                        UpdateMinMax<IsMin>(currentState, stateUpdated, value);
                    }
                } else {
                    auto nullBitmapPtr = array->GetValues<uint8_t>(0, 0);
                    for (int64_t i = 0; i < len; ++i) {
                        ui64 fullIndex = i + array->offset;
                        if ((nullBitmapPtr[fullIndex >> 3] >> (fullIndex & 0x07)) & 1) {
                            NUdf::TStringRef value(data + offsets[i], offsets[i + 1] - offsets[i]);
                            UpdateMinMax<IsMin>(currentState, stateUpdated, value);
                        }
                    }
                }
            } else {
                const auto& filterDatum = TArrowBlock::From(columns[*FilterColumn_]).GetDatum();
                const auto& filterArray = filterDatum.array();
                MKQL_ENSURE(filterArray->GetNullCount() == 0, "Expected non-nullable bool column");
                const ui8* filterBitmap = filterArray->template GetValues<uint8_t>(1);
                if (array->GetNullCount() == 0) {
                    for (int64_t i = 0; i < len; ++i) {
                        if (filterBitmap[i]) {
                            NUdf::TStringRef value(data + offsets[i], offsets[i + 1] - offsets[i]);
                            UpdateMinMax<IsMin>(currentState, stateUpdated, value);
                        }
                    }
                } else {
                    auto nullBitmapPtr = array->GetValues<uint8_t>(0, 0);
                    for (int64_t i = 0; i < len; ++i) {
                        ui64 fullIndex = i + array->offset;
                        if (filterBitmap[i] && ((nullBitmapPtr[fullIndex >> 3] >> (fullIndex & 0x07)) & 1)) {
                            NUdf::TStringRef value(data + offsets[i], offsets[i + 1] - offsets[i]);
                            UpdateMinMax<IsMin>(currentState, stateUpdated, value);
                        }
                    }
                }
            }
        }

        if (stateUpdated) {
            auto newState = MakeString(*currentState);
            typedState.DeleteUnreferenced();
            typedState = std::move(newState);
        }
    }

    NUdf::TUnboxedValue FinishOne(const void* state) final {
        auto typedState = *static_cast<const TGenericState*>(state);
        return typedState;
    }


private:
    const ui32 ArgColumn_;
};

template<typename TStringType, bool IsMin>
class TMinMaxBlockStringAggregator<TCombineKeysTag, TStringType, IsMin> : public TCombineKeysTag::TBase {
public:
    using TBase = TCombineKeysTag::TBase;

    TMinMaxBlockStringAggregator(TType* type, std::optional<ui32> filterColumn, ui32 argColumn, TComputationContext& ctx)
        : TBase(sizeof(TGenericState), filterColumn, ctx)
        , ArgColumn_(argColumn)
        , Type_(type)
    {
    }

    void InitKey(void* state, ui64 batchNum, const NUdf::TUnboxedValue* columns, ui64 row) final {
        new(state) TGenericState();
        UpdateKey(state, batchNum, columns, row);
    }

    void DestroyState(void* state) noexcept final {
        auto typedState = static_cast<TGenericState*>(state);
        typedState->DeleteUnreferenced();
        *typedState = TGenericState();
    }

    void UpdateKey(void* state, ui64 batchNum, const NUdf::TUnboxedValue* columns, ui64 row) final {
        Y_UNUSED(batchNum);
        auto typedState = static_cast<TGenericState*>(state);
        const auto& datum = TArrowBlock::From(columns[ArgColumn_]).GetDatum();
        PushValueToState<TStringType, IsMin>(typedState, datum, row);
    }

    std::unique_ptr<IAggColumnBuilder> MakeStateBuilder(ui64 size) final {
        return std::make_unique<TGenericColumnBuilder>(size, Type_, Ctx_);
    }

private:
    const ui32 ArgColumn_;
    TType* const Type_;
};

template<typename TStringType, bool IsMin>
class TMinMaxBlockStringAggregator<TFinalizeKeysTag, TStringType, IsMin> : public TFinalizeKeysTag::TBase {
public:
    using TBase = TFinalizeKeysTag::TBase;

    TMinMaxBlockStringAggregator(TType* type, std::optional<ui32> filterColumn, ui32 argColumn, TComputationContext& ctx)
        : TBase(sizeof(TGenericState), filterColumn, ctx)
        , ArgColumn_(argColumn)
        , Type_(type)
    {
    }

    void LoadState(void* state, ui64 batchNum, const NUdf::TUnboxedValue* columns, ui64 row) final {
        new(state) TGenericState();
        UpdateState(state, batchNum, columns, row);
    }

    void DestroyState(void* state) noexcept final {
        auto typedState = static_cast<TGenericState*>(state);
        typedState->DeleteUnreferenced();
        *typedState = TGenericState();
    }

    void UpdateState(void* state, ui64 batchNum, const NUdf::TUnboxedValue* columns, ui64 row) final {
        Y_UNUSED(batchNum);
        auto typedState = static_cast<TGenericState*>(state);
        const auto& datum = TArrowBlock::From(columns[ArgColumn_]).GetDatum();
        PushValueToState<TStringType, IsMin>(typedState, datum, row);
    }

    std::unique_ptr<IAggColumnBuilder> MakeResultBuilder(ui64 size) final {
        return std::make_unique<TGenericColumnBuilder>(size, Type_, Ctx_);
    }

private:
    const ui32 ArgColumn_;
    TType* const Type_;
};

template <bool IsNullable, bool IsScalar, typename TIn, bool IsMin>
class TMinMaxBlockFixedAggregator<TCombineAllTag, IsNullable, IsScalar, TIn, IsMin> : public TCombineAllTag::TBase {
public:
    using TBase = TCombineAllTag::TBase;
    using TStateType = TState<IsNullable, TIn, IsMin>;
    using TInScalar = typename TPrimitiveDataType<TIn>::TScalarResult;

    TMinMaxBlockFixedAggregator(TType* type, std::optional<ui32> filterColumn, ui32 argColumn, TComputationContext& ctx)
        : TBase(sizeof(TStateType), filterColumn, ctx)
        , ArgColumn_(argColumn)
    {
        Y_UNUSED(type);
    }

    void InitState(void* ptr) final {
        TStateType state;
        WriteUnaligned<TStateType>(ptr, state);
    }

    void DestroyState(void* state) noexcept final {
        static_assert(std::is_trivially_destructible<TStateType>::value);
        Y_UNUSED(state);
    }

    void AddMany(void* state, const NUdf::TUnboxedValue* columns, ui64 batchLength, std::optional<ui64> filtered) final {
        auto typedState = MakeStateWrapper<TStateType>(state);
        Y_UNUSED(batchLength);
        const auto& datum = TArrowBlock::From(columns[ArgColumn_]).GetDatum();
        if constexpr (IsScalar) {
            Y_ENSURE(datum.is_scalar());
            if constexpr (IsNullable) {
                if (datum.scalar()->is_valid) {
                    typedState->Value = TIn(Cast(datum.scalar_as<TInScalar>().value));
                    typedState->IsValid = 1;
                }
            } else {
                typedState->Value = TIn(Cast(datum.scalar_as<TInScalar>().value));
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
                TIn value = typedState->Value;
                if constexpr (IsNullable) {
                    typedState->IsValid = 1;
                }

                if (IsNullable && nullCount != 0) {
                    auto nullBitmapPtr = array->GetValues<uint8_t>(0, 0);
                    for (int64_t i = 0; i < len; ++i) {
                        ui64 fullIndex = i + array->offset;
                        ui8 notNull = (nullBitmapPtr[fullIndex >> 3] >> (fullIndex & 0x07)) & 1;
                        value = UpdateMinMax<IsMin>(value, SelectArg(notNull, ptr[i], value));
                    }
                } else {
                    for (int64_t i = 0; i < len; ++i) {
                        value = UpdateMinMax<IsMin>(value, ptr[i]);
                    }
                }

                typedState->Value = value;
            } else {
                const auto& filterDatum = TArrowBlock::From(columns[*FilterColumn_]).GetDatum();
                const auto& filterArray = filterDatum.array();
                MKQL_ENSURE(filterArray->GetNullCount() == 0, "Expected non-nullable bool column");
                const ui8* filterBitmap = filterArray->template GetValues<uint8_t>(1);

                TIn value = typedState->Value;
                ui64 validCount = 0;
                if (IsNullable && nullCount != 0) {
                    auto nullBitmapPtr = array->GetValues<uint8_t>(0, 0);
                    for (int64_t i = 0; i < len; ++i) {
                        ui64 fullIndex = i + array->offset;
                        ui8 notNullAndFiltered = ((nullBitmapPtr[fullIndex >> 3] >> (fullIndex & 0x07)) & 1) & filterBitmap[i];
                        value = UpdateMinMax<IsMin>(value, SelectArg(notNullAndFiltered, ptr[i], value));
                        validCount += notNullAndFiltered;
                    }
                } else {
                    for (int64_t i = 0; i < len; ++i) {
                        ui8 filtered = filterBitmap[i];
                        value = UpdateMinMax<IsMin>(value, SelectArg(filtered, ptr[i], value));
                        validCount += filtered;
                    }
                }

                if constexpr (IsNullable) {
                    typedState->IsValid |= validCount ? 1 : 0;
                }
                typedState->Value = value;
            }
        }
    }

    NUdf::TUnboxedValue FinishOne(const void* state) final {
        auto typedState = MakeStateWrapper<TStateType>(state);
        if constexpr (IsNullable) {
            if (!typedState->IsValid) {
                return NUdf::TUnboxedValuePod();
            }
        }

        return NUdf::TUnboxedValuePod(typedState->Value);
    }

private:
    const ui32 ArgColumn_;
};

template <bool IsNullable, bool IsScalar, typename TIn, bool IsMin>
static void PushValueToState(TState<IsNullable, TIn, IsMin>* typedState, const arrow::Datum& datum, ui64 row) {
    using TInScalar = typename TPrimitiveDataType<TIn>::TScalarResult;
    if constexpr (IsScalar) {
        Y_ENSURE(datum.is_scalar());
        if constexpr (IsNullable) {
            if (datum.scalar()->is_valid) {
                typedState->Value = TIn(Cast(datum.scalar_as<TInScalar>().value));
                typedState->IsValid = 1;
            }
        } else {
            typedState->Value = TIn(Cast(datum.scalar_as<TInScalar>().value));
        }
    } else {
        const auto &array = datum.array();
        auto ptr = array->GetValues<TIn>(1);
        if constexpr (IsNullable) {
            if (array->GetNullCount() == 0) {
                typedState->IsValid = 1;
                typedState->Value = UpdateMinMax<IsMin>(typedState->Value, ptr[row]);
            } else {
                auto nullBitmapPtr = array->GetValues<uint8_t>(0, 0);
                ui64 fullIndex = row + array->offset;
                ui8 notNull = (nullBitmapPtr[fullIndex >> 3] >> (fullIndex & 0x07)) & 1;
                typedState->Value = UpdateMinMax<IsMin>(typedState->Value, SelectArg(notNull, ptr[row], typedState->Value));
                typedState->IsValid |= notNull;
            }
        } else {
            typedState->Value = UpdateMinMax<IsMin>(typedState->Value, ptr[row]);
        }
    }
}

template <bool IsNullable, bool IsScalar, typename TIn, bool IsMin>
class TMinMaxBlockFixedAggregator<TCombineKeysTag, IsNullable, IsScalar, TIn, IsMin> : public TCombineKeysTag::TBase {
public:
    using TBase = TCombineKeysTag::TBase;
    using TStateType = TState<IsNullable, TIn, IsMin>;

    TMinMaxBlockFixedAggregator(TType* type, std::optional<ui32> filterColumn, ui32 argColumn, TComputationContext& ctx)
        : TBase(sizeof(TStateType), filterColumn, ctx)
        , ArgColumn_(argColumn)
        , Type_(type)
    {
    }

    void InitKey(void* state, ui64 batchNum, const NUdf::TUnboxedValue* columns, ui64 row) final {
        TStateType st;
        WriteUnaligned<TStateType>(state, st);
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
        PushValueToState<IsNullable, IsScalar, TIn, IsMin>(typedState.Get(), datum, row);
    }

    std::unique_ptr<IAggColumnBuilder> MakeStateBuilder(ui64 size) final {
        return std::make_unique<TColumnBuilder<IsNullable, TIn, IsMin>>(size, Type_, Ctx_);
    }

private:
    const ui32 ArgColumn_;
    const std::shared_ptr<arrow::DataType> BuilderDataType_;
    TType* const Type_;
};

template <bool IsNullable, bool IsScalar, typename TIn, bool IsMin>
class TMinMaxBlockFixedAggregator<TFinalizeKeysTag, IsNullable, IsScalar, TIn, IsMin> : public TFinalizeKeysTag::TBase {
public:
    using TBase = TFinalizeKeysTag::TBase;
    using TStateType = TState<IsNullable, TIn, IsMin>;

    TMinMaxBlockFixedAggregator(TType* type, std::optional<ui32> filterColumn, ui32 argColumn, TComputationContext& ctx)
        : TBase(sizeof(TStateType), filterColumn, ctx)
        , ArgColumn_(argColumn)
        , Type_(type)
    {
    }

    void LoadState(void* state, ui64 batchNum, const NUdf::TUnboxedValue* columns, ui64 row) final {
        TStateType st;
        WriteUnaligned<TStateType>(state, st);
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
        PushValueToState<IsNullable, IsScalar, TIn, IsMin>(typedState.Get(), datum, row);
    }

    std::unique_ptr<IAggColumnBuilder> MakeResultBuilder(ui64 size) final {
        return std::make_unique<TColumnBuilder<IsNullable, TIn, IsMin>>(size, Type_, Ctx_);
    }

private:
    const ui32 ArgColumn_;
    TType* const Type_;
};

template<typename TTag, typename TStringType, bool IsMin>
class TPreparedMinMaxBlockStringAggregator : public TTag::TPreparedAggregator {
public:
    using TBase = typename TTag::TPreparedAggregator;

    TPreparedMinMaxBlockStringAggregator(TType* type, std::optional<ui32> filterColumn, ui32 argColumn)
        : TBase(sizeof(TGenericState))
        , Type_(type)
        , FilterColumn_(filterColumn)
        , ArgColumn_(argColumn)
    {}

    std::unique_ptr<typename TTag::TAggregator> Make(TComputationContext& ctx) const final {
        return std::make_unique<TMinMaxBlockStringAggregator<TTag, TStringType, IsMin>>(Type_, FilterColumn_, ArgColumn_, ctx);
    }
private:
    TType* const Type_;
    const std::optional<ui32> FilterColumn_;
    const ui32 ArgColumn_;
};

template <typename TTag, bool IsNullable, bool IsScalar, typename TIn, bool IsMin>
class TPreparedMinMaxBlockFixedAggregator : public TTag::TPreparedAggregator {
public:
    using TBase = typename TTag::TPreparedAggregator;
    using TStateType = TState<IsNullable, TIn, IsMin>;

    TPreparedMinMaxBlockFixedAggregator(TType* type, std::optional<ui32> filterColumn, ui32 argColumn)
        : TBase(sizeof(TStateType))
        , Type_(type)
        , FilterColumn_(filterColumn)
        , ArgColumn_(argColumn)
    {}

    std::unique_ptr<typename TTag::TAggregator> Make(TComputationContext& ctx) const final {
        return std::make_unique<TMinMaxBlockFixedAggregator<TTag, IsNullable, IsScalar, TIn, IsMin>>(Type_, FilterColumn_, ArgColumn_, ctx);
    }

private:
    TType* const Type_;
    const std::optional<ui32> FilterColumn_;
    const ui32 ArgColumn_;
};

template <typename TTag, bool IsMin>
class TPreparedMinMaxBlockGenericAggregator : public TTag::TPreparedAggregator {
public:
    using TBase = typename TTag::TPreparedAggregator;

    TPreparedMinMaxBlockGenericAggregator(TType* type, std::optional<ui32> filterColumn, ui32 argColumn)
        : TBase(sizeof(TGenericState))
        , Type_(type)
        , FilterColumn_(filterColumn)
        , ArgColumn_(argColumn)
    {}

    std::unique_ptr<typename TTag::TAggregator> Make(TComputationContext& ctx) const final {
        return std::make_unique<TMinMaxBlockGenericAggregator<TTag, IsMin>>(Type_, FilterColumn_, ArgColumn_, ctx);
    }

private:
    TType* const Type_;
    const std::optional<ui32> FilterColumn_;
    const ui32 ArgColumn_;
};

template<typename TTag, typename TIn, bool IsMin>
std::unique_ptr<typename TTag::TPreparedAggregator> PrepareMinMaxFixed(TType* type, bool isOptional, bool isScalar, std::optional<ui32> filterColumn, ui32 argColumn) {
    if (isScalar) {
        if (isOptional) {
            return std::make_unique<TPreparedMinMaxBlockFixedAggregator<TTag, true, true, TIn, IsMin>>(type, filterColumn, argColumn);
        }
        return std::make_unique<TPreparedMinMaxBlockFixedAggregator<TTag, false, true, TIn, IsMin>>(type, filterColumn, argColumn);
    }
    if (isOptional) {
        return std::make_unique<TPreparedMinMaxBlockFixedAggregator<TTag, true, false, TIn, IsMin>>(type, filterColumn, argColumn);
    }
    return std::make_unique<TPreparedMinMaxBlockFixedAggregator<TTag, false, false, TIn, IsMin>>(type, filterColumn, argColumn);
}

template <typename TTag, bool IsMin>
std::unique_ptr<typename TTag::TPreparedAggregator> PrepareMinMax(TTupleType* tupleType, std::optional<ui32> filterColumn, ui32 argColumn) {
    auto blockType = AS_TYPE(TBlockType, tupleType->GetElementType(argColumn));
    const bool isScalar = blockType->GetShape() == TBlockType::EShape::Scalar;
    auto argType = blockType->GetItemType();

    bool isOptional;
    auto unpacked = UnpackOptional(argType, isOptional);
    if (!unpacked->IsData()) {
        return std::make_unique<TPreparedMinMaxBlockGenericAggregator<TTag, IsMin>>(argType, filterColumn, argColumn);
    }

    auto dataType = AS_TYPE(TDataType, unpacked);
    const auto slot = *dataType->GetDataSlot();
    if (slot == NUdf::EDataSlot::String) {
        using TStringType = char*;
        return std::make_unique<TPreparedMinMaxBlockStringAggregator<TTag, TStringType, IsMin>>(argType, filterColumn, argColumn);
    } else if (slot == NUdf::EDataSlot::Utf8) {
        using TStringType = NUdf::TUtf8;
        return std::make_unique<TPreparedMinMaxBlockStringAggregator<TTag, TStringType, IsMin>>(argType, filterColumn, argColumn);
    }
    switch (slot) {
    case NUdf::EDataSlot::Int8:
        return PrepareMinMaxFixed<TTag, i8, IsMin>(dataType, isOptional, isScalar, filterColumn, argColumn);
    case NUdf::EDataSlot::Bool:
    case NUdf::EDataSlot::Uint8:
        return PrepareMinMaxFixed<TTag, ui8, IsMin>(dataType, isOptional, isScalar, filterColumn, argColumn);
    case NUdf::EDataSlot::Int16:
        return PrepareMinMaxFixed<TTag, i16, IsMin>(dataType, isOptional, isScalar, filterColumn, argColumn);
    case NUdf::EDataSlot::Uint16:
    case NUdf::EDataSlot::Date:
        return PrepareMinMaxFixed<TTag, ui16, IsMin>(dataType, isOptional, isScalar, filterColumn, argColumn);
    case NUdf::EDataSlot::Int32:
    case NUdf::EDataSlot::Date32:
        return PrepareMinMaxFixed<TTag, i32, IsMin>(dataType, isOptional, isScalar, filterColumn, argColumn);
    case NUdf::EDataSlot::Uint32:
    case NUdf::EDataSlot::Datetime:
        return PrepareMinMaxFixed<TTag, ui32, IsMin>(dataType, isOptional, isScalar, filterColumn, argColumn);
    case NUdf::EDataSlot::Int64:
    case NUdf::EDataSlot::Interval:
    case NUdf::EDataSlot::Interval64:
    case NUdf::EDataSlot::Timestamp64:
    case NUdf::EDataSlot::Datetime64:
        return PrepareMinMaxFixed<TTag, i64, IsMin>(dataType, isOptional, isScalar, filterColumn, argColumn);
    case NUdf::EDataSlot::Uint64:
    case NUdf::EDataSlot::Timestamp:
        return PrepareMinMaxFixed<TTag, ui64, IsMin>(dataType, isOptional, isScalar, filterColumn, argColumn);
    case NUdf::EDataSlot::Float:
        return PrepareMinMaxFixed<TTag, float, IsMin>(dataType, isOptional, isScalar, filterColumn, argColumn);
    case NUdf::EDataSlot::Double:
        return PrepareMinMaxFixed<TTag, double, IsMin>(dataType, isOptional, isScalar, filterColumn, argColumn);
    case NUdf::EDataSlot::Decimal:
        return PrepareMinMaxFixed<TTag, NYql::NDecimal::TInt128, IsMin>(dataType, isOptional, isScalar, filterColumn, argColumn);
    default:
        throw yexception() << "Unsupported MIN/MAX input type";
    }
}

template <bool IsMin>
class TBlockMinMaxFactory : public IBlockAggregatorFactory {
public:
    std::unique_ptr<TCombineAllTag::TPreparedAggregator> PrepareCombineAll(
        TTupleType* tupleType,
        std::optional<ui32> filterColumn,
        const std::vector<ui32>& argsColumns,
        const TTypeEnvironment& env) const final {
        Y_UNUSED(env);
        return PrepareMinMax<TCombineAllTag, IsMin>(tupleType, filterColumn, argsColumns[0]);
    }

    std::unique_ptr<TCombineKeysTag::TPreparedAggregator> PrepareCombineKeys(
        TTupleType* tupleType,
        const std::vector<ui32>& argsColumns,
        const TTypeEnvironment& env) const final {
        Y_UNUSED(env);
        return PrepareMinMax<TCombineKeysTag, IsMin>(tupleType, std::optional<ui32>(), argsColumns[0]);
    }

    std::unique_ptr<TFinalizeKeysTag::TPreparedAggregator> PrepareFinalizeKeys(
        TTupleType* tupleType,
        const std::vector<ui32>& argsColumns,
        const TTypeEnvironment& env,
        TType* returnType) const final {
        Y_UNUSED(env);
        Y_UNUSED(returnType);
        return PrepareMinMax<TFinalizeKeysTag, IsMin>(tupleType, std::optional<ui32>(), argsColumns[0]);
    }
};

} // namespace

std::unique_ptr<IBlockAggregatorFactory> MakeBlockMinFactory() {
    return std::make_unique<TBlockMinMaxFactory<true>>();
}

std::unique_ptr<IBlockAggregatorFactory> MakeBlockMaxFactory() {
    return std::make_unique<TBlockMinMaxFactory<false>>();
}

}
}
