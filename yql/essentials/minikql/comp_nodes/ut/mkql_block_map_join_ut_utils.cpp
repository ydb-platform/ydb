#include "mkql_block_map_join_ut_utils.h"

#include <yql/essentials/minikql/computation/mkql_block_impl.h>
#include <yql/essentials/minikql/computation/mkql_block_reader.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/public/udf/arrow/args_dechunker.h>
#include <yql/essentials/public/udf/arrow/block_builder.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TWideStreamThrottlerWrapper: public TMutableComputationNode<TWideStreamThrottlerWrapper> {
    typedef TMutableComputationNode<TWideStreamThrottlerWrapper> TBaseComputation;
public:
    class TStreamValue : public TComputationValue<TStreamValue> {
    public:
        using TBase = TComputationValue<TStreamValue>;

        TStreamValue(TMemoryUsageInfo* memInfo, NUdf::TUnboxedValue origStream)
            : TBase(memInfo)
            , OrigStream_(std::move(origStream))
        {
        }

    private:
        NUdf::EFetchStatus WideFetch(NUdf::TUnboxedValue* output, ui32 width) {
            if (Counter_++ % 3) {
                return NUdf::EFetchStatus::Yield;
            }

            TUnboxedValueVector items(width);
            switch (OrigStream_.WideFetch(items.data(), width)) {
            case NUdf::EFetchStatus::Yield:
                return NUdf::EFetchStatus::Yield;
            case NUdf::EFetchStatus::Ok:
                for (size_t i = 0; i < width; i++) {
                    output[i] = std::move(items[i]);
                }
                return NUdf::EFetchStatus::Ok;
            case NUdf::EFetchStatus::Finish:
                return NUdf::EFetchStatus::Finish;
            }
        }

    private:
        NUdf::TUnboxedValue OrigStream_;
        size_t Counter_ = 0;
    };

    TWideStreamThrottlerWrapper(TComputationMutables& mutables, IComputationNode* origStream)
        : TBaseComputation(mutables)
        , OrigStream_(origStream)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.Create<TStreamValue>(OrigStream_->GetValue(ctx));
    }

private:
    void RegisterDependencies() const final {
        DependsOn(OrigStream_);
    }

private:
    IComputationNode* OrigStream_;
};

class TWideStreamDethrottlerWrapper: public TMutableComputationNode<TWideStreamDethrottlerWrapper> {
    typedef TMutableComputationNode<TWideStreamDethrottlerWrapper> TBaseComputation;
public:
    class TStreamValue : public TComputationValue<TStreamValue> {
    public:
        using TBase = TComputationValue<TStreamValue>;

        TStreamValue(TMemoryUsageInfo* memInfo, NUdf::TUnboxedValue origStream)
            : TBase(memInfo)
            , OrigStream_(std::move(origStream))
        {
        }

    private:
        NUdf::EFetchStatus WideFetch(NUdf::TUnboxedValue* output, ui32 width) {
            TUnboxedValueVector items(width);
            for (;;) {
                switch (OrigStream_.WideFetch(items.data(), width)) {
                case NUdf::EFetchStatus::Yield:
                    continue;
                case NUdf::EFetchStatus::Ok:
                    for (size_t i = 0; i < width; i++) {
                        output[i] = std::move(items[i]);
                    }
                    return NUdf::EFetchStatus::Ok;
                case NUdf::EFetchStatus::Finish:
                    return NUdf::EFetchStatus::Finish;
                }
            }
        }

    private:
        NUdf::TUnboxedValue OrigStream_;
    };

    TWideStreamDethrottlerWrapper(TComputationMutables& mutables, IComputationNode* origStream)
        : TBaseComputation(mutables)
        , OrigStream_(origStream)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.Create<TStreamValue>(OrigStream_->GetValue(ctx));
    }

private:
    void RegisterDependencies() const final {
        DependsOn(OrigStream_);
    }

private:
    IComputationNode* OrigStream_;
};

IComputationNode* WrapWideStreamThrottler(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 arg");
    const auto origStream = LocateNode(ctx.NodeLocator, callable, 0);
    return new TWideStreamThrottlerWrapper(ctx.Mutables, origStream);
}

IComputationNode* WrapWideStreamDethrottler(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 arg");
    const auto origStream = LocateNode(ctx.NodeLocator, callable, 0);
    return new TWideStreamDethrottlerWrapper(ctx.Mutables, origStream);
}

}

TType* MakeBlockTupleType(TProgramBuilder& pgmBuilder, TType* tupleType, bool scalar) {
    const auto itemTypes = AS_TYPE(TTupleType, tupleType)->GetElements();
    const auto ui64Type = pgmBuilder.NewDataType(NUdf::TDataType<ui64>::Id);
    const auto blockLenType = pgmBuilder.NewBlockType(ui64Type, TBlockType::EShape::Scalar);

    TVector<TType*> blockItemTypes;
    std::transform(itemTypes.cbegin(), itemTypes.cend(), std::back_inserter(blockItemTypes),
        [&](const auto& itemType) {
            return pgmBuilder.NewBlockType(itemType, scalar ? TBlockType::EShape::Scalar : TBlockType::EShape::Many);
        });
    // XXX: Mind the last block length column.
    blockItemTypes.push_back(blockLenType);

    return pgmBuilder.NewTupleType(blockItemTypes);
}

NUdf::TUnboxedValuePod ToBlocks(TComputationContext& ctx, size_t blockSize,
    const TArrayRef<TType* const> types, const NUdf::TUnboxedValuePod& values
) {
    const auto maxLength = CalcBlockLen(std::accumulate(types.cbegin(), types.cend(), 0ULL,
        [](size_t max, const TType* type) {
            return std::max(max, CalcMaxBlockItemSize(type));
        }));
    TVector<std::unique_ptr<NUdf::IArrayBuilder>> builders;
    std::transform(types.cbegin(), types.cend(), std::back_inserter(builders),
        [&](const auto& type) {
            return MakeArrayBuilder(TTypeInfoHelper(), type, ctx.ArrowMemoryPool,
                                    maxLength, &ctx.Builder->GetPgBuilder());
        });

    const auto& holderFactory = ctx.HolderFactory;
    const size_t width = types.size();
    const size_t total = values.GetListLength();
    NUdf::TUnboxedValue iterator = values.GetListIterator();
    NUdf::TUnboxedValue current;
    size_t converted = 0;
    TDefaultListRepresentation listValues;
    while (converted < total) {
        for (size_t i = 0; i < blockSize && iterator.Next(current); i++, converted++) {
            for (size_t j = 0; j < builders.size(); j++) {
                const NUdf::TUnboxedValuePod& item = current.GetElement(j);
                builders[j]->Add(item);
            }
        }
        std::vector<arrow::Datum> batch;
        batch.reserve(width);
        for (size_t i = 0; i < width; i++) {
            batch.emplace_back(builders[i]->Build(converted >= total));
        }

        NUdf::TArgsDechunker dechunker(std::move(batch));
        std::vector<arrow::Datum> chunk;
        ui64 chunkLen = 0;
        while (dechunker.Next(chunk, chunkLen)) {
            NUdf::TUnboxedValue* items = nullptr;
            const auto tuple = holderFactory.CreateDirectArrayHolder(width + 1, items);
            for (size_t i = 0; i < width; i++) {
                items[i] = holderFactory.CreateArrowBlock(std::move(chunk[i]));
            }
            items[width] = MakeBlockCount(holderFactory, chunkLen);

            listValues = listValues.Append(std::move(tuple));
        }
    }
    return holderFactory.CreateDirectListHolder(std::move(listValues));
}

NUdf::TUnboxedValuePod MakeUint64ScalarBlock(TComputationContext& ctx, size_t blockSize,
    const TArrayRef<TType* const> types, const NUdf::TUnboxedValuePod& values
) {
    // Creates a block of scalar values using the first element of the given list

    for (auto type : types) {
        // Because IScalarBuilder has no implementations
        Y_ENSURE(AS_TYPE(TDataType, type)->GetDataSlot() == NYql::NUdf::EDataSlot::Uint64);
    }

    const auto& holderFactory = ctx.HolderFactory;
    const size_t width = types.size();
    const size_t rowsCount = values.GetListLength();

    NUdf::TUnboxedValue row;
    Y_ENSURE(values.GetListIterator().Next(row));
    TDefaultListRepresentation listValues;
    for (size_t rowOffset = 0; rowOffset < rowsCount; rowOffset += blockSize) {
        NUdf::TUnboxedValue* items = nullptr;
        const auto tuple = holderFactory.CreateDirectArrayHolder(width + 1, items);
        for (size_t i = 0; i < width; i++) {
            const NUdf::TUnboxedValuePod& item = row.GetElement(i);
            items[i] = holderFactory.CreateArrowBlock(arrow::Datum(static_cast<uint64_t>(item.Get<ui64>())));
        }
        items[width] = MakeBlockCount(holderFactory, std::min(blockSize, rowsCount - rowOffset));
        listValues = listValues.Append(std::move(tuple));
    }

    return holderFactory.CreateDirectListHolder(std::move(listValues));
}

NUdf::TUnboxedValuePod FromBlocks(TComputationContext& ctx,
    const TArrayRef<TType* const> types, const NUdf::TUnboxedValuePod& values
) {
    TVector<std::unique_ptr<IBlockReader>> readers;
    TVector<std::unique_ptr<IBlockItemConverter>> converters;
    for (const auto& type : types) {
        const auto blockItemType = AS_TYPE(TBlockType, type)->GetItemType();
        readers.push_back(MakeBlockReader(TTypeInfoHelper(), blockItemType));
        converters.push_back(MakeBlockItemConverter(TTypeInfoHelper(), blockItemType,
                                                    ctx.Builder->GetPgBuilder()));
    }

    const auto& holderFactory = ctx.HolderFactory;
    const size_t width = types.size() - 1;
    TDefaultListRepresentation listValues;
    NUdf::TUnboxedValue iterator = values.GetListIterator();
    NUdf::TUnboxedValue current;
    while (iterator.Next(current)) {
        const auto blockLengthValue = current.GetElement(width);
        const auto blockLengthDatum = TArrowBlock::From(blockLengthValue).GetDatum();
        Y_ENSURE(blockLengthDatum.is_scalar());
        const auto blockLength = blockLengthDatum.scalar_as<arrow::UInt64Scalar>().value;
        for (size_t i = 0; i < blockLength; i++) {
            NUdf::TUnboxedValue* items = nullptr;
            const auto tuple = holderFactory.CreateDirectArrayHolder(width, items);
            for (size_t j = 0; j < width; j++) {
                const auto arrayValue = current.GetElement(j);
                const auto arrayDatum = TArrowBlock::From(arrayValue).GetDatum();
                UNIT_ASSERT(arrayDatum.is_array());
                const auto blockItem = readers[j]->GetItem(*arrayDatum.array(), i);
                items[j] = converters[j]->MakeValue(blockItem, holderFactory);
            }
            listValues = listValues.Append(std::move(tuple));
        }
    }
    return holderFactory.CreateDirectListHolder(std::move(listValues));
}

TComputationNodeFactory GetNodeFactory() {
    return [](TCallable& callable, const TComputationNodeFactoryContext& ctx) -> IComputationNode* {
        if (callable.GetType()->GetName() == "WideStreamThrottler") {
            return WrapWideStreamThrottler(callable, ctx);
        } else if (callable.GetType()->GetName() == "WideStreamDethrottler") {
            return WrapWideStreamDethrottler(callable, ctx);
        }
        return GetBuiltinFactory()(callable, ctx);
    };
}

TRuntimeNode ThrottleStream(TProgramBuilder& pgmBuilder, TRuntimeNode stream) {
    TCallableBuilder callableBuilder(pgmBuilder.GetTypeEnvironment(), "WideStreamThrottler", stream.GetStaticType());
    callableBuilder.Add(stream);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode DethrottleStream(TProgramBuilder& pgmBuilder, TRuntimeNode stream) {
    TCallableBuilder callableBuilder(pgmBuilder.GetTypeEnvironment(), "WideStreamDethrottler", stream.GetStaticType());
    callableBuilder.Add(stream);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TVector<NUdf::TUnboxedValue> ConvertListToVector(const NUdf::TUnboxedValue& list) {
    NUdf::TUnboxedValue current;
    NUdf::TUnboxedValue iterator = list.GetListIterator();
    TVector<NUdf::TUnboxedValue> items;
    while (iterator.Next(current)) {
        items.push_back(current);
    }
    return items;
}

void CompareResults(const TType* type, const NUdf::TUnboxedValue& expected,
                    const NUdf::TUnboxedValue& got
) {
    const auto itemType = AS_TYPE(TListType, type)->GetItemType();
    const NUdf::ICompare::TPtr compare = MakeCompareImpl(itemType);
    const NUdf::IEquate::TPtr equate = MakeEquateImpl(itemType);
    // XXX: Stub both keyTypes and isTuple arguments, since
    // ICompare/IEquate are used.
    TKeyTypes keyTypesStub;
    bool isTupleStub = false;
    const TValueLess valueLess(keyTypesStub, isTupleStub, compare.Get());
    const TValueEqual valueEqual(keyTypesStub, isTupleStub, equate.Get());

    auto expectedItems = ConvertListToVector(expected);
    auto gotItems = ConvertListToVector(got);
    UNIT_ASSERT_VALUES_EQUAL(expectedItems.size(), gotItems.size());
    Sort(expectedItems, valueLess);
    Sort(gotItems, valueLess);
    for (size_t i = 0; i < expectedItems.size(); i++) {
        UNIT_ASSERT(valueEqual(gotItems[i], expectedItems[i]));
    }
}

TVector<TString> GenerateValues(size_t level) {
    constexpr size_t alphaSize = 'Z' - 'A' + 1;
    if (level == 1) {
        TVector<TString> alphabet(alphaSize);
        std::iota(alphabet.begin(), alphabet.end(), 'A');
        return alphabet;
    }
    const auto subValues = GenerateValues(level - 1);
    TVector<TString> values;
    values.reserve(alphaSize * subValues.size());
    for (char ch = 'A'; ch <= 'Z'; ch++) {
        for (const auto& tail : subValues) {
            values.emplace_back(ch + tail);
        }
    }
    return values;
}

TSet<ui64> GenerateFibonacci(size_t count) {
    TSet<ui64> fibSet;
    ui64 a = 0, b = 1;
    fibSet.insert(a);
    while (count--) {
        a = std::exchange(b, a + b);
        fibSet.insert(b);
    }
    return fibSet;
}

} // namespace NMiniKQL
} // namespace NKikimr
