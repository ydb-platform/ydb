#include "block_array_tree.h"

#include <arrow/array/builder_base.h>
#include <yql/essentials/public/udf/arrow/util.h>

namespace NYql::NUdf {

namespace {

struct TBlockArrayTreeState {
    using Ptr = TMaybe<TBlockArrayTreeState>;

    struct TChildrenState {
        size_t CurrentCount = 0;
    };
    std::shared_ptr<arrow::ArrayData> EmptyPayloadSlice;
    TChildrenState Union;
    TVector<Ptr> Children;
};

TBlockArrayTreeState MakeTreeStateImpl(const TBlockArrayTree& tree, std::shared_ptr<arrow::ArrayData> emptyPayload, arrow::MemoryPool* pool) {
    TBlockArrayTreeState state;
    Y_ENSURE(tree.Children.size() == emptyPayload->child_data.size());
    for (size_t i = 0; i < tree.Children.size(); ++i) {
        state.Children.push_back(MakeTreeStateImpl(*tree.Children[i], emptyPayload->child_data[i], pool));
    }
    state.EmptyPayloadSlice = emptyPayload;
    return state;
}

TBlockArrayTreeState MakeTreeState(const TBlockArrayTree& tree, arrow::MemoryPool* pool) {
    return MakeTreeStateImpl(tree, MakeEmptyArray(tree.Payload.front()->type, pool), pool);
}

size_t CalcSliceSize(const TBlockArrayTree& tree, TBlockArrayTreeState& state);

std::shared_ptr<arrow::ArrayData> Slice(TBlockArrayTree& tree, TBlockArrayTreeState& state, size_t size);

size_t CalcSliceSizeUnion(const TBlockArrayTree& tree, TBlockArrayTreeState& state) {
    if (tree.Payload.empty()) {
        return 0;
    }

    Y_ABORT_UNLESS(tree.Payload.size() == 1);

    auto& unionState = state.Union;
    if (unionState.CurrentCount > 0) {
        return unionState.CurrentCount;
    }
    const size_t numChildren = tree.Children.size();

    TVector<size_t> availableForSlice(numChildren);
    for (size_t childIndex = 0; childIndex < numChildren; ++childIndex) {
        availableForSlice[childIndex] = CalcSliceSize(*tree.Children[childIndex], *state.Children[childIndex]);
    }

    const i8* typeCodes = tree.Payload.front()->GetValues<i8>(1);
    const size_t remaining = static_cast<size_t>(tree.Payload.front()->length);

    for (size_t i = 0; i < remaining; ++i) {
        const size_t childIndex = static_cast<size_t>(static_cast<i8>(typeCodes[i]));
        Y_DEBUG_ABORT_UNLESS(childIndex < numChildren, "Dense union: invalid type code");
        if (!availableForSlice[childIndex]) {
            return unionState.CurrentCount;
        }
        availableForSlice[childIndex]--;
        unionState.CurrentCount++;
    }
    return remaining;
}

std::shared_ptr<arrow::ArrayData> SliceUnion(
    TBlockArrayTree& tree, TBlockArrayTreeState& state, size_t size)
{
    auto& unionState = state.Union;
    auto& main = tree.Payload.front();
    const size_t numChildren = tree.Children.size();

    const i8* typeCodes = main->GetValues<i8>(1);
    i32* valueOffsets = main->GetMutableValues<i32>(2);
    TVector<size_t> sizeToTrimOnCurrentSlice(numChildren, 0);
    for (size_t i = 0; i < size; ++i) {
        const size_t childIndex = typeCodes[i];
        valueOffsets[i] = sizeToTrimOnCurrentSlice[childIndex];
        sizeToTrimOnCurrentSlice[childIndex]++;
        unionState.CurrentCount--;
    }

    std::shared_ptr<arrow::ArrayData> sliced;
    if (size == static_cast<size_t>(main->length)) {
        sliced = main;
        tree.Payload.pop_front();
    } else {
        sliced = Chop(main, size);
    }

    sliced->child_data.resize(numChildren);
    for (size_t childIndex = 0; childIndex < numChildren; ++childIndex) {
        const size_t childSizeToTrim = sizeToTrimOnCurrentSlice[childIndex];
        auto& childTree = *tree.Children[childIndex];
        if (childSizeToTrim == 0) {
            sliced->child_data[childIndex] = state.Children[childIndex]->EmptyPayloadSlice;
            continue;
        }
        sliced->child_data[childIndex] = Slice(childTree, *state.Children[childIndex], childSizeToTrim);
    }
    return sliced;
}

size_t CalcSliceSize(const TBlockArrayTree& tree, TBlockArrayTreeState& state) {
    if (tree.Payload.empty()) {
        return 0;
    }
    Y_ENSURE(tree.Payload.front()->type, "Expected non null type");
    if (tree.Payload.front()->type->id() == arrow::Type::DENSE_UNION) {
        return CalcSliceSizeUnion(tree, state);
    }

    if (!tree.Children.empty()) {
        Y_ABORT_UNLESS(tree.Payload.size() == 1);
        size_t result = std::numeric_limits<size_t>::max();
        for (size_t i = 0; i < tree.Children.size(); ++i) {
            result = std::min(result, CalcSliceSize(*tree.Children[i], *state.Children[i]));
        }
        Y_ABORT_UNLESS(result <= size_t(tree.Payload.front()->length));
        return result;
    }

    const i64 result = tree.Payload.front()->length;
    return static_cast<size_t>(result);
}

std::shared_ptr<arrow::ArrayData> Slice(
    TBlockArrayTree& tree, TBlockArrayTreeState& state, size_t size)
{
    Y_ENSURE(tree.Payload.front()->type, "Expected non null type");
    if (tree.Payload.front()->type->id() == arrow::Type::DENSE_UNION) {
        return SliceUnion(tree, state, size);
    }

    auto& main = tree.Payload.front();
    std::shared_ptr<arrow::ArrayData> sliced;
    if (size == static_cast<size_t>(main->length)) {
        sliced = main;
        tree.Payload.pop_front();
    } else {
        sliced = Chop(main, size);
    }

    if (!tree.Children.empty()) {
        std::vector<std::shared_ptr<arrow::ArrayData>> children;
        children.reserve(tree.Children.size());
        for (size_t i = 0; i < tree.Children.size(); ++i) {
            children.push_back(Slice(*tree.Children[i], *state.Children[i], size));
        }
        sliced->child_data = std::move(children);
    }
    return sliced;
}

} // namespace

arrow::Datum ToChunkedArray(TBlockArrayTree& tree, arrow::MemoryPool* pool) {
    auto state = MakeTreeState(tree, pool);
    TVector<std::shared_ptr<arrow::ArrayData>> chunks;
    while (size_t size = CalcSliceSize(tree, state)) {
        chunks.push_back(Slice(tree, state, size));
    }
    if (chunks.empty()) {
        return arrow::Datum(state.EmptyPayloadSlice);
    }
    return MakeArray(chunks);
}

} // namespace NYql::NUdf
