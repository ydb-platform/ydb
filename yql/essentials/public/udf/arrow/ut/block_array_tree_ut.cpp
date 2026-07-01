#include <library/cpp/testing/unittest/registar.h>

#include <yql/essentials/public/udf/arrow/block_array_tree.h>
#include <yql/essentials/public/udf/arrow/util.h>

#include <arrow/array/data.h>
#include <arrow/buffer.h>
#include <arrow/memory_pool.h>
#include <arrow/type.h>

#include <cstring>

using namespace NYql::NUdf;

namespace {

template <typename T>
std::shared_ptr<arrow::Buffer> MakeBuffer(const TVector<T>& values) {
    auto pool = arrow::default_memory_pool();
    auto buf = arrow::AllocateBuffer(static_cast<i64>(values.size() * sizeof(T)), pool).ValueOrDie();
    std::memcpy(buf->mutable_data(), values.data(), values.size() * sizeof(T));
    return buf;
}

std::shared_ptr<arrow::ArrayData> MakeI64Data(const TVector<i64>& values) {
    return arrow::ArrayData::Make(arrow::int64(), values.size(), {nullptr, MakeBuffer(values)});
}

TBlockArrayTree::Ptr MakeI64Tree(const TVector<i64>& values) {
    auto tree = std::make_shared<TBlockArrayTree>();
    tree->Payload.push_back(MakeI64Data(values));
    return tree;
}

TBlockArrayTree::Ptr MakeI64Tree(const TVector<TVector<i64>>& chunks) {
    auto tree = std::make_shared<TBlockArrayTree>();
    for (const auto& chunk : chunks) {
        tree->Payload.push_back(MakeI64Data(chunk));
    }
    return tree;
}

TBlockArrayTree::Ptr MakeTupleTree(
    const TVector<TVector<i64>>& f0Chunks,
    const TVector<TVector<i64>>& f1Chunks)
{
    i64 totalRows = 0;
    for (const auto& c : f0Chunks) {
        totalRows += static_cast<i64>(c.size());
    }

    auto tree = std::make_shared<TBlockArrayTree>();
    tree->Children.push_back(MakeI64Tree(f0Chunks));
    tree->Children.push_back(MakeI64Tree(f1Chunks));

    auto structType = arrow::struct_({
        arrow::field("f0", arrow::int64()),
        arrow::field("f1", arrow::int64()),
    });
    tree->Payload.push_back(arrow::ArrayData::Make(structType, totalRows, {nullptr}));
    return tree;
}

TBlockArrayTree::Ptr MakeTupleTree(const TVector<i64>& f0, const TVector<i64>& f1) {
    return MakeTupleTree(TVector<TVector<i64>>{f0}, TVector<TVector<i64>>{f1});
}

std::shared_ptr<arrow::ArrayData> MakeUnionData(
    const TVector<i8>& typeCodes, int numChildren)
{
    auto pool = arrow::default_memory_pool();
    std::shared_ptr<arrow::Buffer> offsets =
        arrow::AllocateBuffer(
            static_cast<i64>(typeCodes.size() * sizeof(i32)), pool)
            .ValueOrDie();
    std::memset(offsets->mutable_data(), 0, offsets->size());

    std::vector<std::shared_ptr<arrow::Field>> fields;
    std::vector<i8> typeIds;
    for (int i = 0; i < numChildren; ++i) {
        fields.push_back(arrow::field("f" + std::to_string(i), arrow::int64()));
        typeIds.push_back(static_cast<i8>(i));
    }

    return arrow::ArrayData::Make(
        arrow::dense_union(fields, typeIds),
        static_cast<i64>(typeCodes.size()),
        {nullptr, MakeBuffer(typeCodes), offsets});
}

TBlockArrayTree::Ptr MakeUnionTree(
    const TVector<i8>& typeCodes,
    const TVector<TVector<i64>>& child0Chunks,
    const TVector<TVector<i64>>& child1Chunks)
{
    auto tree = std::make_shared<TBlockArrayTree>();
    tree->Payload.push_back(MakeUnionData(typeCodes, 2));
    tree->Children.push_back(MakeI64Tree(child0Chunks));
    tree->Children.push_back(MakeI64Tree(child1Chunks));
    return tree;
}

TVector<TVector<i64>> ReadI64Chunks(const arrow::Datum& datum) {
    TVector<TVector<i64>> result;
    ForEachArrayData(datum, [&](const std::shared_ptr<arrow::ArrayData>& data) {
        const i64* ptr = data->template GetValues<i64>(1);
        result.emplace_back(ptr, ptr + data->length);
    });
    return result;
}

TVector<TVector<std::pair<i64, i64>>> ReadTupleChunks(const arrow::Datum& datum) {
    TVector<TVector<std::pair<i64, i64>>> result;
    ForEachArrayData(datum, [&](const std::shared_ptr<arrow::ArrayData>& data) {
        TVector<std::pair<i64, i64>> chunk;
        const i64* v0 = data->child_data[0]->template GetValues<i64>(1);
        const i64* v1 = data->child_data[1]->template GetValues<i64>(1);
        for (i64 i = 0; i < data->length; ++i) {
            chunk.emplace_back(v0[i], v1[i]);
        }
        result.push_back(std::move(chunk));
    });
    return result;
}

TVector<TVector<std::pair<i8, i64>>> ReadUnionChunks(const arrow::Datum& datum) {
    TVector<TVector<std::pair<i8, i64>>> result;
    ForEachArrayData(datum, [&](const std::shared_ptr<arrow::ArrayData>& data) {
        TVector<std::pair<i8, i64>> chunk;
        const i8* typeCodes = data->template GetValues<i8>(1);
        const i32* offsets = data->template GetValues<i32>(2);
        for (i64 i = 0; i < data->length; ++i) {
            i8 tc = typeCodes[i];
            i32 vo = offsets[i];
            const i64* childVals = data->child_data[tc]->template GetValues<i64>(1);
            chunk.emplace_back(tc, childVals[vo]);
        }
        result.push_back(std::move(chunk));
    });
    return result;
}

arrow::Datum ToChunkedArray(TBlockArrayTree& tree) {
    return ToChunkedArray(tree, arrow::default_memory_pool());
}

} // namespace

Y_UNIT_TEST_SUITE(TI64LeafTreeTest) {

Y_UNIT_TEST(SinglePayload_ProducesSingleChunk) {
    auto tree = MakeI64Tree({10, 20, 30, 40, 50});
    auto chunks = ReadI64Chunks(ToChunkedArray(*tree));

    UNIT_ASSERT_EQUAL(chunks.size(), 1U);
    UNIT_ASSERT_EQUAL(chunks[0], (TVector<i64>{10, 20, 30, 40, 50}));
}

Y_UNIT_TEST(TwoPayloads_ProduceTwoChunks) {
    auto tree = MakeI64Tree(TVector<TVector<i64>>{{1, 2, 3}, {4, 5}});
    auto chunks = ReadI64Chunks(ToChunkedArray(*tree));

    UNIT_ASSERT_EQUAL(chunks.size(), 2U);
    UNIT_ASSERT_EQUAL(chunks[0], (TVector<i64>{1, 2, 3}));
    UNIT_ASSERT_EQUAL(chunks[1], (TVector<i64>{4, 5}));
}

Y_UNIT_TEST(ThreePayloads_PreservesAllValues) {
    auto tree = MakeI64Tree(TVector<TVector<i64>>{{10}, {20, 30}, {40, 50, 60}});
    auto chunks = ReadI64Chunks(ToChunkedArray(*tree));

    UNIT_ASSERT_EQUAL(chunks.size(), 3U);
    UNIT_ASSERT_EQUAL(chunks[0], (TVector<i64>{10}));
    UNIT_ASSERT_EQUAL(chunks[1], (TVector<i64>{20, 30}));
    UNIT_ASSERT_EQUAL(chunks[2], (TVector<i64>{40, 50, 60}));
}

Y_UNIT_TEST(SingleElement_SingleChunk) {
    auto tree = MakeI64Tree({42});
    auto chunks = ReadI64Chunks(ToChunkedArray(*tree));

    UNIT_ASSERT_EQUAL(chunks.size(), 1U);
    UNIT_ASSERT_EQUAL(chunks[0], (TVector<i64>{42}));
}

} // Y_UNIT_TEST_SUITE(TI64LeafTreeTest)

Y_UNIT_TEST_SUITE(TTupleTreeTest) {

Y_UNIT_TEST(UniformChildren_SingleChunk) {
    auto tree = MakeTupleTree({1, 2, 3}, {10, 20, 30});
    auto chunks = ReadTupleChunks(ToChunkedArray(*tree));

    using Row = std::pair<i64, i64>;
    UNIT_ASSERT_EQUAL(chunks.size(), 1U);
    UNIT_ASSERT_EQUAL(chunks[0], (TVector<Row>{{1, 10}, {2, 20}, {3, 30}}));
}

Y_UNIT_TEST(ChildrenWithDifferentGranularity_ProducesMultipleChunks) {
    auto tree = MakeTupleTree(
        TVector<TVector<i64>>{{10, 20, 30}, {40, 50}},
        TVector<TVector<i64>>{{100, 200}, {300, 400, 500}});

    using Row = std::pair<i64, i64>;
    auto chunks = ReadTupleChunks(ToChunkedArray(*tree));

    UNIT_ASSERT_EQUAL(chunks.size(), 3U);
    UNIT_ASSERT_EQUAL(chunks[0], (TVector<Row>{{10, 100}, {20, 200}}));
    UNIT_ASSERT_EQUAL(chunks[1], (TVector<Row>{{30, 300}}));
    UNIT_ASSERT_EQUAL(chunks[2], (TVector<Row>{{40, 400}, {50, 500}}));
}

Y_UNIT_TEST(SingleElementPayloads_EachBecomesOwnChunk) {
    auto tree = MakeTupleTree(
        TVector<TVector<i64>>{{1}, {2}, {3}},
        TVector<TVector<i64>>{{10}, {20}, {30}});

    using Row = std::pair<i64, i64>;
    auto chunks = ReadTupleChunks(ToChunkedArray(*tree));

    UNIT_ASSERT_EQUAL(chunks.size(), 3U);
    UNIT_ASSERT_EQUAL(chunks[0], (TVector<Row>{{1, 10}}));
    UNIT_ASSERT_EQUAL(chunks[1], (TVector<Row>{{2, 20}}));
    UNIT_ASSERT_EQUAL(chunks[2], (TVector<Row>{{3, 30}}));
}

Y_UNIT_TEST(OneFieldHasLargerChunks_SlicedBySmaller) {
    auto tree = MakeTupleTree(
        TVector<TVector<i64>>{{1, 2, 3, 4, 5, 6}},
        TVector<TVector<i64>>{{10, 20}, {30, 40}, {50, 60}});

    using Row = std::pair<i64, i64>;
    auto chunks = ReadTupleChunks(ToChunkedArray(*tree));

    UNIT_ASSERT_EQUAL(chunks.size(), 3U);
    UNIT_ASSERT_EQUAL(chunks[0], (TVector<Row>{{1, 10}, {2, 20}}));
    UNIT_ASSERT_EQUAL(chunks[1], (TVector<Row>{{3, 30}, {4, 40}}));
    UNIT_ASSERT_EQUAL(chunks[2], (TVector<Row>{{5, 50}, {6, 60}}));
}

} // Y_UNIT_TEST_SUITE(TTupleTreeTest)

Y_UNIT_TEST_SUITE(TUnionTreeTest) {

Y_UNIT_TEST(AllElementsFit_SingleChunk) {
    auto tree = MakeUnionTree(
        {0, 1, 0, 1, 0},
        {{10, 30, 50}},
        {{20, 40}});

    using Row = std::pair<i8, i64>;
    auto chunks = ReadUnionChunks(ToChunkedArray(*tree));

    UNIT_ASSERT_EQUAL(chunks.size(), 1U);
    UNIT_ASSERT_EQUAL(chunks[0], (TVector<Row>{{0, 10}, {1, 20}, {0, 30}, {1, 40}, {0, 50}}));
}

Y_UNIT_TEST(ChildExhaustsEarly_SlicedIntoTwoChunks) {
    auto tree = MakeUnionTree(
        {0, 1, 0, 1, 0, 1},
        {{10, 30}, {50}},
        {{20}, {40, 60}});

    using Row = std::pair<i8, i64>;
    auto chunks = ReadUnionChunks(ToChunkedArray(*tree));

    UNIT_ASSERT_EQUAL(chunks.size(), 2U);
    UNIT_ASSERT_EQUAL(chunks[0], (TVector<Row>{{0, 10}, {1, 20}, {0, 30}}));
    UNIT_ASSERT_EQUAL(chunks[1], (TVector<Row>{{1, 40}, {0, 50}, {1, 60}}));
}

Y_UNIT_TEST(SingleChild_AllSameTypeCode) {
    auto tree = MakeUnionTree(
        {0, 0, 0, 0},
        {{7, 8, 9, 10}},
        {{0}}); // child1 must be non-empty to avoid abort on Slice(child1, 0)

    using Row = std::pair<i8, i64>;
    auto chunks = ReadUnionChunks(ToChunkedArray(*tree));

    UNIT_ASSERT_EQUAL(chunks.size(), 1U);
    UNIT_ASSERT_EQUAL(chunks[0], (TVector<Row>{{0, 7}, {0, 8}, {0, 9}, {0, 10}}));
}

Y_UNIT_TEST(SingleElement) {
    auto tree = MakeUnionTree({1}, {{0}}, {{42}});

    using Row = std::pair<i8, i64>;
    auto chunks = ReadUnionChunks(ToChunkedArray(*tree));

    UNIT_ASSERT_EQUAL(chunks.size(), 1U);
    UNIT_ASSERT_EQUAL(chunks[0], (TVector<Row>{{1, 42}}));
}

Y_UNIT_TEST(ConsecutiveSameTypeCode_ExhaustsChildMidSequence) {
    auto tree = MakeUnionTree(
        {0, 0, 1, 0, 0, 1},
        {{10, 20}, {30, 40}},
        {{100}, {200}});

    using Row = std::pair<i8, i64>;
    auto chunks = ReadUnionChunks(ToChunkedArray(*tree));

    UNIT_ASSERT_EQUAL(chunks.size(), 2U);
    UNIT_ASSERT_EQUAL(chunks[0], (TVector<Row>{{0, 10}, {0, 20}, {1, 100}}));
    UNIT_ASSERT_EQUAL(chunks[1], (TVector<Row>{{0, 30}, {0, 40}, {1, 200}}));
}

Y_UNIT_TEST(ChildPayloadChopped_OffsetCorrectInNextChunk) {
    auto tree = MakeUnionTree(
        {0, 0, 1, 0, 1},
        {{10, 20}, {30}},
        {{100, 200}});

    using Row = std::pair<i8, i64>;
    auto chunks = ReadUnionChunks(ToChunkedArray(*tree));

    UNIT_ASSERT_EQUAL(chunks.size(), 2U);
    UNIT_ASSERT_EQUAL(chunks[0], (TVector<Row>{{0, 10}, {0, 20}, {1, 100}}));
    UNIT_ASSERT_EQUAL(chunks[1], (TVector<Row>{{0, 30}, {1, 200}}));
}

Y_UNIT_TEST(BothChildrenCycleThroughPayloads_TwoChunks) {
    auto tree = MakeUnionTree(
        {0, 1, 0, 1, 0, 1},
        {{10, 20}, {30}},
        {{100}, {200, 300}});

    using Row = std::pair<i8, i64>;
    auto chunks = ReadUnionChunks(ToChunkedArray(*tree));

    UNIT_ASSERT_EQUAL(chunks.size(), 2U);
    UNIT_ASSERT_EQUAL(chunks[0], (TVector<Row>{{0, 10}, {1, 100}, {0, 20}}));
    UNIT_ASSERT_EQUAL(chunks[1], (TVector<Row>{{1, 200}, {0, 30}, {1, 300}}));
}

Y_UNIT_TEST(ThreeChunks_RepeatPattern) {
    auto tree = MakeUnionTree(
        {0, 1, 0, 1, 0, 1, 0, 1, 0},
        {{1, 2}, {3}, {4, 5}},
        {{10}, {20, 30}, {40}});

    using Row = std::pair<i8, i64>;
    auto chunks = ReadUnionChunks(ToChunkedArray(*tree));

    UNIT_ASSERT_EQUAL(chunks.size(), 3U);
    UNIT_ASSERT_EQUAL(chunks[0], (TVector<Row>{{0, 1}, {1, 10}, {0, 2}}));
    UNIT_ASSERT_EQUAL(chunks[1], (TVector<Row>{{1, 20}, {0, 3}, {1, 30}}));
    UNIT_ASSERT_EQUAL(chunks[2], (TVector<Row>{{0, 4}, {1, 40}, {0, 5}}));
}

Y_UNIT_TEST(ExhaustionOnLastElement_NaturalLoopEnd) {
    auto tree = MakeUnionTree(
        {0, 1, 0, 1, 0},
        {{10, 20, 30}},
        {{100, 200}});

    using Row = std::pair<i8, i64>;
    auto chunks = ReadUnionChunks(ToChunkedArray(*tree));

    UNIT_ASSERT_EQUAL(chunks.size(), 1U);
    UNIT_ASSERT_EQUAL(chunks[0], (TVector<Row>{{0, 10}, {1, 100}, {0, 20}, {1, 200}, {0, 30}}));
}

} // Y_UNIT_TEST_SUITE(TUnionTreeTest)
