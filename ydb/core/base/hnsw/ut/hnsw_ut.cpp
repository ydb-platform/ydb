#include <ydb/core/base/hnsw.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/cast.h>

namespace NKikimr::NTableIndex::NHnsw {
namespace {

double Distance(TStringBuf a, TStringBuf b) {
    return std::abs(FromString<double>(a) - FromString<double>(b));
}

TBuilder Build(ui32 count, TSettings settings = {}) {
    TBuilder builder(settings, 17, Distance);
    for (ui32 i = 0; i < count; ++i) {
        builder.Add(ToString(i), ToString((i * 397) % 1009));
    }
    return builder;
}

}

Y_UNIT_TEST_SUITE(THnsw) {
    Y_UNIT_TEST(EmptyAndSingleNode) {
        auto empty = Build(0);
        TSearch search(empty.GetMeta(), "0", 1);
        UNIT_ASSERT(search.Step([](ui64) -> std::optional<TNode> { UNIT_FAIL("Unexpected read"); return std::nullopt; }, Distance, 1)
            == TSearch::EStatus::Done);
        UNIT_ASSERT(search.GetResult().empty());
        auto single = Build(1);
        TSearch one(single.GetMeta(), "1", 1);
        auto read = [&](ui64 id) -> std::optional<TNode> { return single.GetNodes().at(id - 1); };
        while (one.Step(read, Distance, 1) != TSearch::EStatus::Done) {}
        UNIT_ASSERT_VALUES_EQUAL(one.GetResult().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(one.GetResult()[0].Id, 1);
    }

    Y_UNIT_TEST(DeterministicPersistentGraph) {
        const auto first = Build(256);
        const auto second = Build(256);
        UNIT_ASSERT_VALUES_EQUAL(first.GetMeta().EntryId, second.GetMeta().EntryId);
        UNIT_ASSERT_VALUES_EQUAL(first.GetMeta().Level, second.GetMeta().Level);
        for (size_t i = 0; i < first.GetNodes().size(); ++i) {
            const auto& node = first.GetNodes()[i];
            const auto encoded = EncodeNeighbors(node.Neighbors);
            UNIT_ASSERT_VALUES_EQUAL(encoded, EncodeNeighbors(second.GetNodes()[i].Neighbors));
            const auto decoded = DecodeNeighbors(encoded, node.Neighbors.size() - 1, 16, first.GetMeta().Count);
            UNIT_ASSERT_VALUES_EQUAL(EncodeNeighbors(decoded), encoded);
            for (const auto& layer : decoded) {
                UNIT_ASSERT(std::find(layer.begin(), layer.end(), i + 1) == layer.end());
            }
        }
    }

    Y_UNIT_TEST(ResumeAfterEveryReadAndCpuQuantum) {
        const auto graph = Build(256);
        auto read = [&](ui64 id) -> std::optional<TNode> { return graph.GetNodes().at(id - 1); };
        TSearch reference(graph.GetMeta(), "503", 64);
        while (reference.Step(read, Distance, 64) != TSearch::EStatus::Done) {}
        const auto expected = reference.GetResult();
        TSearch resumed(graph.GetMeta(), "503", 64);
        bool fault = true;
        ui32 faults = 0;
        auto faultyRead = [&](ui64 id) -> std::optional<TNode> {
            fault = !fault;
            if (!fault) { ++faults; return std::nullopt; }
            return read(id);
        };
        ui32 steps = 0;
        while (resumed.Step(faultyRead, Distance, 1) != TSearch::EStatus::Done) {
            UNIT_ASSERT(++steps < 10000);
        }
        UNIT_ASSERT(faults > 10);
        const auto actual = resumed.GetResult();
        UNIT_ASSERT_VALUES_EQUAL(actual.size(), expected.size());
        for (size_t i = 0; i < actual.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(actual[i].Id, expected[i].Id);
            UNIT_ASSERT_VALUES_EQUAL(actual[i].Distance, expected[i].Distance);
        }
    }

    Y_UNIT_TEST(RecallAndPointReads) {
        const auto graph = Build(1000);
        ui32 matches = 0;
        ui64 reads = 0;
        for (ui32 query = 0; query < 20; ++query) {
            auto target = ToString(query * 47 + 0.25);
            TSearch search(graph.GetMeta(), target, 64);
            auto read = [&](ui64 id) -> std::optional<TNode> { ++reads; return graph.GetNodes().at(id - 1); };
            while (search.Step(read, Distance, 64) != TSearch::EStatus::Done) {}
            auto actual = search.GetResult();
            TVector<TCandidate> expected;
            for (ui32 id = 1; id <= graph.GetNodes().size(); ++id) {
                expected.push_back({Distance(graph.GetNodes()[id - 1].Embedding, target), id});
            }
            std::sort(expected.begin(), expected.end());
            UNIT_ASSERT(actual.size() >= 10);
            for (ui32 i = 0; i < 10; ++i) matches += actual[i].Id == expected[i].Id;
        }
        UNIT_ASSERT_C(matches >= 190, matches);
        UNIT_ASSERT_C(reads < 20 * 500, reads);
    }

    Y_UNIT_TEST(RejectCorruptAdjacency) {
        const auto data = EncodeNeighbors({{3, 1}, {2}});
        UNIT_ASSERT_EXCEPTION(DecodeNeighbors(data, 0, 16, 3), yexception);
        UNIT_ASSERT_EXCEPTION(DecodeNeighbors(data, 1, 16, 2), yexception);
        UNIT_ASSERT_EXCEPTION(DecodeNeighbors(data + "x", 1, 16, 3), yexception);
        for (size_t n = 0; n < data.size(); ++n) {
            UNIT_ASSERT_EXCEPTION(DecodeNeighbors(TStringBuf(data).SubStr(0, n), 1, 16, 3), yexception);
        }
        auto version = data;
        version[0] = 2;
        UNIT_ASSERT_EXCEPTION(DecodeNeighbors(version, 1, 16, 3), yexception);
        UNIT_ASSERT_EXCEPTION(DecodeNeighbors(TString(11, '\xff'), 1, 16, 3), yexception);
        UNIT_ASSERT_EXCEPTION(DecodeNeighbors(EncodeNeighbors({{1, 2, 3, 4, 5}}), 0, 2, 5), yexception);
    }

    Y_UNIT_TEST(DuplicateVectorsRemainReachable) {
        TSettings settings;
        settings.M = 4;
        settings.EfConstruction = 16;
        auto distance = [](TStringBuf, TStringBuf) { return 0.; };
        TBuilder graph(settings, 17, distance);
        for (ui32 id = 0; id < 128; ++id) graph.Add(ToString(id), "same");
        TSearch search(graph.GetMeta(), "same", 64);
        auto read = [&](ui64 id) -> std::optional<TNode> { return graph.GetNodes().at(id - 1); };
        while (search.Step(read, distance, 128) != TSearch::EStatus::Done) {}
        const auto result = search.GetResult();
        UNIT_ASSERT_VALUES_EQUAL(result.size(), 64);
        THashSet<ui64> ids;
        for (const auto& candidate : result) ids.insert(candidate.Id);
        UNIT_ASSERT_VALUES_EQUAL(ids.size(), result.size());
        for (const auto& node : graph.GetNodes()) {
            DecodeNeighbors(EncodeNeighbors(node.Neighbors), node.Neighbors.size() - 1, settings.M, graph.GetMeta().Count);
        }
    }

    Y_UNIT_TEST(ResourceLimitsAndSettings) {
        TSettings settings;
        settings.M = 0;
        UNIT_ASSERT_EXCEPTION(settings.Validate(), yexception);
        settings = {};
        settings.EfConstruction = 15;
        UNIT_ASSERT_EXCEPTION(settings.Validate(), yexception);
        settings = {};
        settings.MaxNodes = 1;
        UNIT_ASSERT_EXCEPTION(Build(2, settings), yexception);
        settings = {};
        settings.MaxBytes = 1;
        UNIT_ASSERT_EXCEPTION(Build(1, settings), yexception);
        auto graph = Build(256);
        TSearch search(graph.GetMeta(), "100", 64, 1);
        auto read = [&](ui64 id) -> std::optional<TNode> { return graph.GetNodes().at(id - 1); };
        UNIT_ASSERT_EXCEPTION(search.Step(read, Distance, 1000), yexception);
    }
}

}
