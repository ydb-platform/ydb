#include <ydb/library/generated_columns/generated_column_dependencies.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/cast.h>
#include <util/string/split.h>

using namespace NKikimr::NGeneratedColumns;

namespace {

TGeneratedColumnDependencyGraph BuildGraph(const TVector<TString>& columns,
    const TMap<TString, TVector<TString>>& dependencies)
{
    TGeneratedColumnDependencyGraph graph;
    TString error;
    UNIT_ASSERT_C(BuildGeneratedColumnDependencyGraph(columns, dependencies, graph, error), error);
    UNIT_ASSERT(error.empty());
    UNIT_ASSERT_VALUES_EQUAL(graph.EvaluationOrder.size(), dependencies.size());
    THashMap<TString, size_t> positions;
    for (size_t i = 0; i < graph.EvaluationOrder.size(); ++i) {
        UNIT_ASSERT(positions.emplace(graph.EvaluationOrder[i], i).second);
    }
    for (const auto& [name, refs] : dependencies) {
        for (const auto& ref : refs) {
            if (dependencies.contains(ref)) {
                UNIT_ASSERT_C(positions.at(ref) < positions.at(name), ref << " must precede " << name);
            }
        }
    }
    return graph;
}

TString Reject(const TVector<TString>& columns, const TMap<TString, TVector<TString>>& dependencies) {
    TGeneratedColumnDependencyGraph graph;
    graph.Dependencies["stale"] = {};
    graph.Dependents["stale"] = {"stale"};
    graph.EvaluationOrder = {"stale"};
    TString error;
    UNIT_ASSERT(!BuildGeneratedColumnDependencyGraph(columns, dependencies, graph, error));
    UNIT_ASSERT(!error.empty());
    UNIT_ASSERT(graph.Dependencies.empty());
    UNIT_ASSERT(graph.Dependents.empty());
    UNIT_ASSERT(graph.EvaluationOrder.empty());
    return error;
}

void RejectCycle(const TVector<TString>& columns, const TMap<TString, TVector<TString>>& dependencies) {
    const auto error = Reject(columns, dependencies);
    const TString prefix = "Generated column dependency cycle: ";
    UNIT_ASSERT_C(error.StartsWith(prefix), error);
    const TVector<TString> path = StringSplitter(error.substr(prefix.size())).SplitByString(" -> ").ToList<TString>();
    UNIT_ASSERT(path.size() >= 3);
    UNIT_ASSERT_VALUES_EQUAL(path.front(), path.back());
    for (size_t i = 1; i < path.size(); ++i) {
        const auto& refs = dependencies.at(path[i - 1]);
        UNIT_ASSERT_C(std::find(refs.begin(), refs.end(), path[i]) != refs.end(), error);
    }
}

} // namespace

Y_UNIT_TEST_SUITE(TGeneratedColumnDependencies) {
    Y_UNIT_TEST(Empty) {
        const auto graph = BuildGraph({}, {});
        UNIT_ASSERT(graph.Dependents.empty());
    }

    Y_UNIT_TEST(IndependentConstants) {
        BuildGraph({"a", "b", "c"}, {{"a", {}}, {"b", {}}, {"c", {}}});
    }

    Y_UNIT_TEST(ChainAndForwardReference) {
        BuildGraph({"g2", "g1", "a"}, {{"g2", {"g1"}}, {"g1", {"a"}}});
    }

    Y_UNIT_TEST(Diamond) {
        const auto graph = BuildGraph({"a", "b", "c", "d"},
            {{"a", {}}, {"b", {"a"}}, {"c", {"a"}}, {"d", {"b", "c"}}});
        UNIT_ASSERT_VALUES_EQUAL(graph.Dependents.at("a").size(), 2);
    }

    Y_UNIT_TEST(DisconnectedCycle) {
        RejectCycle({"a", "b", "c", "d"}, {{"a", {}}, {"b", {"a"}}, {"c", {"d"}}, {"d", {"c"}}});
    }

    Y_UNIT_TEST(SelfCycle) {
        UNIT_ASSERT_STRING_CONTAINS(Reject({"a"}, {{"a", {"a"}}}), "can not reference itself");
    }

    Y_UNIT_TEST(TwoNodeCycle) {
        RejectCycle({"a", "b"}, {{"a", {"b"}}, {"b", {"a"}}});
    }

    Y_UNIT_TEST(LongCycle) {
        RejectCycle({"a", "b", "c", "d"}, {{"a", {"b"}}, {"b", {"c"}}, {"c", {"d"}}, {"d", {"b"}}});
    }

    Y_UNIT_TEST(UnknownDependency) {
        UNIT_ASSERT_STRING_CONTAINS(Reject({"a"}, {{"a", {"missing"}}}), "missing");
        UNIT_ASSERT_STRING_CONTAINS(Reject({"a"}, {{"missing", {"a"}}}), "missing");
    }

    Y_UNIT_TEST(DuplicateReferences) {
        const auto graph = BuildGraph({"a", "b", "c"}, {{"c", {"b", "a", "b", "a"}}});
        UNIT_ASSERT_VALUES_EQUAL(graph.Dependencies.at("c"), (TVector<TString>{"a", "b"}));
        UNIT_ASSERT_VALUES_EQUAL(graph.Dependents.at("a"), (TVector<TString>{"c"}));
    }

    Y_UNIT_TEST(DeterministicDiagnostics) {
        const auto first = Reject({"a", "b", "c"}, {{"a", {"c", "b"}}, {"b", {"a"}}, {"c", {"a"}}});
        const auto second = Reject({"c", "b", "a"}, {{"a", {"b", "c", "b"}}, {"b", {"a"}}, {"c", {"a"}}});
        UNIT_ASSERT_VALUES_EQUAL(first, second);
    }

    Y_UNIT_TEST(AffectedClosure) {
        const auto graph = BuildGraph({"a", "b", "g1", "g2", "g3", "other"},
            {{"g1", {"a"}}, {"g2", {"g1"}}, {"g3", {"g1", "g2"}}, {"other", {"b"}}});
        const auto affected = CollectAffectedGeneratedColumns(graph, {"a"});
        UNIT_ASSERT_VALUES_EQUAL(affected.size(), 3);
        UNIT_ASSERT(affected.contains("g1") && affected.contains("g2") && affected.contains("g3"));
        UNIT_ASSERT(CollectAffectedGeneratedColumns(graph, {"unknown"}).empty());
        const auto descendants = CollectAffectedGeneratedColumns(graph, {"g1"});
        UNIT_ASSERT_VALUES_EQUAL(descendants.size(), 2);
        UNIT_ASSERT(descendants.contains("g2") && descendants.contains("g3"));
    }

    Y_UNIT_TEST(DeepChain) {
        TVector<TString> columns;
        TMap<TString, TVector<TString>> dependencies;
        for (size_t i = 0; i < 10000; ++i) {
            const auto name = ToString(i);
            columns.push_back(name);
            dependencies[name] = i == 9999 ? TVector<TString>{} : TVector<TString>{ToString(i + 1)};
        }
        const auto graph = BuildGraph(columns, dependencies);
        UNIT_ASSERT_VALUES_EQUAL(CollectAffectedGeneratedColumns(graph, {"9999"}).size(), 9999);
    }
}
