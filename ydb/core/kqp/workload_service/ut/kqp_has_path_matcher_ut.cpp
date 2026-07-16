#include <ydb/core/kqp/workload_service/kqp_has_path_matcher.h>
#include <ydb/core/resource_pools/regex_predicate.h>

#include <ydb/library/yql/providers/pq/proto/dq_io.pb.h>

#include <google/protobuf/any.pb.h>

#include <library/cpp/testing/unittest/registar.h>


namespace NKikimr::NKqp {

using NResourcePool::TRegexPredicate;
using NKqpProto::TKqpPhyQuery;


namespace {

NKqpProto::TKqpPhyStage* GetOrAddStage(TKqpPhyQuery& phy) {
    auto* tx = phy.TransactionsSize() ? phy.MutableTransactions(0) : phy.AddTransactions();
    return tx->StagesSize() ? tx->MutableStages(0) : tx->AddStages();
}

void AddTxTable(TKqpPhyQuery& phy, const TString& path) {
    auto* tx = phy.TransactionsSize() ? phy.MutableTransactions(0) : phy.AddTransactions();
    tx->AddTables()->MutableId()->SetPath(path);
}

void AddPqTopicSource(TKqpPhyQuery& phy, const TString& topicPath) {
    auto* source = GetOrAddStage(phy)->AddSources();
    NYql::NPq::NProto::TDqPqTopicSource pqSource;
    pqSource.SetTopicPath(topicPath);
    source->MutableExternalSource()->MutableSettings()->PackFrom(pqSource);
}

void AddPqTopicSink(TKqpPhyQuery& phy, const TString& topicPath) {
    auto* sink = GetOrAddStage(phy)->AddSinks();
    NYql::NPq::NProto::TDqPqTopicSink pqSink;
    pqSink.SetTopicPath(topicPath);
    sink->MutableExternalSink()->MutableSettings()->PackFrom(pqSink);
}

// Fixed-pattern helpers: each suite pins the predicate so tests read as pure
// build-and-check one-liners (mirrors kqp_has_full_scan_matcher_ut.cpp style).

// Runs the matcher against a phyQuery holding one tx-level table entry.
// Predicate + path both vary so predicate-behaviour tests can exercise glob
// semantics and canonicalization in isolation.
const auto MatchesTxTable = [](const TString& pattern, const TString& path) {
    TKqpPhyQuery phy;
    AddTxTable(phy, path);
    return NWorkload::MatchesPath(TRegexPredicate::FromGlob(pattern), {}, phy);
};

// Runs the matcher with pattern `/Root/t*` against the given queryTables list.
const auto MatchesQueryTables = [](const TVector<TString>& queryTables) {
    TKqpPhyQuery phy;
    return NWorkload::MatchesPath(TRegexPredicate::FromGlob("/Root/t*"), queryTables, phy);
};

// Runs the matcher with pattern `/Root/db/target` against a phyQuery built by `build`.
const auto MatchesTx = [](auto build) {
    TKqpPhyQuery phy;
    build(phy);
    return NWorkload::MatchesPath(TRegexPredicate::FromGlob("/Root/db/target"), {}, phy);
};

// Runs the matcher with pattern `/Root/db/topic` against a phyQuery built by `build`.
const auto MatchesTopics = [](auto build) {
    TKqpPhyQuery phy;
    build(phy);
    return NWorkload::MatchesPath(TRegexPredicate::FromGlob("/Root/db/topic"), {}, phy);
};

}  // anonymous namespace


Y_UNIT_TEST_SUITE(TPathMatcherPredicate) {

    Y_UNIT_TEST(EmptyPredicateAcceptsAny) {
        TKqpPhyQuery phy;
        AddTxTable(phy, "/Root/anything");
        UNIT_ASSERT(NWorkload::MatchesPath(std::nullopt, {}, phy));
    }

    Y_UNIT_TEST(NonMatchingPathIsNotAccepted) {
        UNIT_ASSERT(!MatchesTxTable("/Root/critical", "/Root/other"));
    }

    Y_UNIT_TEST(GlobPatternMatches) {
        UNIT_ASSERT(MatchesTxTable("/Root/db/archive*", "/Root/db/archive_2024"));
    }

    Y_UNIT_TEST(CanonizesPathWithoutLeadingSlash) {
        // Plan-side paths may omit the leading '/'. CanonizePath must be applied
        // so patterns anchored with '/' still match.
        UNIT_ASSERT(MatchesTxTable("/Root/db/orders", "Root/db/orders"));
    }
}

Y_UNIT_TEST_SUITE(TPathMatcherQueryTables) {

    Y_UNIT_TEST(SinglePathMatches) {
        UNIT_ASSERT(MatchesQueryTables({"/Root/t"}));
    }

    Y_UNIT_TEST(NoMatchingPath) {
        UNIT_ASSERT(!MatchesQueryTables({"/Root/other"}));
    }

    Y_UNIT_TEST(MixedPathsReturnTrueOnAnyMatch) {
        UNIT_ASSERT(MatchesQueryTables({"/Root/other", "/Root/t"}));
    }

    Y_UNIT_TEST(IndexSubTablePath) {
        // Secondary-index sub-tables show up in QueryTables at .../indexImplTable.
        UNIT_ASSERT(MatchesQueryTables({"/Root/t/idx/indexImplTable"}));
    }
}

Y_UNIT_TEST_SUITE(TPathMatcherTxTables) {

    Y_UNIT_TEST(TableInTxMatches) {
        UNIT_ASSERT(MatchesTx([](auto& phy) { AddTxTable(phy, "/Root/db/target"); }));
    }

    Y_UNIT_TEST(BothExternalEntriesGetWalked) {
        // The compiler emits BOTH the External Table's path and its underlying
        // External Data Source's path as separate TKqpPhyTable entries. A
        // classifier targeting either one fires — this test proves the walk
        // reaches the second entry.
        UNIT_ASSERT(MatchesTx([](auto& phy) {
            AddTxTable(phy, "/Root/db/other");
            AddTxTable(phy, "/Root/db/target");
        }));
    }

    Y_UNIT_TEST(MultipleTransactions) {
        UNIT_ASSERT(MatchesTx([](auto& phy) {
            phy.AddTransactions()->AddTables()->MutableId()->SetPath("/Root/db/other");
            phy.AddTransactions()->AddTables()->MutableId()->SetPath("/Root/db/target");
        }));
    }
}

Y_UNIT_TEST_SUITE(TPathMatcherTopics) {

    Y_UNIT_TEST(PqTopicSourceMatches) {
        UNIT_ASSERT(MatchesTopics([](auto& phy) { AddPqTopicSource(phy, "/Root/db/topic"); }));
    }

    Y_UNIT_TEST(PqTopicSinkMatches) {
        UNIT_ASSERT(MatchesTopics([](auto& phy) { AddPqTopicSink(phy, "/Root/db/topic"); }));
    }

    Y_UNIT_TEST(NonPqExternalSourceIsIgnored) {
        // ExternalSource whose Settings is not TDqPqTopicSource/Sink must not
        // spuriously match. Empty Any is a non-PQ shape.
        UNIT_ASSERT(!MatchesTopics([](auto& phy) {
            GetOrAddStage(phy)->AddSources()->MutableExternalSource();
        }));
    }

    Y_UNIT_TEST(TopicPathWithoutLeadingSlashMatches) {
        UNIT_ASSERT(MatchesTopics([](auto& phy) { AddPqTopicSource(phy, "Root/db/topic"); }));
    }
}

}  // namespace NKikimr::NKqp
