#include <ydb/services/workload_manager/has_full_scan_matcher.h>
#include <ydb/core/resource_pools/regex_predicate.h>

#include <library/cpp/testing/unittest/registar.h>


namespace NKikimr::NWorkloadManager {

using NResourcePool::TRegexPredicate;
using NKqpProto::TKqpPhyQuery;
using NKqpProto::TKqpPhyTableOperation;
using NKqpProto::TKqpReadRangesSource;


namespace {

TKqpPhyTableOperation* AddOp(TKqpPhyQuery& phy, const TString& path) {
    auto* tx = phy.TransactionsSize() ? phy.MutableTransactions(0) : phy.AddTransactions();
    auto* stage = tx->StagesSize() ? tx->MutableStages(0) : tx->AddStages();
    auto* op = stage->AddTableOps();
    op->MutableTable()->SetPath(path);
    return op;
}

TKqpReadRangesSource* AddSource(TKqpPhyQuery& phy, const TString& path) {
    auto* tx = phy.TransactionsSize() ? phy.MutableTransactions(0) : phy.AddTransactions();
    auto* stage = tx->StagesSize() ? tx->MutableStages(0) : tx->AddStages();
    auto* source = stage->AddSources();
    auto* rs = source->MutableReadRangesSource();
    rs->MutableTable()->SetPath(path);
    return rs;
}

// Build a phyQuery on /Root/t via `builder` (AddOp or AddSource), let `configure`
// tweak the resulting op/source, and return the matcher verdict against /Root/t.
const auto Matches = [](auto builder, auto configure) {
    TKqpPhyQuery phy;
    configure(builder(phy, TString("/Root/t")));
    return MatchesFullScan(TRegexPredicate::FromGlob("/Root/t"), phy);
};

const auto AssertHasFullScan       = [](auto c) { UNIT_ASSERT( Matches(AddOp,     c)); };
const auto AssertNoFullScan        = [](auto c) { UNIT_ASSERT(!Matches(AddOp,     c)); };
const auto AssertHasFullScanSource = [](auto c) { UNIT_ASSERT( Matches(AddSource, c)); };
const auto AssertNoFullScanSource  = [](auto c) { UNIT_ASSERT(!Matches(AddSource, c)); };

}  // anonymous namespace


Y_UNIT_TEST_SUITE(TFullScanMatcherPredicate) {

    Y_UNIT_TEST(EmptyPredicateAcceptsAny) {
        TKqpPhyQuery phy;
        UNIT_ASSERT(MatchesFullScan(std::nullopt, phy));
    }

    Y_UNIT_TEST(NonMatchingPathIsIgnored) {
        auto pred = TRegexPredicate::FromGlob("/Root/critical");
        TKqpPhyQuery phy;
        AddOp(phy, "/Root/other")->MutableReadRange()->MutableKeyRange();
        UNIT_ASSERT(!MatchesFullScan(pred, phy));
    }

    Y_UNIT_TEST(GlobPatternMatches) {
        auto pred = TRegexPredicate::FromGlob("/Root/db/orders_archive*");
        TKqpPhyQuery phy;
        AddOp(phy, "/Root/db/orders_archive_2024")->MutableReadRange()->MutableKeyRange();
        UNIT_ASSERT(MatchesFullScan(pred, phy));
    }

    Y_UNIT_TEST(MixedOpsReturnTrueOnAnyMatch) {
        auto pred = TRegexPredicate::FromGlob("/Root/t");
        TKqpPhyQuery phy;
        AddOp(phy, "/Root/other")->MutableReadRange()->MutableKeyRange();
        AddOp(phy, "/Root/t")->MutableReadRange()->MutableKeyRange();
        UNIT_ASSERT(MatchesFullScan(pred, phy));
    }
}

Y_UNIT_TEST_SUITE(TFullScanMatcherOps) {

    Y_UNIT_TEST(ReadRangeEmptyBoundsIsFullScan) {
        AssertHasFullScan([](auto* op) { op->MutableReadRange()->MutableKeyRange(); });
    }

    Y_UNIT_TEST(ReadRangeWithBoundsIsNotFullScan) {
        AssertNoFullScan([](auto* op) {
            op->MutableReadRange()->MutableKeyRange()->MutableFrom()->AddValues();
        });
    }

    Y_UNIT_TEST(ReadRangesEmptyParamNameIsFullScan) {
        // ParamName defaults to "" — matches the documented "full scan" convention.
        AssertHasFullScan([](auto* op) { op->MutableReadRanges()->MutableKeyRanges(); });
    }

    Y_UNIT_TEST(ReadRangesWithParamNameIsNotFullScan) {
        AssertNoFullScan([](auto* op) {
            op->MutableReadRanges()->MutableKeyRanges()->SetParamName("keys");
        });
    }

    Y_UNIT_TEST(ReadOlapRangeEmptyParamNameIsFullScan) {
        AssertHasFullScan([](auto* op) { op->MutableReadOlapRange()->MutableKeyRanges(); });
    }
}

Y_UNIT_TEST_SUITE(TFullScanMatcherSources) {

    Y_UNIT_TEST(SourceEmptyKeyRangeIsFullScan) {
        AssertHasFullScanSource([](auto* rs) { rs->MutableKeyRange(); });
    }

    Y_UNIT_TEST(SourceBoundedKeyRangeIsNotFullScan) {
        AssertNoFullScanSource([](auto* rs) {
            rs->MutableKeyRange()->MutableFrom()->AddValues();
        });
    }

    Y_UNIT_TEST(SourceRangesEmptyParamNameIsFullScan) {
        AssertHasFullScanSource([](auto* rs) { rs->MutableRanges(); });
    }

    Y_UNIT_TEST(SourceRangesWithParamNameIsNotFullScan) {
        AssertNoFullScanSource([](auto* rs) { rs->MutableRanges()->SetParamName("keys"); });
    }

    Y_UNIT_TEST(SourceWithoutRangesOrKeyRangeIsFullScan) {
        // neither KeyRange nor Ranges set
        AssertHasFullScanSource([](auto*) {});
    }
}

// LIMIT is intentionally ignored: a `WHERE filter LIMIT N` with a zero-selectivity
// filter scans the whole table at runtime, so we do not let LIMIT bypass the classifier.
Y_UNIT_TEST_SUITE(TFullScanMatcherLimit) {

    Y_UNIT_TEST(ReadRangeWithLimitIsFullScan) {
        AssertHasFullScan([](auto* op) {
            op->MutableReadRange()->MutableKeyRange();
            op->MutableReadRange()->MutableItemsLimit();
        });
    }

    Y_UNIT_TEST(ReadRangesWithLimitIsFullScan) {
        AssertHasFullScan([](auto* op) {
            op->MutableReadRanges()->MutableKeyRanges();
            op->MutableReadRanges()->MutableItemsLimit();
        });
    }

    Y_UNIT_TEST(ReadOlapRangeWithLimitIsFullScan) {
        AssertHasFullScan([](auto* op) {
            op->MutableReadOlapRange()->MutableKeyRanges();
            op->MutableReadOlapRange()->MutableItemsLimit();
        });
    }

    Y_UNIT_TEST(SourceWithLimitIsFullScan) {
        AssertHasFullScanSource([](auto* rs) { rs->MutableItemsLimit(); });
    }
}

}  // namespace NKikimr::NWorkloadManager
