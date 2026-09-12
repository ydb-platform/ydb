#include <ydb/core/tx/columnshard/engines/reader/plain_reader/constructor/constructor.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/constructor/constructor.h>
#include <ydb/core/tx/columnshard/engines/reader/trivial_reader/constructor/constructor.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NOlap::NReader {

// A scan cursor stores a position in the sources order, so only ESourcesSorting decides which cursor
// shape a resumed scan accepts. Deriving it from anything else (the sorting the query asked for, say)
// makes a node reject the cursor a differently-versioned node produced for the very same order.
Y_UNIT_TEST_SUITE(ScanCursorCompatibility) {
    using TImpl = NKikimrKqp::TEvKqpScanCursor::ImplementationCase;

    NKikimrKqp::TEvKqpScanCursor MakeCursorProto(const TImpl impl) {
        NKikimrKqp::TEvKqpScanCursor proto;
        switch (impl) {
            case NKikimrKqp::TEvKqpScanCursor::kColumnShardSimple:
                proto.MutableColumnShardSimple()->SetSourceIdx(3);
                proto.MutableColumnShardSimple()->SetStartRecordIndex(7);
                break;
            case NKikimrKqp::TEvKqpScanCursor::kColumnShardNotSortedSimple:
                proto.MutableColumnShardNotSortedSimple()->SetSourceIdx(3);
                proto.MutableColumnShardNotSortedSimple()->SetStartRecordIndex(7);
                break;
            case NKikimrKqp::TEvKqpScanCursor::kDeprecatedColumnShardSimple:
                proto.MutableDeprecatedColumnShardSimple()->SetSourceId(3);
                proto.MutableDeprecatedColumnShardSimple()->SetStartRecordIndex(7);
                break;
            case NKikimrKqp::TEvKqpScanCursor::kDeprecatedColumnShardNotSortedSimple:
                proto.MutableDeprecatedColumnShardNotSortedSimple()->SetSourceId(3);
                proto.MutableDeprecatedColumnShardNotSortedSimple()->SetStartRecordIndex(7);
                break;
            case NKikimrKqp::TEvKqpScanCursor::kColumnShardPlain:
                proto.MutableColumnShardPlain();
                break;
            case NKikimrKqp::TEvKqpScanCursor::IMPLEMENTATION_NOT_SET:
                break;
        }
        return proto;
    }

    template <class TConstructor>
    TConclusion<std::shared_ptr<IScanCursor>> BuildCursor(const ESourcesSorting sorting, const TImpl impl) {
        const TConstructor constructor{ TScannerConstructorContext(TSnapshot::Zero(), 0) };
        return constructor.BuildCursorFromProto(MakeCursorProto(impl), sorting);
    }

    template <class TConstructor, class TExpectedCursor>
    void CheckBuilds(const ESourcesSorting sorting, const TImpl impl) {
        auto conclusion = BuildCursor<TConstructor>(sorting, impl);
        UNIT_ASSERT_C(!conclusion.IsFail(), conclusion.GetErrorMessage());
        UNIT_ASSERT(std::dynamic_pointer_cast<TExpectedCursor>(conclusion.GetResult()));
    }

    template <class TConstructor>
    void CheckFails(const ESourcesSorting sorting, const TImpl impl) {
        UNIT_ASSERT(BuildCursor<TConstructor>(sorting, impl).IsFail());
    }

    template <class TConstructor>
    void CheckAllPairs() {
        // Sources ordered by key: what every ORDER BY scan gets, and what a deduplicating scan gets even
        // with no ORDER BY. The last one is why the cursor must not be chosen by the requested sorting.
        for (const auto sorting : { ESourcesSorting::FirstPkAsc, ESourcesSorting::LastPkAsc, ESourcesSorting::LastPkDesc }) {
            CheckBuilds<TConstructor, TSourceIndexScanCursor>(sorting, NKikimrKqp::TEvKqpScanCursor::kColumnShardSimple);
            CheckBuilds<TConstructor, TSourceIdScanCursor>(sorting, NKikimrKqp::TEvKqpScanCursor::kDeprecatedColumnShardSimple);
            CheckFails<TConstructor>(sorting, NKikimrKqp::TEvKqpScanCursor::kColumnShardNotSortedSimple);
            CheckFails<TConstructor>(sorting, NKikimrKqp::TEvKqpScanCursor::kDeprecatedColumnShardNotSortedSimple);
            CheckFails<TConstructor>(sorting, NKikimrKqp::TEvKqpScanCursor::kColumnShardPlain);
            CheckFails<TConstructor>(sorting, NKikimrKqp::TEvKqpScanCursor::IMPLEMENTATION_NOT_SET);
        }

        const auto sorting = ESourcesSorting::SourceIdAsc;
        CheckBuilds<TConstructor, TSourceIndexScanCursor>(sorting, NKikimrKqp::TEvKqpScanCursor::kColumnShardNotSortedSimple);
        CheckBuilds<TConstructor, TSourceIdScanCursor>(sorting, NKikimrKqp::TEvKqpScanCursor::kDeprecatedColumnShardNotSortedSimple);
        CheckFails<TConstructor>(sorting, NKikimrKqp::TEvKqpScanCursor::kColumnShardSimple);
        CheckFails<TConstructor>(sorting, NKikimrKqp::TEvKqpScanCursor::kDeprecatedColumnShardSimple);
        CheckFails<TConstructor>(sorting, NKikimrKqp::TEvKqpScanCursor::kColumnShardPlain);
        CheckFails<TConstructor>(sorting, NKikimrKqp::TEvKqpScanCursor::IMPLEMENTATION_NOT_SET);
    }

    Y_UNIT_TEST(TrivialReader) {
        CheckAllPairs<NTrivial::TIndexScannerConstructor>();
    }

    Y_UNIT_TEST(SimpleReader) {
        CheckAllPairs<NSimple::TIndexScannerConstructor>();
    }

    Y_UNIT_TEST(PlainReader) {
        for (const auto sorting :
            { ESourcesSorting::SourceIdAsc, ESourcesSorting::FirstPkAsc, ESourcesSorting::LastPkAsc, ESourcesSorting::LastPkDesc }) {
            CheckBuilds<NPlain::TIndexScannerConstructor, TPlainScanCursor>(sorting, NKikimrKqp::TEvKqpScanCursor::kColumnShardPlain);
            CheckFails<NPlain::TIndexScannerConstructor>(sorting, NKikimrKqp::TEvKqpScanCursor::kColumnShardSimple);
            CheckFails<NPlain::TIndexScannerConstructor>(sorting, NKikimrKqp::TEvKqpScanCursor::kColumnShardNotSortedSimple);
        }
    }
}

}   // namespace NKikimr::NOlap::NReader
