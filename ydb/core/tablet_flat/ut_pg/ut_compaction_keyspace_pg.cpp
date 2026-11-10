#include <ydb/core/tablet_flat/flat_compaction_context.h>
#include <ydb/core/tablet_flat/test/libs/table/test_writer.h>

#include <library/cpp/testing/unittest/registar.h>
#include <yql/essentials/parser/pg_wrapper/interface/type_desc.h>

extern "C" {
#include "postgres.h"
#include "catalog/pg_type_d.h"
}

namespace NKikimr::NTable {
namespace {

    constexpr TTag TenantTag = 1;
    constexpr TTag NumericTag = 2;
    constexpr TTag ValueTag = 8;

    TString Numeric(const TString& text) {
        auto value = NPg::PgNativeBinaryFromNativeText(text,
            NPg::TypeDescFromPgTypeId(NUMERICOID));
        UNIT_ASSERT_C(!value.Error, *value.Error);
        return std::move(value.Str);
    }

    struct TFixture {
        const bool NativePrefix;
        const TString StoredNumeric = Numeric("1");
        TIntrusiveConstPtr<TRowScheme> Scheme;
        TPartView Part;

        explicit TFixture(bool nativePrefix = false)
            : NativePrefix(nativePrefix)
        {
            TVector<TColumn> columns;
            if (NativePrefix) {
                columns.emplace_back("tenant", TenantTag, NScheme::TTypeInfo(NScheme::NTypeIds::Uint32), "");
                columns.back().KeyOrder = 0;
            }
            columns.emplace_back("numeric", NumericTag,
                NScheme::TTypeInfo(NPg::TypeDescFromPgTypeId(NUMERICOID)), "");
            columns.back().KeyOrder = NativePrefix ? 1 : 0;
            columns.emplace_back("value", ValueTag, NScheme::TTypeInfo(NScheme::NTypeIds::Uint32), "");
            Scheme = TRowScheme::Make(columns, NUtil::TAsIs());

            NPage::TConf conf;
            conf.Final = false;
            conf.ByKeyFilterPrefixes.push_back({1, 0.000001});
            if (NativePrefix) {
                conf.ByKeyFilterPrefixes.push_back({2, 0.000001});
            }
            NTest::TPartCook cook(Scheme, conf);
            Add(cook, 10);
            if (NativePrefix) {
                Add(cook, 1000);
            }
            Part = cook.Finish().ToPartView();
            UNIT_ASSERT_VALUES_EQUAL(Part->ByKeyPrefixes.size(), NativePrefix ? 2 : 1);
        }

        TVector<TCell> Key(const TString& binary, ui32 tenant = 10) const {
            TVector<TCell> key;
            if (NativePrefix) {
                key.push_back(TCell::Make(tenant));
            }
            key.emplace_back(binary.data(), binary.size());
            return key;
        }

        TString EquivalentKeyRejectedByRawBloom() const {
            const auto storedKey = Key(StoredNumeric);
            const auto& [length, bloom] = Part->ByKeyPrefixes.back();
            UNIT_ASSERT(bloom);
            UNIT_ASSERT_VALUES_EQUAL(length, Scheme->Keys->Size());
            // A bloom may have false positives; find an equivalent encoding
            // that it actually rejects so the regression is deterministic.
            for (ui32 scale = 1; scale <= 64; ++scale) {
                auto binary = Numeric(TString("1.") + TString(scale, '0'));
                const auto key = Key(binary);
                const NBloom::TPrefix prefix(key);
                if (!bloom->MightHave(prefix.Get(length))) {
                    UNIT_ASSERT(binary != StoredNumeric);
                    UNIT_ASSERT_VALUES_EQUAL(CompareTypedCellVectors(storedKey.data(), key.data(),
                        Scheme->Keys->Types.data(), key.size()), 0);
                    return binary;
                }
            }
            UNIT_FAIL("No equivalent numeric encoding rejected by the raw bloom");
            return {};
        }

    private:
        void Add(NTest::TPartCook& cook, ui32 tenant) const {
            NTest::TRow row;
            if (NativePrefix) {
                row.Do(TenantTag, tenant);
            }
            row.Do(NumericTag, StoredNumeric, NScheme::NTypeIds::Pg);
            row.Do(ValueTag, ui32(42));
            cook.Add(row);
        }
    };

}

Y_UNIT_TEST_SUITE(TCompactionKeySpacePgTest) {

    Y_UNIT_TEST(EquivalentNumericEncodingCannotProveAbsence) {
        TFixture f;
        const auto equivalent = f.EquivalentKeyRejectedByRawBloom();
        const auto key = f.Key(equivalent);
        const TVector<TCompactionPartInfo> remaining{{f.Part, 3, 42}};
        auto mask = MakeCompactionKeySpace(f.Scheme->Keys, remaining);
        UNIT_ASSERT(mask->HasKey(key));

        // Typed slice bounds can still prove absence for a different value.
        const auto outside = Numeric("2");
        UNIT_ASSERT(!mask->HasKey(f.Key(outside)));
    }

    Y_UNIT_TEST(CapStopsAtEquivalentNumericKeyInBothDirections) {
        TFixture f;
        const auto equivalent = f.EquivalentKeyRejectedByRawBloom();
        const auto key = f.Key(equivalent);
        const TVector<TCompactionPartInfo> remaining{{f.Part, 3, 42}};
        TCompactionTierCap cap(f.Scheme->Keys, remaining, {0, 42, 7});
        UNIT_ASSERT_VALUES_EQUAL(cap.LimitTier(key, 0, 7), 42);
        cap.Reset();
        UNIT_ASSERT_VALUES_EQUAL(cap.LimitTier(key, 7, 0), 42);
    }

    Y_UNIT_TEST(GarbageConfKeepsEraseForEquivalentNumericKey) {
        TFixture f;
        const auto equivalent = f.EquivalentKeyRejectedByRawBloom();
        const TVector<TCompactionPartInfo> remaining{{f.Part, 3, 42}};
        auto garbage = MakeCompactionGarbageConf(f.Scheme->Keys, remaining, false);
        UNIT_ASSERT(!garbage.Final);
        UNIT_ASSERT(garbage.UnderlayMask);
        NPage::TConf conf;
        conf.Final = garbage.Final;
        conf.UnderlayMask = garbage.UnderlayMask.Get();
        NTest::TRow erase;
        erase.Do(NumericTag, equivalent, NScheme::NTypeIds::Pg);
        const auto eggs = NTest::TPartCook(f.Scheme, conf).Add(erase, ERowOp::Erase).Finish();
        UNIT_ASSERT(eggs.Written);
        UNIT_ASSERT_VALUES_EQUAL(eggs.Written->Rows, 1);
        UNIT_ASSERT_VALUES_EQUAL(eggs.Written->Drops, 1);
    }

    Y_UNIT_TEST(NativePrefixBeforePgColumnStillProvesAbsence) {
        TFixture f(true);
        const auto equivalent = f.EquivalentKeyRejectedByRawBloom();
        const TVector<TCompactionPartInfo> remaining{{f.Part, 3, 42}};
        auto mask = MakeCompactionKeySpace(f.Scheme->Keys, remaining);
        UNIT_ASSERT(mask->HasKey(f.Key(equivalent)));

        const auto& [length, bloom] = f.Part->ByKeyPrefixes.front();
        UNIT_ASSERT_VALUES_EQUAL(length, 1);
        UNIT_ASSERT(bloom);
        for (ui32 tenant = 11; tenant < 1000; ++tenant) {
            const auto key = f.Key(equivalent, tenant);
            const NBloom::TPrefix prefix(key);
            if (!bloom->MightHave(prefix.Get(length))) {
                // This key lies inside the part's slice bounds, so only the
                // native prefix bloom can prove its absence.
                UNIT_ASSERT(!mask->HasKey(key));
                return;
            }
        }
        UNIT_FAIL("No absent native prefix rejected by the bloom");
    }

}

}
