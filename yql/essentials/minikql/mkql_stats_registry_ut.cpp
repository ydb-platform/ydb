#include "mkql_stats_registry.h"

#include <library/cpp/testing/unittest/registar.h>


using namespace NKikimr;
using namespace NMiniKQL;

Y_UNIT_TEST_SUITE(TStatsRegistryTest) {

    Y_UNIT_TEST(Stats) {
        static TStatKey key("key", false);
        auto stats = CreateDefaultStatsRegistry();

        stats->SetStat(key, 37);
        UNIT_ASSERT_EQUAL(stats->GetStat(key), 37);

        stats->AddStat(key, 3);
        UNIT_ASSERT_EQUAL(stats->GetStat(key), 40);

        stats->IncStat(key);
        UNIT_ASSERT_EQUAL(stats->GetStat(key), 41);

        stats->DecStat(key);
        UNIT_ASSERT_EQUAL(stats->GetStat(key), 40);

        stats->SetMaxStat(key, 3);
        UNIT_ASSERT_EQUAL(stats->GetStat(key), 40);

        stats->SetMaxStat(key, 43);
        UNIT_ASSERT_EQUAL(stats->GetStat(key), 43);

        stats->SetMinStat(key, 57);
        UNIT_ASSERT_EQUAL(stats->GetStat(key), 43);

        stats->SetMinStat(key, 34);
        UNIT_ASSERT_EQUAL(stats->GetStat(key), 34);
    }

    Y_UNIT_TEST(ForEach) {
        static TStatKey key1("key1", false), key2("key2", true);
        UNIT_ASSERT(!key1.IsDeriv());
        UNIT_ASSERT(key2.IsDeriv());

        auto stats = CreateDefaultStatsRegistry();
        stats->SetStat(key1, 42);
        stats->SetStat(key2, 37);

        stats->ForEachStat([](const TStatKey& key, i64 value) {
            if (key.GetName() == "key1") {
                UNIT_ASSERT_EQUAL(value, 42);
            } else if (key.GetName() == "key2") {
                UNIT_ASSERT_EQUAL(value, 37);
            } else {
                // depending on how to run tests other keys can be linked
                // together and appears here
            }
        });
    }

    Y_UNIT_TEST(DuplicateKeys) {
        TString error;
        try {
            static TStatKey key("my_key", false), sameKey("my_key", false);
            // avoid variables elimitation
            Cerr << key.GetId() << ": " << key.GetName() << '\n'
                 << sameKey.GetId() << ": " << sameKey.GetName() << Endl;
        } catch (const yexception& e) {
             error = e.AsStrBuf();
        }

        UNIT_ASSERT(error.Contains("duplicated stat key: my_key"));
    }
}
