#include "mkql_stats_registry.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NMiniKQL;

Y_UNIT_TEST_SUITE(TStatsRegistryTest) {

Y_UNIT_TEST(Stats) {
    static TStatKey Key("key", false);
    auto stats = CreateDefaultStatsRegistry();

    stats->SetStat(Key, 37);
    UNIT_ASSERT_EQUAL(stats->GetStat(Key), 37);

    stats->AddStat(Key, 3);
    UNIT_ASSERT_EQUAL(stats->GetStat(Key), 40);

    stats->IncStat(Key);
    UNIT_ASSERT_EQUAL(stats->GetStat(Key), 41);

    stats->DecStat(Key);
    UNIT_ASSERT_EQUAL(stats->GetStat(Key), 40);

    stats->SetMaxStat(Key, 3);
    UNIT_ASSERT_EQUAL(stats->GetStat(Key), 40);

    stats->SetMaxStat(Key, 43);
    UNIT_ASSERT_EQUAL(stats->GetStat(Key), 43);

    stats->SetMinStat(Key, 57);
    UNIT_ASSERT_EQUAL(stats->GetStat(Key), 43);

    stats->SetMinStat(Key, 34);
    UNIT_ASSERT_EQUAL(stats->GetStat(Key), 34);
}

Y_UNIT_TEST(ForEach) {
    static TStatKey Key1("key1", false), Key2("key2", true);
    UNIT_ASSERT(!Key1.IsDeriv());
    UNIT_ASSERT(Key2.IsDeriv());

    auto stats = CreateDefaultStatsRegistry();
    stats->SetStat(Key1, 42);
    stats->SetStat(Key2, 37);

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
        static TStatKey Key("my_key", false), SameKey("my_key", false);
        // avoid variables elimitation
        Cerr << Key.GetId() << ": " << Key.GetName() << '\n'
             << SameKey.GetId() << ": " << SameKey.GetName() << Endl;
    } catch (const yexception& e) {
        error = e.AsStrBuf();
    }

    UNIT_ASSERT(error.Contains("duplicated stat key: my_key"));
}
} // Y_UNIT_TEST_SUITE(TStatsRegistryTest)
