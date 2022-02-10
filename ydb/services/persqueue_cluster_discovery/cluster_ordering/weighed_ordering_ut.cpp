#include <ydb/services/persqueue_cluster_discovery/cluster_ordering/weighed_ordering.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>

using namespace NKikimr::NPQ::NClusterDiscovery::NClusterOrdering;

Y_UNIT_TEST_SUITE(TWeighedOrderingTest) {
    struct TCluster {
        TString Name;
        ui64 Weight = 0;

        bool operator==(const TCluster& other) const {
            return Name == other.Name && Weight == other.Weight;
        }
    };

    Y_UNIT_TEST(SimpleSelectionTest) {
        std::vector<TCluster> clusters = {{"sas", 1}, {"man", 1}};

        UNIT_ASSERT_STRINGS_EQUAL(SelectByHashAndWeight(begin(clusters), end(clusters), 0, [](const TCluster& c){ return c.Weight; })->Name, "sas");
        UNIT_ASSERT_STRINGS_EQUAL(SelectByHashAndWeight(begin(clusters), end(clusters), 1, [](const TCluster& c){ return c.Weight; })->Name, "man");
        UNIT_ASSERT_STRINGS_EQUAL(SelectByHashAndWeight(begin(clusters), end(clusters), 100500, [](const TCluster& c){ return c.Weight; })->Name, "sas");
        UNIT_ASSERT_STRINGS_EQUAL(SelectByHashAndWeight(begin(clusters), end(clusters), 100501, [](const TCluster& c){ return c.Weight; })->Name, "man");
    }

    Y_UNIT_TEST(WeighedSelectionTest) {
        std::vector<TCluster> clusters = {{"sas", 10}, {"man", 10000}};

        THashMap<TString, ui64> counters;
        for (ui64 hashValue = 0; hashValue < 10000000; hashValue += 661) {
            ++counters[SelectByHashAndWeight(begin(clusters), end(clusters), hashValue, [](const TCluster& c){ return c.Weight; })->Name];
        }

        UNIT_ASSERT_VALUES_EQUAL(counters.size(), 2);
        UNIT_ASSERT(counters["man"] > counters["sas"] * 1000); // ensure the imbalance
    }

    Y_UNIT_TEST(WeighedOrderingTest) {
        std::vector<TCluster> clusters = {{"iva", 1}, {"man", 2}, {"myt", 3}, {"sas", 4}, {"vla", 5}};

        auto firstOrder = clusters;
        OrderByHashAndWeight(begin(clusters), end(clusters), 100501, [](const TCluster& c){ return c.Weight; });
        auto secondOrder = clusters;
        OrderByHashAndWeight(begin(clusters), end(clusters), 100501, [](const TCluster& c){ return c.Weight; });
        auto thirdOrder = clusters;

        UNIT_ASSERT(firstOrder != secondOrder);
        UNIT_ASSERT(secondOrder != thirdOrder);
        UNIT_ASSERT(firstOrder != thirdOrder);
    }
}
