#include <library/cpp/testing/unittest/registar.h>

#include "mv_object_map.h"
#include "ut_helpers.h"

using namespace NKikimr::NBsController;

Y_UNIT_TEST_SUITE(TMultiversionObjectMap) {

    Y_UNIT_TEST(MonteCarlo) {
        using TMap = std::unordered_map<ui32, ui32>;
        using TVersionedMap = std::map<ui64, TMap>;
        TVersionedMap vm;
        TMultiversionObjectMap<ui32, ui32> test;

        for (ui64 version = 1; version < 100; ++version) {
            TMap prev;
            if (vm.size()) {
                prev = std::prev(vm.end())->second;
            }
            TMap& m = vm[version];
            m = prev;
            test.BeginTx(version);

            Ctest << "begin version " << version << Endl;

            for (size_t iter = 0; iter < 100; ++iter) {
                ui32 key = RandomNumber<ui32>(1000);
                ui32 value = RandomNumber<ui32>();
//                Ctest << key << " -> " << value << Endl;
                m[key] = value;
                if (ui32 *p = test.FindForUpdate(key)) {
                    *p = value;
                } else {
                    test.CreateNewEntry(key, value);
                }
            }

            for (size_t iter = 0; iter < 10000; ++iter) {
                if (m.size() >= 50 && RandomNumber(2u) == 0) {
                    auto it = m.begin();
                    std::advance(it, RandomNumber(m.size()));
//                    Ctest << "delete " << it->first << Endl;
                    test.DeleteExistingEntry(it->first);
                    m.erase(it);
                } else if (!m.size() || RandomNumber(2u) == 0) {
                    // create new entry
                    for (;;) {
                        ui32 key = RandomNumber<ui32>(1000);
                        if (!m.count(key)) {
                            ui32 value = RandomNumber<ui32>();
                            m.emplace(key, value);
                            test.CreateNewEntry(key, value);
//                            Ctest << "new " << key << " -> " << value << Endl;
                            break;
                        }
                    }
                } else if (RandomNumber(2u) == 0) {
                    // modify existing entry
                    auto it = m.begin();
                    std::advance(it, RandomNumber(m.size()));
                    const auto *readp = test.FindLatest(it->first);
                    UNIT_ASSERT(readp);
                    UNIT_ASSERT_VALUES_EQUAL(*readp, it->second);
                    const ui32 value = RandomNumber<ui32>();
//                    Ctest << "modify " << it->first << " -> " << value << Endl;
                    auto *p = test.FindForUpdate(it->first);
                    UNIT_ASSERT(p);
                    UNIT_ASSERT_VALUES_EQUAL(*p, it->second);
                    it->second = *p = value;
                } else {
                    // do scan
                    TMap x = m;
                    test.ScanRangeLatest([&](ui32 key, ui32 value, auto) {
                        UNIT_ASSERT(x.count(key));
                        UNIT_ASSERT_VALUES_EQUAL(value, x[key]);
                        x.erase(key);
                        return true;
                    });
                    UNIT_ASSERT(x.empty());
                }
            }

            test.FinishTx();

            if (version % 2 == 0) {
                Ctest << "drop version " << version << Endl;
                test.Drop(version);
                vm.erase(version);
            }

            if (vm.size() >= 3 && RandomNumber(2u) == 0) {
                auto it = vm.begin();
                ui32 version = it->first;
                Ctest << "commit version " << version << Endl;
                test.Commit(version);
                TMap& x = it->second;
                for (ui32 key = 0; key < 1000; ++key) {
                    const ui32 *p = test.FindCommitted(key);
                    if (x.count(key)) {
                        UNIT_ASSERT(p);
                        UNIT_ASSERT_VALUES_EQUAL(*p, x[key]);
                    } else {
                        UNIT_ASSERT(!p);
                    }
                }
                test.ScanRangeCommitted([&](ui32 key, ui32 value) {
                    UNIT_ASSERT(x.count(key));
                    UNIT_ASSERT_VALUES_EQUAL(value, x[key]);
                    x.erase(key);
                    return true;
                });
                UNIT_ASSERT(x.empty());
                vm.erase(it);
            }
        }
    }

}
