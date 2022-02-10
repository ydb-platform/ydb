#include "defs.h"
#include "vdisk_lsnmngr.h"
#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/ptr.h>
#include <util/stream/null.h>
#include <util/system/thread.h>

#define STR Cnull

using namespace NKikimr;

namespace NKikimr {

    Y_UNIT_TEST_SUITE(TLsnAllocTrackerTests) {
        Y_UNIT_TEST(Test1) {
            TLsnAllocTracker t(100);
            STR << t.ToString() << "\n";
            UNIT_ASSERT(t.ToString() == "{InFly# [] ConfirmedLsn# 100}");
            t.Allocated({101, 102});
            STR << t.ToString() << "\n";
            UNIT_ASSERT(t.ToString() == "{InFly# [ [101, 102]] ConfirmedLsn# 100}");
            t.Confirmed({101, 102});
            t.Allocated({103, 103});
            t.Allocated({104, 104});
            t.Allocated({105, 105});

            t.Confirmed({103, 103});
            t.Confirmed({104, 104});
            STR << t.ToString() << "\n";
            UNIT_ASSERT(t.ToString() == "{InFly# [ [105, 105]] ConfirmedLsn# 104}");

            t.Allocated({106, 106});
            t.Allocated({107, 107});
            t.Allocated({108, 108});
            t.Allocated({109, 109});
            t.Allocated({110, 110});
            STR << t.ToString() << "\n";
            UNIT_ASSERT(t.ToString() ==
                "{InFly# [ [105, 105] [106, 106] [107, 107] [108, 108] [109, 109] [110, 110]] ConfirmedLsn# 104}");
        }
    }

    // Below are some performance tests for Lsn allocation with and without contention

    Y_UNIT_TEST_SUITE(TLsnMngrTests) {

        struct TParam {
            TLsnMngr *Mngr;
            ui64 Iterations;
        };

        const ui64 Its = ui64(100000000ul);

        static void *AllocLsnForLocalUseFunc(void *ptr) {
            TParam *p = reinterpret_cast<TParam*>(ptr);
            for (ui64 i = 0; i < p->Iterations; ++i) {
                TLsnSeg seg = p->Mngr->AllocLsnForLocalUse();
                Y_UNUSED(seg);
            }
            return nullptr;
        }

        Y_UNIT_TEST(AllocLsnForLocalUse) {
            TLsnMngr mngr(0, 0, false);
            TParam p {&mngr, Its};
            auto start = Now();
            AllocLsnForLocalUseFunc(&p);
            auto finish = Now();
            STR << "Working time: " << (finish - start) << "\n";
        }

        Y_UNIT_TEST(AllocLsnForLocalUse2Threads) {
            TLsnMngr mngr(0, 0, false);
            TParam p {&mngr, Its / 2};
            auto start = Now();
            TThread t1(&AllocLsnForLocalUseFunc, &p);
            TThread t2(&AllocLsnForLocalUseFunc, &p);
            t1.Start();
            t2.Start();
            t1.Join();
            t2.Join();
            auto finish = Now();
            STR << "Working time: " << (finish - start) << "\n";
        }

        Y_UNIT_TEST(AllocLsnForLocalUse10Threads) {
            const ui64 threads = 10;
            TLsnMngr mngr(0, 0, false);
            TParam p {&mngr, Its / threads};
            auto start = Now();
            TVector<std::unique_ptr<TThread>> ts(threads);
            for (auto &t : ts) {
                t = std::make_unique<TThread>(&AllocLsnForLocalUseFunc, &p);
            }
            for (auto &t : ts) {
                t->Start();
            }
            for (auto &t : ts) {
                t->Join();
            }
            auto finish = Now();
            STR << "Working time: " << (finish - start) << "\n";
        }
    }

} // NKikimr
