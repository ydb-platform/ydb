#pragma once

#include "iface.h"

#include <ydb/core/tablet_flat/flat_mem_warm.h>
#include <ydb/core/tablet_flat/flat_row_state.h>
#include <ydb/core/tablet_flat/flat_row_scheme.h>
#include <ydb/core/tablet_flat/flat_iterator.h>
#include <ydb/core/tablet_flat/test/libs/table/test_part.h>
#include <ydb/core/tablet_flat/test/libs/table/test_mixer.h>
#include <ydb/core/tablet_flat/test/libs/table/test_make.h>
#include <ydb/core/tablet_flat/test/libs/table/wrap_iter.h>

namespace NKikimr {
namespace NTable {
namespace NPerf {

    class TDoIter : public IPerfTable {
    public:
        TDoIter(ILogger*, const TMass &mass, ui32 parts, bool mixed)
            : Iter(*Make(mass, parts, mixed))
        {

        }

        static TAutoPtr<TSubset> Make(const TMass &mass, ui32 parts, bool mixed)
        {
            using namespace NTest;

            if (parts == 0) {
                return TMake(mass).Mixed(1, 0, TMixerOne{ });
            } else if (mixed) {
                return TMake(mass).Mixed(0, parts, TMixerRnd(parts));
            } else /* sequential parts */ {
                TMixerSeq mixer(parts, mass.Saved.Size());

                return TMake(mass).Mixed(0, parts, mixer);
            }
        }

        void Seek(TRawVals key, ESeek seek) override
        {
            Iter.Make(&Env);
            auto ready = Iter.Seek(key, seek);

            Y_ABORT_UNLESS(ready != EReady::Page);
        }

        ui64 Scan(ui64 items, TSponge &aggr) override
        {
            for (; items-- && Iter; Iter.Next())
                aggr(Iter.Apply());

            return items + 1;
        }

        void Once(TRawVals key, TSponge &aggr) override
        {
            Seek(key, ESeek::Exact);

            if (Iter)
                aggr(Iter.Apply());
        }

    private:
        NTest::TTestEnv Env;
        NTest::TWrapIter Iter;
    };
}
}
}
