#pragma once

#include "defs.h"
#include "iface.h"
#include "meter.h"
#include "sponge.h"

#include <ydb/core/tablet_flat/test/libs/rows/tool.h>
#include <ydb/core/tablet_flat/test/libs/rows/mass.h>

namespace NKikimr {
namespace NTable {
namespace NPerf {

    struct TConf {
        ui64        Span    = 1;
        TDuration   Spent   = TDuration::Seconds(5);
        ui64        Least   = 1024;
        ui64        Limit   = Max<ui64>();
    };

    class TRunner {
    public:
        using TMeter = NTest::TMeter;

        TRunner(ILogger *logger, const NTest::TMass &mass,
                    NTest::TSponge &sponge, const TConf &conf)
            : Logger(logger)
            , Conf(conf)
            , Mass(mass)
            , Sponge(sponge)
        {
            if (auto logl = Logger->Log(NKikiSched::ELnLev::INFO)) {
                logl
                    << "Collect [" << NFmt::TLarge(Conf.Least)
                    << ", " << NFmt::TLarge(Conf.Limit) << "]"
                    << " samples over " << NFmt::TDelay(Conf.Spent)
                    << " per test";
            }
        }

        template<typename TOne, typename ... TArgs>
        void Do(TArgs&& ... args)
        {
            TOne one(Logger, Mass, std::forward<TArgs>(args)...);

            Run(&TRunner::DoScan, static_cast<IPerfTable&>(one));
            Run(&TRunner::DoOnce, static_cast<IPerfTable&>(one), ui64(3), true);
            Run(&TRunner::DoOnce, static_cast<IPerfTable&>(one), ui64(-1), true);
            Run(&TRunner::DoOnce, static_cast<IPerfTable&>(one), ui64(-1), false);
        }

    private:
        template<typename ... TArgs>
        void Run(void(TRunner::*fn)(TMeter&, TArgs...), TArgs&& ... args)
        {
            TMeter meter(Conf.Spent, Conf.Least, Conf.Limit);

            (this->*fn)(meter, std::forward<TArgs>(args)...);

            Cout << " " << meter.Report();
        }

        void DoScan(TMeter &meter, IPerfTable &test)
        {
            ui64 last = 0;

            test.Seek({ }, ESeek::Lower);

            while (const auto once = meter.Take(last)) {
                if (last = test.Scan(once, Sponge)) {
                    test.Seek({ }, ESeek::Lower);
                }

                last = once - last;
            }
        }

        void DoOnce(TMeter &meter, IPerfTable &test, ui64 span_, bool exists)
        {
            TMersenne<ui64> rnd;

            const NTest::TRowTool tool(*Mass.Model->Scheme);
            const auto &heap = exists ? Mass.Saved : Mass.Holes;
            const auto span = Min(span_, heap.Size());

            for (ui64 off = 0; meter.Take(1);) {
                const auto key = tool.LookupKey(*(heap.begin() + off));

                test.Once(key, Sponge);

                off = (off + rnd.Uniform(0, span)) % heap.Size();
            }
        }

    private:
        ILogger * const Logger = nullptr;
        const NPerf::TConf Conf;
        const NTest::TMass &Mass;
        NTest::TSponge &Sponge;
    };
}
}
}
