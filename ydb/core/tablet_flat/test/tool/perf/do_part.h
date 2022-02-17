#pragma once

#include "iface.h"

#include <ydb/core/tablet_flat/flat_row_state.h>
#include <ydb/core/tablet_flat/flat_row_scheme.h>
#include <ydb/core/tablet_flat/test/libs/table/wrap_part.h>
#include <ydb/core/tablet_flat/test/libs/table/test_writer.h>
#include <ydb/core/tablet_flat/test/libs/table/test_make.h>

namespace NKikimr {
namespace NTable {
namespace NPerf {

    class TDoPart : public IPerfTable {
        struct TStats {
            ui64 Pages() const noexcept
            {
                return Plain + Encoded;
            }

            ui64 Bytes      = 0;    /* Total bytes obtained by rows   */
            ui64 Plain      = 0;
            ui64 Encoded    = 0;
        };

    public:
        TDoPart(ILogger *logger, const TMass &mass, bool compress)
            : Eggs(NTest::TMake(mass, Config(compress)).Part())
            , Wrap(Eggs, nullptr, true)
        {
            if (auto logl = logger->Log(NKikiSched::ELnLev::INF1)) {
                const auto &part = *Eggs.Lone();
                const auto st = Stats(part);

                logl
                    << "part has { " << st.Plain << " " << st.Encoded << "e }"
                    << " pages in " << NFmt::TLarge(st.Bytes) << "b"
                    << " avg "
                        << NFmt::TAverage(st.Pages(), st.Bytes, false) << "b"
                    << ", " << NFmt::TLarge(part.IndexesRawSize) << "b index";
            }
        }

        static NPage::TConf Config(bool compress) noexcept
        {
            NPage::TConf conf;

            if (compress) {
                conf.Group(0).PageSize = 32 * 1024;
                conf.Group(0).Codec = NPage::ECodec::LZ4;
            }

            return conf;
        }

        static TStats Stats(const TPart &part_) noexcept
        {
            auto &part = dynamic_cast<const NTest::TPartStore&>(part_);

            TStats stats;

            for (auto &page: part.Store->PageCollectionArray(0 /* main room */)) {
                auto got = NPage::TLabelWrapper().Read(page, EPage::Undef);

                if (got == NPage::EPage::DataPage) {
                    stats.Bytes += page.size();

                    if (got == NPage::ECodec::Plain) {
                        stats.Plain++;
                    } else {
                        stats.Encoded++;
                    }
                }
            }

            return stats;
        }

        void Seek(TRawVals key, ESeek seek) override
        {
            Wrap.Make(&Env);
            Wrap.Seek(key, seek);
        }

        ui64 Scan(ui64 items, TSponge &aggr) override
        {
            for (; items-- && Wrap; Wrap.Next())
                aggr(Wrap.Apply());

            return items + 1;
        }

        void Once(TRawVals key, TSponge &aggr) override
        {
            Seek(key, ESeek::Exact);

            if (Wrap)
                aggr(Wrap.Apply());
        }

    private:
        NTest::TPartEggs Eggs;
        NTest::TTestEnv Env;
        NTest::TWrapPart Wrap;
    };
}
}
}
