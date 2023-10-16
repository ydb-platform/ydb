#pragma once

#include "test_part.h"
#include "test_writer.h"

#include <ydb/core/tablet_flat/util_basics.h>
#include <ydb/core/tablet_flat/flat_row_versions.h>
#include <ydb/core/tablet_flat/flat_table_subset.h>
#include <ydb/core/tablet_flat/flat_scan_feed.h>
#include <ydb/core/tablet_flat/test/libs/rows/layout.h>

namespace NKikimr {
namespace NTable {
namespace NTest {

    class TCompaction final : protected IVersionScan {
        class TMyScan : public TFeed {
        public:
            TMyScan(IScan *scan, const TSubset &subset, IPages *env)
                : TFeed(scan, subset)
                , Env(env)
            {
                Conf.NoErased = false; /* Need all technical rows */
            }

            IPages* MakeEnv() noexcept override
            {
                return Env;
            }

            TPartView LoadPart(const TIntrusiveConstPtr<TColdPart>&) noexcept override {
                Y_ABORT("not supported in test scans");
            }

            IPages * const Env = nullptr;
         };

    public:
        using TConf = NPage::TConf;

        TCompaction() : TCompaction(nullptr) { }

        TCompaction(TAutoPtr<IPages> env, TConf conf = { }, ui32 ret = Max<ui32>())
            : Conf(conf)
            , Retries(ret)
            , Env(env ? env : new TTestEnv)
        {

        }

        TCompaction& WithRemovedRowVersions(const TRowVersionRanges& ranges) {
            RemovedRowVersions = ranges.Snapshot();
            return *this;
        }

        TPartEggs Do(TIntrusiveConstPtr<TMemTable> table)
        {
            return Do(TSubset(TEpoch::Zero(), table->Scheme, { TMemTableSnapshot{ table, table->Immediate() } }));
        }

        TPartEggs Do(const TPartEggs &eggs)
        {
            return Do(eggs.Scheme, { &eggs } );
        }

        TPartEggs Do(TIntrusiveConstPtr<TRowScheme> scheme, TVector<const TPartEggs*> eggs)
        {
            TVector<TPartView> partView;

            for (auto &one: eggs) {
                for (const auto &part : one->Parts) {
                    Y_ABORT_UNLESS(part->Slices, "Missing part slices");
                    partView.push_back({ part, nullptr, part->Slices });
                }
            }

            return Do(TSubset(TEpoch::Zero(), std::move(scheme), std::move(partView)));
        }

        TPartEggs Do(const TSubset &subset)
        {
            return Do(subset, TLogoBlobID(0, 0, ++Serial, 0, 0, 0));
        }

        TPartEggs Do(const TSubset &subset, TLogoBlobID logo)
        {
            auto *scheme = new TPartScheme(subset.Scheme->Cols);

            TWriterBundle blocks(scheme->Groups.size(), logo);

            Tags = subset.Scheme->Tags();
            NPage::TConf conf = Conf;
            conf.MaxRows = subset.MaxRows();
            conf.MinRowVersion = subset.MinRowVersion();
            Writer = new TPartWriter(scheme, Tags, blocks, conf, subset.Epoch());

            TMyScan scanner(this, subset, Env.Get());

            while (true) {
                const auto ready = scanner.Process();

                if (ready == EReady::Data) {
                    /* Just scan iterruption for unknown reason, go on */
                } else if (ready == EReady::Gone) {
                    auto eggs = blocks.Flush(subset.Scheme, Writer->Finish());

                    Y_ABORT_UNLESS(eggs.Written->Rows <= conf.MaxRows);

                    return eggs;
                } else if (ready != EReady::Page) {
                     Y_ABORT("Subset scanner give unexpected cycle result");
                } else if (Failed++ > Retries) {

                    /* Early termination without any complete result, event
                        an ommited empty part. Used for scan abort tests. */

                    return { nullptr, nullptr, { } };

                } else if (Failed > 32 && Retries == Max<ui32>()) {

                    /* Can happen on EScan::Sleep and on real page fail. Our
                        IScan impl never yields EScan::Sleep and this case
                        is always page fail of some supplied Env. So, go on
                        until there is some progress.
                     */

                    Y_ABORT("Mocked compaction failied to make any progress");
                }
            }
        }

    private:
        virtual TInitialState Prepare(IDriver*, TIntrusiveConstPtr<TScheme>) noexcept override
        {
            Y_ABORT("IScan::Prepare(...) isn't used in test env compaction");
        }

        EScan Seek(TLead &lead, ui64 seq) noexcept override
        {
            Y_ABORT_UNLESS(seq < 2, "Test IScan impl Got too many Seek() calls");

            lead.To(Tags, { }, ESeek::Lower);

            return seq == 0 ? EScan::Feed : EScan::Final;
        }

        EScan BeginKey(TArrayRef<const TCell> key) noexcept override
        {
            Writer->BeginKey(key);

            return Failed = 0, EScan::Feed;
        }

        EScan BeginDeltas() noexcept override
        {
            return Failed = 0, EScan::Feed;
        }

        EScan Feed(const TRow &row, ui64 txId) noexcept override
        {
            Writer->AddKeyDelta(row, txId);

            return Failed = 0, EScan::Feed;
        }

        EScan EndDeltas() noexcept override
        {
            return Failed = 0, EScan::Feed;
        }

        EScan Feed(const TRow &row, TRowVersion &rowVersion) noexcept override
        {
            if (RemovedRowVersions) {
                rowVersion = RemovedRowVersions.AdjustDown(rowVersion);
            }

            Writer->AddKeyVersion(row, rowVersion);

            return Failed = 0, EScan::Feed;
        }

        EScan EndKey() noexcept override
        {
            Writer->EndKey();

            return Failed = 0, EScan::Feed;
        }

        TAutoPtr<IDestructable> Finish(EAbort) noexcept override
        {
            Y_ABORT("IScan::Finish(...) shouldn't be called in test env");
        }

        void Describe(IOutputStream &out) const noexcept override
        {
            out << "Compact{test env}";
        }

    private:
        const NPage::TConf Conf { true, 7 * 1024 };
        const ui32 Retries = Max<ui32>();
        ui32 Serial = 0;
        ui32 Failed = 0;
        TAutoPtr<IPages> Env;
        TVector<ui32> Tags;
        TAutoPtr<TPartWriter> Writer;
        TRowVersionRanges::TSnapshot RemovedRowVersions;
    };

}
}
}
