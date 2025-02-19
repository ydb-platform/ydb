#pragma once

#include "test_steps.h"
#include "test_comp.h"

#include <ydb/core/tablet_flat/flat_database.h>
#include <ydb/core/tablet_flat/flat_dbase_naked.h>
#include <ydb/core/tablet_flat/flat_dbase_apply.h>
#include <ydb/core/tablet_flat/flat_dbase_change.h>
#include <ydb/core/tablet_flat/flat_sausage_grind.h>
#include <ydb/core/tablet_flat/flat_util_binary.h>
#include <ydb/core/tablet_flat/util_fmt_desc.h>

#include <ydb/core/tablet_flat/test/libs/rows/cook.h>
#include <ydb/core/tablet_flat/test/libs/rows/tool.h>
#include <ydb/core/tablet_flat/test/libs/table/test_iter.h>
#include <ydb/core/tablet_flat/test/libs/table/test_envs.h>
#include <ydb/core/tablet_flat/test/libs/table/wrap_dbase.h>
#include <ydb/core/tablet_flat/test/libs/table/wrap_select.h>

namespace NKikimr {
namespace NTable {
namespace NTest {

    enum class EPlay {
        Boot    = 1,    /* Apply redo log through base booter   */
        Redo    = 2,    /* Roll up redo log through follower iface */
    };

    struct TDbExec : public TSteps<TDbExec> {
        using TRow = NTest::TRow;
        using TRedoLog = TDeque<TAutoPtr<TChange>>;
        using TSteppedCookieAllocator = NPageCollection::TSteppedCookieAllocator;

        using TCheckIter = TChecker<NTest::TWrapDbIter, TDatabase&>;
        using TCheckSelect = TChecker<NTest::TWrapDbSelect, TDatabase&>;

        enum class EOnTx {
            None    = 0,
            Auto    = 1,    /* Automatic read only transaction  */
            Real    = 2,    /* Explicitly prepared user rw tx   */
        };

        struct THeader {
            ui64 Serial;    /* Change serial stamp data         */
            ui32 Alter;     /* Bytes of scheme delta block      */
            ui32 Redo;      /* Bytes of redo log block          */
            ui32 Affects;   /* Items in affects array           */
            ui32 Pad0;
        };

        static TAutoPtr<TDatabase> Make(TAutoPtr<TSchemeChanges> delta)
        {
            TAutoPtr<TScheme> scheme = new TScheme;

            TSchemeModifier(*scheme).Apply(*delta);

            return new TDatabase(new TDatabaseImpl(0, scheme, nullptr));
        }

        TDbExec() : Base(new TDatabase) { Birth(); }

        TDbExec(TAutoPtr<TSchemeChanges> delta) : Base(Make(delta)) { }

        const TRedoLog& GetLog() const noexcept { return RedoLog; }

        const TChange& BackLog() const noexcept
        {
            Y_ABORT_UNLESS(RedoLog, "Redo log is empty, cannot get last entry");

            return *RedoLog.back();
        }

        TDatabase* operator->() const noexcept { return Base.Get(); }

        TDbExec& Begin() noexcept
        {
            return DoBegin(true);
        }

        TDbExec& Commit() { return DoCommit(true, true); }
        TDbExec& Reject() { return DoCommit(true, false); }
        TDbExec& Relax()  { return DoCommit(false, true); }

        TDbExec& Cleanup() {
            if (OnTx == EOnTx::Auto) {
                DoCommit(false, false);
            }

            Y_ABORT_UNLESS(OnTx == EOnTx::None);
            return *this;
        }

        TDbExec& ReadVer(TRowVersion readVersion) {
            DoBegin(false);

            ReadVersion = readVersion;

            return *this;
        }

        TDbExec& ReadTx(ui64 txId) {
            DoBegin(false);

            ReadTxId = txId;

            return *this;
        }

        TDbExec& WriteVer(TRowVersion writeVersion) {
            Y_ABORT_UNLESS(OnTx != EOnTx::None);

            WriteVersion = writeVersion;
            WriteTxId = 0;

            return *this;
        }

        TDbExec& WriteTx(ui64 txId) {
            Y_ABORT_UNLESS(OnTx != EOnTx::None);

            WriteVersion = TRowVersion::Min();
            WriteTxId = txId;

            return *this;
        }

        TDbExec& CommitTx(ui32 table, ui64 txId) {
            Y_ABORT_UNLESS(OnTx != EOnTx::None);

            Base->CommitTx(table, txId, WriteVersion);

            return *this;
        }

        TDbExec& RemoveTx(ui32 table, ui64 txId) {
            Y_ABORT_UNLESS(OnTx != EOnTx::None);

            Base->RemoveTx(table, txId);

            return *this;
        }

        TDbExec& RollbackChanges() {
            Y_ABORT_UNLESS(OnTx != EOnTx::None);

            Base->RollbackChanges();

            return *this;
        }

        TDbExec& Add(ui32 table, const TRow &row, ERowOp rop = ERowOp::Upsert)
        {
            const NTest::TRowTool tool(RowSchemeFor(table));
            auto pair = tool.Split(row, true, rop != ERowOp::Erase);

            if (WriteTxId != 0) {
                Base->UpdateTx(table, rop, pair.Key, pair.Ops, WriteTxId);
            } else {
                Base->Update(table, rop, pair.Key, pair.Ops, WriteVersion);
            }

            return *this;
        }

        template<typename ... Args>
        inline TDbExec& Put(ui32 table, const TRow &row, const Args& ... left)
        {
            return Add(table, row, ERowOp::Upsert), Put(table, left...);
        }

        inline TDbExec& Put(ui32 table, const TRow &row)
        {
            return Add(table, row, ERowOp::Upsert);
        }

        template<typename ...TArgs>
        inline TDbExec& PutN(ui32 table, TArgs&&... args)
        {
            return Put(table, *SchemedCookRow(table).Col(std::forward<TArgs>(args)...));
        }

        TDbExec& Apply(const TSchemeChanges &delta)
        {
            Last = Max<ui32>(), Altered = true;

            return Base->Alter().Merge(delta), *this;
        }

        TEpoch Snapshot(ui32 table)
        {
            return Base->TxSnapTable(table);
        }

        NTest::TSchemedCookRow SchemedCookRow(ui32 table) noexcept
        {
            return { RowSchemeFor(table) };
        }

        TCheckIter Iter(ui32 table, bool erased = true) noexcept
        {
            DoBegin(false), RowSchemeFor(table);

            TCheckIter check{ *Base, { nullptr, 0, erased }, table, Scheme, ReadVersion, ReadTxId };

            return check.To(CurrentStep()), check;
        }

        TCheckIter IterData(ui32 table) noexcept
        {
            DoBegin(false), RowSchemeFor(table);

            TCheckIter check{ *Base, { nullptr, 0, true }, table, Scheme, ReadVersion, ReadTxId, ENext::Data };

            return check.To(CurrentStep()), check;
        }

        TCheckSelect Select(ui32 table, bool erased = true) noexcept
        {
            DoBegin(false), RowSchemeFor(table);

            TCheckSelect check{ *Base, { nullptr, 0, erased }, table, Scheme, ReadVersion, ReadTxId };

            return check.To(CurrentStep()), check;
        }

        TDbExec& Snap(ui32 table)
        {
            Cleanup();

            const auto scn = Base->Head().Serial + 1;

            RedoLog.emplace_back(new TChange({ Gen, ++Step }, scn));
            RedoLog.back()->Redo = Base->SnapshotToLog(table, { Gen, Step });
            RedoLog.back()->Affects = { table };

            Y_ABORT_UNLESS(scn == Base->Head().Serial);

            return *this;
        }

        TDbExec& Compact(ui32 table, bool last = true)
        {
            TAutoPtr<TSubset> subset;

            if (last /* make full subset */) {
                subset = Base->Subset(table, TEpoch::Max(), { }, { });
            } else /* only flush memtables */ {
                subset = Base->Subset(table, { }, TEpoch::Max());
            }

            TLogoBlobID logo(1, Gen, ++Step, 1, 0, 0);

            auto *family = Base->GetScheme().DefaultFamilyFor(table);

            NPage::TConf conf{ last, 8291, family->Large };

            conf.ByKeyFilter = Base->GetScheme().GetTableInfo(table)->ByKeyFilter;
            conf.MaxRows = subset->MaxRows();
            conf.MinRowVersion = subset->MinRowVersion();
            conf.SmallEdge = family->Small;

            auto keys = subset->Scheme->Tags(true /* only keys */);

            /* Mocked NFwd emulates real compaction partially: it cannot pass
                external blobs from TMemTable to TPart by reference, so need to
                materialize it on this compaction.
             */

            TAutoPtr<IPages> env = new TForwardEnv(128, 256, keys, Max<ui32>());

            auto eggs = TCompaction(env, conf)
                    .WithRemovedRowVersions(Base->GetRemovedRowVersions(table))
                    .Do(*subset, logo);

            Y_ABORT_UNLESS(!eggs.NoResult(), "Unexpected early termination");

            TVector<TPartView> partViews;
            for (auto &part : eggs.Parts)
                partViews.push_back({ part, nullptr, part->Slices });

            Base->Replace(table, std::move(partViews), *subset);

            return *this;
        }

        TDbExec& Replay(EPlay play)
        {
            Y_ABORT_UNLESS(OnTx != EOnTx::Real, "Commit TX before replaying");

            Cleanup();

            const ui64 serial = Base->Head().Serial;

            Birth(), Base = nullptr, OnTx = EOnTx::None;

            ReadVersion = TRowVersion::Max();
            WriteVersion = TRowVersion::Min();

            if (play == EPlay::Boot) {
                TAutoPtr<TScheme> scheme = new TScheme;

                for (auto &change: RedoLog) {
                    if (auto &raw = change->Scheme) {
                        TSchemeChanges delta;
                        bool ok = delta.ParseFromString(raw);

                        Y_ABORT_UNLESS(ok, "Cannot read serialized scheme delta");

                        TSchemeModifier(*scheme).Apply(delta);
                    }
                }

                TAutoPtr<TDatabaseImpl> naked = new TDatabaseImpl({Gen, Step}, scheme, nullptr);

                for (auto &change: RedoLog)
                    if (auto &redo = change->Redo) {
                        naked->Switch(change->Stamp);
                        naked->Assign(change->Annex);
                        naked->ApplyRedo(redo);
                        naked->GrabAnnex();
                    }

                UNIT_ASSERT(serial == naked->Serial());

                Base = new TDatabase(naked.Release());

            } else if (play == EPlay::Redo) {

                Base = new TDatabase{ };

                for (const auto &it: RedoLog)
                    Base->RollUp(it->Stamp, it->Scheme, it->Redo, it->Annex);

                UNIT_ASSERT(serial == Base->Head().Serial);
            }

            return *this;
        }

        const TRowScheme& RowSchemeFor(ui32 table) noexcept
        {
            if (std::exchange(Last, table) != table) {
                // Note: it's ok if the table has been altered, since
                // we should observe updated schema on all reads.
                Scheme = Base->GetRowScheme(table);
            }

            return *Scheme;
        }

        TDbExec& Affects(ui32 back, std::initializer_list<ui32> tables)
        {
            Y_ABORT_UNLESS(back < RedoLog.size(), "Out of redo log entries");

            const auto &have = RedoLog[RedoLog.size() - (1 + back)]->Affects;

            if (have.size() == tables.size()
                && std::equal(have.begin(), have.end(), tables.begin())) {

            } else {
                TSteps<TDbExec>::Log()
                    << "For " << NFmt::Do(*RedoLog[RedoLog.size() - (1 + back)])
                    << " expected affects " << NFmt::Arr(tables)
                    << Endl;

                UNIT_ASSERT(false);
            }

            return *this;
        }

        void DumpChanges(IOutputStream &stream) const noexcept
        {
            for (auto &one: RedoLog) {
                NUtil::NBin::TOut(stream)
                    .Put<ui64>(one->Serial)
                    .Put<ui32>(one->Scheme.size())
                    .Put<ui32>(one->Redo.size())
                    .Put<ui32>(one->Affects.size())
                    .Put<ui32>(0 /* paddings */)
                    .Array(one->Scheme)
                    .Array(one->Redo)
                    .Array(one->Affects);
            }
        }

        static TRedoLog RestoreChanges(IInputStream &in)
        {
            TRedoLog changes;
            THeader header;

            while (auto got = in.Load(&header, sizeof(header))) {
                Y_ABORT_UNLESS(got == sizeof(header), "Invalid changes stream");

                const auto abytes = sizeof(ui32) * header.Affects;

                TString alter = TString::TUninitialized(header.Alter);

                if (in.Load((void*)alter.data(), alter.size()) != alter.size())
                    Y_ABORT("Cannot read alter chunk data in change page");

                TString redo = TString::TUninitialized(header.Redo);

                if (in.Load((void*)redo.data(), redo.size()) != redo.size())
                    Y_ABORT("Cannot read redo log data in change page");

                if (in.Skip(abytes) != abytes)
                    Y_ABORT("Cannot read affects array in change page");

                changes.push_back(new TChange{ header.Serial, header.Serial });
                changes.back()->Scheme = std::move(alter);
                changes.back()->Redo = std::move(redo);
            }

            return changes;
        }

    private:
        void Birth() noexcept
        {
            Annex = new TSteppedCookieAllocator(1, ui64(++Gen) << 32, { 0, 999 }, {{ 1, 7 }});
        }

        TDbExec& DoBegin(bool real) noexcept
        {
            if (OnTx == EOnTx::Real && real) {
                Y_ABORT("Cannot run multiple tx at the same time");
            } else if (OnTx == EOnTx::Auto && real) {
                DoCommit(false, false);
            }

            if (OnTx == EOnTx::None || real) {
                Annex->Switch(++Step, true /* require step switch */);
                Base->Begin({ Gen, Step }, Env.emplace());

                OnTx = (real ? EOnTx::Real : EOnTx::Auto);
            }

            return *this;
        }

        TDbExec& DoCommit(bool real, bool apply)
        {
            const auto was = std::exchange(OnTx, EOnTx::None);

            if (was != (real ? EOnTx::Real : EOnTx::Auto))
                Y_ABORT("There is no active dbase tx");

            auto prod = Base->Commit({ Gen, Step }, apply, Annex.Get());
            auto up = std::move(prod.Change);
            Env.reset();

            Last = Max<ui32>(), Altered = false;

            UNIT_ASSERT(!up->HasAny() || apply);
            UNIT_ASSERT(!up->HasAny() || was != EOnTx::Auto);
            UNIT_ASSERT(!up->Annex || up->Redo);

            if (apply) {
                std::sort(up->Affects.begin(), up->Affects.end());

                auto end = std::unique(up->Affects.begin(), up->Affects.end());

                if (end != up->Affects.end()) {
                    TSteps<TDbExec>::Log()
                        << NFmt::Do(*up) << " has denormalized affects" << Endl;

                    UNIT_ASSERT(false);
                }

                RedoLog.emplace_back(std::move(up));

                for (auto& callback : prod.OnPersistent) {
                    callback();
                }
            }

            ReadVersion = TRowVersion::Max();
            WriteVersion = TRowVersion::Min();
            ReadTxId = 0;
            WriteTxId = 0;

            return *this;
        }

    private:
        TAutoPtr<TDatabase> Base;
        std::optional<TTestEnv> Env;
        ui32 Gen = 0;
        ui32 Step = 0;
        ui32 Last = Max<ui32>();
        bool Altered = false;
        EOnTx OnTx = EOnTx::None;
        TIntrusiveConstPtr<TRowScheme> Scheme;
        TRedoLog RedoLog;
        TAutoPtr<TSteppedCookieAllocator> Annex;
        TRowVersion ReadVersion = TRowVersion::Max();
        TRowVersion WriteVersion = TRowVersion::Min();
        ui64 ReadTxId = 0;
        ui64 WriteTxId = 0;
    };

}
}
}
