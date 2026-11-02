#include <ydb/core/tablet_flat/test/libs/table/test_dbase.h>
#include <ydb/core/tablet_flat/test/libs/rows/cook.h>
#include <ydb/core/tablet_flat/flat_redo_player.h>
#include <ydb/core/tablet_flat/flat_redo_writer.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/resource/resource.h>
#include <util/stream/file.h>
#include <util/generic/yexception.h>
#include <util/string/builder.h>

namespace NKikimr {
namespace NTable {


Y_UNIT_TEST_SUITE(Redo) {
    namespace ETypes = NScheme::NTypeIds;

    using namespace NTest;

    struct TReplaySink {
        TVector<TString> Applied;
        TVector<NPageCollection::TMemGlob> Annex;

        bool NeedIn(ui32 table) const { return table == 1; }
        void DoBegin(ui32, ui32, ui64, ui64) {}
        void DoFlush(ui32, ui64, TEpoch) {}
        void DoLockRowTx(ui32, ELockMode, TRawVals, ui64) {}

        void DoAnnex(TArrayRef<const TStdPad<NPageCollection::TGlobId>> blobs) {
            if (Annex.empty()) {
                for (const auto& blob : blobs) {
                    Annex.emplace_back(*blob, TSharedData::Copy("blob", 4));
                }
            }
        }

        void DoUpdate(ui32, ERowOp, TRawVals, TArrayRef<const TUpdateOp>, TRowVersion) {
            Applied.push_back("update");
        }

        void DoUpdateTx(ui32, ERowOp, TRawVals, TArrayRef<const TUpdateOp>, ui64, ui32) {
            Applied.push_back("update");
        }

        void DoCommitTx(ui32, ui64, TRowVersion) {
            Applied.push_back("commit");
        }

        void DoRemoveTx(ui32, ui64) {
            Applied.push_back("remove");
        }
    };

    struct TRecordingObserver : IRedoObserver {
        TReplaySink& Sink;
        TVector<TString> Events;
        bool Fail = false;

        explicit TRecordingObserver(TReplaySink& sink) : Sink(sink) {}

        void Record(TString kind, TString event) {
            UNIT_ASSERT_VALUES_EQUAL(Sink.Applied.size(), Events.size() + 1);
            UNIT_ASSERT_VALUES_EQUAL(Sink.Applied.back(), kind);
            Events.push_back(std::move(event));
            if (Fail) {
                ythrow yexception() << "observer failure";
            }
        }

        void OnRedoUpdate(ui32 table, ERowOp op, TRawVals key, TArrayRef<const TUpdateOp> updates,
                TArrayRef<const NPageCollection::TMemGlob> annex, TRowVersion version,
                ui64 txId, ui32 savepoint) override
        {
            UNIT_ASSERT_VALUES_EQUAL(table, 1);
            if (key) {
                UNIT_ASSERT_VALUES_EQUAL(key.size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(key[0].Type(), ui32(ETypes::Uint64));
                UNIT_ASSERT_VALUES_EQUAL(TCell(key[0].AsRef()).AsValue<ui64>(), 17);
            }
            for (const auto& update : updates) {
                UNIT_ASSERT_VALUES_EQUAL(update.Tag, 2);
                UNIT_ASSERT(update.Op == ECellOp::Set);
                if (update.Op == ELargeObj::Extern) {
                    UNIT_ASSERT_VALUES_EQUAL(update.AsCell().AsValue<ui32>(), 0);
                    UNIT_ASSERT_VALUES_EQUAL(annex.size(), 1);
                    UNIT_ASSERT_VALUES_EQUAL(TStringBuf(annex[0].Data.data(), annex[0].Data.size()), "blob");
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(update.Value.ToStringBuf(), "inline");
                }
            }
            Record("update", TStringBuilder() << "update " << ui32(op) << " " << version.Step << " "
                << version.TxId << " " << txId << " " << savepoint);
        }

        void OnRedoCommit(ui32 table, ui64 txId, TRowVersion version) override {
            UNIT_ASSERT_VALUES_EQUAL(table, 1);
            Record("commit", TStringBuilder() << "commit " << txId << " " << version.Step << " " << version.TxId);
        }

        void OnRedoRemove(ui32 table, ui64 txId) override {
            UNIT_ASSERT_VALUES_EQUAL(table, 1);
            Record("remove", TStringBuilder() << "remove " << txId);
        }
    };

    Y_UNIT_TEST(PlayerObserver) {
        const auto key = NScheme::TUint64::TInstance(17);
        const TString value = "inline";
        const auto rawValue = NScheme::TString::TInstance(value);
        const TUpdateOp update{2, ECellOp::Set, rawValue};
        const auto index = NScheme::TUint32::TInstance(0);
        const TUpdateOp external{2, {ECellOp::Set, ELargeObj::Extern}, index};
        const NPageCollection::TGlobId blob{TLogoBlobID(1, 1, 1, 1, 4, 0), 7};
        NRedo::TWriter writer;
        writer.EvAnnex({blob});
        writer.EvUpdate(1, ERowOp::Upsert, {key}, {update}, TRowVersion::Min());
        writer.EvUpdate(1, ERowOp::Upsert, {key}, {external}, TRowVersion(23, 24));
        writer.EvUpdateTx(1, ERowOp::Upsert, {key}, {update}, 41);
        writer.EvUpdateTx(1, ERowOp::Upsert, {key}, {update}, 42, 9);
        writer.EvCommitTx(1, 41, TRowVersion(23, 24));
        writer.EvRemoveTx(1, 42);
        writer.EvUpdate(1, ERowOp::Erase, {key}, {}, TRowVersion::Min());
        writer.EvFlush(1, 0, TEpoch::Zero());
        writer.EvLockRowTx(1, ELockMode::Exclusive, {key}, 43);
        // Rejected records must neither be applied nor reach the observer.
        writer.EvUpdate(2, ERowOp::Upsert, {key}, {update}, TRowVersion::Min());
        writer.EvUpdateTx(2, ERowOp::Upsert, {key}, {update}, 51, 2);
        writer.EvCommitTx(2, 51, TRowVersion(23, 24));
        writer.EvRemoveTx(2, 51);
        const TString redo = std::move(writer).Finish();

        TReplaySink sink;
        TRecordingObserver observer(sink);
        NRedo::TPlayer<TReplaySink> player(sink);
        // DoAnnex fills this initially empty container during replay.
        player.Replay(redo, sink.Annex, &observer);
        const TVector<TString> expected{
            "update 1 0 0 0 0", "update 1 23 24 0 0", "update 1 0 0 41 0",
            "update 1 0 0 42 9", "commit 41 23 24", "remove 42", "update 2 0 0 0 0"};
        UNIT_ASSERT_VALUES_EQUAL(observer.Events.size(), expected.size());
        for (size_t i = 0; i < expected.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(observer.Events[i], expected[i]);
        }

        // A later replay without an observer must not retain the previous one.
        player.Replay(redo);
        UNIT_ASSERT_VALUES_EQUAL(observer.Events.size(), expected.size());
        UNIT_ASSERT_VALUES_EQUAL(sink.Applied.size(), 2 * expected.size());
    }

    Y_UNIT_TEST(PlayerObserverRestoredAfterException) {
        NRedo::TWriter writer;
        writer.EvUpdate(1, ERowOp::Upsert, {}, {}, TRowVersion::Min());
        const TString redo = std::move(writer).Finish();
        TReplaySink sink;
        TRecordingObserver observer(sink);
        observer.Fail = true;
        NRedo::TPlayer<TReplaySink> player(sink);
        UNIT_ASSERT_EXCEPTION(player.Replay(redo, sink.Annex, &observer), yexception);
        player.Replay(redo);
        UNIT_ASSERT_VALUES_EQUAL(observer.Events.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sink.Applied.size(), 2);
    }

    Y_UNIT_TEST(PlayerObserverLegacy) {
        const NRedo::TEvUpdate_Legacy update{{1, sizeof(NRedo::TEvUpdate_Legacy), NRedo::ERedo::Update}, 0, 0};
        const NRedo::TEvErase_Legacy erase{{1, sizeof(NRedo::TEvErase_Legacy), NRedo::ERedo::Erase}, 0};
        const NRedo::TEvUpdate_Legacy skipped{{2, sizeof(NRedo::TEvUpdate_Legacy), NRedo::ERedo::Update}, 0, 0};
        TString redo(reinterpret_cast<const char*>(&update), sizeof(update));
        redo.append(reinterpret_cast<const char*>(&erase), sizeof(erase));
        redo.append(reinterpret_cast<const char*>(&skipped), sizeof(skipped));
        TReplaySink sink;
        TRecordingObserver observer(sink);
        NRedo::TPlayer<TReplaySink>(sink).Replay(redo, sink.Annex, &observer);
        UNIT_ASSERT_VALUES_EQUAL(observer.Events.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(observer.Events[0], "update 1 0 0 0 0");
        UNIT_ASSERT_VALUES_EQUAL(observer.Events[1], "update 2 0 0 0 0");
    }

    Y_UNIT_TEST(ABI_008)
    {
        /* Redo log taken from DBase::Basics test that cover all events */

        TLayoutCook lay;

        lay
            .Col(0, 1,  ETypes::String)
            .Col(0, 4,  ETypes::Uint64, Cimple(77_u64))
            .Col(0, 5,  ETypes::String)
            .Key({ 1 });

        const auto foo = *TSchemedCookRow(*lay).Col("foo", 33_u64);
        const auto bar = *TSchemedCookRow(*lay).Col("bar", 11_u64);

        const auto raw = NResource::Find("abi/008_basics_db.redo");

        TStringInput input(raw);

        auto changes = TDbExec::RestoreChanges(input);

        UNIT_ASSERT_C(changes.size() == 9, "Unexpected changes in test data");

        TDbExec me;
        ui32 serial = 0;

        for (auto &one: changes) {
            me.To(++serial).Cleanup()->RollUp(one->Stamp, one->Scheme, one->Redo, { }, { });

            if (1 == serial) {
                /* Just applied initial alter script, nothing to check */

                UNIT_ASSERT(me->Subset(1, TEpoch::Max(), { }, { })->Head == TEpoch::FromIndex(1));

            } else if (2 == serial) {
                me.Iter(1).Has(foo).HasN(nullptr, 77_u64).NoKey(bar);
            } else if (3 == serial) {
                me.Iter(1).Has(foo).Has(bar);
            } else if (4 == serial) {
                /* Table flush (snapshot) event received with scheme updates */

                UNIT_ASSERT(me->Subset(1, TEpoch::Max(), { }, { })->Head == TEpoch::FromIndex(2));

            } else if (5 == serial) {
                me.Iter(1, false).NoKey(foo).NoVal(bar);
                me.Iter(1, false).HasN("bar", 11_u64, "yo");
            } else if (6 == serial) {
                me.Iter(1, false).HasN("bar", 77_u64, "me");
            } else if (7 == serial) {
                me.Iter(1, false).HasN("bar", 99_u64, nullptr);
            } else if (8 == serial) {
                me.Iter(1, false).HasN("bar", nullptr, "eh");
            } else if (9 == serial) {
                me.Iter(1, false).Has(bar).NoKey(foo);
            } else {
                UNIT_FAIL("Got to many change entries in redo samples");
            }
        }
    }
}


}
}
