#include "data.h"
#include "schema.h"
#include "garbage_collection.h"
#include "coro_tx.h"

namespace NKikimr::NBlobDepot {

    using TData = TBlobDepot::TData;

    void TData::StartLoad() {
        Self->Execute(std::make_unique<TCoroTx>(Self, TTokens{{Self->Token}}, [&](TCoroTx::TContextBase& tx) {
            bool progress = false;

            ui64 lastTimestamp = GetCycleCountFast();
            auto passed = [&] {
                const ui64 timestamp = GetCycleCountFast();
                return timestamp - std::exchange(lastTimestamp, timestamp);
            };

            auto account = [&](ui64& cycles) {
                const ui64 n = passed();
                cycles += n;
                LoadTotalCycles += n;
            };

            auto smartRestart = [&] {
                if (std::exchange(progress, false)) {
                    // we have already processed something, so start the next transaction to prevent keeping already
                    // processed data in memory
                    account(LoadProcessingCycles);
                    tx.FinishTx();
                    account(LoadFinishTxCycles);
                    tx.RunSuccessorTx();
                    account(LoadRunSuccessorTxCycles);
                    ++LoadRunSuccessorTx;
                } else {
                    // we haven't read anything at all, so we restart the transaction with the request to read some
                    // data
                    account(LoadProcessingCycles);
                    tx.RestartTx();
                    account(LoadRestartTxCycles);
                    ++LoadRestartTx;
                }
            };

            TString trash;
            while (!LoadTrash(*tx, trash, progress)) {
                smartRestart();
            }

            TScanRange r{
                .Begin = TKey::Min(),
                .End = TKey::Max(),
                .PrechargeRows = 10'000,
                .PrechargeBytes = 1'000'000,
            };

            progress = false;
            while (!ScanRange(r, tx.GetTxc(), &progress, [](const TKey&, const TValue&) { return true; })) {
                smartRestart();
            }

            account(LoadProcessingCycles);
            tx.FinishTx();
            account(LoadFinishTxCycles);
            Self->Data->OnLoadComplete();
        }));
    }

    bool TData::LoadTrash(NTabletFlatExecutor::TTransactionContext& txc, TString& from, bool& progress) {
        NIceDb::TNiceDb db(txc.DB);
        auto table = db.Table<Schema::Trash>().GreaterOrEqual(from);
        static constexpr ui64 PrechargeRows = 10'000;
        static constexpr ui64 PrechargeBytes = 1'000'000;
        if (!table.Precharge(PrechargeRows, PrechargeBytes)) {
            return false;
        }
        auto rows = table.Select();
        if (!rows.IsReady()) {
            return false;
        }
        while (rows.IsValid()) {
            if (auto key = rows.GetKey(); key != from) {
                Self->Data->AddTrashOnLoad(TLogoBlobID::FromBinary(key));
                from = std::move(key);
                progress = true;
            }
            if (!rows.Next()) {
                return false;
            }
        }
        return true;
    }

    void TData::OnLoadComplete() {
        Self->Data->LoadedKeys([&](const TKey& left, const TKey& right) {
            // verify that LoadedKeys == {Min, Max} exactly
            Y_VERIFY_S(left == TKey::Min() && right == TKey::Max() && !Loaded, "Id# " << Self->GetLogId()
                << " Left# " << left.ToString()
                << " Right# " << right.ToString()
                << " Loaded# " << Loaded
                << " LoadedKeys# " << LoadedKeys.ToString());
            Loaded = true;
            return true;
        });
        Y_ABORT_UNLESS(Loaded);
        Self->OnDataLoadComplete();

        // prepare records for all groups in history
        for (const auto& channel : Self->Info()->Channels) {
            Y_ABORT_UNLESS(channel.Channel < Self->Channels.size());
            if (Self->Channels[channel.Channel].ChannelKind != NKikimrBlobDepot::TChannelKind::Data) {
                continue; // skip non-data channels
            }
            for (const auto& entry : channel.History) {
                RecordsPerChannelGroup.try_emplace(std::make_tuple(channel.Channel, entry.GroupID), channel.Channel,
                    entry.GroupID);
            }
        }

        for (auto& [key, record] : RecordsPerChannelGroup) {
            record.CollectIfPossible(this);
        }
    }

    bool TData::EnsureKeyLoaded(const TKey& key, NTabletFlatExecutor::TTransactionContext& txc) {
        if (IsKeyLoaded(key)) {
            return true;
        }

        NIceDb::TNiceDb db(txc.DB);
        using Table = Schema::Data;
        auto row = db.Table<Table>().Key(key.MakeBinaryKey()).Select();
        if (!row.IsReady()) {
            return false;
        } else {
            if (row.IsValid()) {
                AddDataOnLoad(key, row.GetValue<Table::Value>(), row.GetValueOrDefault<Table::UncertainWrite>());
            }
            Self->Data->LoadedKeys |= {key, key};
            return true;
        }
    }

    void TBlobDepot::StartDataLoad() {
        Data->StartLoad();
    }

    void TBlobDepot::OnDataLoadComplete() {
        BarrierServer->OnDataLoaded();
        StartGroupAssimilator();
    }

} // NKikimr::NBlobDepot
