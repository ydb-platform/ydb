#include "data.h"
#include "schema.h"
#include "garbage_collection.h"
#include "coro_tx.h"
#include "s3.h"

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

            TS3Locator s3;
            progress = false;
            while (!LoadTrashS3(*tx, s3, progress)) {
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

    bool TData::LoadTrashS3(NTabletFlatExecutor::TTransactionContext& txc, TS3Locator& from, bool& progress) {
        NIceDb::TNiceDb db(txc.DB);
        auto table = db.Table<Schema::TrashS3>().GreaterOrEqual(from.Generation, from.KeyId);
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
            TS3Locator item{
                .Len = rows.GetValue<Schema::TrashS3::Len>(),
                .Generation = rows.GetValue<Schema::TrashS3::Generation>(),
                .KeyId = rows.GetValue<Schema::TrashS3::KeyId>(),
            };
            if (item != from) {
                Self->S3Manager->AddTrashToCollect(item);
                from = item;
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

    bool TData::EnsureKeyLoaded(const TKey& key, NTabletFlatExecutor::TTransactionContext& txc, bool *progress) {
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
            if (progress) {
                *progress = true;
            }
            return true;
        }
    }

    template<typename TRecord>
    bool TData::LoadMissingKeys(const TRecord& record, NTabletFlatExecutor::TTransactionContext& txc) {
        if (IsLoaded()) {
            return true;
        }
        for (const auto& item : record.GetItems()) {
            auto key = TKey::FromBinaryKey(item.GetKey(), Self->Config);
            if (!EnsureKeyLoaded(key, txc)) {
                return false;
            }
        }
        return true;
    }

    template bool TData::LoadMissingKeys(const NKikimrBlobDepot::TEvCommitBlobSeq& record, NTabletFlatExecutor::TTransactionContext& txc);
    template bool TData::LoadMissingKeys(const NKikimrBlobDepot::TEvPrepareWriteS3& record, NTabletFlatExecutor::TTransactionContext& txc);

    void TBlobDepot::StartDataLoad() {
        Data->StartLoad();
    }

    void TBlobDepot::OnDataLoadComplete() {
        BarrierServer->OnDataLoaded();
        StartGroupAssimilator();
        TabletCounters->Simple()[NKikimrBlobDepot::COUNTER_MODE_LOADING_KEYS] = 0;
        TabletCounters->Simple()[NKikimrBlobDepot::COUNTER_MODE_LOADED] = 1;
    }

} // NKikimr::NBlobDepot
