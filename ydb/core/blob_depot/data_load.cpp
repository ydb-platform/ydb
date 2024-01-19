#include "data.h"
#include "schema.h"
#include "garbage_collection.h"
#include "coro_tx.h"

namespace NKikimr::NBlobDepot {

    using TData = TBlobDepot::TData;

    void TData::StartLoad() {
        Self->Execute(std::make_unique<TCoroTx>(Self, TTokens{{Self->Token}}, [&] {
            bool progress = false;

            TString trash;
            bool trashLoaded = false;

            TScanRange r{
                .Begin = TKey::Min(),
                .End = TKey::Max(),
                .PrechargeRows = 10'000,
                .PrechargeBytes = 1'000'000,
            };

            while (!(trashLoaded = LoadTrash(*TCoroTx::GetTxc(), trash, progress)) ||
                    !ScanRange(r, TCoroTx::GetTxc(), &progress, [](const TKey&, const TValue&) { return true; })) {
                if (std::exchange(progress, false)) {
                    TCoroTx::FinishTx();
                    TCoroTx::RunSuccessorTx();
                } else {
                    TCoroTx::RestartTx();
                }
            }

            TCoroTx::FinishTx();
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
