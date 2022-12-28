#include "data.h"
#include "schema.h"
#include "garbage_collection.h"

namespace NKikimr::NBlobDepot {

    using TData = TBlobDepot::TData;

    class TData::TTxDataLoad : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
        std::optional<TString> LastTrashKey;
        std::optional<TString> LastDataKey;
        bool TrashLoaded = false;
        bool SuccessorTx = true;

    public:
        TTxType GetTxType() const override { return NKikimrBlobDepot::TXTYPE_DATA_LOAD; }

        TTxDataLoad(TBlobDepot *self)
            : TTransactionBase(self)
        {}

        TTxDataLoad(TTxDataLoad& predecessor)
            : TTransactionBase(predecessor.Self)
            , LastTrashKey(std::move(predecessor.LastTrashKey))
            , LastDataKey(std::move(predecessor.LastDataKey))
            , TrashLoaded(predecessor.TrashLoaded)
        {}

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT28, "TData::TTxDataLoad::Execute", (Id, Self->GetLogId()));

            NIceDb::TNiceDb db(txc.DB);
            bool progress = false;

            auto load = [&](auto t, auto& lastKey, auto callback) {
                auto table = t.GreaterOrEqual(lastKey.value_or(TString()));
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
                    if (auto key = rows.GetKey(); key != lastKey) {
                        callback(key, rows);
                        lastKey.emplace(std::move(key));
                        progress = true;
                    }
                    if (!rows.Next()) {
                        return false;
                    }
                }
                lastKey.reset();
                return true;
            };

            if (!TrashLoaded) {
                auto addTrash = [this](const auto& key, const auto& /*rows*/) {
                    Self->Data->AddTrashOnLoad(TLogoBlobID::FromBinary(key));
                };
                if (!load(db.Table<Schema::Trash>(), LastTrashKey, addTrash)) {
                    return progress;
                }
                TrashLoaded = true;
            }

            auto addData = [this](const auto& key, const auto& rows) {
                auto k = TData::TKey::FromBinaryKey(key, Self->Config);
                Self->Data->AddDataOnLoad(k, rows.template GetValue<Schema::Data::Value>(),
                    rows.template GetValueOrDefault<Schema::Data::UncertainWrite>(), false);
                Y_VERIFY(!Self->Data->LastLoadedKey || *Self->Data->LastLoadedKey < k);
                Self->Data->LastLoadedKey = std::move(k);
            };
            if (!load(db.Table<Schema::Data>(), LastDataKey, addData)) {
                return progress;
            }

            SuccessorTx = false; // everything loaded
            return true;
        }

        void Complete(const TActorContext&) override {
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT29, "TData::TTxDataLoad::Complete", (Id, Self->GetLogId()),
                (TrashLoaded, TrashLoaded), (SuccessorTx, SuccessorTx));

            if (SuccessorTx) {
                Self->Execute(std::make_unique<TTxDataLoad>(*this));
            } else {
                Self->Data->OnLoadComplete();
            }
        }
    };

    void TData::StartLoad() {
        Self->Execute(std::make_unique<TTxDataLoad>(Self));
    }

    void TData::OnLoadComplete() {
        Loaded = true;
        LoadSkip.clear();
        Self->OnDataLoadComplete();
        for (auto& [key, record] : RecordsPerChannelGroup) {
            record.CollectIfPossible(this);
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
