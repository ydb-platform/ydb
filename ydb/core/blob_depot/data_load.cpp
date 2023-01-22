#include "data.h"
#include "schema.h"
#include "garbage_collection.h"

namespace NKikimr::NBlobDepot {

    using TData = TBlobDepot::TData;

    class TData::TTxDataLoad : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
        std::optional<TString> LastTrashKey;
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

            for (;;) {
                // calculate a set of keys we need to load
                TClosedIntervalSet<TKey> needed;
                needed |= {TKey::Min(), TKey::Max()};
                const auto interval = needed.PartialSubtract(Self->Data->LoadedKeys);
                if (!interval) {
                    break;
                }

                const auto& [first, last] = *interval;

                auto makeNeeded = [&] {
                    TStringStream s("{");
                    bool flag = true;
                    needed([&](const TKey& first, const TKey& last) {
                        s << (std::exchange(flag, false) ? "" : "-");
                        first.Output(s);
                        last.Output(s << '-');
                        return true;
                    });
                    s << '}';
                    return s.Str();
                };
                STLOG(PRI_DEBUG, BLOB_DEPOT, BDT83, "TTxDataLoad iteration", (Id, Self->GetLogId()), (Needed, makeNeeded()),
                    (FirstKey, first), (LastKey, last));

                bool status = true;
                std::optional<TKey> lastKey; // the actual last processed key

                auto table = db.Table<Schema::Data>().GreaterOrEqual(first.MakeBinaryKey());
                static constexpr ui64 PrechargeRows = 10'000;
                static constexpr ui64 PrechargeBytes = 1'000'000;
                if (!table.Precharge(PrechargeRows, PrechargeBytes)) {
                    status = false;
                } else if (auto rows = table.Select(); !rows.IsReady()) {
                    status = false;
                } else {
                    for (;;) {
                        if (!rows.IsValid()) { // finished reading the range
                            lastKey.emplace(last);
                            break;
                        }
                        auto key = TKey::FromBinaryKey(rows.GetKey(), Self->Config);
                        lastKey.emplace(key);
                        if (last <= key) { // stop iteration -- we are getting out of range
                            break;
                        } else if (first < key && !Self->Data->IsKeyLoaded(key)) {
                            Self->Data->AddDataOnLoad(std::move(key), rows.template GetValue<Schema::Data::Value>(),
                                rows.template GetValueOrDefault<Schema::Data::UncertainWrite>());
                            progress = true;
                        }
                        if (!rows.Next()) {
                            status = false;
                            break;
                        }
                    }
                }

                STLOG(PRI_DEBUG, BLOB_DEPOT, BDT84, "TTxDataLoad iteration complete", (Id, Self->GetLogId()),
                    (FirstKey, first), (LastKey, lastKey), (Status, status), (Progress, progress));

                if (lastKey) {
                    Self->Data->LoadedKeys |= {first, *lastKey};
                }
                if (!status) {
                    return progress;
                }
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
