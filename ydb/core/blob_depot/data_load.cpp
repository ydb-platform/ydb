#include "data.h"
#include "schema.h"
#include "garbage_collection.h"

namespace NKikimr::NBlobDepot {

    using TData = TBlobDepot::TData;

    class TData::TTxDataLoad : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
        template<typename TTable_, typename TInProgressState_, typename TNextState_, bool Initial_>
        struct TItem {
            using TTable = TTable_;
            using TInProgressState = TInProgressState_;
            using TNextState = TNextState_;
            static constexpr bool Initial = Initial_;
            typename TTable::TKey::KeyValuesType Key;
        };

        struct TLoadFinished {};
        struct TLoadGcInProgress : TItem<Schema::GC, TLoadGcInProgress, TLoadFinished, false> {};
        struct TLoadGcBegin : TItem<Schema::GC, TLoadGcInProgress, TLoadFinished, true> {};
        struct TLoadTrashInProgress : TItem<Schema::Trash, TLoadTrashInProgress, TLoadGcBegin, false> {};
        struct TLoadTrashBegin : TItem<Schema::Trash, TLoadTrashInProgress, TLoadGcBegin, true> {};
        struct TLoadDataInProgress : TItem<Schema::Data, TLoadDataInProgress, TLoadTrashBegin, false> {};
        struct TLoadDataBegin : TItem<Schema::Data, TLoadDataInProgress, TLoadTrashBegin, true> {};

        using TLoadState = std::variant<
            TLoadDataBegin,
            TLoadDataInProgress,
            TLoadTrashBegin,
            TLoadTrashInProgress,
            TLoadGcBegin,
            TLoadGcInProgress,
            TLoadFinished
        >;

        TLoadState LoadState;
        bool SuccessorTx = false;

    public:
        TTxType GetTxType() const override { return NKikimrBlobDepot::TXTYPE_DATA_LOAD; }

        TTxDataLoad(TBlobDepot *self, TLoadState loadState = TLoadDataBegin{})
            : TTransactionBase(self)
            , LoadState(std::move(loadState))
        {}

        TTxDataLoad(TTxDataLoad& predecessor)
            : TTransactionBase(predecessor.Self)
            , LoadState(std::move(predecessor.LoadState))
        {}

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT28, "TData::TTxDataLoad::Execute", (Id, Self->GetLogId()));

            NIceDb::TNiceDb db(txc.DB);
            bool progress = false;
            auto visitor = [&](auto& item) { return ExecuteLoad(db, item, progress); };

            if (std::visit(visitor, LoadState)) {
                // load finished
                return true;
            } else if (progress) {
                // something was read, but not all
                SuccessorTx = true;
                return true;
            } else {
                // didn't read anything this time
                return false;
            }
        }

        template<typename T>
        bool ExecuteLoad(NIceDb::TNiceDb& db, T& item, bool& progress) {
            if constexpr (T::Initial) {
                return LoadTable(db, db.Table<typename T::TTable>().All(), item, progress);
            } else {
                auto greaterOrEqual = [&](auto&&... key) { return db.Table<typename T::TTable>().GreaterOrEqual(key...); };
                return LoadTable(db, std::apply(greaterOrEqual, item.Key), item, progress);
            }
        }

        bool ExecuteLoad(NIceDb::TNiceDb&, TLoadFinished&, bool&) {
            return true;
        }

        template<typename T, typename TItem>
        bool LoadTable(NIceDb::TNiceDb& db, T&& table, TItem& item, bool& progress) {
            if (!table.Precharge(TItem::TTable::PrechargeRows, TItem::TTable::PrechargeBytes)) {
                return false;
            }

            for (auto rowset = table.Select();; rowset.Next()) {
                if (!rowset.IsReady()) {
                    return false;
                } else if (!rowset.IsValid()) {
                    break;
                }

                typename TItem::TTable::TKey::KeyValuesType key(rowset.GetKey());
                bool processRow = true;
                if constexpr (!TItem::Initial) {
                    processRow = item.Key < key;
                    Y_VERIFY_DEBUG(processRow || item.Key == key);
                }
                if (processRow) {
                    progress = true;
                    typename TItem::TInProgressState state;
                    state.Key = std::move(key);
                    LoadState = std::move(state);
                    ProcessRow(rowset, static_cast<typename TItem::TTable*>(nullptr));
                }
            }

            // table was read completely, advance to next state
            LoadState = typename TItem::TNextState{};
            return ExecuteLoad(db, std::get<typename TItem::TNextState>(LoadState), progress);
        }

        template<typename T>
        void ProcessRow(T&& row, Schema::Data*) {
            auto key = TData::TKey::FromBinaryKey(row.template GetValue<Schema::Data::Key>(), Self->Config);
            Self->Data->AddDataOnLoad(key, row.template GetValue<Schema::Data::Value>(),
                row.template GetValueOrDefault<Schema::Data::UncertainWrite>(), false);
            Y_VERIFY(!Self->Data->LastLoadedKey || *Self->Data->LastLoadedKey < key);
            Self->Data->LastLoadedKey = std::move(key);
        }

        template<typename T>
        void ProcessRow(T&& row, Schema::Trash*) {
            Self->Data->AddTrashOnLoad(TLogoBlobID::FromBinary(row.template GetValue<Schema::Trash::BlobId>()));
        }

        template<typename T>
        void ProcessRow(T&& row, Schema::GC*) {
            Self->Data->AddGenStepOnLoad(
                row.template GetValue<Schema::GC::Channel>(),
                row.template GetValue<Schema::GC::GroupId>(),
                TGenStep(row.template GetValueOrDefault<Schema::GC::IssuedGenStep>()),
                TGenStep(row.template GetValueOrDefault<Schema::GC::ConfirmedGenStep>())
            );
        }

        void Complete(const TActorContext&) override {
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT29, "TData::TTxDataLoad::Complete", (Id, Self->GetLogId()),
                (SuccessorTx, SuccessorTx), (LoadState.index, LoadState.index()));

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
    }

    void TBlobDepot::StartDataLoad() {
        Data->StartLoad();
    }

    void TBlobDepot::OnDataLoadComplete() {
        BarrierServer->OnDataLoaded();
        StartGroupAssimilator();
    }

} // NKikimr::NBlobDepot
