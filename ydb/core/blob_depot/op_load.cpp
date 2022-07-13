#include "blob_depot_tablet.h"
#include "schema.h"
#include "blocks.h"
#include "data.h"
#include "garbage_collection.h"

namespace NKikimr::NBlobDepot {

    void TBlobDepot::ExecuteTxLoad() {
        class TTxLoad : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
            bool Configured = false;

        public:
            TTxLoad(TBlobDepot *self)
                : TTransactionBase(self)
            {}

            bool Execute(TTransactionContext& txc, const TActorContext&) override {
                STLOG(PRI_DEBUG, BLOB_DEPOT, BDT15, "TTxLoad::Execute", (TabletId, Self->TabletID()));

                NIceDb::TNiceDb db(txc.DB);

                if (!Precharge(db)) {
                    return false;
                }

                // Config
                {
                    auto table = db.Table<Schema::Config>().Key(Schema::Config::Key::Value).Select();
                    if (!table.IsReady()) {
                        return false;
                    } else if (table.IsValid()) {
                        if (table.HaveValue<Schema::Config::ConfigProtobuf>()) {
                            Configured = Self->Config.ParseFromString(table.GetValue<Schema::Config::ConfigProtobuf>());
                            Y_VERIFY(Configured);
                        }
                    }
                }

                // Blocks
                {
                    auto table = db.Table<Schema::Blocks>().Select();
                    if (!table.IsReady()) {
                        return false;
                    }
                    while (table.IsValid()) {
                        Self->BlocksManager->AddBlockOnLoad(
                            table.GetValue<Schema::Blocks::TabletId>(),
                            table.GetValue<Schema::Blocks::BlockedGeneration>(),
                            table.GetValue<Schema::Blocks::IssuerGuid>()
                        );
                        if (!table.Next()) {
                            return false;
                        }
                    }
                }

                // Barriers
                {
                    auto table = db.Table<Schema::Barriers>().Select();
                    if (!table.IsReady()) {
                        return false;
                    }
                    while (table.IsValid()) {
                        Self->BarrierServer->AddBarrierOnLoad(
                            table.GetValue<Schema::Barriers::TabletId>(),
                            table.GetValue<Schema::Barriers::Channel>(),
                            table.GetValue<Schema::Barriers::RecordGeneration>(),
                            table.GetValue<Schema::Barriers::PerGenerationCounter>(),
                            TGenStep(table.GetValue<Schema::Barriers::Soft>()),
                            TGenStep(table.GetValue<Schema::Barriers::Hard>())
                        );
                        if (!table.Next()) {
                            return false;
                        }
                    }
                }

                // Data
                {
                    auto table = db.Table<Schema::Data>().Select();
                    if (!table.IsReady()) {
                        return false;
                    }
                    while (table.IsValid()) {
                        Self->Data->AddDataOnLoad(
                            TData::TKey::FromBinaryKey(table.GetValue<Schema::Data::Key>(), Self->Config),
                            table.GetValue<Schema::Data::Value>()
                        );
                        if (!table.Next()) {
                            return false;
                        }
                    }
                }

                // Trash
                {
                    auto table = db.Table<Schema::Trash>().Select();
                    if (!table.IsReady()) {
                        return false;
                    }
                    while (table.IsValid()) {
                        const TString& blobId = table.GetValue<Schema::Trash::BlobId>();
                        Self->Data->AddTrashOnLoad(TLogoBlobID(reinterpret_cast<const ui64*>(blobId.data())));
                        if (!table.Next()) {
                            return false;
                        }
                    }
                }

                // GC
                {
                    using T = Schema::GC;
                    auto table = db.Table<T>().Select();
                    if (!table.IsReady()) {
                        return false;
                    }
                    while (table.IsValid()) {
                        Self->Data->AddGenStepOnLoad(table.GetValue<T::Channel>(),
                            table.GetValue<T::GroupId>(),
                            TGenStep(table.GetValueOrDefault<T::IssuedGenStep>()),
                            TGenStep(table.GetValueOrDefault<T::ConfirmedGenStep>()));
                        if (!table.Next()) {
                            return false;
                        }
                    }
                }

                return true;
            }

            bool Precharge(NIceDb::TNiceDb& db) {
                auto config = db.Table<Schema::Config>().Select();
                auto blocks = db.Table<Schema::Blocks>().Select();
                auto barriers = db.Table<Schema::Barriers>().Select();
                auto data = db.Table<Schema::Data>().Select();
                auto trash = db.Table<Schema::Trash>().Select();
                auto confirmedGC = db.Table<Schema::GC>().Select();
                return config.IsReady() && blocks.IsReady() && barriers.IsReady() && data.IsReady() && trash.IsReady() &&
                    confirmedGC.IsReady();
            }

            void Complete(const TActorContext&) override {
                STLOG(PRI_DEBUG, BLOB_DEPOT, BDT16, "TTxLoad::Complete", (TabletId, Self->TabletID()),
                    (Configured, Configured));

                if (Configured) {
                    Self->InitChannelKinds();
                    Self->Data->HandleTrash();
                }

                Self->OnLoadFinished();
            }
        };

        Execute(std::make_unique<TTxLoad>(this));
    }

} // NKikimr::NBlobDepot
