#include "impl.h"
#include <ydb/core/base/feature_flags.h>


namespace NKikimr {
namespace NBsController {

class TBlobStorageController::TTxMigrate : public TTransactionBase<TBlobStorageController> {
    class TTxBase {
    protected:
        TBlobStorageController *Self = nullptr;

    public:
        virtual ~TTxBase() = default;
        virtual bool Execute(TTransactionContext& txc) = 0;
        virtual void Complete() {}
        void SetController(TBlobStorageController *self) { Self = self; }
    };

    class TTxQueue : public TTransactionBase<TBlobStorageController> {
        TDeque<THolder<TTxBase>> Queue;

    public:
        TTxQueue(TBlobStorageController *controller, TDeque<THolder<TTxBase>> queue)
            : TTransactionBase(controller)
            , Queue(std::move(queue))
        {
            Y_ABORT_UNLESS(Queue);
            auto& front = Queue.front();
            front->SetController(controller);
        }

        TTxType GetTxType() const override { return NBlobStorageController::TXTYPE_MIGRATE; }

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            auto& front = Queue.front();
            STLOG(PRI_DEBUG, BS_CONTROLLER, BSCTXM03, "Execute tx from queue", (Type, TypeName(*front)));
            return front->Execute(txc);
        }

        void Complete(const TActorContext&) override {
            auto& front = Queue.front();
            STLOG(PRI_DEBUG, BS_CONTROLLER, BSCTXM04, "Complete tx from queue", (Type, TypeName(*front)));
            front->Complete();
            Queue.pop_front();
            if (Queue) {
                Self->Execute(new TTxQueue(Self, std::move(Queue)));
            } else {
                Self->Execute(Self->CreateTxLoadEverything());
            }
        }
    };

    class TTxTrimUnusedSlots : public TTxBase {
    public:
        bool Execute(TTransactionContext& txc) override {
            NIceDb::TNiceDb db(txc.DB);

            using Table = Schema::VSlot;

            TVector<Table::TKey::TupleType> eraseList;

            auto slots = db.Table<Table>().Range().Select();
            if (!slots.IsReady()) {
                return false;
            }
            while (!slots.EndOfSet()) {
                if (!slots.GetValue<Table::GroupID>().GetRawId()) {
                    // item scheduled for deletion
                    eraseList.push_back(slots.GetKey());
                }
                if (!slots.Next()) {
                    return false;
                }
            }

            for (const auto& key : eraseList) {
                db.Table<Table>().Key(key).Delete();
            }

            return true;
        }
    };

    class TTxUpdateSchemaVersion : public TTxBase {
    public:
        bool Execute(TTransactionContext& txc) override {
            NIceDb::TNiceDb(txc.DB).Table<Schema::State>().Key(true).Update<Schema::State::SchemaVersion>(Schema::CurrentSchemaVersion);
            return true;
        }
    };

    class TTxGenerateInstanceId : public TTxBase {
    public:
        bool Execute(TTransactionContext& txc) override {
            NIceDb::TNiceDb(txc.DB).Table<Schema::State>().Key(true).Update<Schema::State::InstanceId>(CreateGuidAsString());
            return true;
        }
    };

    class TTxUpdateStaticPDiskInfo : public TTxBase {
    public:
        bool Execute(TTransactionContext& txc) override {
            NIceDb::TNiceDb db(txc.DB);
            using T = Schema::PDisk;
            using TUpdateItem = std::tuple<T::Category::Type, T::Guid::Type, T::PDiskConfig::Type>;
            std::deque<std::pair<T::TKey::Type, TUpdateItem>> updates;
            for (const auto& [key, value] : Self->StaticPDiskMap) {
                const NKikimrBlobStorage::TNodeWardenServiceSet::TPDisk& pdisk = value;
                auto table = db.Table<T>().Key(key.GetKey()).Select();
                if (!table.IsReady()) {
                    return false;
                } else if (table.IsValid()) {
                    TString pdiskConfig;
                    Y_PROTOBUF_SUPPRESS_NODISCARD pdisk.GetPDiskConfig().SerializeToString(&pdiskConfig);
                    updates.emplace_back(key.GetKey(), TUpdateItem(pdisk.GetPDiskCategory(), pdisk.GetPDiskGuid(), pdiskConfig));
                }
            }
            for (const auto& [key, value] : updates) {
                db.Table<T>().Key(key).Update<T::Category, T::Guid, T::PDiskConfig>(std::get<0>(value),
                    std::get<1>(value), std::get<2>(value));
            }
            return true;
        }
    };

    class TTxFillInNonNullConfigForPDisk : public TTxBase {
    public:
        bool Execute(TTransactionContext& txc) override {
            NIceDb::TNiceDb db(txc.DB);
            using T = Schema::PDisk;
            std::deque<T::TKey::Type> keys;
            auto table = db.Table<T>().Select();
            if (!table.IsReady()) {
                return false;
            }
            while (table.IsValid()) {
                if (!table.HaveValue<T::PDiskConfig>()) {
                    keys.emplace_back(table.GetKey());
                }
                if (!table.Next()) {
                    return false;
                }
            }
            for (const auto& key : keys) {
                db.Table<T>().Key(key).Update<T::PDiskConfig>(TString());
            }
            return true;
        }
    };

    class TTxDropDriveStatus : public TTxBase {
    public:
        bool Execute(TTransactionContext& txc) override {
            if (txc.DB.GetScheme().GetTableInfo(Schema::DriveStatusTableId)) {
                txc.DB.Alter().DropTable(Schema::DriveStatusTableId);
            }
            return true;
        }
    };

    class TTxUpdateCompatibilityInfo : public TTxBase {
    public:
        bool Execute(TTransactionContext& txc) override {
            TString currentCompatibilityInfo;
            auto componentId = NKikimrConfig::TCompatibilityRule::BlobStorageController;
            bool success = CompatibilityInfo.MakeStored(componentId).SerializeToString(&currentCompatibilityInfo);
            Y_ABORT_UNLESS(success);
            NIceDb::TNiceDb(txc.DB).Table<Schema::State>().Key(true).Update<Schema::State::CompatibilityInfo>(currentCompatibilityInfo);
            return true;
        }
    };

    TDeque<THolder<TTxBase>> Queue;

public:
    using TTransactionBase::TTransactionBase;

    TTxType GetTxType() const override { return NBlobStorageController::TXTYPE_MIGRATE; }

    bool Execute(TTransactionContext &txc, const TActorContext&) override {
        STLOG(PRI_DEBUG, BS_CONTROLLER, BSCTXM01, "Execute tx");
        Queue.clear();

        NIceDb::TNiceDb db(txc.DB);

        auto state = db.Table<Schema::State>().Select<Schema::State::SchemaVersion, Schema::State::InstanceId,
                Schema::State::CompatibilityInfo>();

        if (!state.IsReady()) {
            return false;
        }
        bool hasInstanceId = false;
        if (state.IsValid()) {
            std::optional<NKikimrConfig::TStoredCompatibilityInfo> stored;
            if (state.HaveValue<Schema::State::CompatibilityInfo>()) {
                stored.emplace();
                bool success = stored->ParseFromString(state.GetValue<Schema::State::CompatibilityInfo>());
                Y_ABORT_UNLESS(success);
            }
            if (!AppData()->FeatureFlags.GetSuppressCompatibilityCheck() && !CompatibilityInfo.CheckCompatibility(
                        stored ? &*stored : nullptr, 
                        NKikimrConfig::TCompatibilityRule::BlobStorageController,
                        CompatibilityError)) {
                IncompatibleData = true;
                return true;
            }

            const ui32 version = state.GetValue<Schema::State::SchemaVersion>();
            if (Schema::CurrentSchemaVersion >= Schema::BoxHostMigrationSchemaVersion && version < Schema::BoxHostMigrationSchemaVersion) {
                Y_ABORT("unsupported schema");
            }
            hasInstanceId = state.HaveValue<Schema::State::InstanceId>();
        }

        // trim unused VSlots to prevent them from loading
        Queue.emplace_back(new TTxTrimUnusedSlots);

        // update schema version to current value
        Queue.emplace_back(new TTxUpdateSchemaVersion);

        // generate cluster instance id
        if (!hasInstanceId) {
            Queue.emplace_back(new TTxGenerateInstanceId);
        }

        Queue.emplace_back(new TTxUpdateStaticPDiskInfo);

        Queue.emplace_back(new TTxFillInNonNullConfigForPDisk);

        Queue.emplace_back(new TTxDropDriveStatus);
    
        Queue.emplace_back(new TTxUpdateCompatibilityInfo);

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        STLOG(PRI_DEBUG, BS_CONTROLLER, BSCTXM02, "Complete tx", (IncompatibleData, IncompatibleData));
        if (IncompatibleData) {
            STLOG(PRI_ALERT, BS_CONTROLLER, BSCTXM00, "CompatibilityInfo check failed", (ErrorReason, CompatibilityError));
            ctx.Send(new IEventHandle(TEvents::TSystem::Poison, 0, Self->SelfId(), {}, nullptr, 0));
        } else {
            Self->Execute(new TTxQueue(Self, std::move(Queue)));
        }
    }

private:
    bool IncompatibleData = false;
    TString CompatibilityError;
};

ITransaction* TBlobStorageController::CreateTxMigrate() {
    return new TTxMigrate(this);
}

} // NBlobStorageController
} // NKikimr
