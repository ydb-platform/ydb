#include "impl.h"

#include <ydb/core/protos/blobstorage_ddisk.pb.h>

#define YDB_LOG_THIS_FILE_COMPONENT BS_CONTROLLER

namespace NKikimr::NBsController {

namespace {

void SetError(NKikimrBlobStorage::TEvControllerDDiskInfoGetTabletResult& record, const TString& reason) {
    record.SetStatus(NKikimrProto::ERROR);
    record.SetErrorReason(reason);
}

} // anonymous namespace

class TBlobStorageController::TTxListDDiskInfoTablets
    : public TTransactionBase<TBlobStorageController>
{
    std::unique_ptr<TEventHandle<TEvBlobStorage::TEvControllerDDiskInfoListTablets>> Request;
    std::unique_ptr<TEvBlobStorage::TEvControllerDDiskInfoListTabletsResult> Result;

public:
    TTxListDDiskInfoTablets(
            TBlobStorageController* self,
            std::unique_ptr<TEventHandle<TEvBlobStorage::TEvControllerDDiskInfoListTablets>> request)
        : TBase(self)
        , Request(std::move(request))
    {}

    TTxType GetTxType() const override { return NBlobStorageController::TXTYPE_LIST_DDISK_INFO_TABLETS; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        Result = std::make_unique<TEvBlobStorage::TEvControllerDDiskInfoListTabletsResult>();
        auto& record = Result->Record;
        record.SetStatus(NKikimrProto::OK);

        NIceDb::TNiceDb db(txc.DB);
        auto rows = db.Table<Schema::DirectBlockGroupTabletState>().Range().Select();
        if (!rows.IsReady()) {
            return false;
        }

        while (!rows.EndOfSet()) {
            auto* tablet = record.AddTablets();
            tablet->SetTabletId(rows.GetValue<Schema::DirectBlockGroupTabletState::TabletId>());
            tablet->SetRevision(rows.GetValueOrDefault<Schema::DirectBlockGroupTabletState::Revision>(0));
            tablet->SetLastChangedAt(rows.GetValueOrDefault<Schema::DirectBlockGroupTabletState::LastChangedAt>(TInstant::Zero()).MicroSeconds());
            if (!rows.Next()) {
                return false;
            }
        }

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        TActivationContext::Send(new IEventHandle(
            Request->Sender,
            ctx.SelfID,
            Result.release(),
            0,
            Request->Cookie));
    }
};

class TBlobStorageController::TTxGetDDiskInfoTablet
    : public TTransactionBase<TBlobStorageController>
{
    std::unique_ptr<TEventHandle<TEvBlobStorage::TEvControllerDDiskInfoGetTablet>> Request;
    std::unique_ptr<TEvBlobStorage::TEvControllerDDiskInfoGetTabletResult> Result;

public:
    TTxGetDDiskInfoTablet(
            TBlobStorageController* self,
            std::unique_ptr<TEventHandle<TEvBlobStorage::TEvControllerDDiskInfoGetTablet>> request)
        : TBase(self)
        , Request(std::move(request))
    {}

    TTxType GetTxType() const override { return NBlobStorageController::TXTYPE_GET_DDISK_INFO_TABLET; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        Result = std::make_unique<TEvBlobStorage::TEvControllerDDiskInfoGetTabletResult>();
        auto& record = Result->Record;
        const ui64 tabletId = Request->Get()->Record.GetTabletId();
        record.SetTabletId(tabletId);
        record.SetStatus(NKikimrProto::OK);

        NIceDb::TNiceDb db(txc.DB);
        auto revision = db.Table<Schema::DirectBlockGroupTabletState>().Key(tabletId).Select();
        auto rows = db.Table<Schema::DirectBlockGroupClaims>().Prefix(tabletId).Select();
        if (!revision.IsReady() || !rows.IsReady()) {
            return false;
        }

        record.SetRevision(revision.IsValid()
            ? revision.GetValueOrDefault<Schema::DirectBlockGroupTabletState::Revision>(0)
            : 0);

        while (!rows.EndOfSet()) {
            NKikimrBlobStorage::NDDisk::TDirectBlockGroupAllocation allocation;
            const TString data = rows.GetValue<Schema::DirectBlockGroupClaims::Allocation>();
            if (!allocation.ParseFromString(data)) {
                SetError(record, TStringBuilder() << "failed to parse allocation for tablet " << tabletId);
                return true;
            }

            auto* group = record.AddGroups();
            group->SetDirectBlockGroupId(rows.GetValue<Schema::DirectBlockGroupClaims::DirectBlockGroupId>());
            for (const auto& item : allocation.GetDDiskRecord()) {
                group->SetNumVChunksClaimed(Max(group->GetNumVChunksClaimed(), item.GetNumChunksClaimed()));
                if (item.HasDDiskId()) {
                    group->AddDDiskId()->CopyFrom(item.GetDDiskId());
                } else {
                    group->AddDDiskId();
                }
            }
            for (const auto& item : allocation.GetPersistentBufferDDiskId()) {
                group->AddPersistentBufferDDiskId()->CopyFrom(item);
            }

            if (!rows.Next()) {
                return false;
            }
        }

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        TActivationContext::Send(new IEventHandle(
            Request->Sender,
            ctx.SelfID,
            Result.release(),
            0,
            Request->Cookie));
    }
};

void TBlobStorageController::Handle(TEvBlobStorage::TEvControllerDDiskInfoListTablets::TPtr ev) {
    Execute(std::make_unique<TTxListDDiskInfoTablets>(this,
        std::unique_ptr<TEventHandle<TEvBlobStorage::TEvControllerDDiskInfoListTablets>>(ev.Release())));
}

void TBlobStorageController::Handle(TEvBlobStorage::TEvControllerDDiskInfoGetTablet::TPtr ev) {
    Execute(std::make_unique<TTxGetDDiskInfoTablet>(this,
        std::unique_ptr<TEventHandle<TEvBlobStorage::TEvControllerDDiskInfoGetTablet>>(ev.Release())));
}

} // namespace NKikimr::NBsController
