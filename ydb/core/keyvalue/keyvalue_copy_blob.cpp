#include "keyvalue_flat_impl.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/core/util/stlog.h>

namespace NKikimr {
namespace NKeyValue {

class TKeyValueCopyBlobActor : public TActorBootstrapped<TKeyValueCopyBlobActor> {
    TActorId KeyValueActorId;
    TIntrusivePtr<TTabletStorageInfo> TabletInfo;
    TLogoBlobID BlobId;
    TLogoBlobID NewBlobId;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KEYVALUE_ACTOR;
    }

    TKeyValueCopyBlobActor(
            const TActorId& keyValueActorId,
            TTabletStorageInfo* tabletInfo,
            const TLogoBlobID& blobId,
            const TLogoBlobID& newBlobId)
        : KeyValueActorId(keyValueActorId)
        , TabletInfo(tabletInfo)
        , BlobId(blobId)
        , NewBlobId(newBlobId)
    {}

    void Bootstrap() {
        auto groupId = TabletInfo->GroupFor(BlobId.Channel(), BlobId.Generation());

        YDB_LOG_DEBUG_COMP(NKikimrServices::KEYVALUE, "KeyValueCopyBlobActor started, send EvGet",
            {"marker", "KVCB01"},
            {"keyValue", TabletInfo->TabletID},
            {"groupId", groupId},
            {"blobId", BlobId.ToString()});

        auto deadline = TActivationContext::Now() + TDuration::Seconds(60);

        auto ev = std::make_unique<TEvBlobStorage::TEvGet>(
            BlobId, 0, BlobId.BlobSize(), deadline, NKikimrBlobStorage::EGetHandleClass::LowRead);
        SendToBSProxy(TActivationContext::AsActorContext(), groupId, ev.release(), 0, NWilson::TTraceId());

        Become(&TThis::StateGet);
    }

    void Handle(TEvBlobStorage::TEvGetResult::TPtr& ev) {
        auto groupId = TabletInfo->GroupFor(BlobId.Channel(), BlobId.Generation());

        if (ev->Get()->GroupId != groupId) {
            YDB_LOG_ERROR_COMP(NKikimrServices::KEYVALUE, "KeyValueCopyBlobActor: unexpected EvGet result: invalid group id",
                {"marker", "KVCB02"},
                {"keyValue", TabletInfo->TabletID},
                {"groupId", ev->Get()->GroupId},
                {"expectedGroupId", groupId},
                {"blobId", BlobId.ToString()});
            HandleErrorAndDie();
            return;
        }

        NKikimrProto::EReplyStatus status = ev->Get()->Status;
        if (status != NKikimrProto::OK) {
            YDB_LOG_ERROR_COMP(NKikimrServices::KEYVALUE, "KeyValueCopyBlobActor: unexpected EvGet result: status is not OK",
                {"marker", "KVCB03"},
                {"keyValue", TabletInfo->TabletID},
                {"groupId", groupId},
                {"blobId", BlobId.ToString()},
                {"status", NKikimrProto::EReplyStatus_Name(status)},
                {"errorReason", ev->Get()->ErrorReason});
            HandleErrorAndDie();
            return;
        }

        Y_ABORT_UNLESS(ev->Get()->ResponseSz == 1);
        auto& response = ev->Get()->Responses[0];

        if (response.Status != NKikimrProto::OK) {
            YDB_LOG_ERROR_COMP(NKikimrServices::KEYVALUE, "KeyValueCopyBlobActor: unexpected EvGet result: response status is not OK",
                {"marker", "KVCB04"},
                {"keyValue", TabletInfo->TabletID},
                {"groupId", groupId},
                {"blobId", BlobId.ToString()},
                {"status", NKikimrProto::EReplyStatus_Name(response.Status)});
            HandleErrorAndDie();
            return;
        }

        auto& buffer = response.Buffer;
        if (buffer.size() != BlobId.BlobSize()) {
            YDB_LOG_ERROR_COMP(NKikimrServices::KEYVALUE, "KeyValueCopyBlobActor: unexpected EvGet result: buffer size is not equal to blob size",
                {"marker", "KVCB05"},
                {"keyValue", TabletInfo->TabletID},
                {"groupId", groupId},
                {"blobId", BlobId.ToString()},
                {"bufferSize", buffer.size()},
                {"blobSize", BlobId.BlobSize()});
            HandleErrorAndDie();
            return;
        }

        // TODO: more error handing

        auto deadline = TActivationContext::Now() + TDuration::Seconds(60);

        THolder<TEvBlobStorage::TEvPut> put(new TEvBlobStorage::TEvPut(TEvBlobStorage::TEvPut::TParameters{
            .BlobId = NewBlobId,
            .Buffer = std::move(buffer),
            .Deadline = deadline,
            .HandleClass = NKikimrBlobStorage::AsyncBlob,
            .Tactic = TEvBlobStorage::TEvPut::TacticDefault,
            .WriteSource = TWriteSource::KeyValueMoveData,
        }));

        ui32 newGroupId = TabletInfo->GroupFor(NewBlobId.Channel(), NewBlobId.Generation());

        YDB_LOG_DEBUG_COMP(NKikimrServices::KEYVALUE, "Send EvPut",
            {"marker", "KVCB06"},
            {"keyValue", TabletInfo->TabletID},
            {"newGroupId", newGroupId},
            {"newBlobId", NewBlobId.ToString()});

        SendPutToGroup(TActivationContext::AsActorContext(), newGroupId, TabletInfo.Get(), std::move(put));

        Become(&TThis::StatePut);
    }

    void Handle(TEvBlobStorage::TEvPutResult::TPtr& ev) {
        ui32 newGroupId = TabletInfo->GroupFor(NewBlobId.Channel(), NewBlobId.Generation());

        if (ev->Get()->GroupId != newGroupId) {
            YDB_LOG_ERROR_COMP(NKikimrServices::KEYVALUE, "KeyValueCopyBlobActor: unexpected EvPut result: invalid group id",
                {"marker", "KVCB07"},
                {"keyValue", TabletInfo->TabletID},
                {"groupId", ev->Get()->GroupId},
                {"expectedGroupId", newGroupId},
                {"newBlobId", NewBlobId.ToString()});
            HandleErrorAndDie();
            return;
        }

        NKikimrProto::EReplyStatus status = ev->Get()->Status;
        if (status != NKikimrProto::OK) {
            YDB_LOG_ERROR_COMP(NKikimrServices::KEYVALUE, "KeyValueCopyBlobActor: unexpected EvPut result: status is not OK",
                {"marker", "KVCB08"},
                {"keyValue", TabletInfo->TabletID},
                {"newGroupId", newGroupId},
                {"newBlobId", NewBlobId.ToString()},
                {"status", NKikimrProto::EReplyStatus_Name(status)},
                {"errorReason", ev->Get()->ErrorReason});
            HandleErrorAndDie();
            return;
        }

        // TODO: more error handing

        FinishAndDie();
    }

    void FinishAndDie() {
        Send(KeyValueActorId, new TEvKeyValue::TEvBlobCopied(BlobId, NewBlobId));
        PassAway();
    }

    void HandleErrorAndDie() {
        YDB_LOG_ERROR_COMP(NKikimrServices::KEYVALUE, "KeyValueCopyBlobActor: error while copying blob, send PoisonPill to the tablet",
            {"marker", "KVCB09"},
            {"keyValue", TabletInfo->TabletID},
            {"blobId", BlobId.ToString()},
            {"newBlobId", NewBlobId.ToString()});
        Send(KeyValueActorId, new TEvents::TEvPoisonPill());
        PassAway();
    }

    STFUNC(StateGet) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBlobStorage::TEvGetResult, Handle);
            default:
                break;
        }
    }

    STFUNC(StatePut) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBlobStorage::TEvPutResult, Handle);
            default:
                break;
        }
    }
};

IActor* CreateKeyValueCopyBlobActor(
        const TActorId& keyValueActorId,
        TTabletStorageInfo* tabletInfo,
        const TLogoBlobID& blobId,
        const TLogoBlobID& newBlobId) {
    return new TKeyValueCopyBlobActor(keyValueActorId, tabletInfo, blobId, newBlobId);
}

} // NKeyValue
} // NKikimr
