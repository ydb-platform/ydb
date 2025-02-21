#include "skeleton_vmultiput_actor.h"

#include <ydb/core/blobstorage/vdisk/common/vdisk_response.h>
#include <ydb/core/blobstorage/base/batched_vec.h>

namespace NKikimr {
    namespace NPrivate {

        class TBufferVMultiPutActor : public TActorBootstrapped<TBufferVMultiPutActor> {
            friend TActorBootstrapped<TBufferVMultiPutActor>;

            struct TItem {
                NKikimrProto::EReplyStatus Status = NKikimrProto::UNKNOWN;
                TString ErrorReason;
                TLogoBlobID BlobId;
                ui64 Cookie = 0;
                ui32 StatusFlags = 0;
                bool Received = false;
                bool HasCookie = false;
                bool WrittenBeyondBarrier;

                TString ToString() const {
                    return TStringBuilder()
                        << "{"
                        << " Status# " << NKikimrProto::EReplyStatus_Name(Status)
                        << " ErrorReason# " << '"' << EscapeC(ErrorReason) << '"'
                        << " BlobId# " << BlobId.ToString()
                        << " HasCookie# " << HasCookie
                        << " Cookie# " << Cookie
                        << " StatusFlags# " << NPDisk::StatusFlagsToString(StatusFlags)
                        << " Received# " << Received
                        << " WrittenBeyondBarrier# " << WrittenBeyondBarrier
                        << " }";
                }
            };

            TBatchedVec<TItem> Items;
            ui64 ReceivedResults;
            TActorIDPtr SkeletonFrontIDPtr;
            ::NMonitoring::TDynamicCounters::TCounterPtr MultiPutResMsgsPtr;

            TEvBlobStorage::TEvVMultiPut::TPtr Event;
            TActorId LeaderId;
            TOutOfSpaceStatus OOSStatus;

            const ui64 IncarnationGuid;

            TIntrusivePtr<TVDiskContext> VCtx;

        public:
            TBufferVMultiPutActor(TActorId leaderId, const TBatchedVec<NKikimrProto::EReplyStatus> &statuses,
                    TOutOfSpaceStatus oosStatus, TEvBlobStorage::TEvVMultiPut::TPtr &ev,
                    TActorIDPtr skeletonFrontIDPtr, ::NMonitoring::TDynamicCounters::TCounterPtr multiPutResMsgsPtr,
                    ui64 incarnationGuid, TIntrusivePtr<TVDiskContext>& vCtx)
                : TActorBootstrapped()
                , Items(ev->Get()->Record.ItemsSize())
                , ReceivedResults(0)
                , SkeletonFrontIDPtr(skeletonFrontIDPtr)
                , MultiPutResMsgsPtr(multiPutResMsgsPtr)
                , Event(ev)
                , LeaderId(leaderId)
                , OOSStatus(oosStatus)
                , IncarnationGuid(incarnationGuid)
                , VCtx(vCtx)
            {
                Y_ABORT_UNLESS(statuses.size() == Items.size());
                for (ui64 idx = 0; idx < Items.size(); ++idx) {
                    Items[idx].Status = statuses[idx];
                }
            }

        private:
            void SendResponseAndDie(const TActorContext &ctx) {
                NKikimrBlobStorage::TEvVMultiPut &vMultiPutRecord = Event->Get()->Record;
                TVDiskID vdisk = VDiskIDFromVDiskID(vMultiPutRecord.GetVDiskID());

                ui64 cookieValue;
                ui64 *cookie = nullptr;
                if (vMultiPutRecord.HasCookie()) {
                    cookieValue = vMultiPutRecord.GetCookie();
                    cookie = &cookieValue;
                }

                TInstant now = TAppData::TimeProvider->Now();
                const ui64 bufferSizeBytes = Event->Get()->GetBufferBytes();
                auto vMultiPutResult = std::make_unique<TEvBlobStorage::TEvVMultiPutResult>(NKikimrProto::OK, vdisk, cookie,
                    now, Event->Get()->GetCachedByteSize(), &vMultiPutRecord, SkeletonFrontIDPtr, MultiPutResMsgsPtr,
                    nullptr, bufferSizeBytes, IncarnationGuid, TString());

                for (ui64 idx = 0; idx < Items.size(); ++idx) {
                    TItem &result = Items[idx];
                    vMultiPutResult->AddVPutResult(result.Status, result.ErrorReason, result.BlobId,
                        result.HasCookie ? &result.Cookie : nullptr, result.StatusFlags, result.WrittenBeyondBarrier);
                }

                vMultiPutResult->Record.SetStatusFlags(OOSStatus.Flags);

                SendVDiskResponse(ctx, Event->Sender, vMultiPutResult.release(), Event->Cookie, VCtx, Event->Get()->Record.GetHandleClass());
                PassAway();
            }

            void Handle(TEvVMultiPutItemResult::TPtr &ev, const TActorContext &ctx) {
                TLogoBlobID blobId = ev->Get()->BlobId;
                ui64 idx = ev->Get()->ItemIdx;
                Y_ABORT_UNLESS(idx < Items.size(), "itemIdx# %" PRIu64 " ItemsSize# %" PRIu64, idx, (ui64)Items.size());
                TItem &item = Items[idx];
                Y_ABORT_UNLESS(blobId == item.BlobId, "itemIdx# %" PRIu64 " blobId# %s item# %s", idx, blobId.ToString().data(), item.ToString().data());

                Y_ABORT_UNLESS(!item.Received, "itemIdx# %" PRIu64 " item# %s", idx, item.ToString().data());
                item.Received = true;
                item.Status = ev->Get()->Status;
                item.ErrorReason = ev->Get()->ErrorReason;
                item.WrittenBeyondBarrier = ev->Get()->WrittenBeyondBarrier;

                ReceivedResults++;

                if (ReceivedResults == Items.size()) {
                    SendResponseAndDie(ctx);
                }
            }

            void Bootstrap() {
                NKikimrBlobStorage::TEvVMultiPut &record = Event->Get()->Record;

                for (ui64 idx = 0; idx < record.ItemsSize(); ++idx) {
                    auto &recItem = record.GetItems(idx);
                    TItem &item = Items[idx];

                    TLogoBlobID blobId = LogoBlobIDFromLogoBlobID(recItem.GetBlobID());
                    item.BlobId = blobId;

                    item.HasCookie = recItem.HasCookie();
                    if (item.HasCookie) {
                        item.Cookie = recItem.GetCookie();
                    }

                    if (item.Status != NKikimrProto::OK) {
                        item.Received = true;
                        ReceivedResults++;
                    }
                }

                Become(&TThis::StateWait);
            }

            void PassAway() override {
                TActivationContext::Send(new IEventHandle(TEvents::TSystem::ActorDied, 0, LeaderId, SelfId(), nullptr, 0));
                TActorBootstrapped::PassAway();
            }

            STRICT_STFUNC(StateWait,
                HFunc(TEvVMultiPutItemResult, Handle);
                cFunc(TEvents::TSystem::Poison, PassAway);
            )
        };

    } // NPrivate

    IActor* CreateSkeletonVMultiPutActor(TActorId leaderId, const TBatchedVec<NKikimrProto::EReplyStatus> &statuses,
            TOutOfSpaceStatus oosStatus, TEvBlobStorage::TEvVMultiPut::TPtr &ev,
            TActorIDPtr skeletonFrontIDPtr, ::NMonitoring::TDynamicCounters::TCounterPtr counterPtr,
            ui64 incarnationGuid, TIntrusivePtr<TVDiskContext>& vCtx) {
        return new NPrivate::TBufferVMultiPutActor(leaderId, statuses, oosStatus, ev,
                skeletonFrontIDPtr, counterPtr, incarnationGuid, vCtx);
    }

} // NKikimr
