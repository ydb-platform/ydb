#pragma once

#include <ydb/core/blobstorage/vdisk/common/vdisk_response.h>

namespace NKikimr {

    class TDonorQueryActor : public TActorBootstrapped<TDonorQueryActor> {
        std::unique_ptr<TEvBlobStorage::TEvVGet> Query;
        const TActorId Sender;
        const ui64 Cookie;
        std::unique_ptr<TEvBlobStorage::TEvVGetResult> Result;
        TActorId ParentId;
        std::deque<std::pair<TVDiskID, TActorId>> Donors;

    public:
        TDonorQueryActor(TEvBlobStorage::TEvEnrichNotYet& msg, std::deque<std::pair<TVDiskID, TActorId>> donors)
            : Query(msg.Query->Release().Release())
            , Sender(msg.Query->Sender)
            , Cookie(msg.Query->Cookie)
            , Result(std::move(msg.Result))
            , Donors(std::move(donors))
        {
            Y_VERIFY(!Query->Record.HasRangeQuery());
        }

        void Bootstrap(const TActorId& parentId) {
            ParentId = parentId;
            Become(&TThis::StateFunc);
            LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::BS_VDISK_GET, SelfId() << " starting Donor-mode query");
            Step();
        }

        void Step() {
            if (Donors.empty()) {
                return PassAway();
            }

            auto [vdiskId, actorId] = Donors.back();
            Donors.pop_back();

            // we use AsyncRead priority as we are going to use the replication queue for the VDisk; also this doesn't
            // matter too much as this is the only point of access to that disk
            const auto& record = Query->Record;
            const auto fun = record.GetIndexOnly()
                ? &TEvBlobStorage::TEvVGet::CreateExtremeIndexQuery
                : &TEvBlobStorage::TEvVGet::CreateExtremeDataQuery;
            const auto flags = record.GetShowInternals()
                ? TEvBlobStorage::TEvVGet::EFlags::ShowInternals
                : TEvBlobStorage::TEvVGet::EFlags::None;
            auto query = fun(vdiskId, TInstant::Max(), NKikimrBlobStorage::EGetHandleClass::AsyncRead, flags, {}, {}, 0);

            bool action = false;

            const auto& result = Result->Record;
            for (ui64 i = 0; i < result.ResultSize(); ++i) {
                const auto& r = result.GetResult(i);
                if (r.GetStatus() == NKikimrProto::NOT_YET) {
                    query->AddExtremeQuery(LogoBlobIDFromLogoBlobID(r.GetBlobID()), r.GetShift(), r.GetSize(), &i);
                    action = true;
                }
            }

            if (action) {
                const TActorId temp(actorId);
                LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::BS_VDISK_GET, SelfId() << " sending " << query->ToString()
                    << " to " << temp);
                Send(actorId, query.release());
            } else {
                PassAway();
            }
        }

        void Handle(TEvBlobStorage::TEvVGetResult::TPtr ev) {
            LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::BS_VDISK_GET, SelfId() << " received " << ev->Get()->ToString());
            auto& result = Result->Record;
            for (const auto& item : ev->Get()->Record.GetResult()) {
                auto *res = result.MutableResult(item.GetCookie());
                if (item.GetStatus() == NKikimrProto::OK || (item.GetStatus() == NKikimrProto::ERROR && res->GetStatus() == NKikimrProto::NOT_YET)) {
                    std::optional<ui64> cookie = res->HasCookie() ? std::make_optional(res->GetCookie()) : std::nullopt;
                    res->CopyFrom(item);
                    if (cookie) { // retain original cookie
                        res->SetCookie(*cookie);
                    } else {
                        res->ClearCookie();
                    }
                }
            }
            Step();
        }

        void PassAway() override {
            LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::BS_VDISK_GET, SelfId() << " finished query");
            Send(ParentId, new TEvents::TEvActorDied);
            SendVDiskResponse(TActivationContext::AsActorContext(), Sender, Result.release(), Cookie);
            TActorBootstrapped::PassAway();
        }

        STRICT_STFUNC(StateFunc,
            hFunc(TEvBlobStorage::TEvVGetResult, Handle);
            cFunc(TEvents::TSystem::Poison, PassAway);
        )
    };

} // NKikimr
