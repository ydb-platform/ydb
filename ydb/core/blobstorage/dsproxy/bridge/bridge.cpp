#include "bridge.h"

#include <ydb/core/blobstorage/dsproxy/dsproxy.h>

namespace NKikimr {

    template<typename T>
    concept HasStatusFlags = requires(T value) {
        value.StatusFlags;
    };

    template<typename T>
    concept HasApproximateFreeSpaceShare = requires(T value) {
        value.ApproximateFreeSpaceShare;
    };

    class TBridgedBlobStorageProxyActor : public TActor<TBridgedBlobStorageProxyActor> {
        TIntrusivePtr<TBlobStorageGroupInfo> Info;

        struct TRequest {
            TActorId Sender;
            ui64 Cookie;
            ui32 ResponsesPending = 0;
            TStorageStatusFlags StatusFlags;
            float ApproximateFreeSpaceShare = 0;

            TRequest(TActorId sender, ui64 cookie)
                : Sender(sender)
                , Cookie(cookie)
            {}
        };
        THashMap<ui64, TRequest> RequestsInFlight;
        ui64 LastRequestCookie = 0;

    public:
        TBridgedBlobStorageProxyActor(TIntrusivePtr<TBlobStorageGroupInfo> info)
            : TActor(&TThis::StateFunc)
            , Info(std::move(info))
        {
        }

        template<typename TEvent>
        void HandleProxyRequest(TAutoPtr<TEventHandle<TEvent>>& ev) {
            const ui64 cookie = ++LastRequestCookie;
            const auto [it, inserted] = RequestsInFlight.try_emplace(cookie, ev->Sender, ev->Cookie);
            Y_ABORT_UNLESS(inserted);

            const auto& bridgeGroupIds = Info->GetBridgeGroupIds();
            for (size_t i = 0; i < bridgeGroupIds.size(); ++i) {
                std::unique_ptr<IEventBase> eventToSend(
                    i + 1 != bridgeGroupIds.size()
                        ? new TEvent(TEvBlobStorage::CloneEventPolicy, *ev->Get())
                        : ev->ReleaseBase().Release()
                );
                SendToBSProxy(SelfId(), bridgeGroupIds[i], eventToSend.release(), cookie); 
                ++it->second.ResponsesPending;
            }
        }

        template<typename TEvent>
        void HandleProxyResult(TAutoPtr<TEventHandle<TEvent>>& ev) {
            const auto it = RequestsInFlight.find(ev->Cookie);
            if (it == RequestsInFlight.end()) {
                return; // request has already been completed
            }
            TRequest& request = it->second;

            Y_ABORT_UNLESS(request.ResponsesPending);
            --request.ResponsesPending;

            auto *msg = ev->Get();

            if constexpr (HasStatusFlags<TEvent>) {
                request.StatusFlags.Merge(msg->StatusFlags.Raw);
            }
            if constexpr (HasApproximateFreeSpaceShare<TEvent>) {
                if (msg->ApproximateFreeSpaceShare) {
                    request.ApproximateFreeSpaceShare = request.ApproximateFreeSpaceShare
                        ? Min(request.ApproximateFreeSpaceShare, msg->ApproximateFreeSpaceShare)
                        : msg->ApproximateFreeSpaceShare;
                }
            }

            if (msg->Status == NKikimrProto::OK) {
            } else if (msg->Status == NKikimrProto::ERROR) {
            }

            RequestsInFlight.erase(it);
        }

#define HANDLE_REQUEST(NAME) hFunc(NAME, HandleProxyRequest)
#define HANDLE_RESULT(NAME) hFunc(NAME##Result, HandleProxyResult)

        STRICT_STFUNC(StateFunc,
            DSPROXY_ENUM_EVENTS(HANDLE_REQUEST)
            DSPROXY_ENUM_EVENTS(HANDLE_RESULT)

            cFunc(TEvents::TSystem::Poison, PassAway)
        )

#undef HANDLE_RESULT
#undef HANDLE_REQUEST
    };

    IActor *CreateBridgeProxyActor(TIntrusivePtr<TBlobStorageGroupInfo> info) {
        return new TBridgedBlobStorageProxyActor(std::move(info));
    }

} // NKikimr
