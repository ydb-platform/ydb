#include "bridge.h"

#include <ydb/core/blobstorage/dsproxy/dsproxy.h>
#include <ydb/core/util/stlog.h>

namespace NKikimr {

    template<typename T>
    concept HasStatusFlags = requires(T value) {
        value.StatusFlags;
    };

    template<typename T>
    concept HasApproximateFreeSpaceShare = requires(T value) {
        value.ApproximateFreeSpaceShare;
    };

    template<typename T>
    concept HasGroupId = requires(T value) {
        value.GroupId;
    };

    class TBridgedBlobStorageProxyActor : public TActorBootstrapped<TBridgedBlobStorageProxyActor> {
        TIntrusivePtr<TBlobStorageGroupInfo> Info;
        TGroupId GroupId;

        using TClusterState = NKikimrBridge::TClusterState;

        struct TRequest {
            ui64 RequestId = RandomNumber<ui64>();
            TActorId Sender;
            ui64 Cookie;
            ui32 ResponsesPending = 0;
            TStorageStatusFlags StatusFlags;
            float ApproximateFreeSpaceShare = 0;
            std::unique_ptr<IEventBase> CombinedResponse;
            bool Finished = false;

            TLogoBlobID From;
            TLogoBlobID To;

            size_t NumResponses;
            TArrayHolder<TEvBlobStorage::TEvGetResult::TResponse> Responses;
            bool IsIndexOnly;
            bool MustRestoreFirst;

            template<typename TEvRequest>
            TRequest(TActorId sender, ui64 cookie, const TEvRequest& request)
                : Sender(sender)
                , Cookie(cookie)
            {
                if constexpr (std::is_same_v<TEvRequest, TEvBlobStorage::TEvGet>) {
                    Y_ABORT_UNLESS(!request.PhantomCheck);
                    Y_ABORT_UNLESS(!request.IsInternal);
                    NumResponses = request.QuerySize;
                    Responses.Reset(new TEvBlobStorage::TEvGetResult::TResponse[NumResponses]);
                    IsIndexOnly = request.IsIndexOnly;
                    MustRestoreFirst = request.MustRestoreFirst;
                } else if constexpr (std::is_same_v<TEvRequest, TEvBlobStorage::TEvRange>) {
                    From = request.From;
                    To = request.To;
                }
            }

            template<typename TEvent, typename... TArgs>
            std::unique_ptr<IEventBase> CreateWithErrorReason(TEvent *origin, TArgs&&... args) {
                auto event = std::make_unique<TEvent>(std::forward<TArgs>(args)...);
                event->ErrorReason = std::move(origin->ErrorReason);
                return event;
            }

            std::unique_ptr<IEventBase> MakeErrorFrom(TThis& self, TEvBlobStorage::TEvGetResult *origin) {
                return CreateWithErrorReason(origin, origin->Status, NumResponses, std::move(origin->Responses), self.GroupId);
            }

            std::unique_ptr<IEventBase> MakeErrorFrom(TThis& /*self*/, TEvBlobStorage::TEvGetBlockResult *origin) {
                return CreateWithErrorReason(origin, origin->Status, 0, 0);
            }

            std::unique_ptr<IEventBase> MakeErrorFrom(TThis& /*self*/, TEvBlobStorage::TEvDiscoverResult *origin) {
                return CreateWithErrorReason(origin, origin->Status, 0, 0);
            }

            std::unique_ptr<IEventBase> MakeErrorFrom(TThis& self, TEvBlobStorage::TEvRangeResult *origin) {
                return CreateWithErrorReason(origin, origin->Status, From, To, self.GroupId);
            }

            std::unique_ptr<IEventBase> Combine(TThis& self, TEvBlobStorage::TEvPutResult *ev, auto *current) {
                Y_DEBUG_ABORT_UNLESS(!current || current->Id == ev->Id);
                return CreateWithErrorReason(ev, ev->Status, ev->Id, StatusFlags, self.GroupId, ApproximateFreeSpaceShare);
            }

            std::unique_ptr<IEventBase> Combine(TThis& /*self*/, TEvBlobStorage::TEvBlockResult *ev, auto* /*current*/) {
                return CreateWithErrorReason(ev, ev->Status);
            }

            std::unique_ptr<IEventBase> Combine(TThis& /*self*/, TEvBlobStorage::TEvCollectGarbageResult *ev, auto *current) {
                Y_DEBUG_ABORT_UNLESS(!current || (current->TabletId == ev->TabletId
                    && current->RecordGeneration == ev->RecordGeneration
                    && current->PerGenerationCounter == ev->PerGenerationCounter
                    && current->Channel == ev->Channel));
                return CreateWithErrorReason(ev, ev->Status, ev->TabletId, ev->RecordGeneration,
                    ev->PerGenerationCounter, ev->Channel);
            }

            std::unique_ptr<IEventBase> Combine(TThis& /*self*/, TEvBlobStorage::TEvStatusResult *ev, auto* /*current*/) {
                return CreateWithErrorReason(ev, ev->Status, StatusFlags, ApproximateFreeSpaceShare);
            }

            std::unique_ptr<IEventBase> Combine(TThis& self, TEvBlobStorage::TEvPatchResult *ev, auto *current) {
                Y_DEBUG_ABORT_UNLESS(!current || current->Id == ev->Id);
                return CreateWithErrorReason(ev, ev->Status, ev->Id, StatusFlags, self.GroupId, ApproximateFreeSpaceShare);
            }

            template<typename TEvent>
            std::unique_ptr<IEventBase> ProcessFullQuorumResponse(TThis& self, std::unique_ptr<TEvent> ev) {
                // combine responses
                if (CombinedResponse) {
                    Y_DEBUG_ABORT_UNLESS(dynamic_cast<TEvent*>(CombinedResponse.get()));
                }
                CombinedResponse = Combine(self, ev.get(), static_cast<TEvent*>(CombinedResponse.get()));
                const bool readyToReply = !ResponsesPending ||
                    (ev->Status != NKikimrProto::OK && ev->Status != NKikimrProto::NODATA);
                return readyToReply
                    ? std::exchange(CombinedResponse, nullptr)
                    : nullptr;
            }

            template<typename TEvent>
            std::unique_ptr<IEventBase> ProcessPrimaryPileResponse(TThis& self, std::unique_ptr<TEvent> ev,
                    const TBridgeInfo::TPile& pile) {
                if (CombinedResponse) {
                    Y_DEBUG_ABORT_UNLESS(dynamic_cast<TEvent*>(CombinedResponse.get()));
                }
                if (ev->Status != NKikimrProto::OK && ev->Status != NKikimrProto::NODATA) {
                    // if any pile reports error, we finish with error
                    return MakeErrorFrom(self, ev.get());
                }
                if (pile.IsPrimary) {
                    return std::move(ev);
                }
                Y_ABORT_UNLESS(ResponsesPending);
                return nullptr;
            }

            std::unique_ptr<IEventBase> ProcessResponse(TThis& self, std::unique_ptr<TEvBlobStorage::TEvPutResult> ev,
                    const TBridgeInfo::TPile& /*pile*/) {
                return ProcessFullQuorumResponse(self, std::move(ev));
            }

            std::unique_ptr<IEventBase> ProcessResponse(TThis& self, std::unique_ptr<TEvBlobStorage::TEvGetResult> ev,
                    const TBridgeInfo::TPile& pile) {
                return ProcessPrimaryPileResponse(self, std::move(ev), pile);
            }

            std::unique_ptr<IEventBase> ProcessResponse(TThis& self, std::unique_ptr<TEvBlobStorage::TEvBlockResult> ev,
                    const TBridgeInfo::TPile& /*pile*/) {
                return ProcessFullQuorumResponse(self, std::move(ev));
            }

            std::unique_ptr<IEventBase> ProcessResponse(TThis& self, std::unique_ptr<TEvBlobStorage::TEvGetBlockResult> ev,
                    const TBridgeInfo::TPile& pile) {
                return ProcessPrimaryPileResponse(self, std::move(ev), pile);
            }

            std::unique_ptr<IEventBase> ProcessResponse(TThis& self, std::unique_ptr<TEvBlobStorage::TEvDiscoverResult> ev,
                    const TBridgeInfo::TPile& pile) {
                return ProcessPrimaryPileResponse(self, std::move(ev), pile);
            }

            std::unique_ptr<IEventBase> ProcessResponse(TThis& self, std::unique_ptr<TEvBlobStorage::TEvRangeResult> ev,
                    const TBridgeInfo::TPile& pile) {
                return ProcessPrimaryPileResponse(self, std::move(ev), pile);
            }

            std::unique_ptr<IEventBase> ProcessResponse(TThis& self,
                    std::unique_ptr<TEvBlobStorage::TEvCollectGarbageResult> ev, const TBridgeInfo::TPile& /*pile*/) {
                return ProcessFullQuorumResponse(self, std::move(ev));
            }

            std::unique_ptr<IEventBase> ProcessResponse(TThis& self, std::unique_ptr<TEvBlobStorage::TEvStatusResult> ev,
                    const TBridgeInfo::TPile& /*pile*/) {
                return ProcessFullQuorumResponse(self, std::move(ev));
            }

            std::unique_ptr<IEventBase> ProcessResponse(TThis& self, std::unique_ptr<TEvBlobStorage::TEvPatchResult> ev,
                    const TBridgeInfo::TPile& /*pile*/) {
                return ProcessFullQuorumResponse(self, std::move(ev));
            }

            std::unique_ptr<IEventBase> ProcessResponse(TThis& self, std::unique_ptr<TEvBlobStorage::TEvAssimilateResult> ev,
                    const TBridgeInfo::TPile& /*pile*/) {
                (void)self, (void)ev;
                return nullptr;
            }

            std::unique_ptr<IEventBase> ProcessResponse(TThis& self, std::unique_ptr<TEvBlobStorage::TEvCheckIntegrityResult> ev,
                    const TBridgeInfo::TPile& /*pile*/) {
                (void)self, (void)ev;
                return nullptr;
            }

            TString MakeRequestId() const {
                return Sprintf("%016" PRIx64, RequestId);
            }
        };
        struct TRequestInFlight {
            std::shared_ptr<TRequest> Request;
            TBridgeInfo::TPtr BridgeInfo;
            const TBridgeInfo::TPile *Pile;
            TGroupId GroupId;
        };
        THashMap<ui64, TRequestInFlight> RequestsInFlight;
        ui64 LastRequestCookie = 0;

        std::shared_ptr<const NKikimrBlobStorage::TStorageConfig> StorageConfig;
        TBridgeInfo::TPtr BridgeInfo;

        std::deque<std::unique_ptr<IEventHandle>> PendingQ;

    public:
        TBridgedBlobStorageProxyActor(TIntrusivePtr<TBlobStorageGroupInfo> info)
            : Info(std::move(info))
            , GroupId(Info->GroupID)
        {}

        void Bootstrap() {
            Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()), new TEvNodeWardenQueryStorageConfig(/*subscribe=*/ true));
            Become(&TThis::StateWaitBridgeInfo);
        }

        void PassAway() override {
            TActivationContext::Send(new IEventHandle(TEvents::TSystem::Unsubscribe, 0,
                MakeBlobStorageNodeWardenID(SelfId().NodeId()), SelfId(), nullptr, 0));
            TActorBootstrapped::PassAway();
        }

        void Handle(TEvNodeWardenStorageConfig::TPtr ev) {
            StorageConfig = std::move(ev->Get()->Config);
            BridgeInfo = std::move(ev->Get()->BridgeInfo);
        }

        template<typename TEvent>
        void HandleProxyRequest(TAutoPtr<TEventHandle<TEvent>>& ev) {
            Y_ABORT_UNLESS(BridgeInfo);

            auto request = std::make_shared<TRequest>(ev->Sender, ev->Cookie, *ev->Get());

            STLOG(PRI_DEBUG, BS_PROXY_BRIDGE, BPB00, "new request", (RequestId, request->MakeRequestId()),
                (GroupId, GroupId), (Request, ev->Get()->ToString()));

            const auto& bridgeGroupIds = Info->GetBridgeGroupIds();
            for (size_t i = 0; i < bridgeGroupIds.size(); ++i) {
                const auto bridgePileId = TBridgePileId::FromPileIndex(i);
                const TBridgeInfo::TPile *pile = BridgeInfo->GetPile(bridgePileId);
                if (!NBridge::PileStateTraits(pile->State).AllowsConnection) {
                    continue; // ignore this pile, it is disconnected
                }

                // allocate cookie for this specific request and bind it to the common one
                const ui64 cookie = ++LastRequestCookie;
                const auto [it, inserted] = RequestsInFlight.try_emplace(cookie, request, BridgeInfo, pile,
                    bridgeGroupIds[i]);
                Y_DEBUG_ABORT_UNLESS(inserted);

                // generate event and send it to corresponding proxy
                std::unique_ptr<IEventBase> eventToSend(
                    i + 1 != bridgeGroupIds.size()
                        ? new TEvent(TEvBlobStorage::CloneEventPolicy, *ev->Get())
                        : ev->ReleaseBase().Release()
                );
                SendToBSProxy(SelfId(), bridgeGroupIds[i], eventToSend.release(), cookie);
                ++request->ResponsesPending;
            }

            Y_ABORT_UNLESS(request->ResponsesPending);
        }

        template<typename TEvent>
        void HandleProxyResult(TAutoPtr<TEventHandle<TEvent>>& ev) {
            const auto it = RequestsInFlight.find(ev->Cookie);
            if (it == RequestsInFlight.end()) {
                return; // request has already been completed
            }
            auto& item = it->second;
            auto& pile = *item.Pile;
            auto& request = item.Request;

            const bool isError = ev->Get()->Status != NKikimrProto::OK
                && ev->Get()->Status != NKikimrProto::NODATA
                && NBridge::PileStateTraits(item.Pile->State).RequiresDataQuorum;

            STLOG(isError ? PRI_DEBUG : PRI_NOTICE, BS_PROXY_BRIDGE, BPB02, "intermediate response",
                (RequestId, request->MakeRequestId()),
                (GroupId, item.GroupId),
                (Status, ev->Get()->Status),
                (PileState, pile.State),
                (Response, ev->Get()->ToString()));

            Y_ABORT_UNLESS(request->ResponsesPending);
            --request->ResponsesPending;

            if (!request->Finished) {
                auto *msg = ev->Get();

                if constexpr (HasStatusFlags<TEvent>) {
                    request->StatusFlags.Merge(msg->StatusFlags.Raw);
                }

                if constexpr (HasApproximateFreeSpaceShare<TEvent>) {
                    if (msg->ApproximateFreeSpaceShare) {
                        request->ApproximateFreeSpaceShare = request->ApproximateFreeSpaceShare
                            ? Min(request->ApproximateFreeSpaceShare, msg->ApproximateFreeSpaceShare)
                            : msg->ApproximateFreeSpaceShare;
                    }
                }

                std::unique_ptr<TEvent> ptr(ev->Release().Release());
                if (auto response = request->ProcessResponse(*this, std::move(ptr), pile)) {
                    STLOG(PRI_DEBUG, BS_PROXY_BRIDGE, BPB01, "request finished", (RequestId, request->MakeRequestId()),
                        (Response, response->ToString()));
                    Send(request->Sender, response.release(), 0, request->Cookie);
                    request->Finished = true;
                }
            }

            Y_ABORT_UNLESS(request->ResponsesPending || request->Finished);

            RequestsInFlight.erase(it);
        }

#define HANDLE_REQUEST(NAME) hFunc(NAME, HandleProxyRequest)
#define HANDLE_RESULT(NAME) hFunc(NAME##Result, HandleProxyResult)

        STRICT_STFUNC(StateFunc,
            DSPROXY_ENUM_EVENTS(HANDLE_REQUEST)
            DSPROXY_ENUM_EVENTS(HANDLE_RESULT)

            hFunc(TEvNodeWardenStorageConfig, Handle)
            cFunc(TEvents::TSystem::Poison, PassAway)
        )

#undef HANDLE_RESULT
#undef HANDLE_REQUEST

        STATEFN(StateWaitBridgeInfo) {
            if (ev->GetTypeRewrite() == TEvNodeWardenStorageConfig::EventType) {
                StateFunc(ev);
                Become(&TThis::StateFunc);
                for (auto& ev : std::exchange(PendingQ, {})) {
                    TActivationContext::Send(ev.release());
                }
            } else if (ev->GetTypeRewrite() == TEvents::TSystem::Poison) {
                // TODO(alexvru): make PendingQ events undelivered somehow
                StateFunc(ev);
            } else {
                PendingQ.emplace_back(ev.Release());
            }
        }
    };

    IActor *CreateBridgeProxyActor(TIntrusivePtr<TBlobStorageGroupInfo> info) {
        return new TBridgedBlobStorageProxyActor(std::move(info));
    }

} // NKikimr
