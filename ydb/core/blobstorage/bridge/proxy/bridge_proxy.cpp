#include "bridge_proxy.h"

#include <ydb/core/blobstorage/dsproxy/dsproxy.h>
#include <ydb/core/util/stlog.h>

#include <ydb/library/actors/async/wait_for_event.h>

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

        struct TRequest
            : std::enable_shared_from_this<TRequest>
        {
            ui64 RequestId = RandomNumber<ui64>();
            TActorId Sender;
            ui64 Cookie;
            TIntrusivePtr<TBlobStorageGroupInfo> Info;
            ui32 ResponsesPending = 0;
            TStorageStatusFlags StatusFlags;
            float ApproximateFreeSpaceShare = 0;
            std::unique_ptr<IEventBase> CombinedResponse;
            bool Finished = false;

            struct TDiscoverState {
                TLogoBlobID Id;
                TString Buffer;
                ui32 MinGeneration = 0;
                ui32 BlockedGeneration = 0;
                TBridgePileId Winner;
                TDynBitMap Processed;
                TDynBitMap WriteTo;
            };

            struct TGetState {
                NKikimrBlobStorage::EGetHandleClass GetHandleClass;
                size_t NumResponses;
                TArrayHolder<TEvBlobStorage::TEvGetResult::TResponse> Responses;
                bool IsIndexOnly;
                bool MustRestoreFirst;
                ui32 BlockedGeneration = 0;

                struct TRestoreItem {
                    TBridgePileId ReadFrom;
                    TDynBitMap WriteTo;
                };
                std::vector<TRestoreItem> RestoreQueue;
                size_t RestoreIndex;
                bool IsRestoring = false;
            };

            struct TRangeState {
                TLogoBlobID From;
                TLogoBlobID To;
            };

            struct TPutState {
            };

            using TState = std::variant<std::monostate,
                TDiscoverState,
                TGetState,
                TRangeState,
                TPutState>;

            TState State;

            template<typename TEvRequest>
            TRequest(TActorId sender, ui64 cookie, const TEvRequest& request, TIntrusivePtr<TBlobStorageGroupInfo> info)
                : Sender(sender)
                , Cookie(cookie)
                , Info(std::move(info))
            {
                Y_ABORT_UNLESS(Info);
                Y_ABORT_UNLESS(Info->Group);
                Y_ABORT_UNLESS(Info->Group->HasBridgeGroupState());
                if constexpr (std::is_same_v<TEvRequest, TEvBlobStorage::TEvGet>) {
                    Y_ABORT_UNLESS(!request.PhantomCheck);
                    State = TGetState{
                        .GetHandleClass = request.GetHandleClass,
                        .NumResponses = request.QuerySize,
                        .IsIndexOnly = request.IsIndexOnly,
                        .MustRestoreFirst = request.MustRestoreFirst,
                        .RestoreQueue{request.QuerySize},
                        .RestoreIndex = request.QuerySize,
                    };
                } else if constexpr (std::is_same_v<TEvRequest, TEvBlobStorage::TEvRange>) {
                    State = TRangeState{
                        .From = request.From,
                        .To = request.To,
                    };
                } else if constexpr (std::is_same_v<TEvRequest, TEvBlobStorage::TEvDiscover>) {
                    State = TDiscoverState{
                        .MinGeneration = request.MinGeneration,
                    };
                } else if constexpr (std::is_same_v<TEvRequest, TEvBlobStorage::TEvPut>) {
                    State = TPutState{
                    };
                }
            }

            template<typename TEvent, typename... TArgs>
            std::unique_ptr<IEventBase> CreateWithErrorReason(TEvent *origin, TArgs&&... args) {
                auto event = std::make_unique<TEvent>(std::forward<TArgs>(args)...);
                event->ErrorReason = std::move(origin->ErrorReason);
                return event;
            }

            std::unique_ptr<IEventBase> MakeErrorFrom(TThis& self, TEvBlobStorage::TEvGetResult *origin) {
                return CreateWithErrorReason(origin, origin->Status, origin->ResponseSz, std::move(origin->Responses),
                    self.GroupId);
            }

            std::unique_ptr<IEventBase> MakeErrorFrom(TThis& /*self*/, TEvBlobStorage::TEvGetBlockResult *origin) {
                return CreateWithErrorReason(origin, origin->Status, 0, 0);
            }

            std::unique_ptr<IEventBase> MakeErrorFrom(TThis& /*self*/, TEvBlobStorage::TEvDiscoverResult *origin) {
                return CreateWithErrorReason(origin, origin->Status, 0, 0);
            }

            std::unique_ptr<IEventBase> MakeErrorFrom(TThis& self, TEvBlobStorage::TEvRangeResult *origin) {
                return CreateWithErrorReason(origin, origin->Status, origin->From, origin->To, self.GroupId);
            }

            std::unique_ptr<IEventBase> Combine(TThis& self, TEvBlobStorage::TEvPutResult *ev, auto *current) {
                Y_DEBUG_ABORT_UNLESS(!current || current->Id == ev->Id);
                return CreateWithErrorReason(ev, ev->Status, ev->Id, StatusFlags, self.GroupId, ApproximateFreeSpaceShare);
            }

            std::unique_ptr<IEventBase> Combine(TThis& /*self*/, TEvBlobStorage::TEvBlockResult *ev, auto* /*current*/) {
                return CreateWithErrorReason(ev, ev->Status);
            }

            std::unique_ptr<IEventBase> Combine(TThis& /*self*/, TEvBlobStorage::TEvCollectGarbageResult *ev, auto *current) {
                Y_VERIFY_DEBUG_S(!current || (current->TabletId == ev->TabletId
                    && current->RecordGeneration == ev->RecordGeneration
                    && current->PerGenerationCounter == ev->PerGenerationCounter
                    && current->Channel == ev->Channel), "ev# " << ev->ToString() << " current# " << current->ToString());
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
                    const TBridgeInfo::TPile& pile) {
                return std::visit(TOverloaded{
                    [&](TGetState& state) -> std::unique_ptr<IEventBase> {
                        if (state.IsRestoring) {
                            if (ev->Status != NKikimrProto::OK) { // can't restore this blob
                                state.Responses[state.RestoreIndex] = {};
                                state.Responses[state.RestoreIndex].Status = NKikimrProto::ERROR;
                            }
                            ++state.RestoreIndex;
                            return IssueRestoreGet(self, state);
                        }
                        Y_ABORT();
                    },
                    [&](TDiscoverState& state) -> std::unique_ptr<IEventBase> {
                        if (ev->Status != NKikimrProto::OK) {
                            auto res = std::make_unique<TEvBlobStorage::TEvDiscoverResult>(ev->Status, state.MinGeneration,
                                state.BlockedGeneration);
                            res->ErrorReason = TStringBuilder() << "failed to put blob for bridged discover to PileId# "
                                << pile.BridgePileId << ": " << ev->ErrorReason;
                            return res;
                        }
                        return ResponsesPending
                            ? nullptr
                            : std::make_unique<TEvBlobStorage::TEvDiscoverResult>(state.Id, state.MinGeneration,
                                    state.Buffer, state.BlockedGeneration);
                    },
                    [&](TPutState&) -> std::unique_ptr<IEventBase> {
                        return ProcessFullQuorumResponse(self, std::move(ev));
                    },
                    [&](auto&) -> std::unique_ptr<IEventBase> {
                        Y_ABORT();
                    }
                }, State);
            }

            std::unique_ptr<IEventBase> ProcessResponse(TThis& self, std::unique_ptr<TEvBlobStorage::TEvGetResult> ev,
                    const TBridgeInfo::TPile& pile) {
                return std::visit(TOverloaded{
                    [&](TGetState& state) -> std::unique_ptr<IEventBase> {
                        if (ev->Status != NKikimrProto::OK) {
                            return MakeErrorFrom(self, ev.get());
                        }

                        if (state.IsRestoring) {
                            Y_ABORT_UNLESS(ev->ResponseSz == 1);
                            auto& response = ev->Responses[0];
                            if (response.Status == NKikimrProto::OK) {
                                Y_ABORT_UNLESS(response.Id == state.Responses[state.RestoreIndex].Id);
                                Y_ABORT_UNLESS(response.Buffer.size() == response.Id.BlobSize());
                                IssueRestorePut(self, state, TRcBuf(response.Buffer));
                                return nullptr;
                            } else {
                                state.Responses[state.RestoreIndex] = {};
                                state.Responses[state.RestoreIndex].Status = NKikimrProto::ERROR;
                                return IssueRestoreGet(self, state);
                            }
                        }

                        // ensure we got right number of responses
                        Y_ABORT_UNLESS(ev->ResponseSz == state.NumResponses);

                        if (!state.Responses) {
                            // this is the first response, we just repeat what we got in response
                            state.Responses.Reset(new TEvBlobStorage::TEvGetResult::TResponse[state.NumResponses]);
                            std::move(&ev->Responses[0], &ev->Responses[state.NumResponses], &state.Responses[0]);
                        } else {
                            // this is additional response, merge data with already received responses
                            for (size_t i = 0; i < state.NumResponses; ++i) {
                                auto& existing = state.Responses[i];
                                auto& current = ev->Responses[i];
                                Y_ABORT_UNLESS(existing.Id == current.Id);

                                if (current.Status == NKikimrProto::ERROR) {
                                    existing = current;
                                } else if (existing.Status == NKikimrProto::ERROR) {
                                    // we do not process blobs with ERROR, report them as failed ones
                                } else if (existing.Status == current.Status) {
                                    // status did not change, so there is nothing to do
                                    Y_DEBUG_ABORT_UNLESS(existing.Buffer == current.Buffer);
                                } else if (existing.Status == NKikimrProto::NODATA && current.Status == NKikimrProto::OK) {
                                    // we have to restore blob to existing groups
                                    existing = current;
                                }

                                existing.Keep |= current.Keep;
                                existing.DoNotKeep |= current.DoNotKeep;
                            }
                        }

                        for (size_t i = 0; i < state.NumResponses; ++i) {
                            auto& current = ev->Responses[i];
                            auto& item = state.RestoreQueue[i];

                            if (current.Status == NKikimrProto::OK) {
                                item.ReadFrom = pile.BridgePileId;
                            } else if (current.Status == NKikimrProto::NODATA) {
                                item.WriteTo.Set(pile.BridgePileId.GetPileIndex());
                                state.RestoreIndex = Min(state.RestoreIndex, i); // we gonna restore this
                            }
                        }

                        state.BlockedGeneration = Max(state.BlockedGeneration, ev->BlockedGeneration);

                        return ResponsesPending
                            ? nullptr
                            : IssueRestoreGet(self, state);
                    },
                    [&](TDiscoverState& state) -> std::unique_ptr<IEventBase> {
                        if (ev->Status == NKikimrProto::OK && ev->ResponseSz == 1 && ev->Responses->Status == NKikimrProto::OK) {
                            IssueRestorePut(self, state, static_cast<TRcBuf>(ev->Responses->Buffer));
                            return nullptr;
                        }
                        auto res = std::make_unique<TEvBlobStorage::TEvDiscoverResult>(NKikimrProto::ERROR,
                            state.MinGeneration, state.BlockedGeneration);
                        res->ErrorReason = TStringBuilder() << "failed to get blob for bridged discover from PileId# "
                            << pile.BridgePileId << " Status# " << NKikimrProto::EReplyStatus_Name(ev->Responses->Status)
                            << " ErrorReason# " << ev->ErrorReason;
                        return res;
                    },
                    [&](auto&) -> std::unique_ptr<IEventBase> {
                        Y_ABORT();
                    }
                }, State);
            }

            std::unique_ptr<IEventBase> IssueRestoreGet(TThis& self, TGetState& state) {
                while (state.RestoreIndex < state.RestoreQueue.size()) {
                    auto& response = state.Responses[state.RestoreIndex];
                    auto& item = state.RestoreQueue[state.RestoreIndex];

                    if (response.Status != NKikimrProto::OK || item.WriteTo.Empty()) {
                        ++state.RestoreIndex;
                        continue;
                    }

                    Y_ABORT_UNLESS(item.ReadFrom);

                    if (response.Buffer && response.Buffer.size() == response.Id.BlobSize()) {
                        IssueRestorePut(self, state, static_cast<TRcBuf>(response.Buffer));
                    } else {
                        self.SendQuery(shared_from_this(), item.ReadFrom, std::make_unique<TEvBlobStorage::TEvGet>(
                            response.Id, 0, 0, TInstant::Max(), state.GetHandleClass));
                        state.IsRestoring = true;
                    }

                    break;
                }

                if (state.RestoreIndex == state.RestoreQueue.size()) {
                    auto res = std::make_unique<TEvBlobStorage::TEvGetResult>(NKikimrProto::OK, state.NumResponses,
                        std::move(state.Responses), self.GroupId);
                    res->BlockedGeneration = state.BlockedGeneration;
                    return res;
                }

                return nullptr;
            }

            void IssueRestorePut(TThis& self, TGetState& state, TRcBuf buffer) {
                auto& item = state.RestoreQueue[state.RestoreIndex];
                auto& response = state.Responses[state.RestoreIndex];

                Y_FOR_EACH_BIT(i, item.WriteTo) {
                    const auto bridgePileId = TBridgePileId::FromPileIndex(i);
                    NKikimrBlobStorage::EPutHandleClass handleClass = NKikimrBlobStorage::TabletLog;
                    if (state.GetHandleClass == NKikimrBlobStorage::AsyncRead ||
                            state.GetHandleClass == NKikimrBlobStorage::LowRead) {
                        handleClass = NKikimrBlobStorage::AsyncBlob;
                    }

                    Y_ABORT_UNLESS(response.Id);
                    Y_ABORT_UNLESS(buffer.size() == response.Id.BlobSize());
                    self.SendQuery(shared_from_this(), bridgePileId, std::make_unique<TEvBlobStorage::TEvPut>(
                        response.Id, TRcBuf(buffer), TInstant::Max(), handleClass, TEvBlobStorage::TEvPut::TacticDefault,
                        response.DoNotKeep < response.Keep, true));
                }

                state.IsRestoring = true;
            }

            void IssueRestorePut(TThis& self, TDiscoverState& state, TRcBuf buffer) {
                Y_FOR_EACH_BIT(i, state.WriteTo) {
                    Y_ABORT_UNLESS(state.Id);
                    Y_ABORT_UNLESS(buffer.size() == state.Id.BlobSize());
                    self.SendQuery(shared_from_this(), TBridgePileId::FromPileIndex(i),
                        std::make_unique<TEvBlobStorage::TEvPut>(state.Id, TRcBuf(buffer), TInstant::Max(),
                        NKikimrBlobStorage::TabletLog, TEvBlobStorage::TEvPut::TacticDefault, false, true));
                }
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
                return std::visit(TOverloaded{
                    [&](TDiscoverState& state) -> std::unique_ptr<IEventBase> {
                        state.BlockedGeneration = Max(state.BlockedGeneration, ev->BlockedGeneration);
                        if (ev->Status != NKikimrProto::OK && ev->Status != NKikimrProto::NODATA) {
                            auto res = std::make_unique<TEvBlobStorage::TEvDiscoverResult>(ev->Status, state.MinGeneration,
                                state.BlockedGeneration);
                            res->ErrorReason = TStringBuilder() << "failed to discover PileId# " << pile.BridgePileId
                                << ": " << ev->ErrorReason;
                            return res;
                        }
                        if (ev->Status != NKikimrProto::NODATA && state.Id < ev->Id) { // this record gives newer blob
                            state.WriteTo |= state.Processed;
                            state.Id = ev->Id;
                            state.Buffer = std::move(ev->Buffer);
                            state.Winner = pile.BridgePileId;
                        } else if (ev->Status == NKikimrProto::NODATA || ev->Id < state.Id) { // this pile has older version than already discovered
                            state.WriteTo.Set(pile.BridgePileId.GetPileIndex());
                        } else { // exactly same blob
                            Y_ABORT_UNLESS(state.Buffer == ev->Buffer);
                        }
                        state.Processed.Set(pile.BridgePileId.GetPileIndex());
                        if (ResponsesPending) {
                            return nullptr;
                        }
                        if (!state.Winner || state.WriteTo.Empty()) { // no winner or piles are in full sync
                            return state.Winner
                                ? std::make_unique<TEvBlobStorage::TEvDiscoverResult>(state.Id, state.MinGeneration,
                                    state.Buffer, state.BlockedGeneration)
                                : std::make_unique<TEvBlobStorage::TEvDiscoverResult>(NKikimrProto::NODATA,
                                    state.MinGeneration, state.BlockedGeneration);
                        } else if (state.Buffer) {
                            Y_ABORT_UNLESS(state.Buffer.size() == state.Id.BlobSize());
                            IssueRestorePut(self, state, TRcBuf(TString(state.Buffer)));
                        } else {
                            self.SendQuery(shared_from_this(), state.Winner, std::make_unique<TEvBlobStorage::TEvGet>(
                                state.Id, 0, 0, TInstant::Max(), NKikimrBlobStorage::FastRead));
                        }
                        return nullptr; // no answer yet
                    },
                    [&](auto&) -> std::unique_ptr<IEventBase> {
                        Y_ABORT();
                    }
                }, State);
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

            auto request = std::make_shared<TRequest>(ev->Sender, ev->Cookie, *ev->Get(), Info);

            std::unique_ptr<TEvent> evPtr(ev->Release().Release());

            STLOG(PRI_DEBUG, BS_PROXY_BRIDGE, BPB00, "new request", (RequestId, request->MakeRequestId()),
                (GroupId, GroupId), (Request, evPtr->ToString()));

            Y_ABORT_UNLESS(Info->Group);
            const auto& state = Info->Group->GetBridgeGroupState();
            for (size_t i = 0; i < state.PileSize(); ++i) {
                const auto bridgePileId = TBridgePileId::FromPileIndex(i);
                const TBridgeInfo::TPile *pile = BridgeInfo->GetPile(bridgePileId);
                if (!NBridge::PileStateTraits(pile->State).RequiresDataQuorum) {
                    continue; // skip this pile, no data quorum needed
                }

                // check the sync state for this group
                const auto& groupPileInfo = state.GetPile(i);
                if (auto eventToSend = PrepareEvent(groupPileInfo, evPtr, i + 1 == state.PileSize())) {
                    SendQuery(request, pile->BridgePileId, std::move(eventToSend));
                }
            }

            Y_ABORT_UNLESS(request->ResponsesPending);
        }

        void SendQuery(std::shared_ptr<TRequest> request, TBridgePileId bridgePileId, std::unique_ptr<IEventBase> ev) {
            const auto& state = Info->Group->GetBridgeGroupState();
            const auto& groupPileInfo = state.GetPile(bridgePileId.GetPileIndex());
            const auto groupId = TGroupId::FromProto(&groupPileInfo, &NKikimrBridge::TGroupState::TPile::GetGroupId);

            auto *common = dynamic_cast<TEvBlobStorage::TEvRequestCommon*>(ev.get());
            Y_ABORT_UNLESS(common);
            common->ForceGroupGeneration = groupPileInfo.GetGroupGeneration();

            // allocate cookie for this specific request and bind it to the common one
            const ui64 cookie = ++LastRequestCookie;
            const auto [it, inserted] = RequestsInFlight.try_emplace(cookie, request, BridgeInfo,
                BridgeInfo->GetPile(bridgePileId), groupId);
            Y_DEBUG_ABORT_UNLESS(inserted);

            // send event
            SendToBSProxy(SelfId(), groupId, ev.release(), cookie);
            ++request->ResponsesPending;
        }

        template<typename TEvent>
        std::unique_ptr<IEventBase> PrepareEvent(const NKikimrBridge::TGroupState::TPile& pile,
                std::unique_ptr<TEvent>& ev, bool last) {
            std::unique_ptr<TEvent> res;

            switch (pile.GetStage()) {
                case NKikimrBridge::TGroupState::WRITE_KEEP:
                    if constexpr (std::is_same_v<TEvent, TEvBlobStorage::TEvCollectGarbage>) {
                        if (!ev->Keep) {
                            return nullptr;
                        }
                        // allow only keep flags to be sent
                        res = std::make_unique<TEvBlobStorage::TEvCollectGarbage>(ev->TabletId, ev->RecordGeneration,
                            ev->PerGenerationCounter, ev->Channel, false, 0, 0, ev->Keep.Release(), nullptr, ev->Deadline,
                            true, false, false);
                    }
                    [[fallthrough]];
                case NKikimrBridge::TGroupState::WRITE_KEEP_BARRIER_DONOTKEEP:
                    if (std::is_same_v<TEvents, TEvBlobStorage::TEvCollectGarbage>) {
                        break; // allow any garbage collection commands at this stage
                    }
                    [[fallthrough]];
                case NKikimrBridge::TGroupState::BLOCKS:
                    if (!std::is_same_v<TEvent, TEvBlobStorage::TEvBlock>) {
                        return nullptr; // allow only TEvBlock messages for this stage
                    }
                    break;

                case NKikimrBridge::TGroupState::WRITE_KEEP_BARRIER_DONOTKEEP_DATA:
                case NKikimrBridge::TGroupState::SYNCED:
                    break;

                case NKikimrBridge::TGroupState_EStage_TGroupState_EStage_INT_MIN_SENTINEL_DO_NOT_USE_:
                case NKikimrBridge::TGroupState_EStage_TGroupState_EStage_INT_MAX_SENTINEL_DO_NOT_USE_:
                    Y_ABORT();
            }

            if (!res) {
                res.reset(last ? ev.release() : new TEvent(TEvBlobStorage::CloneEventPolicy, *ev));
            }
            res->ForceGroupGeneration.emplace(pile.GetGroupGeneration());
            return res;
        }

        template<typename TEvent>
        void HandleProxyResult(TAutoPtr<TEventHandle<TEvent>>& ev) {
            ////////////////////////////////////////////////////////////////////////////////////////////////////////////
            // TODO(alexvru): handle RACE properly!
            ////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

        void Handle(TEvBlobStorage::TEvConfigureProxy::TPtr ev) {
            Info = std::move(ev->Get()->Info);
        }

#define HANDLE_REQUEST(NAME) hFunc(NAME, HandleProxyRequest)
#define HANDLE_RESULT(NAME) hFunc(NAME##Result, HandleProxyResult)

        STRICT_STFUNC(StateFunc,
            DSPROXY_ENUM_EVENTS(HANDLE_REQUEST)
            DSPROXY_ENUM_EVENTS(HANDLE_RESULT)

            hFunc(TEvBlobStorage::TEvConfigureProxy, Handle)
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
