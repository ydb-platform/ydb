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

    class TBridgedBlobStorageProxyActor : public TActorBootstrapped<TBridgedBlobStorageProxyActor> {
        TIntrusivePtr<TBlobStorageGroupInfo> Info;
        TGroupId GroupId;
        TActorId InnerProxyId; // DS proxy of the inline pile, if this group is an inline pile one

        using TClusterState = NKikimrBridge::TClusterState;

        // is the part of the group in the pile sure to hold everything written to the group
        static bool PartHoldsEverything(const NKikimrBridge::TGroupState& state, const TBridgeInfo::TPile& pile) {
            const auto *part = NBridge::GetGroupPile(state, pile.BridgePileId);
            if (!part || part->GetStage() != NKikimrBridge::TGroupState::SYNCED) {
                return false;
            } else if (const auto authorityPileId = NBridge::GetAuthorityPileId(state)) {
                // nothing is written to the group without this part, so it holds everything even when its pile has
                // been out, while the other parts may lack something
                return pile.BridgePileId == authorityPileId;
            }
            // the part of the pile that has been out (NOT_SYNCHRONIZED_2 one included) may show the SYNCED stage of its
            // previous sync until BSC resets it, so only the synchronized pile's part is sure to hold everything
            return pile.State == TClusterState::SYNCHRONIZED;
        }

        struct TRequestPayload {
            size_t Index;
        };

        struct TRequest
            : std::enable_shared_from_this<TRequest>
        {
            TString RequestId = Sprintf("%016" PRIx64, RandomNumber<ui64>());
            TActorId Sender;
            ui64 Cookie;
            TIntrusivePtr<TBlobStorageGroupInfo> Info;
            TBridgeInfo::TPtr BridgeInfo; // the one the request was admitted with along with Info
            std::unique_ptr<IEventBase> OriginalRequest;
            ui32 ResponsesPending = 0;
            TStorageStatusFlags StatusFlags;
            float ApproximateFreeSpaceShare = 0;
            std::unique_ptr<IEventBase> CombinedResponse;
            bool Finished = false;
            THashSet<ui64> CookiesInFlight;
            THPTimer Timer;
            std::deque<std::tuple<TString, TDuration>> SubrequestTimings;

            NWilson::TSpan Span;

            struct TDiscoverState {
                TLogoBlobID Id;
                TString Buffer;
                ui32 MinGeneration = 0;
                ui32 BlockedGeneration = 0;
                TBridgePileId Winner;
            };

            struct TGetState {
                size_t NumResponses;
                TArrayHolder<TEvBlobStorage::TEvGetResult::TResponse> Responses;
                ui32 BlockedGeneration = 0;
            };

            struct TRangeState {
                std::unique_ptr<TEvBlobStorage::TEvRangeResult> Response;
            };

            struct TPutState {
            };

            using TState = std::variant<std::monostate,
                TDiscoverState,
                TGetState,
                TRangeState,
                TPutState>;

            TState State;

            struct TRestoreItem {
                TLogoBlobID Id;
                TRcBuf Buffer;
                TBridgePileId ReadFrom;
                TDynBitMap WriteTo;
                bool IssueKeepFlag = false;

                TRestoreItem() = default;

                TRestoreItem(TLogoBlobID id, const TString& buffer)
                    : Id(id)
                    , Buffer(buffer)
                {}
            };
            std::vector<TRestoreItem> RestoreQueue;
            THashMap<TLogoBlobID, size_t> RestoreQueueIndex;
            size_t RestoreIndex = Max<size_t>();
            bool IsRestoring = false;
            bool MustRestoreFirst = false;
            // Restoring a blob into a pile which lacks it is a write made on behalf of the original
            // request, so it is admitted the same way that request would be.
            NKikimrBlobStorage::TDataKind::E DataKind = NKikimrBlobStorage::TDataKind::USER;
            NKikimrBlobStorage::EGetHandleClass GetHandleClass = NKikimrBlobStorage::FastRead;

            TDynBitMap Processed; // a set of piles we already got main reply from

            template<typename TEvRequest>
            TRequest(TActorId sender, ui64 cookie, std::unique_ptr<TEvRequest>&& request, TIntrusivePtr<TBlobStorageGroupInfo> info,
                    TThis& self, ui32 requestType, NWilson::TTraceId&& traceId)
                : Sender(sender)
                , Cookie(cookie)
                , Info(std::move(info))
                , Span(TWilson::BlobStorage, std::forward<NWilson::TTraceId>(traceId), "BridgedRequest", NWilson::EFlags::AUTO_END)
            {
                Y_ABORT_UNLESS(Info);
                Y_ABORT_UNLESS(Info->Group);
                Y_ABORT_UNLESS(Info->Group->HasBridgeGroupState());

                DataKind = request->DataKind;

                if constexpr (std::is_same_v<TEvRequest, TEvBlobStorage::TEvGet>) {
                    Y_ABORT_UNLESS(!request->PhantomCheck);

                    State = TGetState{
                        .NumResponses = request->QuerySize,
                    };

                    MustRestoreFirst = request->MustRestoreFirst,
                    GetHandleClass = request->GetHandleClass;
                    RestoreQueue.resize(request->QuerySize);
                } else if constexpr (std::is_same_v<TEvRequest, TEvBlobStorage::TEvRange>) {
                    State = TRangeState{
                        .Response = std::make_unique<TEvBlobStorage::TEvRangeResult>(NKikimrProto::OK, request->From,
                            request->To, self.GroupId),
                    };

                    MustRestoreFirst = request->MustRestoreFirst;
                } else if constexpr (std::is_same_v<TEvRequest, TEvBlobStorage::TEvDiscover>) {
                    State = TDiscoverState{
                        .MinGeneration = request->MinGeneration,
                    };

                    RestoreQueue.resize(1);
                } else if constexpr (std::is_same_v<TEvRequest, TEvBlobStorage::TEvPut>) {
                    State = TPutState{
                    };
                }

                OriginalRequest = std::move(request);

                if (Span) {
                    Span.Attribute("GroupId", Info->GroupID.GetRawId());
                    Span.Attribute("database", AppData()->TenantName);
                    Span.Attribute("storagePool", Info->GetStoragePoolName());
                    Span.Attribute("RequestType", TEvBlobStorage::TEvRequestCommon::GetRequestName(requestType));
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

            std::unique_ptr<IEventBase> Combine(TThis& /*self*/, TEvBlobStorage::TEvBlockResult *ev, auto *current) {
                auto result = std::make_unique<TEvBlobStorage::TEvBlockResult>(ev->Status);
                result->ErrorReason = std::move(ev->ErrorReason);
                result->IsTabletStorageInfoVersionObsolete = ev->IsTabletStorageInfoVersionObsolete
                    || (current && current->IsTabletStorageInfoVersionObsolete);
                result->ActualGeneration = Max(ev->ActualGeneration, current ? current->ActualGeneration : 0);
                return result;
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

            std::unique_ptr<IEventBase> Combine(TThis& /*self*/, TEvBlobStorage::TEvGetBlockResult *ev, auto *current) {
                Y_ABORT_UNLESS(!current || current->TabletId == ev->TabletId);
                return CreateWithErrorReason(ev, ev->Status, ev->TabletId,
                    Max(current ? current->BlockedGeneration : 0, ev->BlockedGeneration));
            }

            template<typename TEvent>
            std::unique_ptr<IEventBase> ProcessFullQuorumResponse(TThis& self, std::unique_ptr<TEvent> ev) {
                // combine responses
                Y_DEBUG_ABORT_UNLESS(!CombinedResponse || dynamic_cast<TEvent*>(CombinedResponse.get()));
                Y_DEBUG_ABORT_UNLESS(ev->Status != NKikimrProto::NODATA);
                const bool readyToReply = !ResponsesPending || ev->Status != NKikimrProto::OK;
                CombinedResponse = Combine(self, ev.get(), static_cast<TEvent*>(CombinedResponse.get()));
                return readyToReply ? std::move(CombinedResponse) : nullptr;
            }

            std::unique_ptr<IEventBase> ProcessResponse(TThis& self, std::unique_ptr<TEvBlobStorage::TEvPutResult> ev,
                    const TBridgeInfo::TPile& /*pile*/, TRequestPayload& payload) {
                if (IsRestoring) {
                    Y_ABORT_UNLESS(!std::holds_alternative<TPutState>(State));

                    if (ev->Status != NKikimrProto::OK) { // can't restore this blob
                        return std::visit(TOverloaded{
                            [&](TGetState& state) -> std::unique_ptr<IEventBase> {
                                auto& r = state.Responses[payload.Index];
                                r = {};
                                r.Status = NKikimrProto::ERROR;
                                return IssueNextRestoreGet(self);
                            },
                            [&](TRangeState&) -> std::unique_ptr<IEventBase> {
                                return static_cast<TEvBlobStorage::TEvRange&>(*OriginalRequest).MakeErrorResponse(
                                    ev->Status, ev->ErrorReason, self.GroupId);
                            },
                            [&](TDiscoverState&) -> std::unique_ptr<IEventBase> {
                                return static_cast<TEvBlobStorage::TEvDiscover&>(*OriginalRequest).MakeErrorResponse(
                                    ev->Status, ev->ErrorReason, self.GroupId);
                            },
                            [&](auto&) -> std::unique_ptr<IEventBase> {
                                Y_ABORT();
                            }
                        }, State);
                    } else {
                        return IssueNextRestoreGet(self);
                    }
                }

                Y_ABORT_UNLESS(std::holds_alternative<TPutState>(State));
                return ProcessFullQuorumResponse(self, std::move(ev));
            }

            bool DataIsTrustedInPile(const TBridgeInfo::TPile& pile) const {
                return PartHoldsEverything(Info->Group->GetBridgeGroupState(), pile);
            }

            std::unique_ptr<IEventBase> ProcessResponse(TThis& self, std::unique_ptr<TEvBlobStorage::TEvGetResult> ev,
                    const TBridgeInfo::TPile& pile, TRequestPayload& payload) {
                if (ev->Status != NKikimrProto::OK) {
                    return std::visit(TOverloaded{
                        [&](TGetState&) -> std::unique_ptr<IEventBase> {
                            return static_cast<TEvBlobStorage::TEvGet&>(*OriginalRequest).MakeErrorResponse(
                                ev->Status, ev->ErrorReason, self.GroupId);
                        },
                        [&](TRangeState&) -> std::unique_ptr<IEventBase> {
                            return static_cast<TEvBlobStorage::TEvRange&>(*OriginalRequest).MakeErrorResponse(
                                ev->Status, ev->ErrorReason, self.GroupId);
                        },
                        [&](TDiscoverState&) -> std::unique_ptr<IEventBase> {
                            return static_cast<TEvBlobStorage::TEvDiscover&>(*OriginalRequest).MakeErrorResponse(
                                ev->Status, ev->ErrorReason, self.GroupId);
                        },
                        [&](auto&) -> std::unique_ptr<IEventBase> {
                            Y_ABORT();
                        }
                    }, State);
                }

                if (IsRestoring) {
                    Y_ABORT_UNLESS(ev->ResponseSz == 1);
                    if (auto& response = ev->Responses[0]; response.Status == NKikimrProto::OK) {
                        Y_ABORT_UNLESS(response.Id == RestoreQueue[payload.Index].Id);
                        Y_ABORT_UNLESS(response.Buffer.size() == response.Id.BlobSize());
                        RestoreQueue[payload.Index].Buffer = static_cast<TRcBuf>(response.Buffer);
                        // the flag may be known from the earlier response about this blob, while this read may come from
                        // VDisks not knowing it yet
                        RestoreQueue[payload.Index].IssueKeepFlag |= response.DoNotKeep < response.Keep;
                        IssueRestorePut(self, payload.Index);
                        return nullptr;
                    } else {
                        return std::visit(TOverloaded{
                            [&](TGetState& state) -> std::unique_ptr<IEventBase> {
                                auto& r = state.Responses[payload.Index];
                                r = {};
                                r.Status = NKikimrProto::ERROR;
                                return IssueNextRestoreGet(self);
                            },
                            [&](TRangeState&) -> std::unique_ptr<IEventBase> {
                                return static_cast<TEvBlobStorage::TEvRange&>(*OriginalRequest).MakeErrorResponse(
                                    NKikimrProto::ERROR, TStringBuilder() << "couldn't restore blob content across bridge"
                                    << " BlobId# " << response.Id
                                    << " Status# " << NKikimrProto::EReplyStatus_Name(response.Status), self.GroupId);
                            },
                            [&](TDiscoverState&) -> std::unique_ptr<IEventBase> {
                                return static_cast<TEvBlobStorage::TEvDiscover&>(*OriginalRequest).MakeErrorResponse(
                                    NKikimrProto::ERROR, TStringBuilder() << "couldn't restore blob content across bridge"
                                    << " BlobId# " << response.Id
                                    << " Status# " << NKikimrProto::EReplyStatus_Name(response.Status), self.GroupId);
                            },
                            [&](auto&) -> std::unique_ptr<IEventBase> {
                                Y_ABORT();
                            }
                        }, State);
                    }
                }

                Y_ABORT_UNLESS(std::holds_alternative<TGetState>(State));
                auto& state = std::get<TGetState>(State);

                // ensure we got right number of responses
                Y_ABORT_UNLESS(ev->ResponseSz == state.NumResponses);

                if (MustRestoreFirst) {
                    // we are going to restore data, so we have to collect answers from all piles
                } else if (DataIsTrustedInPile(pile)) {
                    // this is response from synced pile, we can fully trust it
                    return ev;
                } else if (ResponsesPending) {
                    // we still have some queries in flight, execute them
                    return nullptr;
                } else {
                    // strange situation: we haven't got response from trusted pile?
                    Y_DEBUG_ABORT("missing response from trusted pile");
                    return static_cast<TEvBlobStorage::TEvGet&>(*OriginalRequest).MakeErrorResponse(
                        NKikimrProto::ERROR, "failed to obtain response from trusted pile", self.GroupId);
                }

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
                    auto& item = RestoreQueue[i];

                    if (current.Status == NKikimrProto::OK) {
                        item.Id = current.Id;
                        item.Buffer = TRcBuf(current.Buffer);
                        item.ReadFrom = pile.BridgePileId;
                        item.IssueKeepFlag = current.DoNotKeep < current.Keep;
                    } else if (current.Status == NKikimrProto::NODATA) {
                        item.WriteTo.Set(pile.BridgePileId.GetPileIndex());
                        RestoreIndex = Min(RestoreIndex, i); // we gonna restore this
                    }
                }

                state.BlockedGeneration = Max(state.BlockedGeneration, ev->BlockedGeneration);

                return IssueNextRestoreGet(self);
            }

            std::unique_ptr<IEventBase> IssueNextRestoreGet(TThis& self) {
                if (ResponsesPending) {
                    return nullptr; // wait for all puts to complete
                }

                while (RestoreIndex < RestoreQueue.size()) {
                    auto& item = RestoreQueue[RestoreIndex];

                    if (!item.ReadFrom || item.WriteTo.Empty()) {
                        ++RestoreIndex;
                        continue;
                    }

                    Y_ABORT_UNLESS(item.Id);

                    if (item.Buffer && item.Buffer.size() == item.Id.BlobSize()) {
                        IssueRestorePut(self, RestoreIndex);
                    } else {
                        auto get = std::make_unique<TEvBlobStorage::TEvGet>(item.Id, 0, 0, TInstant::Max(),
                            GetHandleClass);
                        get->DataKind = DataKind;
                        self.SendQuery(shared_from_this(), item.ReadFrom, std::move(get), {.Index = RestoreIndex});
                        IsRestoring = true;
                    }

                    ++RestoreIndex;
                    return nullptr;
                }

                Y_ABORT_UNLESS(RestoreQueue.size() <= RestoreIndex);

                return std::visit(TOverloaded{
                    [&](TGetState& state) -> std::unique_ptr<IEventBase> {
                        auto res = std::make_unique<TEvBlobStorage::TEvGetResult>(NKikimrProto::OK, state.NumResponses,
                            std::move(state.Responses), self.GroupId);
                        res->BlockedGeneration = state.BlockedGeneration;
                        return res;
                    },
                    [&](TDiscoverState& state) -> std::unique_ptr<IEventBase> {
                        Y_ABORT_UNLESS(state.Id);
                        return std::make_unique<TEvBlobStorage::TEvDiscoverResult>(state.Id, state.MinGeneration,
                            state.Buffer, state.BlockedGeneration);
                    },
                    [&](TRangeState& state) -> std::unique_ptr<IEventBase> {
                        return std::move(state.Response);
                    },
                    [&](auto&) -> std::unique_ptr<IEventBase> {
                        Y_ABORT();
                    }
                }, State);
            }

            void IssueRestorePut(TThis& self, size_t index) {
                auto& item = RestoreQueue[index];

                Y_FOR_EACH_BIT(i, item.WriteTo) {
                    const auto bridgePileId = TBridgePileId::FromPileIndex(i);
                    NKikimrBlobStorage::EPutHandleClass handleClass = NKikimrBlobStorage::TabletLog;
                    if (GetHandleClass == NKikimrBlobStorage::AsyncRead || GetHandleClass == NKikimrBlobStorage::LowRead) {
                        handleClass = NKikimrBlobStorage::AsyncBlob;
                    }

                    Y_ABORT_UNLESS(item.Id);
                    Y_ABORT_UNLESS(item.Buffer.size() == item.Id.BlobSize());
                    self.SendQuery(shared_from_this(), bridgePileId, std::make_unique<TEvBlobStorage::TEvPut>(
                        TEvBlobStorage::TEvPut::TParameters{
                            .BlobId = item.Id,
                            .Buffer = TRope(TRcBuf(item.Buffer)),
                            .Deadline = TInstant::Max(),
                            .HandleClass = handleClass,
                            .Tactic = TEvBlobStorage::TEvPut::TacticDefault,
                            .WriteSource = TWriteSource::BridgeProxyRestorePut,
                            .DataKind = DataKind,
                            .IssueKeepFlag = item.IssueKeepFlag,
                            .IgnoreBlock = true,
                        }), {.Index = index});
                }

                IsRestoring = true;
            }

            std::unique_ptr<IEventBase> ProcessResponse(TThis& self, std::unique_ptr<TEvBlobStorage::TEvBlockResult> ev,
                    const TBridgeInfo::TPile& /*pile*/, TRequestPayload& /*payload*/) {
                return ProcessFullQuorumResponse(self, std::move(ev));
            }

            std::unique_ptr<IEventBase> ProcessResponse(TThis& self, std::unique_ptr<TEvBlobStorage::TEvGetBlockResult> ev,
                    const TBridgeInfo::TPile& /*pile*/, TRequestPayload& /*payload*/) {
                return ProcessFullQuorumResponse(self, std::move(ev));
            }

            std::unique_ptr<IEventBase> ProcessResponse(TThis& self, std::unique_ptr<TEvBlobStorage::TEvDiscoverResult> ev,
                    const TBridgeInfo::TPile& pile, TRequestPayload& /*payload*/) {
                Y_ABORT_UNLESS(std::holds_alternative<TDiscoverState>(State));
                auto& state = std::get<TDiscoverState>(State);

                state.BlockedGeneration = Max(state.BlockedGeneration, ev->BlockedGeneration);

                if (ev->Status != NKikimrProto::OK && ev->Status != NKikimrProto::NODATA) {
                    auto res = std::make_unique<TEvBlobStorage::TEvDiscoverResult>(ev->Status, state.MinGeneration,
                        state.BlockedGeneration);
                    res->ErrorReason = TStringBuilder() << "failed to discover PileId# " << pile.BridgePileId
                        << ": " << ev->ErrorReason;
                    return res;
                }

                auto& item = RestoreQueue.front();
                if (ev->Status == NKikimrProto::OK && state.Id < ev->Id) { // this record gives newer blob
                    state.Id = ev->Id;
                    state.Buffer = std::move(ev->Buffer);
                    state.Winner = pile.BridgePileId;

                    item.Id = state.Id;
                    item.Buffer = TRcBuf(state.Buffer);
                    item.ReadFrom = state.Winner;
                    item.WriteTo |= Processed;
                } else if (ev->Status == NKikimrProto::NODATA || ev->Id < state.Id) { // this pile has older version than already discovered
                    item.WriteTo.Set(pile.BridgePileId.GetPileIndex());
                } else { // exactly same blob
                    Y_ABORT_UNLESS(state.Buffer == ev->Buffer);
                }

                Processed.Set(pile.BridgePileId.GetPileIndex());

                if (ResponsesPending) {
                    return nullptr;
                }
                if (!state.Winner) { // no winner or piles are in full sync
                    return std::make_unique<TEvBlobStorage::TEvDiscoverResult>(NKikimrProto::NODATA,
                        state.MinGeneration, state.BlockedGeneration);
                }
                if (!item.WriteTo.Empty()) {
                    RestoreIndex = 0;
                }
                return IssueNextRestoreGet(self);
            }

            std::unique_ptr<IEventBase> ProcessResponse(TThis& self, std::unique_ptr<TEvBlobStorage::TEvRangeResult> ev,
                    const TBridgeInfo::TPile& pile, TRequestPayload& /*payload*/) {
                if (ev->Status != NKikimrProto::OK) {
                    return MakeErrorFrom(self, ev.get());
                }

                Y_ABORT_UNLESS(std::holds_alternative<TRangeState>(State));
                auto& state = std::get<TRangeState>(State);

                if (!MustRestoreFirst && DataIsTrustedInPile(pile)) {
                    return ev;
                }

                auto getRestoreItem = [&](const auto& item) -> TRestoreItem& {
                    const auto [it, inserted] = RestoreQueueIndex.try_emplace(item.Id, RestoreQueue.size());
                    RestoreIndex = Min(RestoreIndex, it->second);
                    if (inserted) {
                        RestoreQueue.emplace_back(item.Id, item.Buffer);
                    }
                    return RestoreQueue[it->second];
                };

                auto merge = [&](auto&& comp) {
                    auto& target = state.Response->Responses;
                    auto targetIt = std::ranges::begin(target);
                    const auto targetEnd = std::ranges::end(target);

                    auto sourceIt = std::ranges::begin(ev->Responses);
                    const auto sourceEnd = std::ranges::end(ev->Responses);

                    TVector<TEvBlobStorage::TEvRangeResult::TResponse> itemsToAdd;

                    while (sourceIt != sourceEnd || targetIt != targetEnd) {
                        if (sourceIt == sourceEnd || (targetIt != targetEnd && comp(targetIt->Id, sourceIt->Id))) {
                            // restore it unless this is the first response
                            auto& item = getRestoreItem(*targetIt);
                            item.WriteTo.Set(pile.BridgePileId.GetPileIndex());
                            ++targetIt;
                        } else if (targetIt == targetEnd || comp(sourceIt->Id, targetIt->Id)) {
                            // item is in new range, no matching item in target range
                            auto& item = getRestoreItem(*sourceIt);
                            item.ReadFrom = pile.BridgePileId;
                            item.WriteTo |= Processed;
                            item.IssueKeepFlag = sourceIt->DoNotKeep < sourceIt->Keep;
                            itemsToAdd.push_back(std::move(*sourceIt));
                            ++sourceIt;
                        } else { // items in both ranges, so both present
                            targetIt->Keep |= sourceIt->Keep;
                            targetIt->DoNotKeep |= sourceIt->DoNotKeep;
                            Y_ABORT_UNLESS(!targetIt->Buffer == !sourceIt->Buffer);
                            if (targetIt->Buffer && sourceIt->Buffer) {
                                Y_ABORT_UNLESS(targetIt->Buffer == sourceIt->Buffer);
                            }
                            ++sourceIt;
                            ++targetIt;
                        }
                    }
                    if (itemsToAdd.empty()) {
                        return;
                    }
                    if (target.empty()) {
                        target = std::move(itemsToAdd);
                    } else {
                        auto temp = std::exchange(target, {});
                        target.reserve(itemsToAdd.size() + temp.size());
                        std::ranges::set_union(std::move(itemsToAdd), std::move(temp), std::back_inserter(target), comp,
                            &TEvBlobStorage::TEvRangeResult::TResponse::Id,
                            &TEvBlobStorage::TEvRangeResult::TResponse::Id);
                    }
                };

                if (ev->From <= ev->To) {
                    merge(std::less<TLogoBlobID>());
                } else {
                    merge(std::greater<TLogoBlobID>());
                }

                // mark pile as reponded
                Processed.Set(pile.BridgePileId.GetPileIndex());

                return ResponsesPending ? nullptr : IssueNextRestoreGet(self);
            }

            std::unique_ptr<IEventBase> ProcessResponse(TThis& self,
                    std::unique_ptr<TEvBlobStorage::TEvCollectGarbageResult> ev, const TBridgeInfo::TPile& /*pile*/,
                    TRequestPayload& /*payload*/) {
                return ProcessFullQuorumResponse(self, std::move(ev));
            }

            std::unique_ptr<IEventBase> ProcessResponse(TThis& self, std::unique_ptr<TEvBlobStorage::TEvStatusResult> ev,
                    const TBridgeInfo::TPile& /*pile*/, TRequestPayload& /*payload*/) {
                return ProcessFullQuorumResponse(self, std::move(ev));
            }

            std::unique_ptr<IEventBase> ProcessResponse(TThis& self, std::unique_ptr<TEvBlobStorage::TEvPatchResult> ev,
                    const TBridgeInfo::TPile& /*pile*/, TRequestPayload& /*payload*/) {
                return ProcessFullQuorumResponse(self, std::move(ev));
            }

            template<typename TRequestEvent, typename TEvent>
            std::unique_ptr<IEventBase> ProcessTrustedPileResponse(TThis& self, std::unique_ptr<TEvent> ev,
                    const TBridgeInfo::TPile& pile) {
                if (ev->Status != NKikimrProto::OK || DataIsTrustedInPile(pile)) {
                    // synced pile holds the whole content of the group, so its answer is the answer
                    return ev;
                } else if (ResponsesPending) {
                    return nullptr; // wait for the trusted pile
                } else {
                    Y_DEBUG_ABORT("missing response from trusted pile");
                    return static_cast<TRequestEvent&>(*OriginalRequest).MakeErrorResponse(
                        NKikimrProto::ERROR, "failed to obtain response from trusted pile", self.GroupId);
                }
            }

            std::unique_ptr<IEventBase> ProcessResponse(TThis& self, std::unique_ptr<TEvBlobStorage::TEvAssimilateResult> ev,
                    const TBridgeInfo::TPile& pile, TRequestPayload& /*payload*/) {
                return ProcessTrustedPileResponse<TEvBlobStorage::TEvAssimilate>(self, std::move(ev), pile);
            }

            std::unique_ptr<IEventBase> ProcessResponse(TThis& self, std::unique_ptr<TEvBlobStorage::TEvCheckIntegrityResult> ev,
                    const TBridgeInfo::TPile& pile, TRequestPayload& /*payload*/) {
                return ProcessTrustedPileResponse<TEvBlobStorage::TEvCheckIntegrity>(self, std::move(ev), pile);
            }
        };
        struct TRequestInFlight {
            std::shared_ptr<TRequest> Request;
            TBridgeInfo::TPtr BridgeInfo;
            const TBridgeInfo::TPile *Pile;
            TGroupId GroupId;
            TRequestPayload Payload;
            THPTimer Timer;
        };
        THashMap<ui64, TRequestInFlight> RequestsInFlight;
        ui64 LastRequestCookie = 0;

        std::shared_ptr<const NKikimrBlobStorage::TStorageConfig> StorageConfig;
        TBridgeInfo::TPtr BridgeInfo;
        bool NotCapableReported = false;

        std::deque<std::unique_ptr<IEventHandle>> PendingQ;
        std::deque<std::tuple<TMonotonic, std::unique_ptr<IEventHandle>>> PendingForNextGeneration;
        // requests waiting for the cluster state not older than the one the group state was made for
        std::deque<std::tuple<TMonotonic, std::unique_ptr<IEventHandle>>> PendingForClusterState;

    public:
        TBridgedBlobStorageProxyActor(TIntrusivePtr<TBlobStorageGroupInfo> info, TActorId innerProxyId)
            : Info(std::move(info))
            , GroupId(Info->GroupID)
            , InnerProxyId(innerProxyId)
        {
            Y_ABORT_UNLESS(Info->IsInlinePile() == static_cast<bool>(InnerProxyId));
        }

        void Bootstrap() {
            Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()), new TEvNodeWardenQueryStorageConfig(/*subscribe=*/ true));
            Become(&TThis::StateWaitBridgeInfo);
            HandleWakeup();
        }

        void HandleWakeup() {
            TMonotonic dropBefore = TActivationContext::Monotonic() - TDuration::Seconds(2);
            for (auto *queue : {&PendingForNextGeneration, &PendingForClusterState}) {
                while (!queue->empty()) {
                    if (auto& [timestamp, ev] = queue->front(); timestamp < dropBefore) {
                        ReplyWithError(*ev, "bridge request timed out");
                        queue->pop_front();
                    } else {
                        break;
                    }
                }
            }
            TActivationContext::Schedule(TDuration::Seconds(1), new IEventHandle(TEvents::TSystem::Wakeup, 0, SelfId(),
                {}, nullptr, 0));
        }

        void PassAway() override {
            ReplyToAllWithError("bridge proxy is stopped");
            TActivationContext::Send(new IEventHandle(TEvents::TSystem::Unsubscribe, 0,
                MakeBlobStorageNodeWardenID(SelfId().NodeId()), SelfId(), nullptr, 0));
            if (InnerProxyId) {
                TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, InnerProxyId, SelfId(), nullptr, 0));
            }
            TActorBootstrapped::PassAway();
        }

        void Handle(TEvNodeWardenStorageConfig::TPtr ev) {
            StorageConfig = std::move(ev->Get()->Config);
            BridgeInfo = std::move(ev->Get()->BridgeInfo);
            for (auto& [timestamp, ev] : std::exchange(PendingForClusterState, {})) {
                TActivationContext::Send(ev.release());
            }
        }

        template<typename TEvent>
        void ReplyWithError(TEvent& msg, TActorId sender, ui64 cookie, const TString& errorReason) {
            auto response = msg.MakeErrorResponse(NKikimrProto::ERROR, errorReason, GroupId);
            SetExecutionRelay(*response, std::exchange(msg.ExecutionRelay, {}));
            Send(sender, response.release(), 0, cookie);
        }

        void ReplyWithError(IEventBase& msg, TActorId sender, ui64 cookie, const TString& errorReason) {
            switch (msg.Type()) {
#define MAKE_ERROR(TYPE) \
                case TYPE::EventType: \
                    ReplyWithError(static_cast<TYPE&>(msg), sender, cookie, errorReason); \
                    break;

                DSPROXY_ENUM_EVENTS(MAKE_ERROR)
#undef MAKE_ERROR
                case TEvBlobStorage::EvBunchOfEvents:
                    for (auto& ev : static_cast<TEvBlobStorage::TEvBunchOfEvents&>(msg).Bunch) {
                        ReplyWithError(*ev, errorReason);
                    }
                    break;
            }
        }

        void ReplyWithError(IEventHandle& ev, const TString& errorReason) {
            ReplyWithError(*ev.GetBase(), ev.Sender, ev.Cookie, errorReason);
        }

        // reply with error to every request this proxy holds, as DS proxy does when stopped; this happens, for instance,
        // when the group gets decommitted and loses its piles, and the requests are retried through the BlobDepot agent
        void ReplyToAllWithError(const TString& errorReason) {
            for (auto& [cookie, item] : RequestsInFlight) {
                TRequest& request = *item.Request;
                if (!request.Finished) {
                    request.Finished = true;
                    if (request.Span) {
                        request.Span.EndError(errorReason);
                    }
                    ReplyWithError(*request.OriginalRequest, request.Sender, request.Cookie, errorReason);
                }
            }
            RequestsInFlight.clear();
            for (auto *queue : {&PendingForNextGeneration, &PendingForClusterState}) {
                for (auto& [timestamp, ev] : std::exchange(*queue, {})) {
                    ReplyWithError(*ev, errorReason);
                }
            }
            for (auto& ev : std::exchange(PendingQ, {})) {
                ReplyWithError(*ev, errorReason);
            }
        }

        template<typename TEvent>
        static bool IsFencedWrite([[maybe_unused]] const TEvent& ev) {
            if constexpr (std::is_same_v<TEvent, TEvBlobStorage::TEvPut> ||
                    std::is_same_v<TEvent, TEvBlobStorage::TEvBlock> ||
                    std::is_same_v<TEvent, TEvBlobStorage::TEvPatch>) {
                return true;
            } else if constexpr (std::is_same_v<TEvent, TEvBlobStorage::TEvCollectGarbage>) {
                return !TBlobStorageGroupInfo::IsAssimilationBarrier(ev.Hard, ev.RecordGeneration);
            } else {
                return false;
            }
        }

        // service id of decommitted group points to the BlobDepot agent, and this proxy serves only agent's requests
        bool IsBehindAgent() const {
            return Info->BlobDepotId.value_or(0);
        }

        // the request to be processed later is sent to us again; handles of puts restarted by DS proxy come unaddressed
        std::unique_ptr<IEventHandle> MakePendingHandle(IEventHandle& ev) {
            return std::make_unique<IEventHandle>(SelfId(), ev.Sender, ev.ReleaseBase().Release(), 0, ev.Cookie, nullptr,
                std::move(ev.TraceId));
        }

        template<typename TEvent>
        void HandleProxyRequest(TAutoPtr<TEventHandle<TEvent>>& ev) {
            if (InnerProxyId && ev->Get()->ForceGroupGeneration) {
                // request to the physical group of the inline pile (e.g. phantom check from replication), it has to reach
                // DS proxy as is, with its own generation fence, sender and cookie
                TActivationContext::Send(ev->Forward(InnerProxyId));
                return;
            }
            if (!BridgeInfo) {
                // this process runs without BridgeConfig, but the group is bridged one; this happens when the node was
                // not prepared before the cluster got converted to bridged one, and it can't serve bridged groups until
                // restarted with BridgeConfig
                STLOG(NotCapableReported ? PRI_DEBUG : PRI_CRIT, BS_PROXY_BRIDGE, BPB04,
                    "bridged group request on node running without BridgeConfig", (GroupId, GroupId));
                NotCapableReported = true;
                ReplyWithError(*ev->Get(), ev->Sender, ev->Cookie,
                    "node runs without BridgeConfig and can't serve bridged group");
                return;
            }

            if (Info->DecommitBlocksWrites() && IsFencedWrite(*ev->Get())) {
                // piles of bridged group don't know about its decommission, so this proxy is the only one to block
                // writes not coming from the BlobDepot (e.g. requests restarted after the group switched to it)
                ReplyWithError(*ev->Get(), ev->Sender, ev->Cookie, "group is being decommitted");
                return;
            }

            // group state and cluster state come by different paths; stages of piles in the group state are valid with the
            // pile states of the cluster state it was made for, or newer one, as the pile which takes no part in data quorum
            // by older cluster state may have got synced already
            Y_ABORT_UNLESS(Info->Group);
            const auto& state = Info->Group->GetBridgeGroupState();
            if (const ui64 generation = StorageConfig->GetClusterState().GetGeneration();
                    generation < state.GetClusterStateGeneration()) {
                STLOG(PRI_DEBUG, BS_PROXY_BRIDGE, BPB05, "request waits for newer cluster state", (GroupId, GroupId),
                    (GroupGeneration, Info->GroupGeneration), (ClusterStateGeneration, generation),
                    (BridgeGroupState, state));
                PendingForClusterState.emplace_back(TActivationContext::Monotonic(), MakePendingHandle(*ev));
                return;
            }

            // decide which piles get the request with the group state and the cluster state we have now
            const TBridgePileId authorityPileId = NBridge::GetAuthorityPileId(state);
            std::vector<std::tuple<TBridgePileId, std::unique_ptr<IEventBase>>> subrequests;
            NBridge::ForEachGroupPile(state, [&](TBridgePileId bridgePileId,
                    const NKikimrBridge::TGroupState::TPile& groupPileInfo) {
                const TBridgeInfo::TPile *pile = BridgeInfo->FindPile(bridgePileId);
                if (!pile) {
                    // bridge info is not older than group info, so the pile has left the cluster (or the group state comes
                    // from the version not telling its cluster state, and then the pile is just added and holds no data)
                    return;
                } else if (!NBridge::PileStateTraits(pile->State).RequiresDataQuorum) {
                    return; // skip this pile, no data quorum needed
                }
                if constexpr (std::is_same_v<TEvent, TEvBlobStorage::TEvAssimilate> ||
                        std::is_same_v<TEvent, TEvBlobStorage::TEvCheckIntegrity>) {
                    // only the answer of the pile holding the whole content of the group is taken, while an error from
                    // another one (e.g. degraded while being synced) would fail the request
                    if (!PartHoldsEverything(state, *pile)) {
                        return;
                    }
                }

                // check the sync state for this group
                if (auto eventToSend = PrepareEvent(groupPileInfo, *ev->Get(), pile->BridgePileId)) {
                    subrequests.emplace_back(pile->BridgePileId, std::move(eventToSend));
                }
            });
            if (subrequests.empty()) {
                // no pile is able to take this request with the group state we have; it is older than the cluster state
                // (e.g. the node has just got back and the pile which is synced now is not synced in its group state)
                STLOG(PRI_DEBUG, BS_PROXY_BRIDGE, BPB06, "request waits for newer group state", (GroupId, GroupId),
                    (GroupGeneration, Info->GroupGeneration), (BridgeGroupState, state));
                PendingForNextGeneration.emplace_back(TActivationContext::Monotonic(), MakePendingHandle(*ev));
                return;
            }
            if (authorityPileId && std::ranges::none_of(subrequests, [&](const auto& item) {
                    return std::get<0>(item) == authorityPileId;
                })) {
                // the group relies on its part in the single pile, and writes reaching other parts only would be lost
                STLOG(PRI_DEBUG, BS_PROXY_BRIDGE, BPB07, "request waits for the pile of the authority part",
                    (GroupId, GroupId), (GroupGeneration, Info->GroupGeneration), (BridgeGroupState, state));
                PendingForNextGeneration.emplace_back(TActivationContext::Monotonic(), MakePendingHandle(*ev));
                return;
            }

            std::unique_ptr<TEvent> evPtr(ev->Release().Release());
            const TEvent& originalRequest = *evPtr;
            auto request = std::make_shared<TRequest>(ev->Sender, ev->Cookie, std::move(evPtr), Info, *this,
                    ev->GetTypeRewrite(), std::move(ev->TraceId));
            request->BridgeInfo = BridgeInfo;

            STLOG(PRI_DEBUG, BS_PROXY_BRIDGE, BPB00, "new request", (RequestId, request->RequestId),
                (GroupId, GroupId), (GroupGeneration, request->Info->GroupGeneration),
                (BridgeGroupState, request->Info->Group->GetBridgeGroupState()),
                (Request, originalRequest.ToString()));

            for (auto& [bridgePileId, eventToSend] : subrequests) {
                SendQuery(request, bridgePileId, std::move(eventToSend));
            }
        }

        void SendQuery(std::shared_ptr<TRequest> request, TBridgePileId bridgePileId, std::unique_ptr<IEventBase> ev,
                TRequestPayload&& payload = {}) {
            const auto *groupPileInfo = NBridge::GetGroupPile(request->Info->Group->GetBridgeGroupState(),
                bridgePileId);
            Y_ABORT_UNLESS(groupPileInfo);
            const auto groupId = TGroupId::FromProto(groupPileInfo, &NKikimrBridge::TGroupState::TPile::GetGroupId);

            auto *common = dynamic_cast<TEvBlobStorage::TEvRequestCommon*>(ev.get());
            Y_ABORT_UNLESS(common);
            common->ForceGroupGeneration = groupPileInfo->GetGroupGeneration();

            STLOG(PRI_DEBUG, BS_PROXY_BRIDGE, BPB03, "new subrequest", (RequestId, request->RequestId),
                (BridgePileId, bridgePileId), (Request, ev->ToString()), (Cookie, LastRequestCookie + 1),
                (GroupPileInfo, *groupPileInfo));

            // allocate cookie for this specific request and bind it to the common one
            const ui64 cookie = ++LastRequestCookie;
            const auto [it, inserted] = RequestsInFlight.try_emplace(cookie, request, request->BridgeInfo,
                request->BridgeInfo->GetPile(bridgePileId), groupId, std::move(payload));
            Y_DEBUG_ABORT_UNLESS(inserted);
            const auto [it1, inserted1] = request->CookiesInFlight.insert(cookie);
            Y_DEBUG_ABORT_UNLESS(inserted1);

            // send event; inline pile is served by inner proxy, as the service id of this group points to us
            if (groupId == GroupId && !InnerProxyId) {
                // the inline pile has been removed (with its DS proxy) while the request was running against the group
                // configuration having it; the subrequests the inner proxy was running got errors when it stopped, and so
                // do the ones issued afterwards (like restore writes)
                ReplyWithError(*ev, SelfId(), cookie, "inline pile has been removed from the cluster");
            } else if (groupId == GroupId) {
                Send(InnerProxyId, ev.release(), 0, cookie, request->Span.GetTraceId());
            } else {
                SendToBSProxy(SelfId(), groupId, ev.release(), cookie, request->Span.GetTraceId());
            }
            ++request->ResponsesPending;
        }

        template<typename TEvent>
        std::unique_ptr<IEventBase> PrepareEvent(const NKikimrBridge::TGroupState::TPile& pile, const TEvent& ev,
                TBridgePileId bridgePileId) {
            std::unique_ptr<TEvent> res;

            switch (pile.GetStage()) {
                case NKikimrBridge::TGroupState::WRITE_KEEP:
                    if constexpr (std::is_same_v<TEvent, TEvBlobStorage::TEvCollectGarbage>) {
                        if (!ev.Keep) {
                            return nullptr;
                        }
                        // allow only keep flags to be sent
                        res = std::make_unique<TEvBlobStorage::TEvCollectGarbage>(ev.TabletId, ev.RecordGeneration,
                            ev.PerGenerationCounter, ev.Channel, false, 0, 0, new TVector<TLogoBlobID>(*ev.Keep),
                            nullptr, ev.Deadline, true, ev.WriteSource, false, false);
                    }
                    [[fallthrough]];
                case NKikimrBridge::TGroupState::WRITE_KEEP_BARRIER_DONOTKEEP:
                    if (std::is_same_v<TEvent, TEvBlobStorage::TEvCollectGarbage>) {
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

            if constexpr (std::is_same_v<TEvent, TEvBlobStorage::TEvPut>) {
                if (bridgePileId == BridgeInfo->SelfNodePile->BridgePileId) {
                    // send this message as is
                } else if (AppData()->FeatureFlags.GetEnableInterpileTrafficOptimization()) {
                    // enable reducing interpile traffic; clone the original message rather than
                    // rebuilding it, so that fields the remote pile needs (WriteSource, DataKind)
                    // survive the trip
                    res = std::make_unique<TEvBlobStorage::TEvPut>(TEvBlobStorage::CloneEventPolicy, ev,
                        /*reduceInterpileTraffic=*/ true);
                }
            }

            if (!res) {
                res = std::make_unique<TEvent>(TEvBlobStorage::CloneEventPolicy, ev);
            }

            if constexpr (std::is_same_v<TEvent, TEvBlobStorage::TEvAssimilate>) {
                if (Info->DecommitBlocksWrites()) {
                    // pile groups don't carry decommit state of their bridge proxy group, while this proxy blocks writes
                    // for them; let the VDisks of piles proceed with assimilation
                    res->IgnoreDecommitState = true;
                }
            }

            return res;
        }

        template<typename TEvent>
        void HandleProxyResult(TAutoPtr<TEventHandle<TEvent>>& ev) {
            const auto it = RequestsInFlight.find(ev->Cookie);
            if (it == RequestsInFlight.end()) {
                return; // request has already been completed
            }
            auto& item = it->second;
            auto& pile = *item.Pile;
            std::shared_ptr<TRequest> request = item.Request;

            const bool isError = ev->Get()->Status != NKikimrProto::OK && ev->Get()->Status != NKikimrProto::NODATA;

            STLOG(isError ? PRI_NOTICE : PRI_DEBUG, BS_PROXY_BRIDGE, BPB02, "intermediate response",
                (RequestId, request->RequestId),
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

                if (msg->Status == NKikimrProto::RACE) {
                    // route repeated request through node warden to ensure group info propagation before request gets restarted
                    auto& ev = request->OriginalRequest;
                    auto *common = dynamic_cast<TEvBlobStorage::TEvRequestCommon*>(ev.get());
                    Y_ABORT_UNLESS(common);
                    ++common->RestartCounter;
                    auto handle = std::make_unique<IEventHandle>(SelfId(), request->Sender, ev.release(), 0, request->Cookie);

                    // when the group has no part in this pile anymore, our configuration is newer than the one the
                    // request was executed with
                    const auto *groupPile = NBridge::GetGroupPile(Info->Group->GetBridgeGroupState(), pile.BridgePileId);
                    const ui32 myGeneration = groupPile ? groupPile->GetGroupGeneration() : Max<ui32>();

                    Y_VERIFY_DEBUG_S(common->RestartCounter < 100, "GroupId# " << GroupId
                        << " item.GroupId# " << item.GroupId
                        << " myGeneration# " << myGeneration
                        << " RacingGeneration# " << msg->RacingGeneration
                        << " Info.Generation# " << request->Info->GroupGeneration
                        << " Type# " << TypeName<TEvent>()
                        << " RequestId# " << request->RequestId
                        << " ErrorReason# " << msg->ErrorReason); // too often restarts do not make sense

                    if (myGeneration < msg->RacingGeneration) {
                        PendingForNextGeneration.emplace_back(TActivationContext::Monotonic(), std::move(handle));
                    } else if (msg->RacingGeneration < myGeneration) {
                        // when the group is decommitted, its service id points to the BlobDepot agent, which would take
                        // restarted request for its client's one; restart it right here then
                        std::unique_ptr<IEventHandle> h(IsBehindAgent()
                            ? handle.release()
                            : CreateEventForBSProxy(handle->Sender, GroupId, handle->ReleaseBase().Release(),
                                handle->Cookie, std::move(handle->TraceId)));
                        if (TGroupID(item.GroupId).ConfigurationType() == EGroupConfigurationType::Static) {
                            // static group configuration is updated a bit differently, so we can't expect for instant update;
                            // here we introduce a backoff timer
                            constexpr ui32 maxCounter = 20;
                            constexpr double maxFactor = 6.0 / maxCounter;
                            auto timeout = TDuration::MicroSeconds(pow(10, Min(common->RestartCounter - 1, maxCounter) * maxFactor));
                            TActivationContext::Schedule(timeout, h.release());
                        } else {
                            // our generation is higher than the recipient's; we have to route this message through node warden
                            // to ensure proxy's configuration gets in place
                            TActivationContext::Send(h.release());
                        }
                    } else {
                        // we can retry this message (obviously, we HAD request executed at incorrect generation, but
                        // now generation is correct)
                        Y_DEBUG_ABORT_UNLESS(NBridge::GetGroupPile(request->Info->Group->GetBridgeGroupState(),
                            pile.BridgePileId)->GetGroupGeneration() < myGeneration);
                        TActivationContext::Send(handle.release());
                    }
                    request->Finished = true;
                } else {
                    std::unique_ptr<TEvent> ptr(ev->Release().Release());
                    request->SubrequestTimings.emplace_back(TypeName<TEvent>(), TDuration::Seconds(item.Timer.Passed()));

                    if (auto response = request->ProcessResponse(*this, std::move(ptr), pile, item.Payload)) {
                        FixGroupId(*response);

                        auto *common = dynamic_cast<TEvBlobStorage::TEvResultCommon*>(response.get());
                        Y_ABORT_UNLESS(common);
                        Y_DEBUG_ABORT_UNLESS(common->Status != NKikimrProto::RACE);
                        const bool success = common->Status == NKikimrProto::OK
                            || (common->Status == NKikimrProto::NODATA && response->Type() == TEvBlobStorage::EvDiscoverResult);
                        auto makeSubrequestTimings = [&] {
                            return FormatList(std::views::transform(request->SubrequestTimings, [&](const auto& item) {
                                const auto& [name, duration] = item;
                                return TStringBuilder() << name << ':' << duration;
                            }));
                        };

                        STLOG(success ? PRI_INFO : PRI_NOTICE, BS_PROXY_BRIDGE, BPB01, "request finished",
                            (RequestId, request->RequestId),
                            (Status, common->Status),
                            (Response, response->ToString()),
                            (Passed, TDuration::Seconds(request->Timer.Passed())),
                            (SubrequestTimings, makeSubrequestTimings()));

                        if (success) {
                            request->Span.EndOk();
                        } else if (request->Span) {
                            request->Span.EndError(common->ErrorReason);
                        }

                        // subrequests are clones without the relay, so return the one of the original request
                        auto *originalCommon = dynamic_cast<TEvBlobStorage::TEvRequestCommon*>(request->OriginalRequest.get());
                        Y_ABORT_UNLESS(originalCommon);
                        SetExecutionRelay(*response, std::exchange(originalCommon->ExecutionRelay, {}));
                        Send(request->Sender, response.release(), 0, request->Cookie);
                        request->Finished = true;
                    }
                }
            }

            Y_ABORT_UNLESS(request->ResponsesPending || request->Finished);

            request->CookiesInFlight.erase(it->first);
            RequestsInFlight.erase(it);

            if (request->Finished) {
                for (ui64 cookie : request->CookiesInFlight) {
                    RequestsInFlight.erase(cookie);
                }
            }
        }

        void Handle(TEvBlobStorage::TEvConfigureProxy::TPtr ev) {
            auto *msg = ev->Get();
            if (msg->Info && msg->Info->DecommitStatus == NKikimrBlobStorage::TGroupDecommitStatus::DONE) {
                // decommitted group has lost its piles, so there is nothing to route requests to; BlobDepot agent in front
                // of us stops us right away, and the requests we hold are answered with error then
                return;
            }
            if (InnerProxyId && msg->Info && !msg->Info->IsInlinePile()) {
                // the pile of the inline pile has been removed from the cluster along with its VDisks, the group serves
                // as bridge proxy group only now
                TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, InnerProxyId, SelfId(), nullptr, 0));
                InnerProxyId = {};
            }
            if (InnerProxyId) {
                // inner proxy has to see its group as an ordinary pile one
                Y_ABORT_UNLESS(!msg->Info || msg->Info->IsInlinePile());
                Send(InnerProxyId, new TEvBlobStorage::TEvConfigureProxy(
                    msg->Info ? msg->Info->MakeInlinePileView() : nullptr, msg->NodeLayoutInfo, msg->StoragePoolCounters));
            }
            if (!msg->Info) {
                // keep current configuration; the inner proxy (if any) waits for the new one, and subrequests are sent
                // with forced group generation anyway
                return;
            }
            auto prevInfo = std::exchange(Info, std::move(msg->Info));
            Y_ABORT_UNLESS(prevInfo);
            Y_ABORT_UNLESS(Info);
            if (prevInfo->GroupGeneration < Info->GroupGeneration) {
                for (auto& [timestamp, ev] : std::exchange(PendingForNextGeneration, {})) {
                    TActivationContext::Send(ev.release());
                }
            }
        }

        void Handle(TEvBlobStorage::TEvBunchOfEvents::TPtr ev) {
            // puts restarted by DS proxy of the inline pile, which were started before the group got converted and
            // came back through node warden to the service of the group
            ev->Get()->Process(this);
        }

        void FixGroupId(IEventBase& ev) {
            switch (ev.Type()) {
                case TEvBlobStorage::EvPutResult:
                    const_cast<ui32&>(static_cast<TEvBlobStorage::TEvPutResult&>(ev).GroupId) = GroupId.GetRawId();
                    break;

                case TEvBlobStorage::EvGetResult:
                    const_cast<ui32&>(static_cast<TEvBlobStorage::TEvGetResult&>(ev).GroupId) = GroupId.GetRawId();
                    break;

                case TEvBlobStorage::EvRangeResult:
                    const_cast<ui32&>(static_cast<TEvBlobStorage::TEvRangeResult&>(ev).GroupId) = GroupId.GetRawId();
                    break;

                case TEvBlobStorage::EvPatchResult:
                    const_cast<TGroupId&>(static_cast<TEvBlobStorage::TEvPatchResult&>(ev).GroupId) = GroupId;
                    break;
            }
        }

#define HANDLE_REQUEST(NAME) hFunc(NAME, HandleProxyRequest)
#define HANDLE_RESULT(NAME) hFunc(NAME##Result, HandleProxyResult)

        STRICT_STFUNC(StateFunc,
            DSPROXY_ENUM_EVENTS(HANDLE_REQUEST)
            DSPROXY_ENUM_EVENTS(HANDLE_RESULT)

            hFunc(TEvBlobStorage::TEvConfigureProxy, Handle)
            hFunc(TEvBlobStorage::TEvBunchOfEvents, Handle)
            hFunc(TEvNodeWardenStorageConfig, Handle)
            cFunc(TEvents::TSystem::Poison, PassAway)
            cFunc(TEvents::TSystem::Wakeup, HandleWakeup)
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
                StateFunc(ev); // pending requests are answered with error
            } else {
                PendingQ.emplace_back(ev.Release());
            }
        }
    };

    IActor *CreateBridgeProxyActor(TIntrusivePtr<TBlobStorageGroupInfo> info) {
        return new TBridgedBlobStorageProxyActor(std::move(info), TActorId());
    }

    IActor *CreateInlinePileBridgeProxyActor(TIntrusivePtr<TBlobStorageGroupInfo> info, TActorId innerProxyId) {
        return new TBridgedBlobStorageProxyActor(std::move(info), innerProxyId);
    }

} // NKikimr
