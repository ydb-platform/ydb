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

        using TClusterState = NKikimrBridge::TClusterState;

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
                if (!NBridge::PileStateTraits(pile.State).RequiresDataQuorum) {
                    return false;
                }
                const auto& state = Info->Group->GetBridgeGroupState();
                const auto& gp = state.GetPile(pile.BridgePileId.GetPileIndex());
                return gp.GetStage() == NKikimrBridge::TGroupState::SYNCED;
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
                        RestoreQueue[payload.Index].IssueKeepFlag = response.DoNotKeep < response.Keep;
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

                if (!MustRestoreFirst && DataIsTrustedInPile(pile)) {
                    return ev;
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
                        self.SendQuery(shared_from_this(), item.ReadFrom, std::make_unique<TEvBlobStorage::TEvGet>(
                            item.Id, 0, 0, TInstant::Max(), GetHandleClass), {.Index = RestoreIndex});
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
                        item.Id, TRcBuf(item.Buffer), TInstant::Max(), handleClass, TEvBlobStorage::TEvPut::TacticDefault,
                        item.IssueKeepFlag, true), {.Index = index});
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

            std::unique_ptr<IEventBase> ProcessResponse(TThis& self, std::unique_ptr<TEvBlobStorage::TEvAssimilateResult> ev,
                    const TBridgeInfo::TPile& /*pile*/, TRequestPayload& /*payload*/) {
                (void)self, (void)ev;
                return nullptr;
            }

            std::unique_ptr<IEventBase> ProcessResponse(TThis& self, std::unique_ptr<TEvBlobStorage::TEvCheckIntegrityResult> ev,
                    const TBridgeInfo::TPile& /*pile*/, TRequestPayload& /*payload*/) {
                (void)self, (void)ev;
                return nullptr;
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

        std::deque<std::unique_ptr<IEventHandle>> PendingQ;
        std::deque<std::tuple<TMonotonic, std::unique_ptr<IEventHandle>>> PendingForNextGeneration;

    public:
        TBridgedBlobStorageProxyActor(TIntrusivePtr<TBlobStorageGroupInfo> info)
            : Info(std::move(info))
            , GroupId(Info->GroupID)
        {}

        void Bootstrap() {
            Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()), new TEvNodeWardenQueryStorageConfig(/*subscribe=*/ true));
            Become(&TThis::StateWaitBridgeInfo);
            HandleWakeup();
        }

        void HandleWakeup() {
            TMonotonic dropBefore = TActivationContext::Monotonic() - TDuration::Seconds(2);
            while (!PendingForNextGeneration.empty()) {
                if (auto& [timestamp, ev] = PendingForNextGeneration.front(); timestamp < dropBefore) {
                    switch (ev->GetTypeRewrite()) {
#define MAKE_ERROR(TYPE) \
                        case TYPE::EventType: \
                            Send(ev->Sender, static_cast<TYPE*>(ev->GetBase())->MakeErrorResponse(NKikimrProto::ERROR, \
                                "bridge request timed out", GroupId), 0, ev->Cookie); \
                            break;

                        DSPROXY_ENUM_EVENTS(MAKE_ERROR)
#undef MAKE_ERROR
                        default:
                            Y_ABORT();
                    }
                    PendingForNextGeneration.pop_front();
                } else {
                    break;
                }
            }
            TActivationContext::Schedule(TDuration::Seconds(1), new IEventHandle(TEvents::TSystem::Wakeup, 0, SelfId(),
                {}, nullptr, 0));
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

            std::unique_ptr<TEvent> evPtr(ev->Release().Release());
            const TEvent& originalRequest = *evPtr;
            auto request = std::make_shared<TRequest>(ev->Sender, ev->Cookie, std::move(evPtr), Info, *this,
                    ev->GetTypeRewrite(), std::move(ev->TraceId));

            STLOG(PRI_DEBUG, BS_PROXY_BRIDGE, BPB00, "new request", (RequestId, request->RequestId),
                (GroupId, GroupId), (GroupGeneration, request->Info->GroupGeneration),
                (BridgeGroupState, request->Info->Group->GetBridgeGroupState()),
                (Request, originalRequest.ToString()));

            Y_ABORT_UNLESS(request->Info->Group);
            const auto& state = request->Info->Group->GetBridgeGroupState();
            for (size_t i = 0; i < state.PileSize(); ++i) {
                const auto bridgePileId = TBridgePileId::FromPileIndex(i);
                const TBridgeInfo::TPile *pile = BridgeInfo->GetPile(bridgePileId);
                if (!NBridge::PileStateTraits(pile->State).RequiresDataQuorum) {
                    continue; // skip this pile, no data quorum needed
                }

                // check the sync state for this group
                const auto& groupPileInfo = state.GetPile(i);
                if (auto eventToSend = PrepareEvent(groupPileInfo, originalRequest)) {
                    SendQuery(request, pile->BridgePileId, std::move(eventToSend));
                }
            }

            Y_ABORT_UNLESS(request->ResponsesPending);
        }

        void SendQuery(std::shared_ptr<TRequest> request, TBridgePileId bridgePileId, std::unique_ptr<IEventBase> ev,
                TRequestPayload&& payload = {}) {
            const auto& state = request->Info->Group->GetBridgeGroupState();
            const auto& groupPileInfo = state.GetPile(bridgePileId.GetPileIndex());
            const auto groupId = TGroupId::FromProto(&groupPileInfo, &NKikimrBridge::TGroupState::TPile::GetGroupId);

            auto *common = dynamic_cast<TEvBlobStorage::TEvRequestCommon*>(ev.get());
            Y_ABORT_UNLESS(common);
            common->ForceGroupGeneration = groupPileInfo.GetGroupGeneration();

            STLOG(PRI_DEBUG, BS_PROXY_BRIDGE, BPB03, "new subrequest", (RequestId, request->RequestId),
                (BridgePileId, bridgePileId), (Request, ev->ToString()), (Cookie, LastRequestCookie + 1),
                (GroupPileInfo, groupPileInfo));

            // allocate cookie for this specific request and bind it to the common one
            const ui64 cookie = ++LastRequestCookie;
            const auto [it, inserted] = RequestsInFlight.try_emplace(cookie, request, BridgeInfo,
                BridgeInfo->GetPile(bridgePileId), groupId, std::move(payload));
            Y_DEBUG_ABORT_UNLESS(inserted);
            const auto [it1, inserted1] = request->CookiesInFlight.insert(cookie);
            Y_DEBUG_ABORT_UNLESS(inserted1);

            // send event
            SendToBSProxy(SelfId(), groupId, ev.release(), cookie, request->Span.GetTraceId());
            ++request->ResponsesPending;
        }

        template<typename TEvent>
        std::unique_ptr<IEventBase> PrepareEvent(const NKikimrBridge::TGroupState::TPile& pile, const TEvent& ev) {
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
                            nullptr, ev.Deadline, true, false, false);
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

            if (!res) {
                res = std::make_unique<TEvent>(TEvBlobStorage::CloneEventPolicy, ev);
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
                    Y_DEBUG_ABORT_UNLESS(common->RestartCounter < 100); // too often restarts do not make sense
                    auto handle = std::make_unique<IEventHandle>(SelfId(), request->Sender, ev.release(), 0, request->Cookie);

                    const auto& bridgeGroupState = Info->Group->GetBridgeGroupState();
                    const ui32 myGeneration = bridgeGroupState.GetPile(pile.BridgePileId.GetPileIndex()).GetGroupGeneration();

                    if (myGeneration < msg->RacingGeneration) {
                        PendingForNextGeneration.emplace_back(TActivationContext::Monotonic(), std::move(handle));
                    } else if (msg->RacingGeneration < myGeneration) {
                        // our generation is higher than the recipient's; we have to route this message through node warden
                        // to ensure proxy's configuration gets in place
                        SendToBSProxy(handle->Sender, GroupId, handle->ReleaseBase().Release(), handle->Cookie,
                            std::move(handle->TraceId));
                    } else {
                        // we can retry this message (obviously, we HAD request executed at incorrect generation, but
                        // now generation is correct)
                        Y_DEBUG_ABORT_UNLESS(request->Info->Group->GetBridgeGroupState().GetPile(
                            pile.BridgePileId.GetPileIndex()).GetGroupGeneration() < myGeneration);
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
            auto prevInfo = std::exchange(Info, std::move(ev->Get()->Info));
            Y_ABORT_UNLESS(prevInfo);
            Y_ABORT_UNLESS(Info);
            if (prevInfo->GroupGeneration < Info->GroupGeneration) {
                for (auto& [timestamp, ev] : std::exchange(PendingForNextGeneration, {})) {
                    TActivationContext::Send(ev.release());
                }
            }
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
