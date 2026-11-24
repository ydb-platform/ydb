#include "blobstorage_replbroker.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/blobstorage/backpressure/queue_backpressure_client.h>
#include <util/generic/hash_set.h>
#include <util/generic/queue.h>
#include <map>

namespace NKikimr {

    class TReplBroker : public TActor<TReplBroker> {
        struct TPDiskQueue {
            TActorId Holder;
            // Cached priority of Holder; a waiter with a smaller value may request cooperative token yield.
            TReplTokenPriority HolderPriority = DefaultReplTokenPriority;
            bool YieldRequested = false;
            std::map<TReplTokenPriority, TDeque<TActorId>> Waiting;
        };

        struct TRequestInfo {
            const ui32 PDiskId;
            const bool SupportsYield;
            TReplTokenPriority Priority;
            bool HoldingToken = false;
        };

        // Map pdiskId -> replication queue for this pdisk
        // TODO: Rename to PDiskQueues
        THashMap<ui32, TPDiskQueue> VDiskQ;
        THashMap<TActorId, TRequestInfo> Requests;

        struct TMemQueueItem {
            TActorId Sender;
            ui64 Cookie;
            ui64 Bytes;

            TMemQueueItem(const TActorId& sender, ui64 cookie, ui64 bytes)
                : Sender(sender)
                , Cookie(cookie)
                , Bytes(bytes)
            {}
        };
        TDeque<TMemQueueItem> MemQueue;
        i64 MemFree;

        struct TMemToken {
            TActorId Sender;
            ui64 Bytes;

            TMemToken(const TActorId& sender, ui64 bytes)
                : Sender(sender)
                , Bytes(bytes)
            {}
        };
        THashMap<TReplMemTokenId, TMemToken> MemTokens;
        TReplMemTokenId NextMemToken = 1;

        static void RemoveWaitingRequest(TPDiskQueue& queue, TReplTokenPriority priority, const TActorId& sender) {
            auto priorityIt = queue.Waiting.find(priority);
            Y_ABORT_UNLESS(priorityIt != queue.Waiting.end());
            auto& requests = priorityIt->second;
            const auto requestIt = std::find(requests.begin(), requests.end(), sender);
            Y_ABORT_UNLESS(requestIt != requests.end());
            requests.erase(requestIt);
            if (requests.empty()) {
                queue.Waiting.erase(priorityIt);
            }
        }

        void MaybeRequestYield(TPDiskQueue& queue) {
            if (queue.Holder && !queue.YieldRequested && !queue.Waiting.empty()
                    && queue.Waiting.begin()->first < queue.HolderPriority) {
                queue.YieldRequested = true;
                Send(queue.Holder, new TEvYieldReplToken);
            }
        }

        void ProcessQueue(ui32 pdiskId, TPDiskQueue& queue) {
            if (!queue.Holder) {
                // The previous holder may have released the token with no waiters left, so there is nothing to grant.
                if (queue.Waiting.empty()) {
                    return;
                }

                auto priorityIt = queue.Waiting.begin();
                auto& requests = priorityIt->second;
                const TActorId sender = requests.front();
                requests.pop_front();
                if (requests.empty()) {
                    queue.Waiting.erase(priorityIt);
                }

                auto requestIt = Requests.find(sender);
                Y_ABORT_UNLESS(requestIt != Requests.end());
                Y_ABORT_UNLESS(requestIt->second.PDiskId == pdiskId);
                Y_ABORT_UNLESS(!requestIt->second.HoldingToken);
                requestIt->second.HoldingToken = true;
                queue.Holder = sender;
                queue.HolderPriority = requestIt->second.Priority;
                queue.YieldRequested = false;
                Send(sender, new TEvReplToken);
            } else {
                const auto holderIt = Requests.find(queue.Holder);
                Y_ABORT_UNLESS(holderIt != Requests.end());
                Y_ABORT_UNLESS(holderIt->second.PDiskId == pdiskId);
                Y_ABORT_UNLESS(holderIt->second.HoldingToken);
                if (holderIt->second.SupportsYield) {
                    MaybeRequestYield(queue);
                }
            }
        }

    public:
        static constexpr auto ActorActivityType() {
            return NKikimrServices::TActivity::BS_REPL_BROKER;
        }

        TReplBroker(ui64 maxMemBytes)
            : TActor(&TReplBroker::StateFunc)
            , MemFree(maxMemBytes)
        {}

        void Handle(TEvQueryReplToken::TPtr& ev) {
            const auto* msg = ev->Get();
            const ui32 pdiskId = msg->PDiskId;
            const auto i = Requests.emplace(ev->Sender,
                TRequestInfo{
                    .PDiskId = pdiskId,
                    .SupportsYield = msg->SupportsYield,
                    .Priority = msg->Priority});
            Y_ABORT_UNLESS(i.second, "duplicate request for the replication token");
            auto& q = VDiskQ[pdiskId];
            q.Waiting[msg->Priority].push_back(ev->Sender);
            ProcessQueue(pdiskId, q);
        }

        void Handle(TEvReleaseReplToken::TPtr& ev) {
            // first, find the queue for the requested PDisk; it MUST exist as the ReleaseToken operation is only possible
            // after issuing the QueryToken message
            auto it = Requests.find(ev->Sender);
            Y_ABORT_UNLESS(it != Requests.end());
            const TRequestInfo request = it->second;
            const ui32 pdiskId = request.PDiskId;
            auto qIt = VDiskQ.find(pdiskId);
            Y_ABORT_UNLESS(qIt != VDiskQ.end());
            auto& q = qIt->second;
            Requests.erase(it);

            if (request.HoldingToken) {
                Y_ABORT_UNLESS(q.Holder == ev->Sender);
                q.Holder = {};
                q.HolderPriority = DefaultReplTokenPriority;
                q.YieldRequested = false;
            } else {
                RemoveWaitingRequest(q, request.Priority, ev->Sender);
            }

            ProcessQueue(pdiskId, q);
            if (!q.Holder && q.Waiting.empty()) {
                VDiskQ.erase(qIt);
            }
        }

        void Handle(TEvUpdateReplTokenPriority::TPtr& ev) {
            auto requestIt = Requests.find(ev->Sender);
            Y_ABORT_UNLESS(requestIt != Requests.end());
            auto& request = requestIt->second;
            const TReplTokenPriority priority = ev->Get()->Priority;
            if (request.Priority == priority) {
                return;
            }

            auto queueIt = VDiskQ.find(request.PDiskId);
            Y_ABORT_UNLESS(queueIt != VDiskQ.end());
            auto& queue = queueIt->second;
            if (request.HoldingToken) {
                Y_ABORT_UNLESS(queue.Holder == ev->Sender);
                request.Priority = priority;
                queue.HolderPriority = priority;
            } else {
                RemoveWaitingRequest(queue, request.Priority, ev->Sender);
                request.Priority = priority;
                queue.Waiting[priority].push_back(ev->Sender);
            }
            ProcessQueue(request.PDiskId, queue);
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // MEMORY MANAGEMENT
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        void ProcessMemQueue() {
            while (MemQueue && MemFree >= 0) {
                // check if we can allocate one more token -- if we have enough memory, or if we don't have tokens at all
                auto& item = MemQueue.front();
                if (item.Bytes > static_cast<ui64>(MemFree) && MemTokens) {
                    break;
                }

                // allocate token
                const TReplMemTokenId token = NextMemToken++;

                // send response
                Send(item.Sender, new TEvReplMemToken(token), 0, item.Cookie);

                // register token
                MemTokens.emplace(token, TMemToken(item.Sender, item.Bytes));

                // adjust free mem size
                MemFree -= item.Bytes;

                // remove processed request
                MemQueue.pop_front();
            }
        }

        void Handle(TEvQueryReplMemToken::TPtr& ev) {
            MemQueue.emplace_back(ev->Sender, ev->Cookie, ev->Get()->Bytes);
            ProcessMemQueue();
        }

        void Handle(TEvUpdateReplMemToken::TPtr& ev) {
            TEvUpdateReplMemToken *msg = ev->Get();
            auto it = MemTokens.find(msg->Token);
            Y_ABORT_UNLESS(it != MemTokens.end());
            TMemToken& token = it->second;
            MemFree += token.Bytes - msg->ActualBytes;
            token.Bytes = msg->ActualBytes;
            ProcessMemQueue();
        }

        void Handle(TEvReleaseReplMemToken::TPtr& ev) {
            auto it = MemTokens.find(ev->Get()->Token);
            Y_ABORT_UNLESS(it != MemTokens.end());
            MemFree += it->second.Bytes;
            MemTokens.erase(it);
            ProcessMemQueue();
        }

        void Handle(TEvPruneQueue::TPtr& ev) {
            auto pred = [&](const auto& item) {
                return item.Sender == ev->Sender;
            };
            MemQueue.erase(std::remove_if(MemQueue.begin(), MemQueue.end(), pred), MemQueue.end());

            for (auto it = MemTokens.begin(); it != MemTokens.end(); ) {
                if (it->second.Sender == ev->Sender) {
                    MemFree += it->second.Bytes;
                    MemTokens.erase(it++);
                } else {
                    ++it;
                }
            }

            ProcessMemQueue();
        }

        STRICT_STFUNC(StateFunc,
            hFunc(TEvQueryReplToken, Handle)
            hFunc(TEvReleaseReplToken, Handle)
            hFunc(TEvUpdateReplTokenPriority, Handle)
            hFunc(TEvQueryReplMemToken, Handle)
            hFunc(TEvUpdateReplMemToken, Handle)
            hFunc(TEvReleaseReplMemToken, Handle)
            hFunc(TEvPruneQueue, Handle)
        )
    };

    IActor *CreateReplBrokerActor(ui64 maxMemBytes) {
        return new TReplBroker(maxMemBytes);
    }

} // NKikimr
