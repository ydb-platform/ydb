#include "blobstorage_replbroker.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/blobstorage/backpressure/queue_backpressure_client.h>
#include <util/generic/hash_set.h>
#include <util/generic/queue.h>

namespace NKikimr {

    class TReplBroker : public TActor<TReplBroker> {
        // queue of senders waiting for token per PDisk; the first one in queue always has a token
        THashMap<ui32, TDeque<TActorId>> VDiskQ;
        THashMap<TActorId, ui32> SenderToPDisk;

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

    public:
        static constexpr auto ActorActivityType() {
            return NKikimrServices::TActivity::BS_REPL_BROKER;
        }

        TReplBroker(ui64 maxMemBytes)
            : TActor(&TReplBroker::StateFunc)
            , MemFree(maxMemBytes)
        {}

        void Handle(TEvQueryReplToken::TPtr& ev) {
            const ui32 pdiskId = ev->Get()->PDiskId;
            const bool inserted = SenderToPDisk.emplace(ev->Sender, pdiskId).second;
            Y_ABORT_UNLESS(inserted, "duplicate request for the replication token");
            auto& q = VDiskQ[pdiskId];
            q.push_back(ev->Sender);
            if (q.size() == 1) {
                Send(ev->Sender, new TEvReplToken);
            }
        }

        void Handle(TEvReleaseReplToken::TPtr& ev) {
            // first, find the queue for the requested PDisk; it MUST exist as the ReleaseToken operation is only possible
            // after issuing the QueryToken message
            auto it = SenderToPDisk.find(ev->Sender);
            Y_ABORT_UNLESS(it != SenderToPDisk.end());
            const ui32 pdiskId = it->second;
            auto qIt = VDiskQ.find(pdiskId);
            Y_ABORT_UNLESS(qIt != VDiskQ.end());
            auto& q = qIt->second;
            Y_ABORT_UNLESS(q);
            SenderToPDisk.erase(it);

            if (q.front() == ev->Sender) {
                q.pop_front();
                if (q) {
                    // we have next disk to grant replication to, so do it
                    Send(q.front(), new TEvReplToken);
                } else {
                    // replication queue for this PDisk got empty, remove it from the dictionary
                    VDiskQ.erase(qIt);
                }
            } else {
                // just remove pending request from the queue; it has not been granted yet
                auto it = std::find(q.begin(), q.end(), ev->Sender);
                Y_ABORT_UNLESS(it != q.end());
                q.erase(it);
            }
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
