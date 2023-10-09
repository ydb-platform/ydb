#include "flat_load_blob_queue.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>

namespace NKikimr {
namespace NTabletFlatExecutor {

namespace {
    struct TLogPrefix {
        const TLoadBlobQueueConfig& Config;

        friend IOutputStream& operator<<(IOutputStream& out, const TLogPrefix& value) {
            out << (value.Config.Follower ? "Follower" : "Leader")
                << "{" << value.Config.TabletID << ":" << value.Config.Generation << ":-} ";
            return out;
        }
    };

    struct TDumpLogoBlobs {
        const TVector<TLogoBlobID>& Blobs;

        friend IOutputStream& operator<<(IOutputStream& out, const TDumpLogoBlobs& value) {
            out << '{';
            bool first = true;
            for (const TLogoBlobID& id : value.Blobs) {
                if (first) {
                    first = false;
                } else {
                    out << ',';
                }
                out << ' ';
                out << id;
            }
            out << ' ';
            out << '}';
            return out;
        }
    };
}

TLoadBlobQueue::TLoadBlobQueue()
{ }

void TLoadBlobQueue::Clear() {
    Queue.clear();
    Active.clear();
    ActiveBytesInFly = 0;
}

void TLoadBlobQueue::Enqueue(const TLogoBlobID& id, ui32 group, ILoadBlob* load, uintptr_t cookie) {
    TActive::insert_ctx ctx;
    if (Active.find(id, ctx) != Active.end()) {
        Active.emplace_direct(ctx, id, TActiveItem{ load, cookie });
    } else {
        Queue.push_back(TPendingItem{ id, group, load, cookie });
    }
}

bool TLoadBlobQueue::SendRequests(const TActorId& sender) {
    while (ActiveBytesInFly < Config.MaxBytesInFly && Queue) {
        TVector<TLogoBlobID> newBlobs;
        ui64 batchSize = 0;

        ui64 tablet = 0;
        ui32 channel = 0;
        ui32 generation = 0;
        ui32 group = Max<ui32>();

        while (Queue) {
            auto& item = Queue.front();

            TActive::insert_ctx ctx;
            if (Active.find(item.ID, ctx) != Active.end()) {
                Active.emplace_direct(ctx, item.ID, TActiveItem{ item.Load, item.Cookie });
                Queue.pop_front();
                continue;
            }

            if (newBlobs) {
                if (tablet != item.ID.TabletID() ||
                    channel != item.ID.Channel() ||
                    generation != item.ID.Generation() ||
                    group != item.Group)
                {
                    break;
                }
            } else {
                tablet = item.ID.TabletID();
                channel = item.ID.Channel();
                generation = item.ID.Generation();
                group = item.Group;
            }

            newBlobs.push_back(item.ID);
            batchSize += item.ID.BlobSize();
            ActiveBytesInFly += item.ID.BlobSize();
            Active.emplace_direct(ctx, item.ID, TActiveItem{ item.Load, item.Cookie });
            Queue.pop_front();

            if (ActiveBytesInFly > Config.MaxBytesInFly) {
                break;
            }
        }

        if (newBlobs) {
            LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TABLET_EXECUTOR, TLogPrefix{ Config }
                << "sending TEvGet batch " << batchSize
                << " bytes, " << ActiveBytesInFly
                << " total, blobs: " << TDumpLogoBlobs{ newBlobs });
            TArrayHolder<TEvBlobStorage::TEvGet::TQuery> query(new TEvBlobStorage::TEvGet::TQuery[newBlobs.size()]);
            for (size_t i = 0; i < newBlobs.size(); ++i) {
                query[i].Set(newBlobs[i]);
            }
            SendToBSProxy(sender, MakeBlobStorageProxyID(group),
                new TEvBlobStorage::TEvGet(query, newBlobs.size(), TInstant::Max(),
                    Config.ReadPrio));
        }
    }

    while (Queue) {
        TActive::insert_ctx ctx;
        auto& item = Queue.front();
        if (Active.find(item.ID, ctx) == Active.end()) {
            break;
        }
        Active.emplace_direct(ctx, item.ID, TActiveItem{ item.Load, item.Cookie });
        Queue.pop_front();
    }

    return !Active.empty();
}

bool TLoadBlobQueue::ProcessResult(TEvBlobStorage::TEvGetResult* msg) {
    if (msg->Status != NKikimrProto::OK) {
        LOG_ERROR_S(*TlsActivationContext, NKikimrServices::TABLET_EXECUTOR, TLogPrefix{ Config }
            << "EvGet failed, request: " << msg->Print(false));

        if (msg->Status == NKikimrProto::NODATA) {
            if (Config.NoDataCounter) {
                Config.NoDataCounter->Inc();
            }
        }

        return false;
    }

    auto* resBegin = msg->Responses.Get();
    auto* resEnd = resBegin + msg->ResponseSz;
    for (auto* x = resBegin; x != resEnd; ++x) {
        if (x->Status != NKikimrProto::OK) {
            LOG_ERROR_S(*TlsActivationContext, NKikimrServices::TABLET_EXECUTOR, TLogPrefix{ Config }
                << "EvGet failed for blob " << x->Id << ", request: " << msg->Print(false));

            if (x->Status == NKikimrProto::NODATA) {
                if (Config.NoDataCounter) {
                    Config.NoDataCounter->Inc();
                }
            }

            return false;
        }

        Y_ABORT_UNLESS(x->Shift == 0, "Got blob read result with non-zero offset");

        const auto p = Active.equal_range(x->Id);
        if (p.first == p.second) {
            // Could be blob requested by a previous instance
            continue;
        }

        ActiveBytesInFly -= x->Id.BlobSize();

        TString buffer = x->Buffer.ConvertToString();

        if (std::next(p.first) == p.second) {
            // Common case: unique load of the blob
            auto item = p.first->second;
            Active.erase(p.first);
            item.Load->OnBlobLoaded(x->Id, std::move(buffer), item.Cookie);
        } else {
            // May rarely happen: concurrent load of the same blob
            TVector<TActiveItem> items;
            for (auto it = p.first; it != p.second; ++it) {
                items.push_back(it->second);
            }
            Active.erase(p.first, p.second);
            Y_ABORT_UNLESS(items.size() > 1);

            size_t last = items.size() - 1;
            for (size_t i = 0; i < last; ++i) {
                // We have to make a copy of the buffer
                items[i].Load->OnBlobLoaded(x->Id, buffer, items[i].Cookie);
            }
            // Last load may consume our buffer
            items[last].Load->OnBlobLoaded(x->Id, std::move(buffer), items[last].Cookie);
        }
    }

    return true;
}

}
}
