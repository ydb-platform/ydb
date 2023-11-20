#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_pdiskctx.h>

#include <util/generic/queue.h>

namespace NKikimr {

    template<typename TValue>
    class TRangeSet {
        using TRange = std::pair<TValue, TValue>;

        TVector<TRange> Ranges;
        TValue Count = TValue();

    public:
        void Set(TValue first, TValue last) {
            Y_DEBUG_ABORT_UNLESS(first <= last);

            auto begin = std::lower_bound(Ranges.begin(), Ranges.end(), first,
                    [](const TRange& x, const TValue& y) { return x.second < y; });

            auto end = std::lower_bound(Ranges.begin(), Ranges.end(), last,
                    [](const TRange& x, const TValue& y) { return x.second < y; });

            for (auto it = begin; it != end; ++it) {
                Count -= it->second - it->first;
            }

            if (begin != Ranges.end() && begin->first <= first) {
                Y_DEBUG_ABORT_UNLESS(first <= begin->second);
                first = begin->first;
                begin->second = Max(begin->second, last);
            }

            if (end != Ranges.end() && end->first <= last) {
                Y_DEBUG_ABORT_UNLESS(last <= end->second);
                Count -= end->second - end->first;
                end->first = Min(end->first, first);
                last = end->second;
                ++end;
            }

            Count += last - first;
            if (begin < end) {
                *begin++ = TRange(first, last);
                Ranges.erase(begin, end);
            } else {
                Ranges.emplace(end, first, last);
            }
        }

        TValue GetCount() const {
            return Count;
        }

        TRange GetEnclosingRange() const {
            return Ranges ? TRange(Ranges.front().first, Ranges.back().second) : TRange(0, 0);
        }

        template<typename TBitMap>
        void ToBitMap(TBitMap& bm) {
            TValue prev;
            for (size_t i = 0; i < Ranges.size(); ++i) {
                const TRange& r = Ranges[i];
                Y_DEBUG_ABORT_UNLESS(i == 0 || prev < r.first);
                prev = r.second;
                bm.Set(r.first, r.second);
            }
        }
    };

    template<typename TPayload>
    class TCompactReadBatcher {
        struct TReadItem {
            TChunkIdx ChunkIdx;
            ui32 Offset;
            ui32 Size;
            TPayload Payload;
            ui64 Serial;
            NKikimrProto::EReplyStatus Status;
            TRcBuf Content;
        };
        using TReadQueue = TDeque<TReadItem>;
        TReadQueue ReadQueue;
        ui64 NextSerial = 0;

        typename TReadQueue::iterator Iterator;

        TMap<void *, std::pair<typename TReadQueue::iterator, typename TReadQueue::iterator>> ActiveRequests;
        ui64 NextRequestCookie = 1;

        struct TCompareReadItem {
            bool operator ()(const typename TReadQueue::iterator x, const typename TReadQueue::iterator y) const {
                return x->Serial > y->Serial;
            }
        };
        TPriorityQueue<typename TReadQueue::iterator, TVector<typename TReadQueue::iterator>, TCompareReadItem> ReadyItemQueue;
        ui64 NextReadySerial = 0;

        bool Started = false;

        const ui32 MaxReadBlockSize;
        const ui32 SeekCostInBytes;
        const double EfficiencyThreshold;

    public:
        TCompactReadBatcher(ui32 maxReadBlockSize, ui32 seekCostInBytes, double efficiencyThreshold)
            : MaxReadBlockSize(maxReadBlockSize)
            , SeekCostInBytes(seekCostInBytes)
            , EfficiencyThreshold(efficiencyThreshold)
        {}

        // enqueue read item -- a read from specific chunk at desired position and length; function returns serial
        // number of this request; all results are then reported sequently in ascending order of returned serial
        ui64 AddReadItem(TChunkIdx chunkIdx, ui32 offset, ui32 size, TPayload&& payload) {
            const ui64 serial = NextSerial++;
            ReadQueue.push_back(TReadItem{chunkIdx, offset, size, std::move(payload), serial, NKikimrProto::UNKNOWN, {}});
            return serial;
        }

        // start batcher operation; no AddReadItem requests allowed beyond this point
        void Start() {
            Y_ABORT_UNLESS(!Started);
            Started = true;
            Iterator = ReadQueue.begin();
        }

        // generate new message to send to yard; batcher may generate as much messages as requested, it's job of caller
        // to control maximum number of in flight messages; return nullptr if there is nothing to read more
        std::unique_ptr<NPDisk::TEvChunkRead> GetPendingMessage(NPDisk::TOwner owner, NPDisk::TOwnerRound ownerRound,
                ui8 priorityClass) {
            if (!Started || Iterator == ReadQueue.end()) {
                return nullptr;
            }

            // chunk for which we are batching reads
            const TChunkIdx chunkIdx = Iterator->ChunkIdx;

            // range set of chunk we are going to fetch
            TRangeSet<ui32> range;

            // vector of items we are _NOT_ going to read now
            TVector<TReadItem> otherItems;

            // iterator where we store enqueued items
            typename TReadQueue::iterator outIt = Iterator;

            // the range we are going to read
            std::pair<ui32, ui32> enclosingRange;

            // total number of bytes we are going to read
            ui32 totalBytes = 0;

            // current position
            typename TReadQueue::iterator it;
            for (it = Iterator; it != ReadQueue.end(); ++it) {
                if (it->ChunkIdx == chunkIdx) {
                    // try to add this item to range and calculate parameters
                    range.Set(it->Offset, it->Offset + it->Size);
                    const std::pair<ui32, ui32> newEnclosingRange = range.GetEnclosingRange();

                    // calculate new number of bytes we are going to read in single request; if this value exceeds
                    // maximum block size, stop generating request (unless read queue is totally empty)
                    const ui32 newTotalBytes = newEnclosingRange.second - newEnclosingRange.first;
                    Y_ABORT_UNLESS(newTotalBytes);
                    if (newTotalBytes > MaxReadBlockSize && outIt != Iterator) {
                        break;
                    }

                    // calculate number of effective bytes (bytes we are going to send to client) and calculate
                    // request efficiency; compare with threshold and break if it is way too low
                    const ui32 effectiveBytes = range.GetCount();
                    double efficiency = (double)effectiveBytes / newTotalBytes;
                    if (efficiency < EfficiencyThreshold) {
                        // this can't be the first item -- as the efficiency for single item is 1.0
                        Y_ABORT_UNLESS(outIt != Iterator);
                        break;
                    }

                    // calculate number of extra useless bytes we are going to read if we add this request and check
                    // if it is better to seek than to read these extra bytes
                    const i32 addedUselessBytes = newTotalBytes - (totalBytes + it->Size);
                    if (addedUselessBytes > 0 && (ui32)addedUselessBytes > SeekCostInBytes) {
                        // this can't be the first item too for the same reason as above
                        Y_ABORT_UNLESS(outIt != Iterator);
                        break;
                    }

                    // move item in place
                    *outIt++ = std::move(*it);
                    enclosingRange = newEnclosingRange;
                    totalBytes = newTotalBytes;
                } else {
                    otherItems.push_back(std::move(*it));
                }
            }

            ActiveRequests.emplace(reinterpret_cast<void *>(NextRequestCookie), std::make_pair(Iterator, outIt));

            // create message
            const ui32 offset = enclosingRange.first;
            const ui32 size = enclosingRange.second - enclosingRange.first;
            auto msg = std::make_unique<NPDisk::TEvChunkRead>(owner, ownerRound, chunkIdx, offset, size, priorityClass,
                    reinterpret_cast<void *>(NextRequestCookie));
            ++NextRequestCookie;

            // move stored items to their place and advance Iterator
            Y_DEBUG_ABORT_UNLESS(it - outIt == static_cast<ssize_t>(otherItems.size()));
            Iterator = outIt;
            std::move(otherItems.begin(), otherItems.end(), Iterator);

            return msg;
        }

        // apply read result
        void Apply(NPDisk::TEvChunkReadResult *msg) {
            auto& data = msg->Data;

            auto it = ActiveRequests.find(msg->Cookie);
            Y_ABORT_UNLESS(it != ActiveRequests.end());

            for (auto reqIt = it->second.first; reqIt != it->second.second; ++reqIt) {
                TReadItem& item = *reqIt;

                item.Status = msg->Status;
                if (msg->Status == NKikimrProto::OK) {
                    // ensure returned area covers this item
                    Y_ABORT_UNLESS(item.Offset >= msg->Offset && item.Offset + item.Size <= msg->Offset + data.Size());

                    // calculate offset of item inside response
                    const ui32 offset = item.Offset - msg->Offset;

                    // if item is not readable at this point, we return ERROR
                    if (!data.IsReadable(offset, item.Size)) {
                        item.Status = NKikimrProto::ERROR;
                    } else {
                        item.Content = data.Substr(offset, item.Size);
                    }
                }

                ReadyItemQueue.push(reqIt);
            }

            ActiveRequests.erase(it);
        }

        // try to get result item
        bool GetResultItem(ui64 *serial, TPayload *payload, NKikimrProto::EReplyStatus *status, TRcBuf *content) {
            if (!ReadyItemQueue) {
                return false;
            }

            TReadItem& item = *ReadyItemQueue.top();
            if (item.Serial != NextReadySerial) {
                return false;
            }
            ++NextReadySerial;

            *serial = item.Serial;
            *payload = std::move(item.Payload);
            *status = item.Status;
            *content = std::move(item.Content);

            ReadyItemQueue.pop();

            while (ReadQueue && ReadQueue.front().Serial < NextReadySerial) {
                ReadQueue.pop_front();
            }

            return true;
        }

        void Finish() {
            Started = false;
            Y_ABORT_UNLESS(NextSerial == NextReadySerial);
        }
    };

} // NKikimr
