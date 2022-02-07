#pragma once

#include "defs.h"

namespace NKikimr {

    class TDiskResponsivenessTracker {
    public:
        using TDiskId = ui32; // VDisk order number inside group

        struct TPerDiskStats : TThrRefBase {
            struct TInfo {
                TDuration Responsiveness;
                ui32 NumRequests;
                TInstant Since;
                TInstant Until;

                TString ToString() const {
                    const TInstant now = TInstant::Now();
                    const TDuration since = now - Since;
                    const TDuration until = now - Until;
                    return Sprintf("%s/%" PRIu32 "/%s-%s", Responsiveness.ToString().data(), (ui32)NumRequests, since.ToString().data(),
                        until.ToString().data());
                }
            };
            THashMap<TDiskId, TInfo> DiskData;
        };

        using TPerDiskStatsPtr = TIntrusivePtr<TPerDiskStats>;

    private:
        struct TItem {
            TInstant  Timestamp;
            TDuration Value;

            TItem(TInstant timestamp, TDuration value)
                : Timestamp(timestamp)
                , Value(value)
            {}
        };

        const ui32 MaxItems;
        const TDuration Window;
        THashMap<TDiskId, TDeque<TItem>> PerDiskQ;

    public:
        TDiskResponsivenessTracker(ui32 maxItems, TDuration window)
            : MaxItems(maxItems)
            , Window(window)
        {}

        void Register(const TDiskId &id, TInstant timestamp, TDuration value) {
            auto &q = PerDiskQ[id];
            if (q && timestamp < q.back().Timestamp) {
                timestamp = q.back().Timestamp;
            }
            q.emplace_back(timestamp, value);
            if (q.size() > MaxItems) {
                q.pop_front();
            }
        }

        TPerDiskStatsPtr Update(TInstant timestamp) {
            const TInstant barrier = timestamp - Window;
            TPerDiskStatsPtr result = MakeIntrusive<TPerDiskStats>();

            for (auto diskIt = PerDiskQ.begin(); diskIt != PerDiskQ.end(); ) {
                const TDiskId &id = diskIt->first;
                auto &q = diskIt->second;

                // find the 90-th percentile for this disk
                TVector<TDuration> values;
                values.reserve(q.size());
                for (const TItem &item : q) {
                    values.push_back(item.Value);
                }
                std::sort(values.begin(), values.end());
                TDuration value = TDuration::Zero();
                if (values) {
                    const size_t pos = values.size() * 9 / 10;
                    value = values[pos];
                }

                // clamp it to the maximum value of last 3 items
                auto qIter = q.rbegin();
                for (ui32 i = 0; i < 3 && qIter != q.rend(); ++i, ++qIter) {
                    if (qIter->Value > value) {
                        value = qIter->Value;
                    }
                }

                // store it in the result
                TPerDiskStats::TInfo info;
                info.Responsiveness = value;
                info.NumRequests = q.size();
                info.Since = q ? q.front().Timestamp : TInstant();
                info.Until = q ? q.back().Timestamp : TInstant();
                result->DiskData.emplace(id, info);

                // cut excessive items
                auto comp = [](const TItem &x, const TInstant &y) {
                    return x.Timestamp < y;
                };
                auto it = std::lower_bound(q.begin(), q.end(), barrier, comp);
                q.erase(q.begin(), it);

                // advance iterator and delete queue if it becomes empty
                auto current = diskIt;
                ++diskIt;
                if (q.empty()) {
                    PerDiskQ.erase(current);
                }
            }

            return result;
        }

        bool IsEmpty() const {
            return PerDiskQ.empty();
        }
    };

} // NKikimr
