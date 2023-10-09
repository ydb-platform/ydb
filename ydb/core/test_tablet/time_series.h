#pragma once

namespace NKikimr {

    class TTimeSeries {
        const TDuration Window;
        std::deque<std::tuple<TMonotonic, TDuration>> Queue;

    public:
        TTimeSeries(TDuration window)
            : Window(window)
        {}

        void Add(TMonotonic now, TDuration value) {
            Queue.emplace_back(now, value);
            auto it = std::upper_bound(Queue.begin(), Queue.end(), std::make_tuple(now - Window, TDuration::Zero()));
            Queue.erase(Queue.begin(), it);
        }

        std::vector<TDuration> Percentiles(const std::vector<double>& ps) const {
            std::vector<TDuration> q;
            q.reserve(Queue.size());
            for (const auto& [_, duration] : Queue) {
                q.push_back(duration);
            }
            std::sort(q.begin(), q.end());
            std::vector<TDuration> res;
            res.reserve(ps.size());
            for (double p : ps) {
                if (q.empty()) {
                    res.emplace_back();
                } else {
                    const size_t index = Min<size_t>(q.size() - 1, static_cast<size_t>((q.size() - 0.5) * p + 0.5));
                    res.push_back(q[index]);
                }
            }
            return res;
        }

        std::vector<ui32> Intervals(const std::vector<TDuration>& ps) const {
            std::vector<ui32> res(ps.size());
            for (const auto& [_, duration] : Queue) {
                const size_t index = std::lower_bound(ps.begin(), ps.end(), duration) - ps.begin();
                Y_ABORT_UNLESS(index < ps.size());
                ++res[index];
            }
            return res;
        }
    };

    class TSpeedMeter {
        const TDuration Window;
        ui64 BytesAccum = 0;
        std::deque<std::pair<TInstant, ui64>> BytesQ;

    public:
        TSpeedMeter(TDuration window)
            : Window(window)
        {}

        void Add(TInstant ts, ui64 numBytes) {
            BytesAccum += numBytes;
            BytesQ.emplace_back(ts, BytesAccum);
            Trim(ts);
        }

        ui64 GetSpeedInBytesPerSecond(TInstant now, size_t *numPoints, TDuration *timeSpan) {
            Trim(now);
            if (BytesQ.empty()) {
                return 0;
            }
            const auto& [ts, ba] = BytesQ.front();
            if (numPoints) {
                *numPoints = BytesQ.size();
            }
            if (timeSpan) {
                *timeSpan = now - ts;
            }
            return ts != now ? (ui64)1000000 * (BytesAccum - ba) / (now - ts).MicroSeconds() : 0;
        }

        ui64 GetBytesAccum() const {
            return BytesAccum;
        }

    private:
        void Trim(TInstant now) {
            const TInstant barrier = now - Window;
            while (!BytesQ.empty()) {
                auto& [ts0, ba0] = BytesQ.front();
                if (ts0 >= barrier) {
                    break;
                } else if (BytesQ.size() == 1) { // ts[0] < barrier AND only one item in queue -- linear interpolation
                    ba0 += (BytesAccum - ba0) * (barrier - ts0).GetValue() / (now - ts0).GetValue();
                } else if (const auto& [ts1, ba1] = BytesQ[1]; barrier < ts1) { // ts[0] < barrier < ts[1]
                    ba0 += (ba1 - ba0) * (barrier - ts0).GetValue() / (ts1 - ts0).GetValue();
                } else { // ts[0] < ts[1] < barrier
                    BytesQ.pop_front();
                    continue;
                }
                ts0 = barrier;
                break;
            }
        }
    };

} // NKikimr
