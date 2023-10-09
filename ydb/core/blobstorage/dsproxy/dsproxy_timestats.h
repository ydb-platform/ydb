#pragma once

#include "defs.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/blobstorage.h>

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/generic/queue.h>
#include <util/datetime/parser.h>

namespace NKikimr {

class TBlobStorageGroupProxyTimeStats {
    template<typename TItem>
    class TTimeSeries {
        using TQueueItem = std::pair<TInstant, TItem>;

        struct TCompare {
            bool operator ()(const TQueueItem& x, const TQueueItem& y) const {
                return x.first < y.first;
            }
        };

        TDuration& AggrTime;
        TDeque<TQueueItem> Queue;

    public:
        TTimeSeries(TDuration& aggrTime)
            : AggrTime(aggrTime)
        {}

        void Add(TInstant now, TItem&& item) {
            Queue.emplace_back(now, std::move(item));
            Update(now);
        }

        void Update(TInstant now) {
            while (!Queue.empty() && Queue.front().first + AggrTime < now) {
                Queue.pop_front();
            }
        }

        void MergeIn(TTimeSeries&& other) {
            if (!other.Queue.empty()) {
                TDeque<TQueueItem> res;
                std::set_union(Queue.begin(), Queue.end(), other.Queue.begin(), other.Queue.end(),
                        std::back_inserter(res), TCompare());
                Queue.swap(res);
            }
        }

        const TDeque<TQueueItem>& GetVector() const {
            return Queue;
        }
    };

    struct TPutInfo {
        TDuration InSenderQueue;
        TDuration Total;
        TDuration InQueue;
        TDuration Execution;
        TDuration RTT;
    };

    struct THugePutInfo : public TPutInfo {
        TDuration HugeWriteTime;
    };

    template<typename T>
    struct TCompareTimeSeries {
        bool operator ()(ui32 key, const std::pair<ui32, T>& value) const {
            return key < value.first;
        }
    };

private:
    TDuration AggrTime;
    double Percentile;
    bool Enabled;
    TVector<std::pair<ui32, TTimeSeries<TPutInfo>>> Puts;
    TVector<std::pair<ui32, TTimeSeries<THugePutInfo>>> HugePuts;

public:
    TBlobStorageGroupProxyTimeStats(TDuration aggrTime = TDuration::Seconds(5))
        : AggrTime(aggrTime)
        , Percentile(0.9)
        , Enabled(false)
    {
        for (ui32 size : {0, 128, 1024, 4096, 65536, 1048576}) {
            Puts.emplace_back(size, aggrTime);
        }
        for (ui32 sizeKB : {0, 512, 1024, 2048, 3072, 4096, 5120, 6144, 7168}) {
            HugePuts.emplace_back(sizeKB << 10, aggrTime);
        }
    }

    void ApplyPut(ui32 size, const NKikimrBlobStorage::TExecTimeStats& execTimeStats) {
        TInstant submitTimestamp = TInstant::MicroSeconds(execTimeStats.GetSubmitTimestamp());
        TInstant now = TAppData::TimeProvider->Now();
        TDuration rtt = now - submitTimestamp;

        THugePutInfo info;
        info.InSenderQueue = TDuration::MicroSeconds(execTimeStats.GetInSenderQueue());
        info.Total = TDuration::MicroSeconds(execTimeStats.GetTotal());
        info.InQueue = TDuration::MicroSeconds(execTimeStats.GetInQueue());
        info.Execution = TDuration::MicroSeconds(execTimeStats.GetExecution());
        info.RTT = rtt;
        info.HugeWriteTime = TDuration::MicroSeconds(execTimeStats.GetHugeWriteTime());

        if (execTimeStats.HasHugeWriteTime()) {
            auto it = std::upper_bound(HugePuts.begin(), HugePuts.end(), size, TCompareTimeSeries<TTimeSeries<THugePutInfo>>());
            Y_ABORT_UNLESS(it != HugePuts.begin());
            --it;
            it->second.Add(now, std::move(info));
        } else {
            auto it = std::upper_bound(Puts.begin(), Puts.end(), size, TCompareTimeSeries<TTimeSeries<TPutInfo>>());
            Y_ABORT_UNLESS(it != Puts.begin());
            --it;
            it->second.Add(now, std::move(info));
        }
    }

    void MergeIn(TBlobStorageGroupProxyTimeStats&& timeStats) {
        Y_ABORT_UNLESS(Puts.size() == timeStats.Puts.size());
        for (auto it1 = Puts.begin(), it2 = timeStats.Puts.begin(); it1 != Puts.end(); ++it1, ++it2) {
            it1->second.MergeIn(std::move(it2->second));
        }
        Y_ABORT_UNLESS(HugePuts.size() == timeStats.HugePuts.size());
        for (auto it1 = HugePuts.begin(), it2 = timeStats.HugePuts.begin(); it1 != HugePuts.end(); ++it1, ++it2) {
            it1->second.MergeIn(std::move(it2->second));
        }
    }

    void Render(IOutputStream& str) {
        const TInstant now = TAppData::TimeProvider->Now();

#define PARAM(NAME, V) \
            do { \
                TABLER() { \
                    TABLED() { str << "<b>" << #NAME << "</b>"; } \
                    for (auto it = V.begin(); it != V.end(); ++it) { \
                        auto& series = it->second; \
                        series.Update(now); \
                        const auto& v = series.GetVector(); \
                        using T = decltype(std::declval<std::decay<decltype(v)>::type::value_type::second_type>().NAME); \
                        TVector<T> values; \
                        values.reserve(v.size()); \
                        for (const auto& item : v) { \
                            values.push_back(item.second.NAME); \
                        } \
                        std::sort(values.begin(), values.end()); \
                        TABLED() { \
                            if (values) { \
                                size_t index = perc * values.size(); \
                                if (index >= values.size()) { \
                                    index = values.size() - 1; \
                                } \
                                str << values[index]; \
                            } else { \
                                str << "none"; \
                            } \
                        } \
                    } \
                } \
            } while (false)
        double perc = Percentile;
        HTML(str) {
            DIV_CLASS("panel panel-info") {
                DIV_CLASS("panel-heading") {
                    str << "Time accounting setup";
                }
                DIV_CLASS("panel-body") {
                    FORM_CLASS("form-horizontal") {
                        DIV_CLASS("control-group") {
                            DIV_CLASS("controls") {
                                LABEL_CLASS("checkbox") {
                                    str << "<input type=\"checkbox\" name=\"enabled\"";
                                    if (Enabled) {
                                        str << " checked=\"checked\"";
                                    }
                                    str << ">Enabled</input>";
                                }
                            }
                        }
                        DIV_CLASS("control-group") {
                            LABEL_CLASS_FOR("control-label", "inputAggrTime") { str << "Aggregation time"; }
                            DIV_CLASS("controls") {
                                str << "<input id=\"inputAggrTime\" name=\"aggrTime\" type=\"text\" value=\"" << AggrTime << "\"/>";
                            }
                        }
                        DIV_CLASS("control-group") {
                            LABEL_CLASS_FOR("control-label", "inputPercentile") { str << "Percentile"; }
                            DIV_CLASS("controls") {
                                str << "<input id=\"inputPercentile\" name=\"percentile\" type=\"text\" value=\"" << Percentile << "\"/>";
                            }
                        }
                        DIV_CLASS("control-group") {
                            DIV_CLASS("controls") {
                                str << "<button type=\"submit\" name=\"submit_timestats\" class=\"btn btn-default\">Submit</button>";
                            }
                        }
                    }
                }
            }
            DIV_CLASS("panel panel-info") {
                DIV_CLASS("panel-heading") {
                    str << "Put stats percentile# " << perc;
                }
                DIV_CLASS("panel-body") {
                    TABLE_CLASS ("table table-condensed") {
                        TABLEHEAD() {
                            TABLER() {
                                TABLEH() { str << "size"; }
                                for (auto it = Puts.begin(); it != Puts.end(); ++it) {
                                    TABLEH() {
                                        str << it->first << "+";
                                    }
                                }
                            }
                        }
                        TABLEBODY() {
                            PARAM(InSenderQueue, Puts);
                            PARAM(Total, Puts);
                            PARAM(InQueue, Puts);
                            PARAM(Execution, Puts);
                            PARAM(RTT, Puts);
                        }
                    }
                }
            }
            DIV_CLASS("panel panel-info") {
                DIV_CLASS("panel-heading") {
                    str << "HugePut stats percentile# " << perc;
                }
                DIV_CLASS("panel-body") {
                    TABLE_CLASS ("table table-condensed") {
                        TABLEHEAD() {
                            TABLER() {
                                TABLEH() { str << "size"; }
                                for (auto it = HugePuts.begin(); it != HugePuts.end(); ++it) {
                                    TABLEH() {
                                        str << it->first << "+";
                                    }
                                }
                            }
                        }
                        TABLEBODY() {
                            PARAM(InSenderQueue, HugePuts);
                            PARAM(Total, HugePuts);
                            PARAM(InQueue, HugePuts);
                            PARAM(Execution, HugePuts);
                            PARAM(RTT, HugePuts);
                            PARAM(HugeWriteTime, HugePuts);
                        }
                    }
                }
            }
        }

    }

    template<typename TCgiParameters>
    void Submit(const TCgiParameters& cgi) {
        Enabled = cgi.Has("enabled");
        if (cgi.Has("aggrTime")) {
            TString param = cgi.Get("aggrTime");
            TDurationParser parser;
            if (parser.ParsePart(param.data(), param.size())) {
                AggrTime = parser.GetResult(AggrTime);
            }
        }
    }

    bool IsEnabled() const {
        return Enabled;
    }
};

struct TEvTimeStats : public TEventLocal<TEvTimeStats, TEvBlobStorage::EvTimeStats> {
    TBlobStorageGroupProxyTimeStats TimeStats;

    TEvTimeStats(TBlobStorageGroupProxyTimeStats&& timeStats)
        : TimeStats(std::move(timeStats))
    {}
};

} // NKikimr
