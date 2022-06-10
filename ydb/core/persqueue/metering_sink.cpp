#include <ydb/core/metering/metering.h>
#include <library/cpp/json/json_writer.h>
#include <util/generic/size_literals.h>
#include "metering_sink.h"


namespace NKikimr::NPQ {

ui32 TMeteringSink::MeteringCounter_{0};

bool TMeteringSink::Create(TInstant now, const TMeteringSink::TParameters& p,
                           const TSet<EMeteringJson>& whichToFlush,
                           std::function<void(TString)> howToFlush) {
    if (p.PartitionsSize == 0) {
        Created_ = false;
    } else {
        if (!Created_) {
            for (auto which : whichToFlush) {
                LastFlush_[which] = now;
            }
        }
        CurrentPutUnitsQuantity_ = 0;
        Created_ = true;
        Parameters_ = p;
        WhichToFlush_ = whichToFlush;
        FlushFunction_ = howToFlush;
    }
    return Created_;
}

void TMeteringSink::MayFlush(TInstant now) {
    if (Created_) {
        Flush(now, false);
    }
}

void TMeteringSink::MayFlushForcibly(TInstant now) {
    if (Created_) {
        Flush(now, true);
    }
}

void TMeteringSink::Close() {
    Created_ = false;
}

ui64 TMeteringSink::IncreaseQuantity(EMeteringJson meteringJson, ui64 inc) {
    switch (meteringJson) {
    case EMeteringJson::PutEventsV1:
        CurrentPutUnitsQuantity_ += inc;
        return CurrentPutUnitsQuantity_;

    default:
        return 0;
    }
    return 0;
}

TMeteringSink::TParameters TMeteringSink::GetParameters() const {
    return Parameters_;
}

bool TMeteringSink::IsCreated() const {
    return Created_;
}

TString TMeteringSink::GetMeteringJson(const TString& metricBillingId, const TString& schemeName,
                                      const THashMap<TString, ui64>& tags,
                                      const TString& quantityUnit, ui64 quantity,
                                      TInstant start, TInstant end, TInstant now) {
    TStringStream output;
    NJson::TJsonWriter writer(&output, false);

    writer.OpenMap();

    writer.Write("cloud_id", Parameters_.YcCloudId);
    writer.Write("folder_id", Parameters_.YcFolderId);
    writer.Write("resource_id", Parameters_.ResourceId);
    writer.Write("id", TStringBuilder() << metricBillingId <<
                 "-" << Parameters_.YdbDatabaseId <<
                 "-" << Parameters_.TabletId <<
                 "-" << start.MilliSeconds() <<
                 "-" << (++MeteringCounter_));
    writer.Write("schema", schemeName);

    writer.OpenMap("tags");
    for (const auto& [tag, value] : tags) {
        writer.Write(tag, value);
    }
    writer.CloseMap(); // "tags"

    writer.OpenMap("usage");
    writer.Write("quantity", quantity);
    writer.Write("unit", quantityUnit);
    writer.Write("start", start.Seconds());
    writer.Write("finish", end.Seconds());
    writer.CloseMap(); // "usage"

    writer.OpenMap("labels");
    writer.Write("datastreams_stream_name", Parameters_.StreamName);
    writer.Write("ydb_database", Parameters_.YdbDatabaseId);
    writer.CloseMap(); // "labels"

    writer.Write("version", "v1");
    writer.Write("source_id", Parameters_.TabletId);
    writer.Write("source_wt", now.Seconds());
    writer.CloseMap();
    writer.Flush();
    output << Endl;
    return output.Str();
}


void TMeteringSink::Flush(TInstant now, bool force) {
    bool needFlush = force;

    for (auto whichOne : WhichToFlush_) {
        switch (whichOne) {
        case EMeteringJson::PutEventsV1: {
            needFlush |= IsTimeToFlush(now, LastFlush_[whichOne]);
            if (!needFlush) {
                break;
            }
            const auto isTimeToFlushUnits = now.Hours() > LastFlush_[whichOne].Hours();
            if (isTimeToFlushUnits || needFlush) {
                if (CurrentPutUnitsQuantity_ > 0) {
                    // If we jump over a hour edge, report requests metrics for a previous hour
                    const TInstant requestsEndTime = isTimeToFlushUnits
                        ? TInstant::Hours(LastFlush_[whichOne].Hours() + 1) : now;

                    const auto record = GetMeteringJson(
                        "put_units", "yds.events.puts.v1", {}, "put_events", CurrentPutUnitsQuantity_,
                        LastFlush_[whichOne], requestsEndTime, now);
                    FlushFunction_(record);
                }
                CurrentPutUnitsQuantity_ = 0;
                LastFlush_[whichOne] = now;
            }
        }
        break;

        case EMeteringJson::ResourcesReservedV1: {
            needFlush |= IsTimeToFlush(now, LastFlush_[whichOne]);
            if (!needFlush) {
                break;
            }
            const TString name = "reserved_resources";
            const TString schema = "yds.resources.reserved.v1";
            const THashMap<TString, ui64> tags = {
                {"reserved_throughput_bps", Parameters_.WriteQuota},
                {"shard_enhanced_consumers_throughput", Parameters_.ConsumersThroughput},
                {"reserved_storage_bytes", Parameters_.ReservedSpace}
            };
            auto interval = TInstant::Hours(LastFlush_[whichOne].Hours()) + Parameters_.FlushLimit;
            while (interval < now) {
                const auto metricsJson = GetMeteringJson(
                    name, schema, tags, "second",
                    Parameters_.PartitionsSize * (interval - LastFlush_[whichOne]).Seconds(),
                    LastFlush_[whichOne], interval, now);
                LastFlush_[whichOne] = interval;
                FlushFunction_(metricsJson);
                interval += Parameters_.FlushLimit;
            }
            if (LastFlush_[whichOne] < now) {
                const auto metricsJson = GetMeteringJson(
                    name, schema, tags, "second",
                    Parameters_.PartitionsSize * (now - LastFlush_[whichOne]).Seconds(),
                    LastFlush_[whichOne], now, now);
                LastFlush_[whichOne] = now;
                FlushFunction_(metricsJson);
            }
        }
        break;

        case EMeteringJson::ThroughputV1: {
            needFlush |= IsTimeToFlush(now, LastFlush_[whichOne]);
            if (!needFlush) {
                break;
            }
            const TString name = "yds.reserved_resources";
            const TString schema = "yds.throughput.reserved.v1";
            const THashMap<TString, ui64> tags = {
                {"reserved_throughput_bps", Parameters_.WriteQuota},
            };
            auto interval = TInstant::Hours(LastFlush_[whichOne].Hours()) + Parameters_.FlushLimit;
            while (interval < now) {
                const auto metricsJson = GetMeteringJson(
                    name, schema, tags, "second",
                    Parameters_.PartitionsSize * (interval - LastFlush_[whichOne]).Seconds(),
                    LastFlush_[whichOne], interval, now);
                LastFlush_[whichOne] = interval;
                FlushFunction_(metricsJson);
                interval += Parameters_.FlushLimit;
            }
            if (LastFlush_[whichOne] < now) {
                const auto metricsJson = GetMeteringJson(
                    name, schema, tags, "second",
                    Parameters_.PartitionsSize * (now - LastFlush_[whichOne]).Seconds(),
                    LastFlush_[whichOne], now, now);
                LastFlush_[whichOne] = now;
                FlushFunction_(metricsJson);
            }
        }
        break;

        case EMeteringJson::StorageV1: {
            needFlush |= IsTimeToFlush(now, LastFlush_[whichOne]);
            if (!needFlush) {
                break;
            }
            const TString name = "yds.reserved_resources";
            const TString schema = "yds.storage.reserved.v1";
            auto interval = TInstant::Hours(LastFlush_[whichOne].Hours()) + Parameters_.FlushLimit;
            while (interval < now) {
                const auto metricsJson = GetMeteringJson(
                    name, schema, {}, "mbyte*second",
                    Parameters_.PartitionsSize * (Parameters_.ReservedSpace / 1_MB) *
                    (now - LastFlush_[whichOne]).Seconds(),
                    LastFlush_[whichOne], interval, now);
                LastFlush_[whichOne] = interval;
                FlushFunction_(metricsJson);
                interval += Parameters_.FlushLimit;
            }
            if (LastFlush_[whichOne] < now) {
                const auto metricsJson = GetMeteringJson(
                    name, schema, {}, "mbyte*second",
                    Parameters_.PartitionsSize * (Parameters_.ReservedSpace / 1_MB) *
                    (now - LastFlush_[whichOne]).Seconds(),
                    LastFlush_[whichOne], now, now);
                LastFlush_[whichOne] = now;
                FlushFunction_(metricsJson);
            }
        }
        break;

        default:
            Y_VERIFY(false);
        }
    }
}

ui32 TMeteringSink::GetMeteringCounter() const {
    return MeteringCounter_;
}

bool TMeteringSink::IsTimeToFlush(TInstant now, TInstant last) const {
    return (now - last) >= Parameters_.FlushInterval;
}

} // namespace NKikimr::NPQ
