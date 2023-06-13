#include <ydb/core/metering/metering.h>
#include <library/cpp/json/json_writer.h>
#include <util/generic/size_literals.h>
#include "metering_sink.h"


namespace NKikimr::NPQ {

std::atomic<ui64> TMeteringSink::MeteringCounter_{0};

bool TMeteringSink::Create(TInstant now, const TMeteringSink::TParameters& p,
                           const TSet<EMeteringJson>& whichToFlush,
                           std::function<void(TString)> howToFlush) {
    MayFlushForcibly(now);
    if (p.PartitionsSize == 0) {
        Created_ = false;
    } else {
        for (auto which : whichToFlush) {
            LastFlush_[which] = now;
        }
        CurrentPutUnitsQuantity_ = 0;
        CurrentUsedStorage_ = 0;

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
    case EMeteringJson::UsedStorageV1:
        CurrentUsedStorage_ += inc;
        return CurrentUsedStorage_;

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
                                       TInstant start, TInstant end, TInstant now, const TString& version) {
    MeteringCounter_.fetch_add(1);
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
                 "-" << MeteringCounter_.load());
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

    writer.Write("version", version);
    writer.Write("source_id", Parameters_.TabletId);
    writer.Write("source_wt", now.Seconds());
    writer.CloseMap();
    writer.Flush();
    output << Endl;
    return output.Str();
}


void TMeteringSink::Flush(TInstant now, bool force) {

    for (auto whichOne : WhichToFlush_) {
        bool needFlush = force;
        TString units;
        TString schema;
        TString name;

        switch (whichOne) {
        case EMeteringJson::UsedStorageV1: {
            units = "byte*second";
            schema = "ydb.serverless.v1";
            name = "used_storage";

            needFlush |= IsTimeToFlush(now, LastFlush_[whichOne]);
            if (!needFlush) {
                break;
            }
            ui64 duration = (now - LastFlush_[whichOne]).MilliSeconds();
            ui64 avgUsage = CurrentUsedStorage_ * 1_MB * 1000 / duration;
            CurrentUsedStorage_ = 0;
            const THashMap<TString, ui64> tags = {
                {"ydb_size", avgUsage}
            };


            auto interval = TInstant::Hours(LastFlush_[whichOne].Hours()) + Parameters_.FlushLimit;
            while (interval < now) {
                const auto metricsJson = GetMeteringJson(
                    name, schema, tags, "byte*second",
                    (now - LastFlush_[whichOne]).Seconds(),
                    LastFlush_[whichOne], interval, now, "1.0.0");
                LastFlush_[whichOne] = interval;
                FlushFunction_(metricsJson);
                interval += Parameters_.FlushLimit;
            }
            if (LastFlush_[whichOne] < now) {
                const auto metricsJson = GetMeteringJson(
                    name, schema, tags, "byte*second",
                    (now - LastFlush_[whichOne]).Seconds(),
                    LastFlush_[whichOne], now, now, "1.0.0");
                LastFlush_[whichOne] = now;
                FlushFunction_(metricsJson);
            }
        }
        break;



        case EMeteringJson::PutEventsV1: {
            units = "put_units";
            schema = "yds.events.puts.v1";
            name = "put_events";

            auto& putUnits = CurrentPutUnitsQuantity_;

            needFlush |= IsTimeToFlush(now, LastFlush_[whichOne]);
            if (!needFlush) {
                break;
            }
            const auto isTimeToFlushUnits = now.Hours() > LastFlush_[whichOne].Hours();
            if (isTimeToFlushUnits || needFlush) {
                if (putUnits > 0) {
                    // If we jump over a hour edge, report requests metrics for a previous hour
                    const TInstant requestsEndTime = isTimeToFlushUnits
                        ? TInstant::Hours(LastFlush_[whichOne].Hours() + 1) : now;

                    const auto record = GetMeteringJson(
                        units, schema, {}, name, putUnits,
                        LastFlush_[whichOne], requestsEndTime, now);
                    FlushFunction_(record);
                }
                putUnits = 0;
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
                {"reserved_consumers_count", Parameters_.ConsumersCount},
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
                {"reserved_consumers_count", Parameters_.ConsumersCount}
            };
            auto interval = TInstant::Hours(LastFlush_[whichOne].Hours()) + Parameters_.FlushLimit;

            auto tryFlush = [&](TInstant start, TInstant finish) {
                const auto metricsJson = GetMeteringJson(
                    name, schema, tags, "second",
                    Parameters_.PartitionsSize * (finish.Seconds() - start.Seconds()),
                    start, finish, now);
                FlushFunction_(metricsJson);
                LastFlush_[whichOne] = finish;
            };

            while (interval < now) {
                tryFlush(LastFlush_[whichOne], interval);
                interval += Parameters_.FlushLimit;
            }
            if (LastFlush_[whichOne] < now) {
                tryFlush(LastFlush_[whichOne], now);
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
            
            auto tryFlush = [&](TInstant start, TInstant finish) {
                const auto metricsJson = GetMeteringJson(
                    name, schema, {}, "mbyte*second",
                    Parameters_.PartitionsSize * (Parameters_.ReservedSpace / 1_MB) *
                    (finish.Seconds() - start.Seconds()),
                    start, finish, now);
                FlushFunction_(metricsJson);
                LastFlush_[whichOne] = finish;
            };

            while (interval < now) {
                tryFlush(LastFlush_[whichOne], interval);
                interval += Parameters_.FlushLimit;
            }
            if (LastFlush_[whichOne] < now) {
                tryFlush(LastFlush_[whichOne], now);
            }
        }
        break;

        default:
            Y_VERIFY(false);
        }
    }
}

ui64 TMeteringSink::GetMeteringCounter() const {
    return MeteringCounter_.load();
}

bool TMeteringSink::IsTimeToFlush(TInstant now, TInstant last) const {
    return (now - last) >= Parameters_.FlushInterval;
}

} // namespace NKikimr::NPQ
