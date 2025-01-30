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

TString TMeteringSink::GetMeteringJson(const TMeteringSink::FlushParameters& parameters, ui64 quantity,
                                       TInstant start, TInstant end, TInstant now) {
    MeteringCounter_.fetch_add(1);

    TStringStream output;
    NJson::TJsonWriter writer(&output, false);

    writer.OpenMap();

    writer.Write("cloud_id", Parameters_.YcCloudId);
    writer.Write("folder_id", Parameters_.YcFolderId);
    writer.Write("resource_id", Parameters_.ResourceId);
    writer.Write("id", TStringBuilder() << parameters.Name <<
                 "-" << Parameters_.YdbDatabaseId <<
                 "-" << Parameters_.TabletId <<
                 "-" << start.MilliSeconds() <<
                 "-" << GetMeteringCounter());
    writer.Write("schema", parameters.Schema);

    writer.OpenMap("tags");
    for (const auto& [tag, value] : parameters.Tags) {
        writer.Write(tag, value);
    }
    writer.CloseMap(); // "tags"

    writer.OpenMap("usage");
    writer.Write("quantity", quantity);
    writer.Write("unit", parameters.Units);
    writer.Write("start", start.Seconds());
    writer.Write("finish", end.Seconds());
    writer.CloseMap(); // "usage"

    writer.OpenMap("labels");
    writer.Write("datastreams_stream_name", Parameters_.StreamName);
    writer.Write("ydb_database", Parameters_.YdbDatabaseId);
    writer.CloseMap(); // "labels"

    writer.Write("version", parameters.Version);
    writer.Write("source_id", Parameters_.TabletId);
    writer.Write("source_wt", now.Seconds());
    writer.CloseMap();
    writer.Flush();
    output << Endl;
    return output.Str();
}


const TMeteringSink::FlushParameters TMeteringSink::GetFlushParameters(const EMeteringJson type, const TInstant& now, const TInstant& lastFlush) {
    switch (type) {
    case EMeteringJson::PutEventsV1: {
        ui64 putUnits = CurrentPutUnitsQuantity_;

        CurrentPutUnitsQuantity_ = 0;

        return TMeteringSink::FlushParameters(
            "put_units",
            "yds.events.puts.v1",
            "put_events",
            putUnits
        ).withOneFlush();
    }

    case EMeteringJson::ResourcesReservedV1: {
        return TMeteringSink::FlushParameters(
            "reserved_resources",
            "yds.resources.reserved.v1",
            "second",
            Parameters_.PartitionsSize
        ).withTags({
                {"reserved_throughput_bps", Parameters_.WriteQuota},
                {"reserved_consumers_count", Parameters_.ConsumersCount},
                {"reserved_storage_bytes", Parameters_.ReservedSpace}
            });
    }

    case EMeteringJson::ThroughputV1: {
        return TMeteringSink::FlushParameters(
            "yds.reserved_resources",
            "yds.throughput.reserved.v1",
            "second",
            Parameters_.PartitionsSize
        ).withTags({
                {"reserved_throughput_bps", Parameters_.WriteQuota},
                {"reserved_consumers_count", Parameters_.ConsumersCount}
            });
    }

    case EMeteringJson::StorageV1: {
        return TMeteringSink::FlushParameters(
            "yds.reserved_resources",
            "yds.storage.reserved.v1",
            "mbyte*second",
            Parameters_.PartitionsSize * (Parameters_.ReservedSpace / 1_MB)
        );
    }

    case EMeteringJson::UsedStorageV1: {
        ui64 duration = (now - lastFlush).MilliSeconds();
        ui64 avgUsage = duration > 0 ? CurrentUsedStorage_ * 1_MB * 1000 / duration : 0;

        CurrentUsedStorage_ = 0;

        return TMeteringSink::FlushParameters(
            "used_storage",
            "ydb.serverless.v1",
            "byte*second"
        ).withTags({
                {"ydb_size", avgUsage}
            })
        .withVersion("1.0.0");
    }

    default:
        Y_ABORT_UNLESS(false);
    };
}


void TMeteringSink::Flush(TInstant now, bool force) {

    for (auto whichOne : WhichToFlush_) {
        auto& lastFlush = LastFlush_[whichOne];
        bool needFlush = force || IsTimeToFlush(now, lastFlush);
        if (!needFlush) {
            continue;
        }

        auto parameters = GetFlushParameters(whichOne, now, lastFlush);

        if (parameters.OneFlush) {
            const auto isTimeToFlushUnits = now.Hours() > lastFlush.Hours();
            if (parameters.Quantity > 0) {
                // If we jump over a hour edge, report requests metrics for a previous hour
                const TInstant requestsEndTime = isTimeToFlushUnits
                    ? TInstant::Hours(lastFlush.Hours() + 1) : now;

                const auto record = GetMeteringJson(
                    parameters,
                    parameters.Quantity,
                    lastFlush,
                    requestsEndTime,
                    now);
                FlushFunction_(record);
            }

            lastFlush = now;
        } else {
            auto interval = TInstant::Hours(lastFlush.Hours()) + Parameters_.FlushLimit;

            auto tryFlush = [&](TInstant start, TInstant finish) {
                const auto metricsJson = GetMeteringJson(
                    parameters,
                    parameters.Quantity * (finish.Seconds() - start.Seconds()),
                    start,
                    finish,
                    now);
                FlushFunction_(metricsJson);

                lastFlush = finish;
            };

            while (interval < now) {
                tryFlush(lastFlush, interval);
                interval += Parameters_.FlushLimit;
            }
            if (lastFlush < now) {
                tryFlush(lastFlush, now);
            }
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
