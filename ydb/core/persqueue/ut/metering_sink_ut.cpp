#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/metering/metering.h>
#include <ydb/core/persqueue/pqtablet/metering_sink.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <util/generic/size_literals.h>
#include <fmt/format.h>

namespace NKikimr::NPQ {

static TString ReformatJson(TStringBuf json) {
    TMemoryInput in(json);
    const auto value = NJson::ReadJsonTree(&in, true);
    return WriteJson(value, true, true, true);
}

Y_UNIT_TEST_SUITE(TMeteringSink) {

Y_UNIT_TEST(FlushPutEventsV1) {
    TString fullMetering;
    TMeteringSink meteringSink;
    const ui64 creationTs   = 1651752943168786;
    const ui64 emptyFlushTs = 1651753943168786;
    const ui64 flushTs      = 1651754943168786;
    meteringSink.Create(TInstant::FromValue(creationTs), {
            .FlushInterval = TDuration::Seconds(10),
            .TabletId = "tabletId",
            .YcCloudId = "cloudId",
            .YcFolderId = "folderId",
            .YdbDatabaseId = "databaseId",
            .StreamName = "streamName",
            .ResourceId = "streamPath",
            .PartitionsSize = 1,
        }, {EMeteringJson::PutEventsV1}, [&](TString json) {
            fullMetering = TStringBuilder() << fullMetering << '\n' << json;
        });

    meteringSink.MayFlush(TInstant::FromValue(emptyFlushTs));
    UNIT_ASSERT_VALUES_EQUAL(fullMetering.size(), 0);

    meteringSink.MayFlushForcibly(TInstant::FromValue(emptyFlushTs));
    UNIT_ASSERT_VALUES_EQUAL(fullMetering.size(), 0);

    const ui32 quantity = 10;
    meteringSink.IncreaseQuantity(EMeteringJson::PutEventsV1, quantity);
    meteringSink.MayFlushForcibly(TInstant::FromValue(flushTs));

    const TString referencePutUnitsJson = fmt::format(
        R"(
    {{
        "cloud_id": "cloudId",
        "folder_id": "folderId",
        "resource_id": "streamPath",
        "id": "put_units-databaseId-tabletId-{timestamp}-{counter}",
        "schema": "yds.events.puts.v1",
        "tags": {{
        }},
        "usage": {{
            "quantity": {quantity},
            "unit": "put_events",
            "start": {start},
            "finish": {finish}
        }},
        "labels": {{
            "datastreams_stream_name": "streamName",
            "ydb_database": "databaseId",
            "Category": "Topic"
        }},
        "version": "v1",
        "source_id": "tabletId",
        "source_wt": {finish}
    }})",
        fmt::arg("timestamp", emptyFlushTs / 1'000),
        fmt::arg("counter", meteringSink.GetMeteringCounter()),
        fmt::arg("quantity", quantity),
        fmt::arg("start", emptyFlushTs / 1'000'000),
        fmt::arg("finish", flushTs / 1'000'000)
    );

    UNIT_ASSERT_VALUES_EQUAL(ReformatJson(fullMetering), ReformatJson(referencePutUnitsJson));
}

Y_UNIT_TEST(FlushResourcesReservedV1) {
    TString fullMetering;
    TMeteringSink meteringSink;
    const ui64 creationTs = 1651752943168786;
    const ui64 flushTs    = 1651754943168786;
    const ui32 consumersCount = 10;
    const ui64 writeQuota = 512_KB;
    const ui64 reservedSpace = 10_MB;
    const ui32 partitions = 2;
    meteringSink.Create(TInstant::FromValue(creationTs), {
            .FlushInterval = TDuration::Seconds(10),
            .TabletId = "tabletId",
            .YcCloudId = "cloudId",
            .YcFolderId = "folderId",
            .YdbDatabaseId = "databaseId",
            .StreamName = "streamName",
            .ResourceId = "streamPath",
            .PartitionsSize = partitions,
            .WriteQuota = writeQuota,
            .ReservedSpace = reservedSpace,
            .ConsumersCount = consumersCount
        }, {EMeteringJson::ResourcesReservedV1}, [&](TString json) {
            fullMetering = TStringBuilder() << fullMetering << '\n' << json;
        });

    meteringSink.MayFlushForcibly(TInstant::FromValue(flushTs));
    UNIT_ASSERT_VALUES_UNEQUAL(fullMetering.size(), 0);

    const TString referenceResourcesReservedJson = fmt::format(
        R"(
    {{
        "cloud_id": "cloudId",
        "folder_id": "folderId",
        "resource_id": "streamPath",
        "id": "reserved_resources-databaseId-tabletId-{timestamp}-{counter}",
        "schema": "yds.resources.reserved.v1",
        "tags": {{
            "reserved_throughput_bps": {write_quota},
            "reserved_consumers_count": {consumers_count},
            "reserved_storage_bytes": {reserved_space}
        }},
        "usage": {{
            "quantity": {quantity},
            "unit": "second",
            "start": {start},
            "finish": {finish}
        }},
        "labels": {{
            "datastreams_stream_name": "streamName",
            "ydb_database": "databaseId",
            "Category": "Topic"
        }},
        "version": "v1",
        "source_id": "tabletId",
        "source_wt": {finish}
    }})",
        fmt::arg("timestamp", creationTs / 1'000),
        fmt::arg("counter", meteringSink.GetMeteringCounter()),
        fmt::arg("write_quota", writeQuota),
        fmt::arg("consumers_count", consumersCount),
        fmt::arg("reserved_space", reservedSpace),
        fmt::arg("quantity", partitions * (flushTs - creationTs) / 1'000'000),
        fmt::arg("start", creationTs / 1'000'000),
        fmt::arg("finish", flushTs / 1'000'000)
    );

    UNIT_ASSERT_VALUES_EQUAL(ReformatJson(fullMetering), ReformatJson(referenceResourcesReservedJson));
}

Y_UNIT_TEST(FlushThroughputV1) {
    TString fullMetering;
    TMeteringSink meteringSink;
    const ui64 creationTs = 1651752943168786;
    const ui64 flushTs    = 1651754943168786;
    const ui64 writeQuota = 348_KB;
    const ui32 partitions = 3;
    meteringSink.Create(TInstant::FromValue(creationTs), {
            .FlushInterval = TDuration::Seconds(10),
            .TabletId = "tabletId",
            .YcCloudId = "cloudId",
            .YcFolderId = "folderId",
            .YdbDatabaseId = "databaseId",
            .StreamName = "streamName",
            .ResourceId = "streamPath",
            .PartitionsSize = partitions,
            .WriteQuota = writeQuota,
        }, {EMeteringJson::ThroughputV1}, [&](TString json) {
            fullMetering = TStringBuilder() << fullMetering << '\n' << json;
        });

    meteringSink.MayFlushForcibly(TInstant::FromValue(flushTs));
    const TString referenceThrougputJson = fmt::format(
        R"(
    {{
        "cloud_id": "cloudId",
        "folder_id": "folderId",
        "resource_id": "streamPath",
        "id": "yds.reserved_resources-databaseId-tabletId-{timestamp}-{counter}",
        "schema": "yds.throughput.reserved.v1",
        "tags": {{
            "reserved_throughput_bps": {write_quota},
            "reserved_consumers_count": 0
        }},
        "usage": {{
            "quantity": {quantity},
            "unit": "second",
            "start": {start},
            "finish": {finish}
        }},
        "labels": {{
            "datastreams_stream_name": "streamName",
            "ydb_database": "databaseId",
            "Category": "Topic"
        }},
        "version": "v1",
        "source_id": "tabletId",
        "source_wt": {finish}
    }})",
        fmt::arg("timestamp", creationTs / 1'000),
        fmt::arg("counter", meteringSink.GetMeteringCounter()),
        fmt::arg("write_quota", writeQuota),
        fmt::arg("quantity", partitions * (flushTs - creationTs) / 1'000'000),
        fmt::arg("start", creationTs / 1'000'000),
        fmt::arg("finish", flushTs / 1'000'000)
    );
    UNIT_ASSERT_VALUES_EQUAL(ReformatJson(fullMetering), ReformatJson(referenceThrougputJson));
}

Y_UNIT_TEST(FlushStorageV1) {
    TString fullMetering;
    TMeteringSink meteringSink;
    const ui64 creationTs = 1651752943168786;
    const ui64 flushTs    = 1651754943168786;
    const ui32 partitions = 7;
    const ui64 reservedSpace = 42_GB;
    meteringSink.Create(TInstant::FromValue(creationTs), {
            .FlushInterval = TDuration::Seconds(10),
            .TabletId = "tabletId",
            .YcCloudId = "cloudId",
            .YcFolderId = "folderId",
            .YdbDatabaseId = "databaseId",
            .StreamName = "streamName",
            .ResourceId = "streamPath",
            .PartitionsSize = partitions,
            .ReservedSpace = reservedSpace,
        }, {EMeteringJson::StorageV1}, [&](TString json) {
            fullMetering = TStringBuilder() << fullMetering << '\n' << json;
        });

    meteringSink.MayFlushForcibly(TInstant::FromValue(flushTs));
    const TString referenceStorageJson = fmt::format(
        R"(
    {{
        "cloud_id": "cloudId",
        "folder_id": "folderId",
        "resource_id": "streamPath",
        "id": "yds.reserved_resources-databaseId-tabletId-{timestamp}-{counter}",
        "schema": "yds.storage.reserved.v1",
        "tags": {{
        }},
        "usage": {{
            "quantity": {quantity},
            "unit": "mbyte*second",
            "start": {start},
            "finish": {finish}
        }},
        "labels": {{
            "datastreams_stream_name": "streamName",
            "ydb_database": "databaseId",
            "Category": "Topic"
        }},
        "version": "v1",
        "source_id": "tabletId",
        "source_wt": {finish}
    }})",
        fmt::arg("timestamp", creationTs / 1'000),
        fmt::arg("counter", meteringSink.GetMeteringCounter()),
        fmt::arg("quantity", ((flushTs - creationTs) / 1'000'000) * partitions * (reservedSpace / 1_MB)),
        fmt::arg("start", creationTs / 1'000'000),
        fmt::arg("finish", flushTs / 1'000'000)
    );
    UNIT_ASSERT_VALUES_EQUAL(ReformatJson(fullMetering), ReformatJson(referenceStorageJson));
}

Y_UNIT_TEST(UsedStorageV1) {
    TString fullMetering;
    const ui64 creationTs = 1651752943168786;
    const ui64 flushTs    = 1651754943168786;
    const ui32 partitions = 7;
    const ui64 reservedSpace = 42_GB;

    TMeteringSink meteringSink;
    meteringSink.Create(TInstant::FromValue(creationTs), {
            .FlushInterval = TDuration::Seconds(10),
            .TabletId = "tabletId",
            .YcCloudId = "cloudId",
            .YcFolderId = "folderId",
            .YdbDatabaseId = "databaseId",
            .StreamName = "streamName",
            .ResourceId = "streamPath",
            .PartitionsSize = partitions,
            .ReservedSpace = reservedSpace,
        }, {EMeteringJson::UsedStorageV1}, [&](TString json) {
            fullMetering = TStringBuilder() << fullMetering << '\n' << json;
        });

    const ui32 quantity = 13;

    meteringSink.IncreaseQuantity(EMeteringJson::UsedStorageV1, quantity);
    meteringSink.MayFlushForcibly(TInstant::FromValue(flushTs));

    const TString referenceStorageJson = fmt::format(
        R"(
    {{
        "cloud_id": "cloudId",
        "folder_id": "folderId",
        "resource_id": "streamPath",
        "id": "used_storage-databaseId-tabletId-1651752943168-{counter}",
        "schema": "ydb.serverless.v1",
        "tags": {{
            "ydb_size": 6815
        }},
        "usage": {{
            "quantity": 2000,
            "unit": "byte*second",
            "start": 1651752943,
            "finish": 1651754943
        }},
        "labels": {{
            "datastreams_stream_name": "streamName",
            "ydb_database": "databaseId",
            "Category": "Topic"
        }},
        "version": "1.0.0",
        "source_id": "tabletId",
        "source_wt": 1651754943
    }})",
        fmt::arg("counter", meteringSink.GetMeteringCounter())
    );
    UNIT_ASSERT_VALUES_EQUAL(ReformatJson(fullMetering), ReformatJson(referenceStorageJson));
}

Y_UNIT_TEST(UnusedStorageV1) {
    const ui64 creationTs = 1651752943168786;
    const ui64 flushTs    = 1651754943168786;
    const ui32 partitions = 7;
    const ui64 reservedSpace = 42_GB;

    TMeteringSink meteringSink;
    meteringSink.Create(TInstant::FromValue(creationTs), {
            .FlushInterval = TDuration::Seconds(10),
            .TabletId = "tabletId",
            .YcCloudId = "cloudId",
            .YcFolderId = "folderId",
            .YdbDatabaseId = "databaseId",
            .StreamName = "streamName",
            .ResourceId = "streamPath",
            .PartitionsSize = partitions,
            .ReservedSpace = reservedSpace,
        }, {EMeteringJson::UsedStorageV1}, [&](TString json) {
            UNIT_FAIL("Flush should not be called");
            Y_UNUSED(json);
        });

    const ui32 quantity = 0;

    meteringSink.IncreaseQuantity(EMeteringJson::UsedStorageV1, quantity);
    meteringSink.MayFlushForcibly(TInstant::FromValue(flushTs));
}

} // Y_UNIT_TEST_SUITE(MeteringSink)

} // namespace NKikimr::NPQ
