#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/size_literals.h>
#include <ydb/core/metering/metering.h>
#include "metering_sink.h"


namespace NKikimr::NPQ {

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

    const TString referencePutUnitsJson = TStringBuilder() <<
        "\n{\"cloud_id\":\"cloudId\",\"folder_id\":\"folderId\",\"resource_id\":\"streamPath\"" <<
        ",\"id\":\"put_units-databaseId-tabletId-" << emptyFlushTs / 1'000 << "-" <<
        meteringSink.GetMeteringCounter() << "\"," <<
        "\"schema\":\"yds.events.puts.v1\",\"tags\":{},\"usage\":{\"quantity\":" << quantity <<
        ",\"unit\":\"put_events\"," <<
        "\"start\":" << emptyFlushTs / 1'000'000 << ",\"finish\":" << flushTs / 1'000'000 <<
        "},\"labels\":{\"datastreams_stream_name\":\"streamName\"," <<
        "\"ydb_database\":\"databaseId\"},\"version\":\"v1\",\"source_id\":\"tabletId\"," <<
        "\"source_wt\":" << flushTs / 1'000'000 << "}\n";

    UNIT_ASSERT_VALUES_EQUAL(fullMetering, referencePutUnitsJson);
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

    const TString referenceResourcesReservedJson = TStringBuilder() <<
        "\n{\"cloud_id\":\"cloudId\",\"folder_id\":\"folderId\",\"resource_id\":\"streamPath\"" <<
        ",\"id\":\"reserved_resources-databaseId-tabletId-" << creationTs / 1'000 << "-" <<
        meteringSink.GetMeteringCounter() << "\"" <<
        ",\"schema\":\"yds.resources.reserved.v1\",\"tags\":{\"reserved_throughput_bps\":" <<
        writeQuota << ",\"reserved_consumers_count\":" << consumersCount <<
        ",\"reserved_storage_bytes\":" << reservedSpace << "},\"usage\":{\"quantity\":" <<
        partitions * (flushTs - creationTs) / 1'000'000 << ",\"unit\":\"second\"," <<
        "\"start\":" << creationTs / 1'000'000 << ",\"finish\":" << flushTs / 1'000'000 <<
        "},\"labels\":{\"datastreams_stream_name\":\"streamName\"," <<
        "\"ydb_database\":\"databaseId\"},\"version\":\"v1\",\"source_id\":\"tabletId\"," <<
        "\"source_wt\":" << flushTs / 1'000'000 << "}\n";

    UNIT_ASSERT_VALUES_EQUAL(fullMetering, referenceResourcesReservedJson);
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
    const TString referenceThrougputJson = TStringBuilder() <<
        "\n{\"cloud_id\":\"cloudId\",\"folder_id\":\"folderId\",\"resource_id\":\"streamPath\"," <<
        "\"id\":\"yds.reserved_resources-databaseId-tabletId-" << creationTs / 1'000 << "-" <<
        meteringSink.GetMeteringCounter() << "\"" <<
        ",\"schema\":\"yds.throughput.reserved.v1\",\"tags\":" <<
        "{\"reserved_throughput_bps\":" << writeQuota << ",\"reserved_consumers_count\":" << 0 << "}," << "\"usage\":{\"quantity\":" <<
        partitions * (flushTs - creationTs) / 1'000'000 << ",\"unit\":\"second\"" <<
        ",\"start\":" << creationTs / 1'000'000 << ",\"finish\":" << flushTs / 1'000'000 <<
        "},\"labels\":{\"datastreams_stream_name\":\"streamName\",\"ydb_database\":\"databaseId\"}," <<
        "\"version\":\"v1\",\"source_id\":\"tabletId\"," <<
        "\"source_wt\":" << flushTs / 1'000'000 << "}\n";
    UNIT_ASSERT_VALUES_EQUAL(fullMetering, referenceThrougputJson);
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
    const TString referenceStorageJson = TStringBuilder() <<
        "\n{\"cloud_id\":\"cloudId\",\"folder_id\":\"folderId\",\"resource_id\":\"streamPath\"," <<
        "\"id\":\"yds.reserved_resources-databaseId-tabletId-" << creationTs / 1'000 << "-" <<
        meteringSink.GetMeteringCounter() << "\"" <<
        ",\"schema\":\"yds.storage.reserved.v1\",\"tags\":{},\"usage\":{\"quantity\":" <<
        ((flushTs - creationTs) / 1'000'000) * partitions * (reservedSpace / 1_MB) <<
        ",\"unit\":\"mbyte*second\"" <<
        ",\"start\":" << creationTs / 1'000'000 << ",\"finish\":" << flushTs / 1'000'000 <<
        "},\"labels\":{\"datastreams_stream_name\":\"streamName\",\"ydb_database\":\"databaseId\"}," <<
        "\"version\":\"v1\",\"source_id\":\"tabletId\"," <<
        "\"source_wt\":" << flushTs / 1'000'000 << "}\n";
    UNIT_ASSERT_VALUES_EQUAL(fullMetering, referenceStorageJson);
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

    const TString referenceStorageJson = TStringBuilder() <<
        "\n{\"cloud_id\":\"cloudId\",\"folder_id\":\"folderId\",\"resource_id\":\"streamPath\","
        << "\"id\":\"used_storage-databaseId-tabletId-1651752943168-" << meteringSink.GetMeteringCounter() << "\","
        << "\"schema\":\"ydb.serverless.v1\",\"tags\":{\"ydb_size\":6815},\"usage\":{\"quantity\":2000,\"unit\":\"byte*second\",\"start\":1651752943,\"finish\":1651754943},"
        << "\"labels\":{\"datastreams_stream_name\":\"streamName\",\"ydb_database\":\"databaseId\"},\"version\":\"1.0.0\",\"source_id\":\"tabletId\",\"source_wt\":1651754943}\n";
    UNIT_ASSERT_VALUES_EQUAL(fullMetering, referenceStorageJson);
}

} // Y_UNIT_TEST_SUITE(MeteringSink)

} // namespace NKikimr::NPQ
