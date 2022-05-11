#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/size_literals.h>
#include <ydb/core/metering/metering.h>
#include "metering_sink.h"


namespace NKikimr::NPQ {

Y_UNIT_TEST_SUITE(TMeteringSink) {

Y_UNIT_TEST(FlushPutEventsV1) {
    TString fullMetering;
    TMeteringSink meteringSink;
    const ui64 creationTs = 1651752943168786;
    const ui64 flushTs = 1651754943168786;
    meteringSink.Create(TInstant::FromValue(creationTs), {
            .FlushInterval = TDuration::Seconds(10),
            .TabletId = "tabletId",
            .YcCloudId = "cloudId",
            .YcFolderId = "folderId",
            .YdbDatabaseId = "databaseId",
            .StreamName = "streamName",
            .ResourceId = "streamPath",
            .PartitionsSize = 1,
            .WriteQuota = 512_KB,
        }, {EMeteringJson::PutEventsV1}, [&](TString json) {
            fullMetering = TStringBuilder() << fullMetering << '\n' << json;
        });

    meteringSink.MayFlush(TInstant::FromValue(flushTs));
    UNIT_ASSERT_VALUES_EQUAL(fullMetering.size(), 0);

    meteringSink.MayFlushForcibly(TInstant::FromValue(flushTs));
    UNIT_ASSERT_VALUES_EQUAL(fullMetering.size(), 0);

    meteringSink.IncreaseQuantity(EMeteringJson::PutEventsV1, 10);
    meteringSink.MayFlushForcibly(TInstant::FromValue(flushTs));

    const TString referencePutUnitsJson = "\n{\"cloud_id\":\"cloudId\",\"folder_id\":\"folderId\",\"resource_id\":\"streamPath\",\"id\":\"put_units-databaseId-tabletId-1651754943168-1\",\"schema\":\"yds.events.puts.v1\",\"tags\":{},\"usage\":{\"quantity\":10,\"unit\":\"put_events\",\"start\":1651754943,\"finish\":1651754943},\"labels\":{\"datastreams_stream_name\":\"streamName\",\"ydb_database\":\"databaseId\"},\"version\":\"v1\",\"source_id\":\"tabletId\",\"source_wt\":1651754943}\n";

    UNIT_ASSERT_VALUES_EQUAL(fullMetering, referencePutUnitsJson);
}

Y_UNIT_TEST(FlushResourcesReservedV1) {
    TString fullMetering;
    TMeteringSink meteringSink;
    const ui64 creationTs = 1651752943168786;
    const ui64 flushTs = 1651754943168786;
    meteringSink.Create(TInstant::FromValue(creationTs), {
            .FlushInterval = TDuration::Seconds(10),
            .TabletId = "tabletId",
            .YcCloudId = "cloudId",
            .YcFolderId = "folderId",
            .YdbDatabaseId = "databaseId",
            .StreamName = "streamName",
            .ResourceId = "streamPath",
            .PartitionsSize = 1,
            .WriteQuota = 512_KB,
            .ReservedSpace = 512_KB * 2,
            .ConsumersThroughput = 10
        }, {EMeteringJson::ResourcesReservedV1}, [&](TString json) {
            fullMetering = TStringBuilder() << fullMetering << '\n' << json;
        });

    meteringSink.MayFlushForcibly(TInstant::FromValue(flushTs));
    UNIT_ASSERT_VALUES_UNEQUAL(fullMetering.size(), 0);

    const TString referenceResourcesReservedJson = "\n{\"cloud_id\":\"cloudId\",\"folder_id\":\"folderId\",\"resource_id\":\"streamPath\",\"id\":\"reserved_resources-databaseId-tabletId-1651752943168-1\",\"schema\":\"yds.resources.reserved.v1\",\"tags\":{\"reserved_throughput_bps\":524288,\"shard_enhanced_consumers_throughput\":10,\"reserved_storage_bytes\":1048576},\"usage\":{\"quantity\":2000,\"unit\":\"second\",\"start\":1651752943,\"finish\":1651754943},\"labels\":{\"datastreams_stream_name\":\"streamName\",\"ydb_database\":\"databaseId\"},\"version\":\"v1\",\"source_id\":\"tabletId\",\"source_wt\":1651754943}\n";

    UNIT_ASSERT_VALUES_EQUAL(fullMetering, referenceResourcesReservedJson);
}

Y_UNIT_TEST(FlushThroughputV1) {
    TString fullMetering;
    TMeteringSink meteringSink;
    meteringSink.Create(TInstant::Now(), {
            .FlushInterval = TDuration::Seconds(10),
            .TabletId = "tabletId",
            .YcCloudId = "cloudId",
            .YcFolderId = "folderId",
            .YdbDatabaseId = "databaseId",
            .StreamName = "streamName",
            .ResourceId = "streamPath",
            .PartitionsSize = 1,
            .WriteQuota = 512_KB,
        }, {EMeteringJson::ThroughputV1}, [&](TString json) {
            fullMetering = TStringBuilder() << fullMetering << '\n' << json;
        });

    meteringSink.MayFlush(TInstant::Now());
    UNIT_ASSERT_VALUES_EQUAL(fullMetering.size(), 0);
    meteringSink.MayFlushForcibly(TInstant::Now());
    UNIT_ASSERT(fullMetering.size() > 0);
}

Y_UNIT_TEST(FlushStorageV1) {
    TString fullMetering;
    TMeteringSink meteringSink;
    meteringSink.Create(TInstant::Now(), {
            .FlushInterval = TDuration::Seconds(10),
            .TabletId = "tabletId",
            .YcCloudId = "cloudId",
            .YcFolderId = "folderId",
            .YdbDatabaseId = "databaseId",
            .StreamName = "streamName",
            .ResourceId = "streamPath",
            .PartitionsSize = 1,
            .WriteQuota = 512_KB,
        }, {EMeteringJson::StorageV1}, [&](TString json) {
            fullMetering = TStringBuilder() << fullMetering << '\n' << json;
        });

    meteringSink.MayFlush(TInstant::Now());
    UNIT_ASSERT_VALUES_EQUAL(fullMetering.size(), 0);
    meteringSink.MayFlushForcibly(TInstant::Now());
    UNIT_ASSERT(fullMetering.size() > 0);
}

} // Y_UNIT_TEST_SUITE(MeteringSink)

} // namespace NKikimr::NPQ
