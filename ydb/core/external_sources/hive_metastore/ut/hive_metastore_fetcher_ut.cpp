#include <ydb/core/external_sources/hive_metastore/events.h>
#include <ydb/core/external_sources/hive_metastore/hive_metastore_fetcher.h>
#include <ydb/core/external_sources/hive_metastore/ut/common.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NExternalSource {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NExternalSource;

namespace {

//////////////////////////////////////////////////////

using TRuntimePtr = std::shared_ptr<TTestActorRuntime>;

struct TTestBootstrap {
    TRuntimePtr Runtime;

    TTestBootstrap()
        : Runtime(PrepareTestActorRuntime()) {
    }

private:
    TRuntimePtr PrepareTestActorRuntime()
    {
        TRuntimePtr runtime(new TTestBasicRuntime());
        SetupTabletServices(*runtime);
        return runtime;
    }
};

} // namespace

//////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(HiveMetastoreFetcher) {
    Y_UNIT_TEST(SuccessTrinoRequest) {
        const TString host = "0.0.0.0";
        const int32_t port = stoi(GetExternalPort("hive-metastore", "9083"));
        WaitHiveMetastore(host, port, "final");

        TTestBootstrap bootstrap;
        {
            auto fetcher = bootstrap.Runtime->Register(CreateHiveMetastoreFetcherActor(host, port).release());

            auto request = std::make_unique<TEvHiveMetastore::TEvGetTable>();
            request->DatbaseName = "hive";
            request->TableName = "request_logs";
            NThreading::TPromise<TEvHiveMetastore::TTable> promise = NThreading::NewPromise<TEvHiveMetastore::TTable>();
            request->Promise = promise;

            bootstrap.Runtime->Send(new IEventHandle(fetcher, TActorId{}, request.release()));
            auto future = promise.GetFuture();
            while (!future.HasException() && !future.HasValue()) {
                bootstrap.Runtime->DispatchEvents({}, TDuration::Seconds(1));
            }

            auto table = future.GetValue();
            UNIT_ASSERT_VALUES_EQUAL(table.Columns.size(), 7);
            UNIT_ASSERT_VALUES_EQUAL(table.Columns[0].name(), "request_time");
            UNIT_ASSERT_EQUAL(table.Columns[0].type().type_id(), Ydb::Type::TIMESTAMP);
            UNIT_ASSERT_VALUES_EQUAL(table.Columns[1].name(), "url");
            UNIT_ASSERT_EQUAL(table.Columns[1].type().type_id(), Ydb::Type::UTF8);
            UNIT_ASSERT_VALUES_EQUAL(table.Columns[2].name(), "ip");
            UNIT_ASSERT_EQUAL(table.Columns[2].type().type_id(), Ydb::Type::UTF8);
            UNIT_ASSERT_VALUES_EQUAL(table.Columns[3].name(), "user_agent");
            UNIT_ASSERT_EQUAL(table.Columns[3].type().type_id(), Ydb::Type::UTF8);
            UNIT_ASSERT_VALUES_EQUAL(table.Columns[4].name(), "year");
            UNIT_ASSERT_EQUAL(table.Columns[4].type().type_id(), Ydb::Type::UTF8);
            UNIT_ASSERT_VALUES_EQUAL(table.Columns[5].name(), "month");
            UNIT_ASSERT_EQUAL(table.Columns[5].type().type_id(), Ydb::Type::UTF8);
            UNIT_ASSERT_VALUES_EQUAL(table.Columns[6].name(), "day");
            UNIT_ASSERT_EQUAL(table.Columns[6].type().type_id(), Ydb::Type::UTF8);
            UNIT_ASSERT_VALUES_EQUAL(table.Compression, "");
            UNIT_ASSERT_VALUES_EQUAL(table.Format, "parquet");
            UNIT_ASSERT_VALUES_EQUAL(table.Location, "s3://datalake/data/logs/hive/request_logs");
            UNIT_ASSERT_VALUES_EQUAL(table.PartitionedBy.size(), 3);
            UNIT_ASSERT_VALUES_EQUAL(table.PartitionedBy[0], "year");
            UNIT_ASSERT_VALUES_EQUAL(table.PartitionedBy[1], "month");
            UNIT_ASSERT_VALUES_EQUAL(table.PartitionedBy[2], "day");
        }
        {
            auto fetcher = bootstrap.Runtime->Register(CreateHiveMetastoreFetcherActor(host, port).release());

            auto request = std::make_unique<TEvHiveMetastore::TEvGetStatistics>();
            request->DatbaseName = "hive";
            request->TableName = "request_logs";
            NThreading::TPromise<TEvHiveMetastore::TStatistics> promise = NThreading::NewPromise<TEvHiveMetastore::TStatistics>();
            request->Promise = promise;

            auto now = TInstant::Now();
            bootstrap.Runtime->Send(new IEventHandle(fetcher, TActorId{}, request.release()));
            auto future = promise.GetFuture();
            while (!future.HasException() && !future.HasValue()) {
                bootstrap.Runtime->DispatchEvents({}, TDuration::Seconds(1));
                if (TInstant::Now() - now > TDuration::Seconds(1)) {
                    UNIT_ASSERT(false);
                }
            }

            auto table = future.GetValue();
            UNIT_ASSERT(table.Rows);
            UNIT_ASSERT(table.Size);
            UNIT_ASSERT_VALUES_EQUAL(*table.Rows, 6);
            UNIT_ASSERT_VALUES_EQUAL(*table.Size, 4734);
        }
        {
            auto fetcher = bootstrap.Runtime->Register(CreateHiveMetastoreFetcherActor(host, port).release());

            auto request = std::make_unique<TEvHiveMetastore::TEvGetPartitions>();
            request->DatbaseName = "hive";
            request->TableName = "request_logs";
            NThreading::TPromise<TEvHiveMetastore::TPartitions> promise = NThreading::NewPromise<TEvHiveMetastore::TPartitions>();
            request->Promise = promise;

            auto now = TInstant::Now();
            bootstrap.Runtime->Send(new IEventHandle(fetcher, TActorId{}, request.release()));
            auto future = promise.GetFuture();
            while (!future.HasException() && !future.HasValue()) {
                bootstrap.Runtime->DispatchEvents({}, TDuration::Seconds(1));
                if (TInstant::Now() - now > TDuration::Seconds(1)) {
                    UNIT_ASSERT(false);
                }
            }

            auto partitions = future.GetValue();
            UNIT_ASSERT_VALUES_EQUAL(partitions.Partitions.size(), 4);
            UNIT_ASSERT_VALUES_EQUAL(partitions.Partitions[0].Location, "s3://datalake/data/logs/hive/request_logs/year=2024/month=05/day=01");
            UNIT_ASSERT_VALUES_EQUAL(partitions.Partitions[0].Values.size(), 3);
            UNIT_ASSERT_VALUES_EQUAL(partitions.Partitions[0].Values[0], "2024");
            UNIT_ASSERT_VALUES_EQUAL(partitions.Partitions[0].Values[1], "05");
            UNIT_ASSERT_VALUES_EQUAL(partitions.Partitions[0].Values[2], "01");
            UNIT_ASSERT_VALUES_EQUAL(partitions.Partitions[1].Location, "s3://datalake/data/logs/hive/request_logs/year=2024/month=05/day=02");
            UNIT_ASSERT_VALUES_EQUAL(partitions.Partitions[1].Values.size(), 3);
            UNIT_ASSERT_VALUES_EQUAL(partitions.Partitions[1].Values[0], "2024");
            UNIT_ASSERT_VALUES_EQUAL(partitions.Partitions[1].Values[1], "05");
            UNIT_ASSERT_VALUES_EQUAL(partitions.Partitions[1].Values[2], "02");
            UNIT_ASSERT_VALUES_EQUAL(partitions.Partitions[2].Location, "s3://datalake/data/logs/hive/request_logs/year=2024/month=05/day=03");
            UNIT_ASSERT_VALUES_EQUAL(partitions.Partitions[2].Values.size(), 3);
            UNIT_ASSERT_VALUES_EQUAL(partitions.Partitions[2].Values[0], "2024");
            UNIT_ASSERT_VALUES_EQUAL(partitions.Partitions[2].Values[1], "05");
            UNIT_ASSERT_VALUES_EQUAL(partitions.Partitions[2].Values[2], "03");
            UNIT_ASSERT_VALUES_EQUAL(partitions.Partitions[3].Location, "s3://datalake/data/logs/hive/request_logs/year=2024/month=05/day=04");
            UNIT_ASSERT_VALUES_EQUAL(partitions.Partitions[3].Values.size(), 3);
            UNIT_ASSERT_VALUES_EQUAL(partitions.Partitions[3].Values[0], "2024");
            UNIT_ASSERT_VALUES_EQUAL(partitions.Partitions[3].Values[1], "05");
            UNIT_ASSERT_VALUES_EQUAL(partitions.Partitions[3].Values[2], "04");
        }
    }
}

} // namespace NKikimr::NExternalSource
