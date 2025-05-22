#include <ydb/core/external_sources/hive_metastore/events.h>
#include <ydb/core/external_sources/hive_metastore/hive_metastore_client.h>
#include <ydb/core/external_sources/hive_metastore/hive_metastore_converters.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#define LOG_E(stream) LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::KQP_GATEWAY, "[HiveMetastoreFetcher]: " << stream)
#define LOG_W(stream) LOG_WARN_S( *NActors::TlsActivationContext, NKikimrServices::KQP_GATEWAY, "[HiveMetastoreFetcher]: " << stream)
#define LOG_I(stream) LOG_INFO_S( *NActors::TlsActivationContext, NKikimrServices::KQP_GATEWAY, "[HiveMetastoreFetcher]: " << stream)
#define LOG_D(stream) LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::KQP_GATEWAY, "[HiveMetastoreFetcher]: " << stream)
#define LOG_T(stream) LOG_TRACE_S(*NActors::TlsActivationContext, NKikimrServices::KQP_GATEWAY, "[HiveMetastoreFetcher]: " << stream)

namespace NKikimr::NExternalSource {

using namespace NKikimr::NExternalSource;

namespace {

class THiveMetastoreFetcherActor : public NActors::TActor<THiveMetastoreFetcherActor> {
public:
    using TBase = NActors::TActor<THiveMetastoreFetcherActor>;
    THiveMetastoreFetcherActor(const TString& host, int32_t port)
        : TBase(&THiveMetastoreFetcherActor::StateFunc)
        , Client(host, port)
    {}

    STRICT_STFUNC(StateFunc,
        hFunc(TEvHiveMetastore::TEvGetTable, Handle);
        hFunc(TEvHiveMetastore::TEvGetStatistics, Handle);
        hFunc(TEvHiveMetastore::TEvGetPartitions, Handle);
        hFunc(TEvHiveMetastore::TEvHiveGetTableResult, Handle);
        hFunc(TEvHiveMetastore::TEvHiveGetTableStatisticsResult, Handle);
        hFunc(TEvHiveMetastore::TEvHiveGetPartitionsResult, Handle);
    )

    void Handle(TEvHiveMetastore::TEvGetTable::TPtr& ev) {
        Client.GetTable(ev->Get()->DatbaseName, ev->Get()->TableName).Apply([actorSystem = NActors::TActivationContext::ActorSystem(), self = SelfId()](const auto& future) {
            auto result = std::make_unique<TEvHiveMetastore::TEvHiveGetTableResult>();
            try {
                auto table = future.GetValue();
                result->Table = table;
                actorSystem->Send(self, result.release(), 0, 0);
            } catch (...) {
                result->Issues.AddIssue(CurrentExceptionMessage());
                actorSystem->Send(self, result.release(), 0, 0);
            }
        });
        Requests.emplace<0>(ev);
    }

    void Handle(TEvHiveMetastore::TEvGetStatistics::TPtr& ev) {
        Apache::Hadoop::Hive::TableStatsRequest request;
        request.__set_dbName(ev->Get()->DatbaseName);
        request.__set_tblName(ev->Get()->TableName);
        request.__set_colNames(ev->Get()->Columns);

        Client.GetTableStatistics(request).Apply([actorSystem = NActors::TActivationContext::ActorSystem(), self = SelfId()](const auto& future) {
            auto result = std::make_unique<TEvHiveMetastore::TEvHiveGetTableStatisticsResult>();
            try {
                auto statistics = future.GetValue();
                result->Statistics = statistics;
                actorSystem->Send(self, result.release(), 0, 0);
            } catch (...) {
                result->Issues.AddIssue(CurrentExceptionMessage());
                actorSystem->Send(self, result.release(), 0, 0);
            }
        });
        Requests.emplace<1>(ev);
    }

    void Handle(TEvHiveMetastore::TEvGetPartitions::TPtr& ev) {
        SendGetPartitions(ev.Get()->Get()->DatbaseName, ev.Get()->Get()->TableName, THiveMetastoreConverters::GetPartitionsFilter(ev.Get()->Get()->Predicate));
        Requests.emplace<2>(ev);
    }

    void Handle(TEvHiveMetastore::TEvHiveGetTableResult::TPtr& ev) {
        auto request = std::get<0>(Requests);
        const auto& issues = ev.Get()->Get()->Issues;
        if (issues) {
            LOG_E(issues.ToString(true));
            request->Get()->Promise.SetException(std::make_exception_ptr(yexception() << issues.ToString(true)));
            PassAway();
            return;
        }

        try {
            auto hiveMetastoreTable = ev.Get()->Get()->Table;
            TEvHiveMetastore::TTable table;
            table.Columns = THiveMetastoreConverters::GetColumns(hiveMetastoreTable);
            table.Compression = THiveMetastoreConverters::GetCompression(hiveMetastoreTable);
            table.Format = THiveMetastoreConverters::GetFormat(hiveMetastoreTable);
            table.Location = THiveMetastoreConverters::GetLocation(hiveMetastoreTable);
            table.PartitionedBy = THiveMetastoreConverters::GetPartitionedColumns(hiveMetastoreTable);
            request->Get()->Promise.SetValue(table);
        } catch (...) {
            request->Get()->Promise.SetException(std::current_exception());
        }

        PassAway();
    }

    void Handle(TEvHiveMetastore::TEvHiveGetTableStatisticsResult::TPtr& ev) {
        const auto& request = std::get<1>(Requests);
        const auto& issues = ev.Get()->Get()->Issues;
        if (issues) {
            LOG_E(issues.ToString(true));
            request->Get()->Promise.SetException(std::make_exception_ptr(yexception() << issues.ToString(true)));
            PassAway();
            return;
        }

        auto statistics = THiveMetastoreConverters::GetStatistics(ev.Get()->Get()->Statistics);
        if (!statistics.Rows && !statistics.Size) {
            SendGetPartitions(request->Get()->DatbaseName, request->Get()->TableName, {});
            return;
        }

        request->Get()->Promise.SetValue(TEvHiveMetastore::TStatistics{statistics.Rows, statistics.Size});
        PassAway();
    }

    void Handle(TEvHiveMetastore::TEvHiveGetPartitionsResult::TPtr& ev) {
        if (Requests.index() == 1) {
            ProcessGetTableStatistics(ev);
            return;
        }

        ProcessGetPartitions(ev);
    }

    void ProcessGetPartitions(TEvHiveMetastore::TEvHiveGetPartitionsResult::TPtr& ev) {
        auto request = std::get<2>(Requests);
        const auto& issues = ev.Get()->Get()->Issues;
        if (issues) {
            LOG_E(issues.ToString(true));
            request->Get()->Promise.SetException(std::make_exception_ptr(yexception() << issues.ToString(true)));
            PassAway();
            return;
        }

        try {
            TEvHiveMetastore::TPartitions partitions;
            const auto& hiveMetastorePartitions = ev.Get()->Get()->Partitions;
            for (const auto& partition: hiveMetastorePartitions) {
                std::vector<TString> values;
                for (const auto& value: partition.values) {
                    values.push_back(TString{value});
                }
                partitions.Partitions.push_back(TEvHiveMetastore::TPartitions::TPartition{TString{partition.sd.location}, values});
            }
            request->Get()->Promise.SetValue(partitions);
        } catch (...) {
            request->Get()->Promise.SetException(std::current_exception());
        }

        PassAway();
    }

    void ProcessGetTableStatistics(TEvHiveMetastore::TEvHiveGetPartitionsResult::TPtr& ev) {
        auto request = std::get<1>(Requests);
        const auto& issues = ev.Get()->Get()->Issues;
        if (issues) {
            LOG_E(issues.ToString(true));
            request->Get()->Promise.SetException(std::make_exception_ptr(yexception() << issues.ToString(true)));
            PassAway();
            return;
        }

        auto statistics = THiveMetastoreConverters::GetStatistics(ev.Get()->Get()->Partitions);
        if (!statistics.Rows && !statistics.Size) {
            SendGetPartitions(request->Get()->DatbaseName, request->Get()->TableName, {});
            return;
        }

        request->Get()->Promise.SetValue(TEvHiveMetastore::TStatistics{statistics.Rows, statistics.Size});
        PassAway();
    }

private:
    void SendGetPartitions(const TString& databaseName, const TString& tableName, const TString& filter) {
        Client.GetPartitionsByFilter(databaseName, tableName, filter).Apply([actorSystem = NActors::TActivationContext::ActorSystem(), self = SelfId()](const auto& future) {
            auto result = std::make_unique<TEvHiveMetastore::TEvHiveGetPartitionsResult>();
            try {
                auto partitions = future.GetValue();
                result->Partitions = partitions;
                actorSystem->Send(self, result.release(), 0, 0);
            } catch (...) {
                result->Issues.AddIssue(CurrentExceptionMessage());
                actorSystem->Send(self, result.release(), 0, 0);
            }
        });
    }

private:
    NKikimr::NExternalSource::THiveMetastoreClient Client;
    std::variant<TEvHiveMetastore::TEvGetTable::TPtr, TEvHiveMetastore::TEvGetStatistics::TPtr, TEvHiveMetastore::TEvGetPartitions::TPtr> Requests;
};

}

std::unique_ptr<NActors::IActor> CreateHiveMetastoreFetcherActor(const TString& host, int32_t port) {
    return std::make_unique<THiveMetastoreFetcherActor>(host, port);
}

}
