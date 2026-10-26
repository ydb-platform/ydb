#pragma once

#include <ydb/core/protos/kqp.pb.h>

namespace NKikimr::NPrivate {

// Naming adapter for builders shared with the public ALTER request.
// It holds a reference and preserves all field presence and column types.
class TKqpAlterTableRequestView {
public:
    explicit TKqpAlterTableRequestView(const NKikimrKqp::TKqpAlterTableRequest& request)
        : Request(request)
    {}

    static constexpr auto TTL_ACTION_NOT_SET = NKikimrKqp::TKqpAlterTableRequest::TTLACTION_NOT_SET;
    static constexpr auto METRICS_SETTINGS_ACTION_NOT_SET = NKikimrKqp::TKqpAlterTableRequest::METRICSSETTINGSACTION_NOT_SET;

    auto ttl_action_case() const { return Request.GetTtlActionCase(); }
    auto metrics_settings_action_case() const { return Request.GetMetricsSettingsActionCase(); }

    decltype(auto) path() const { return Request.GetPath(); }
    decltype(auto) add_columns() const { return Request.GetAddColumns(); }
    int add_columns_size() const { return Request.AddColumnsSize(); }
    decltype(auto) drop_columns() const { return Request.GetDropColumns(); }
    int drop_columns_size() const { return Request.DropColumnsSize(); }
    decltype(auto) alter_columns() const { return Request.GetAlterColumns(); }
    int alter_columns_size() const { return Request.AlterColumnsSize(); }
    decltype(auto) set_ttl_settings() const { return Request.GetSetTtlSettings(); }
    bool has_set_ttl_settings() const { return Request.HasSetTtlSettings(); }
    bool has_drop_ttl_settings() const { return Request.HasDropTtlSettings(); }
    decltype(auto) add_indexes() const { return Request.GetAddIndexes(); }
    decltype(auto) add_indexes(int index) const { return Request.GetAddIndexes(index); }
    int add_indexes_size() const { return Request.AddIndexesSize(); }
    decltype(auto) drop_indexes() const { return Request.GetDropIndexes(); }
    decltype(auto) drop_indexes(int index) const { return Request.GetDropIndexes(index); }
    int drop_indexes_size() const { return Request.DropIndexesSize(); }
    decltype(auto) alter_storage_settings() const { return Request.GetAlterStorageSettings(); }
    bool has_alter_storage_settings() const { return Request.HasAlterStorageSettings(); }
    decltype(auto) add_column_families() const { return Request.GetAddColumnFamilies(); }
    int add_column_families_size() const { return Request.AddColumnFamiliesSize(); }
    decltype(auto) alter_column_families() const { return Request.GetAlterColumnFamilies(); }
    int alter_column_families_size() const { return Request.AlterColumnFamiliesSize(); }
    decltype(auto) alter_attributes() const { return Request.GetAlterAttributes(); }
    int alter_attributes_size() const { return Request.AlterAttributesSize(); }
    decltype(auto) set_compaction_policy() const { return Request.GetSetCompactionPolicy(); }
    decltype(auto) alter_partitioning_settings() const { return Request.GetAlterPartitioningSettings(); }
    bool has_alter_partitioning_settings() const { return Request.HasAlterPartitioningSettings(); }
    decltype(auto) set_key_bloom_filter() const { return Request.GetSetKeyBloomFilter(); }
    decltype(auto) set_read_replicas_settings() const { return Request.GetSetReadReplicasSettings(); }
    bool has_set_read_replicas_settings() const { return Request.HasSetReadReplicasSettings(); }
    decltype(auto) add_changefeeds() const { return Request.GetAddChangefeeds(); }
    int add_changefeeds_size() const { return Request.AddChangefeedsSize(); }
    decltype(auto) drop_changefeeds() const { return Request.GetDropChangefeeds(); }
    int drop_changefeeds_size() const { return Request.DropChangefeedsSize(); }
    decltype(auto) rename_indexes() const { return Request.GetRenameIndexes(); }
    decltype(auto) rename_indexes(int index) const { return Request.GetRenameIndexes(index); }
    int rename_indexes_size() const { return Request.RenameIndexesSize(); }
    decltype(auto) compact() const { return Request.GetCompact(); }
    bool has_compact() const { return Request.HasCompact(); }
    decltype(auto) set_metrics_settings() const { return Request.GetSetMetricsSettings(); }
    bool has_set_metrics_settings() const { return Request.HasSetMetricsSettings(); }
    bool has_drop_metrics_settings() const { return Request.HasDropMetricsSettings(); }
    decltype(auto) set_not_null() const { return Request.GetSetNotNull(); }
    int set_not_null_size() const { return Request.SetNotNullSize(); }
    decltype(auto) add_statistics() const { return Request.GetAddStatistics(); }
    int add_statistics_size() const { return Request.AddStatisticsSize(); }
    decltype(auto) drop_statistics() const { return Request.GetDropStatistics(); }
    int drop_statistics_size() const { return Request.DropStatisticsSize(); }
    const NKikimrKqp::TKqpAlterTableRequest& GetRequest() const { return Request; }

private:
    const NKikimrKqp::TKqpAlterTableRequest& Request;
};

template <typename TRequest>
const TRequest& GetAlterTableRequest(const TRequest& request) {
    return request;
}

inline const NKikimrKqp::TKqpAlterTableRequest& GetAlterTableRequest(const TKqpAlterTableRequestView& request) {
    return request.GetRequest();
}

} // namespace NKikimr::NPrivate
