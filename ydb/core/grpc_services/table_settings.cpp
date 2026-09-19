#include "table_settings.h"

#include <ydb/core/grpc_services/base/base.h>

namespace NKikimr {
namespace NGRpcService {

bool FillCreateTableSettingsDesc(NKikimrSchemeOp::TTableDescription& out,
    const Ydb::Table::CreateTableRequest& in, const TTableProfiles& profiles,
    Ydb::StatusIds::StatusCode& code, TString& error, TList<TString>& warnings) {

    bool tableProfileSet = false;
    if (in.has_profile()) {
        const auto& profile = in.profile();
        tableProfileSet = profile.preset_name() || profile.has_compaction_policy() || profile.has_execution_policy()
            || profile.has_partitioning_policy() || profile.has_storage_policy() || profile.has_replication_policy()
            || profile.has_caching_policy();
    }

    auto &partitionConfig = *out.MutablePartitionConfig();
    if (!in.compaction_policy().empty()) {
        if (tableProfileSet) {
            MEWarning("CompactionPolicy", warnings);
        }
        if (!profiles.ApplyCompactionPolicy(in.compaction_policy(), partitionConfig, code, error)) {
            return false;
        }
    }

    return NKikimr::FillCreateTableSettingsDesc(out, in, code, error, warnings, tableProfileSet);
}

void ResolveTtlStoragePaths(Ydb::Table::TtlSettings& settings, const IAuditCtx& request) {
    if (!settings.has_tiered_ttl()) {
        return;
    }

    for (auto& tier : *settings.mutable_tiered_ttl()->mutable_tiers()) {
        if (tier.has_evict_to_external_storage()) {
            auto* eviction = tier.mutable_evict_to_external_storage();
            eviction->set_storage(request.GetDatabaseRelativePath(eviction->storage()));
        }
    }
}


} // namespace NGRpcService
} // namespace NKikimr
