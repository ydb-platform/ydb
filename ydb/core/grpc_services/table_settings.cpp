#include "table_settings.h"

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

bool FillAlterTableSettingsDesc(NKikimrSchemeOp::TTableDescription& out,
    const Ydb::Table::AlterTableRequest& in, const TTableProfiles& profiles,
    Ydb::StatusIds::StatusCode& code, TString& error, const TAppData* appData) {

    bool changed = false;
    auto &partitionConfig = *out.MutablePartitionConfig();

    if (in.set_compaction_policy()) {
        if (!profiles.ApplyCompactionPolicy(in.set_compaction_policy(), partitionConfig, code, error, appData)) {
            return false;
        }

        changed = true;
    }

    return NKikimr::FillAlterTableSettingsDesc(out, in, code, error, changed);
}

} // namespace NGRpcService
} // namespace NKikimr
