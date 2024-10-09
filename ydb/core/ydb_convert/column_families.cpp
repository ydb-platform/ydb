#include "column_families.h"

#include <ydb/core/base/appdata.h>


namespace NKikimr {

    bool TColumnFamilyManager::ApplyStorageSettings(
            const Ydb::Table::StorageSettings& settings,
            Ydb::StatusIds::StatusCode* code,
            TString* error)
    {
        if (settings.has_tablet_commit_log0()) {
            auto* dst = MutableDefaultFamily()->MutableStorageConfig()->MutableSysLog();
            dst->SetPreferredPoolKind(settings.tablet_commit_log0().media());
            dst->SetAllowOtherKinds(false);
        }

        if (settings.has_tablet_commit_log1()) {
            auto* dst = MutableDefaultFamily()->MutableStorageConfig()->MutableLog();
            dst->SetPreferredPoolKind(settings.tablet_commit_log1().media());
            dst->SetAllowOtherKinds(false);
        }

        if (settings.has_external()) {
            auto* dst = MutableDefaultFamily()->MutableStorageConfig()->MutableExternal();
            dst->SetPreferredPoolKind(settings.external().media());
            dst->SetAllowOtherKinds(false);
        }

        switch (settings.store_external_blobs()) {
            case Ydb::FeatureFlag::STATUS_UNSPECIFIED:
                break;
            case Ydb::FeatureFlag::ENABLED: {
                if (!AppData()->FeatureFlags.GetEnablePublicApiExternalBlobs()) {
                    *code = Ydb::StatusIds::BAD_REQUEST;
                    *error = TStringBuilder()
                        << "Setting store_external_blobs to ENABLED is not allowed";
                    return false;
                }
                auto* dst = MutableDefaultFamily()->MutableStorageConfig();
                // TODO: it probably needs to be configurable?
                dst->SetExternalThreshold(512 * 1024);
                break;
            }
            case Ydb::FeatureFlag::DISABLED: {
                auto* dst = MutableDefaultFamily()->MutableStorageConfig();
                if (dst->HasExternalThreshold()) {
                    dst->ClearExternalThreshold();
                }
                break;
            }
            default: {
                *code = Ydb::StatusIds::BAD_REQUEST;
                *error = TStringBuilder()
                    << "unknown store_external_blobs feature flag status "
                    << (ui32)settings.store_external_blobs();
                return false;
            }
        }

        return true;
    }
}