#include "schema.h"

namespace NKikimr::NSchemeShard {

bool TOlapOptionsDescription::ApplyUpdate(const TOlapOptionsUpdate& schemaUpdate, IErrorCollector& /*errors*/) {
    using TRequestedOptions = TOlapOptionsUpdate::TRequestedOptions;
    // Resets are applied first: the field is cleared so the columnshard falls back to its defaults
    for (const auto option : schemaUpdate.GetResetOptions()) {
        switch (option) {
            case TRequestedOptions::SCHEME_NEED_ACTUALIZATION:
                SchemeNeedActualization = false;
                break;
            case TRequestedOptions::COMPACTION_PLANNER:
                CompactionPlannerConstructor = {};
                break;
            case TRequestedOptions::METADATA_MEMORY_MANAGER:
                MetadataManagerConstructor = {};
                break;
            case TRequestedOptions::SCAN_READER_POLICY_NAME:
                ScanReaderPolicyName.reset();
                break;
            case TRequestedOptions::DEDUPLICATION_ENABLED:
                DeduplicationEnabled.reset();
                break;
            case TRequestedOptions::INSERT_OPTIONS:
                InsertOptions = {};
                break;
            case TRequestedOptions::CACHE_BLOBS_AFTER_WRITE:
                CacheBlobsAfterWrite.reset();
                break;
        }
    }
    if (schemaUpdate.GetSchemeNeedActualization()) {
        SchemeNeedActualization = *schemaUpdate.GetSchemeNeedActualization();
    }
    if (schemaUpdate.GetScanReaderPolicyName()) {
        ScanReaderPolicyName = *schemaUpdate.GetScanReaderPolicyName();
    }
    if (schemaUpdate.GetDeduplicationEnabled()) {
        DeduplicationEnabled = *schemaUpdate.GetDeduplicationEnabled();
    }
    if (schemaUpdate.GetCacheBlobsAfterWrite()) {
        CacheBlobsAfterWrite = *schemaUpdate.GetCacheBlobsAfterWrite();
    }
    if (schemaUpdate.GetCompactionPlannerConstructor().HasObject()) {
        CompactionPlannerConstructor = schemaUpdate.GetCompactionPlannerConstructor();
    }
    if (schemaUpdate.GetMetadataManagerConstructor().HasObject()) {
        MetadataManagerConstructor = schemaUpdate.GetMetadataManagerConstructor();
    }
    if (schemaUpdate.GetInsertOptionsBuildIndexesEnabled()) {
        InsertOptions.BuildIndexesEnabled = *schemaUpdate.GetInsertOptionsBuildIndexesEnabled();
    }
    if (schemaUpdate.GetInsertOptionsBuildIndexesMinBlobBytes()) {
        InsertOptions.BuildIndexesMinBlobBytes = *schemaUpdate.GetInsertOptionsBuildIndexesMinBlobBytes();
    }
    return true;
}

void TOlapOptionsDescription::Parse(const NKikimrSchemeOp::TColumnTableSchema& tableSchema) {
    SchemeNeedActualization = tableSchema.GetOptions().GetSchemeNeedActualization();
    if (tableSchema.GetOptions().HasScanReaderPolicyName()) {
        ScanReaderPolicyName = tableSchema.GetOptions().GetScanReaderPolicyName();
    }
    if (tableSchema.GetOptions().HasDeduplicationEnabled()) {
        DeduplicationEnabled = tableSchema.GetOptions().GetDeduplicationEnabled();
    }
    if (tableSchema.GetOptions().HasCacheBlobsAfterWrite()) {
        CacheBlobsAfterWrite = tableSchema.GetOptions().GetCacheBlobsAfterWrite();
    }
    if (tableSchema.GetOptions().HasCompactionPlannerConstructor()) {
        AFL_VERIFY(CompactionPlannerConstructor.DeserializeFromProto(tableSchema.GetOptions().GetCompactionPlannerConstructor()));
    }
    if (tableSchema.GetOptions().HasMetadataManagerConstructor()) {
        AFL_VERIFY(MetadataManagerConstructor.DeserializeFromProto(tableSchema.GetOptions().GetMetadataManagerConstructor()));
    }
    if (tableSchema.GetOptions().HasInsertOptions()) {
        const auto& options = tableSchema.GetOptions().GetInsertOptions();
        if (options.HasBuildIndexesEnabled()) {
            InsertOptions.BuildIndexesEnabled = options.GetBuildIndexesEnabled();
        }
        if (options.HasBuildIndexesMinBlobBytes()) {
            InsertOptions.BuildIndexesMinBlobBytes = options.GetBuildIndexesMinBlobBytes();
        }
    }
}

void TOlapOptionsDescription::Serialize(NKikimrSchemeOp::TColumnTableSchema& tableSchema) const {
    tableSchema.MutableOptions()->SetSchemeNeedActualization(SchemeNeedActualization);
    if (ScanReaderPolicyName) {
        tableSchema.MutableOptions()->SetScanReaderPolicyName(*ScanReaderPolicyName);
    }
    if (DeduplicationEnabled) {
        tableSchema.MutableOptions()->SetDeduplicationEnabled(*DeduplicationEnabled);
    }
    if (CacheBlobsAfterWrite) {
        tableSchema.MutableOptions()->SetCacheBlobsAfterWrite(*CacheBlobsAfterWrite);
    }
    if (CompactionPlannerConstructor.HasObject()) {
        CompactionPlannerConstructor.SerializeToProto(*tableSchema.MutableOptions()->MutableCompactionPlannerConstructor());
    }
    if (MetadataManagerConstructor.HasObject()) {
        MetadataManagerConstructor.SerializeToProto(*tableSchema.MutableOptions()->MutableMetadataManagerConstructor());
    }
    if (InsertOptions.IsConfigured()) {
        auto& options = *tableSchema.MutableOptions()->MutableInsertOptions();
        if (InsertOptions.BuildIndexesEnabled) {
            options.SetBuildIndexesEnabled(*InsertOptions.BuildIndexesEnabled);
        }
        if (InsertOptions.BuildIndexesMinBlobBytes) {
            options.SetBuildIndexesMinBlobBytes(*InsertOptions.BuildIndexesMinBlobBytes);
        }
    }
}

bool TOlapOptionsDescription::ValidateForStore(const NKikimrSchemeOp::TColumnTableSchema& opSchema, IErrorCollector& /*errors*/) const {
    if (!opSchema.HasOptions()) {
        return true;
    }
    return true;
}

}
