#include "upsert_opt.h"
#include <util/string/type.h>
#include <library/cpp/json/json_reader.h>

namespace NKikimr::NKqp {

namespace {

template <class T>
TConclusionStatus ExtractOption(
    NYql::TObjectSettingsImpl::TFeaturesExtractor& features, const TString& featureId, std::optional<T>& target) {
    if (auto rawValue = features.Extract(featureId)) {
        T parsed;
        if (!TryFromString(*rawValue, parsed)) {
            return TConclusionStatus::Fail("Incorrect value for " + featureId + ": cannot parse as expected type");
        }
        target = parsed;
    }
    return TConclusionStatus::Success();
}

}   // namespace

TConclusionStatus TUpsertOptionsOperation::DoDeserialize(NYql::TObjectSettingsImpl::TFeaturesExtractor& features) {
    if (auto status = ExtractOption(features, "SCHEME_NEED_ACTUALIZATION", SchemeNeedActualization); status.IsFail()) {
        return status;
    }
    ScanReaderPolicyName = features.Extract<TString>("SCAN_READER_POLICY_NAME");
    if (ScanReaderPolicyName) {
        if (*ScanReaderPolicyName != "PLAIN" && *ScanReaderPolicyName != "SIMPLE" && *ScanReaderPolicyName != "TRIVIAL") {
            return TConclusionStatus::Fail("SCAN_READER_POLICY_NAME have to be in ['PLAIN', 'SIMPLE', 'TRIVIAL']");
        }
    }
    if (auto status = ExtractOption(features, "DEDUPLICATION_ENABLED", DeduplicationEnabled); status.IsFail()) {
        return status;
    }
    if (auto status = ExtractOption(features, "CACHE_BLOBS_AFTER_WRITE", CacheBlobsAfterWrite); status.IsFail()) {
        return status;
    }
    if (auto status = ExtractOption(features, "INSERT_OPTIONS.BUILD_INDEXES_ENABLED", InsertOptionsBuildIndexesEnabled); status.IsFail()) {
        return status;
    }
    if (auto status = ExtractOption(features, "INSERT_OPTIONS.BUILD_INDEXES_MIN_BLOB_BYTES", InsertOptionsBuildIndexesMinBlobBytes);
        status.IsFail()) {
        return status;
    }
    if (const auto className = features.Extract<TString>("COMPACTION_PLANNER.CLASS_NAME")) {
        if (!CompactionPlannerConstructor.Initialize(*className)) {
            return TConclusionStatus::Fail("incorrect class name for compaction planner:" + *className);
        }

        NJson::TJsonValue jsonData = NJson::JSON_MAP;
        auto fValue = features.Extract("COMPACTION_PLANNER.FEATURES");
        if (fValue) {
            if (!NJson::ReadJsonFastTree(*fValue, &jsonData)) {
                return TConclusionStatus::Fail("incorrect json in request COMPACTION_PLANNER.FEATURES parameter");
            }
        }
        auto result = CompactionPlannerConstructor->DeserializeFromJson(jsonData);
        if (result.IsFail()) {
            return result;
        }
    }

    if (const auto className = features.Extract<TString>("METADATA_MEMORY_MANAGER.CLASS_NAME")) {
        if (!MetadataManagerConstructor.Initialize(*className)) {
            return TConclusionStatus::Fail("incorrect class name for metadata manager:" + *className);
        }

        NJson::TJsonValue jsonData = NJson::JSON_MAP;
        auto fValue = features.Extract("METADATA_MEMORY_MANAGER.FEATURES");
        if (fValue) {
            if (!NJson::ReadJsonFastTree(*fValue, &jsonData)) {
                return TConclusionStatus::Fail("incorrect json in request METADATA_MEMORY_MANAGER.FEATURES parameter");
            }
        }
        auto result = MetadataManagerConstructor->DeserializeFromJson(jsonData);
        if (result.IsFail()) {
            return result;
        }
    }

    return TConclusionStatus::Success();
}

void TUpsertOptionsOperation::DoSerializeScheme(NKikimrSchemeOp::TAlterColumnTableSchema& schemaData) const {
    if (SchemeNeedActualization) {
        schemaData.MutableOptions()->SetSchemeNeedActualization(*SchemeNeedActualization);
    }
    if (ScanReaderPolicyName) {
        schemaData.MutableOptions()->SetScanReaderPolicyName(*ScanReaderPolicyName);
    }
    if (DeduplicationEnabled) {
        schemaData.MutableOptions()->SetDeduplicationEnabled(*DeduplicationEnabled);
    }
    if (CacheBlobsAfterWrite) {
        schemaData.MutableOptions()->SetCacheBlobsAfterWrite(*CacheBlobsAfterWrite);
    }
    if (CompactionPlannerConstructor.HasObject()) {
        CompactionPlannerConstructor.SerializeToProto(*schemaData.MutableOptions()->MutableCompactionPlannerConstructor());
    }
    if (MetadataManagerConstructor.HasObject()) {
        MetadataManagerConstructor.SerializeToProto(*schemaData.MutableOptions()->MutableMetadataManagerConstructor());
    }
    if (InsertOptionsBuildIndexesEnabled || InsertOptionsBuildIndexesMinBlobBytes) {
        auto& options = *schemaData.MutableOptions()->MutableInsertOptions();
        if (InsertOptionsBuildIndexesEnabled) {
            options.SetBuildIndexesEnabled(*InsertOptionsBuildIndexesEnabled);
        }
        if (InsertOptionsBuildIndexesMinBlobBytes) {
            options.SetBuildIndexesMinBlobBytes(*InsertOptionsBuildIndexesMinBlobBytes);
        }
    }
}

}
