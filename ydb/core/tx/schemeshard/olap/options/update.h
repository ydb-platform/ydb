#pragma once
#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <ydb/core/tx/columnshard/data_accessor/abstract/constructor.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/abstract/optimizer.h>
#include <ydb/core/tx/schemeshard/olap/common/common.h>

#include <ydb/library/accessor/accessor.h>

#include <util/string/builder.h>

namespace NKikimr::NSchemeShard {

class TOlapOptionsUpdate {
public:
    using TRequestedOptions = NKikimrSchemeOp::TColumnTableRequestedOptions;
    using EOption = TRequestedOptions::EOption;

private:
    YDB_ACCESSOR_DEF(std::optional<bool>, SchemeNeedActualization);
    YDB_ACCESSOR_DEF(std::optional<TString>, ScanReaderPolicyName);
    YDB_ACCESSOR_DEF(std::optional<bool>, DeduplicationEnabled);
    YDB_ACCESSOR_DEF(std::optional<bool>, CacheBlobsAfterWrite);
    YDB_ACCESSOR_DEF(std::optional<bool>, InsertOptionsBuildIndexesEnabled);
    YDB_ACCESSOR_DEF(std::optional<ui64>, InsertOptionsBuildIndexesMinBlobBytes);
    YDB_ACCESSOR_DEF(NOlap::NStorageOptimizer::TOptimizerPlannerConstructorContainer, CompactionPlannerConstructor);
    YDB_ACCESSOR_DEF(NOlap::NDataAccessorControl::TMetadataManagerConstructorContainer, MetadataManagerConstructor);
    YDB_READONLY_DEF(std::set<EOption>, ResetOptions);

    bool IsOptionSet(const EOption option) const {
        switch (option) {
            case TRequestedOptions::SCHEME_NEED_ACTUALIZATION:
                return !!SchemeNeedActualization;
            case TRequestedOptions::COMPACTION_PLANNER:
                return CompactionPlannerConstructor.HasObject();
            case TRequestedOptions::METADATA_MEMORY_MANAGER:
                return MetadataManagerConstructor.HasObject();
            case TRequestedOptions::SCAN_READER_POLICY_NAME:
                return !!ScanReaderPolicyName;
            case TRequestedOptions::DEDUPLICATION_ENABLED:
                return !!DeduplicationEnabled;
            case TRequestedOptions::INSERT_OPTIONS:
                return !!InsertOptionsBuildIndexesEnabled || !!InsertOptionsBuildIndexesMinBlobBytes;
            case TRequestedOptions::CACHE_BLOBS_AFTER_WRITE:
                return !!CacheBlobsAfterWrite;
        }
        return false;
    }

public:
    bool Parse(const NKikimrSchemeOp::TAlterColumnTableSchema& alterRequest, IErrorCollector& errors) {
        if (!alterRequest.HasOptions()) {
            return true;
        }
        if (alterRequest.GetOptions().HasSchemeNeedActualization()) {
            SchemeNeedActualization = alterRequest.GetOptions().GetSchemeNeedActualization();
        }
        if (alterRequest.GetOptions().HasScanReaderPolicyName()) {
            ScanReaderPolicyName = alterRequest.GetOptions().GetScanReaderPolicyName();
        }
        if (alterRequest.GetOptions().HasDeduplicationEnabled()) {
            DeduplicationEnabled = alterRequest.GetOptions().GetDeduplicationEnabled();
        }
        if (alterRequest.GetOptions().HasCacheBlobsAfterWrite()) {
            CacheBlobsAfterWrite = alterRequest.GetOptions().GetCacheBlobsAfterWrite();
        }
        if (alterRequest.GetOptions().HasMetadataManagerConstructor()) {
            auto container = NOlap::NDataAccessorControl::TMetadataManagerConstructorContainer::BuildFromProto(alterRequest.GetOptions().GetMetadataManagerConstructor());
            if (container.IsFail()) {
                errors.AddError(container.GetErrorMessage());
                return false;
            }
            MetadataManagerConstructor = container.DetachResult();
        }
        if (alterRequest.GetOptions().HasCompactionPlannerConstructor()) {
            auto container = NOlap::NStorageOptimizer::TOptimizerPlannerConstructorContainer::BuildFromProto(alterRequest.GetOptions().GetCompactionPlannerConstructor());
            if (container.IsFail()) {
                errors.AddError(container.GetErrorMessage());
                return false;
            }
            CompactionPlannerConstructor = container.DetachResult();
        }
        if (alterRequest.GetOptions().HasInsertOptions()) {
            const auto& optionsProto = alterRequest.GetOptions().GetInsertOptions();
            if (optionsProto.HasBuildIndexesEnabled()) {
                InsertOptionsBuildIndexesEnabled = optionsProto.GetBuildIndexesEnabled();
            }
            if (optionsProto.HasBuildIndexesMinBlobBytes()) {
                InsertOptionsBuildIndexesMinBlobBytes = optionsProto.GetBuildIndexesMinBlobBytes();
            }
        }
        for (const auto rawOption : alterRequest.GetOptions().GetResetOptions()) {
            if (!TRequestedOptions::EOption_IsValid(rawOption)) {
                errors.AddError(NKikimrScheme::StatusInvalidParameter, TStringBuilder() << "unknown option to reset: " << rawOption);
                return false;
            }
            const auto option = static_cast<EOption>(rawOption);
            if (IsOptionSet(option)) {
                errors.AddError(NKikimrScheme::StatusInvalidParameter, TStringBuilder()
                    << "option " << TRequestedOptions::EOption_Name(option) << " is both set and reset");
                return false;
            }
            ResetOptions.emplace(option);
        }
        return true;
    }
    void SerializeToProto(NKikimrSchemeOp::TAlterColumnTableSchema& alterRequest) const {
        if (SchemeNeedActualization) {
            alterRequest.MutableOptions()->SetSchemeNeedActualization(*SchemeNeedActualization);
        }
        if (ScanReaderPolicyName) {
            alterRequest.MutableOptions()->SetScanReaderPolicyName(*ScanReaderPolicyName);
        }
        if (DeduplicationEnabled) {
            alterRequest.MutableOptions()->SetDeduplicationEnabled(*DeduplicationEnabled);
        }
        if (CacheBlobsAfterWrite) {
            alterRequest.MutableOptions()->SetCacheBlobsAfterWrite(*CacheBlobsAfterWrite);
        }
        if (CompactionPlannerConstructor.HasObject()) {
            CompactionPlannerConstructor.SerializeToProto(*alterRequest.MutableOptions()->MutableCompactionPlannerConstructor());
        }
        if (MetadataManagerConstructor.HasObject()) {
            MetadataManagerConstructor.SerializeToProto(*alterRequest.MutableOptions()->MutableMetadataManagerConstructor());
        }
        if (InsertOptionsBuildIndexesEnabled || InsertOptionsBuildIndexesMinBlobBytes) {
            auto& options = *alterRequest.MutableOptions()->MutableInsertOptions();
            if (InsertOptionsBuildIndexesEnabled) {
                options.SetBuildIndexesEnabled(*InsertOptionsBuildIndexesEnabled);
            }
            if (InsertOptionsBuildIndexesMinBlobBytes) {
                options.SetBuildIndexesMinBlobBytes(*InsertOptionsBuildIndexesMinBlobBytes);
            }
        }
        for (const auto option : ResetOptions) {
            alterRequest.MutableOptions()->AddResetOptions(option);
        }
    }
};
}
