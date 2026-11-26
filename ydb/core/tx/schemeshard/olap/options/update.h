#pragma once
#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <ydb/core/tx/columnshard/data_accessor/abstract/constructor.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/abstract/optimizer.h>
#include <ydb/core/tx/schemeshard/olap/common/common.h>

#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NSchemeShard {

class TOlapOptionsUpdate {
private:
    YDB_ACCESSOR(bool, SchemeNeedActualization, false);
    YDB_ACCESSOR_DEF(std::optional<bool>, TieringNeedActualization);
    YDB_ACCESSOR_DEF(std::optional<TString>, ScanReaderPolicyName);
    YDB_ACCESSOR_DEF(std::optional<bool>, DeduplicationEnabled);
    YDB_ACCESSOR_DEF(std::optional<bool>, CacheBlobsAfterWrite);
    YDB_ACCESSOR_DEF(std::optional<bool>, InsertOptionsBuildIndexesEnabled);
    YDB_ACCESSOR_DEF(std::optional<ui64>, InsertOptionsBuildIndexesMinBlobBytes);
    YDB_ACCESSOR_DEF(NOlap::NStorageOptimizer::TOptimizerPlannerConstructorContainer, CompactionPlannerConstructor);
    YDB_ACCESSOR_DEF(NOlap::NDataAccessorControl::TMetadataManagerConstructorContainer, MetadataManagerConstructor);
public:
    static bool IsValidScanReaderPolicyName(const TString& name) {
        return name == "PLAIN" || name == "SIMPLE" || name == "TRIVIAL";
    }

    // Shared by create (TColumnTableSchemeOptions) and alter (TColumnTableRequestedOptions); the two messages have the same fields.
    template <class TOptionsProto>
    bool ParseOptions(const TOptionsProto& options, IErrorCollector& errors) {
        SchemeNeedActualization = options.GetSchemeNeedActualization();
        if (options.HasTieringNeedActualization()) {
            TieringNeedActualization = options.GetTieringNeedActualization();
        }

        if (options.HasScanReaderPolicyName()) {
            if (!IsValidScanReaderPolicyName(options.GetScanReaderPolicyName())) {
                errors.AddError(NKikimrScheme::StatusSchemeError, TStringBuilder() << "Unsupported scan reader policy name '"
                    << options.GetScanReaderPolicyName() << "': only 'PLAIN', 'SIMPLE', 'TRIVIAL' are allowed for column tables");
                return false;
            }
            ScanReaderPolicyName = options.GetScanReaderPolicyName();
        }
        if (options.HasDeduplicationEnabled()) {
            DeduplicationEnabled = options.GetDeduplicationEnabled();
        }
        if (options.HasCacheBlobsAfterWrite()) {
            CacheBlobsAfterWrite = options.GetCacheBlobsAfterWrite();
        }
        if (options.HasMetadataManagerConstructor()) {
            auto container = NOlap::NDataAccessorControl::TMetadataManagerConstructorContainer::BuildFromProto(options.GetMetadataManagerConstructor());
            if (container.IsFail()) {
                errors.AddError(container.GetErrorMessage());
                return false;
            }
            MetadataManagerConstructor = container.DetachResult();
        }
        if (options.HasCompactionPlannerConstructor()) {
            auto container = NOlap::NStorageOptimizer::TOptimizerPlannerConstructorContainer::BuildFromProto(options.GetCompactionPlannerConstructor());
            if (container.IsFail()) {
                errors.AddError(container.GetErrorMessage());
                return false;
            }
            CompactionPlannerConstructor = container.DetachResult();
        }
        if (options.HasInsertOptions()) {
            const auto& optionsProto = options.GetInsertOptions();
            if (optionsProto.HasBuildIndexesEnabled()) {
                InsertOptionsBuildIndexesEnabled = optionsProto.GetBuildIndexesEnabled();
            }
            if (optionsProto.HasBuildIndexesMinBlobBytes()) {
                InsertOptionsBuildIndexesMinBlobBytes = optionsProto.GetBuildIndexesMinBlobBytes();
            }
        }
        return true;
    }

    bool Parse(const NKikimrSchemeOp::TAlterColumnTableSchema& alterRequest, IErrorCollector& errors) {
        return ParseOptions(alterRequest.GetOptions(), errors);
    }
    void SerializeToProto(NKikimrSchemeOp::TAlterColumnTableSchema& alterRequest) const {
        alterRequest.MutableOptions()->SetSchemeNeedActualization(SchemeNeedActualization);
        if (TieringNeedActualization) {
            alterRequest.MutableOptions()->SetTieringNeedActualization(*TieringNeedActualization);
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
    }
};
}
