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
    YDB_ACCESSOR(bool, SchemeNeedActualizationSpecified, false);
    YDB_ACCESSOR(bool, ScanReaderPolicyNameSpecified, false);
    YDB_ACCESSOR_DEF(std::optional<TString>, ScanReaderPolicyName);
    YDB_ACCESSOR_DEF(std::optional<bool>, InsertOptionsBuildIndexesEnabled);
    YDB_ACCESSOR_DEF(std::optional<ui64>, InsertOptionsBuildIndexesMinBlobBytes);
    YDB_ACCESSOR_DEF(NOlap::NStorageOptimizer::TOptimizerPlannerConstructorContainer, CompactionPlannerConstructor);
    YDB_ACCESSOR_DEF(NOlap::NDataAccessorControl::TMetadataManagerConstructorContainer, MetadataManagerConstructor);
public:

    bool Parse(const NKikimrSchemeOp::TAlterColumnTableSchema& alterRequest, IErrorCollector& errors) {
        if (!alterRequest.HasOptions()) {
            return true;
        }
        if (alterRequest.GetOptions().HasSchemeNeedActualization()) {
            SetSchemeNeedActualizationSpecified(true);
            SetSchemeNeedActualization(alterRequest.GetOptions().GetSchemeNeedActualization());
        }
        if (alterRequest.GetOptions().HasScanReaderPolicyName()) {
            SetScanReaderPolicyNameSpecified(true);
            const TString& name = alterRequest.GetOptions().GetScanReaderPolicyName();
            if (name.empty()) {
                ScanReaderPolicyName = std::nullopt;
            } else {
                ScanReaderPolicyName = name;
            }
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
        return true;
    }
    void SerializeToProto(NKikimrSchemeOp::TAlterColumnTableSchema& alterRequest) const {
        if (GetSchemeNeedActualizationSpecified()) {
            alterRequest.MutableOptions()->SetSchemeNeedActualization(GetSchemeNeedActualization());
        }
        if (GetScanReaderPolicyNameSpecified()) {
            if (ScanReaderPolicyName) {
                alterRequest.MutableOptions()->SetScanReaderPolicyName(*ScanReaderPolicyName);
            } else {
                alterRequest.MutableOptions()->SetScanReaderPolicyName(TString{});
            }
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
