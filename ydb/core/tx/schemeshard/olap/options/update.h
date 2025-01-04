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
    YDB_ACCESSOR_DEF(std::optional<TString>, ScanReaderPolicyName);
    YDB_ACCESSOR_DEF(NOlap::NStorageOptimizer::TOptimizerPlannerConstructorContainer, CompactionPlannerConstructor);
    YDB_ACCESSOR_DEF(NOlap::NDataAccessorControl::TMetadataManagerConstructorContainer, MetadataManagerConstructor);
public:
    bool Parse(const NKikimrSchemeOp::TAlterColumnTableSchema& alterRequest, IErrorCollector& errors) {
        SchemeNeedActualization = alterRequest.GetOptions().GetSchemeNeedActualization();
        if (alterRequest.GetOptions().HasScanReaderPolicyName()) {
            ScanReaderPolicyName = alterRequest.GetOptions().GetScanReaderPolicyName();
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
        return true;
    }
    void SerializeToProto(NKikimrSchemeOp::TAlterColumnTableSchema& alterRequest) const {
        alterRequest.MutableOptions()->SetSchemeNeedActualization(SchemeNeedActualization);
        if (ScanReaderPolicyName) {
            alterRequest.MutableOptions()->SetScanReaderPolicyName(*ScanReaderPolicyName);
        }
        if (CompactionPlannerConstructor.HasObject()) {
            CompactionPlannerConstructor.SerializeToProto(*alterRequest.MutableOptions()->MutableCompactionPlannerConstructor());
        }
        if (MetadataManagerConstructor.HasObject()) {
            MetadataManagerConstructor.SerializeToProto(*alterRequest.MutableOptions()->MutableMetadataManagerConstructor());
        }
    }
};
}
