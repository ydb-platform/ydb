#pragma once
#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <ydb/core/tx/columnshard/engines/storage/optimizer/abstract/optimizer.h>
#include <ydb/core/tx/schemeshard/olap/common/common.h>

#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NSchemeShard {

class TOlapOptionsUpdate {
private:
    YDB_ACCESSOR(bool, SchemeNeedActualization, false);
    YDB_ACCESSOR_DEF(std::optional<bool>, ExternalGuaranteeExclusivePK);
    YDB_ACCESSOR_DEF(NOlap::NStorageOptimizer::TOptimizerPlannerConstructorContainer, CompactionPlannerConstructor);
public:
    bool Parse(const NKikimrSchemeOp::TAlterColumnTableSchema& alterRequest, IErrorCollector& errors) {
        SchemeNeedActualization = alterRequest.GetOptions().GetSchemeNeedActualization();
        if (alterRequest.GetOptions().HasExternalGuaranteeExclusivePK()) {
            ExternalGuaranteeExclusivePK = alterRequest.GetOptions().GetExternalGuaranteeExclusivePK();
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
        if (ExternalGuaranteeExclusivePK) {
            alterRequest.MutableOptions()->SetExternalGuaranteeExclusivePK(*ExternalGuaranteeExclusivePK);
        }
        if (CompactionPlannerConstructor.HasObject()) {
            CompactionPlannerConstructor.SerializeToProto(*alterRequest.MutableOptions()->MutableCompactionPlannerConstructor());
        }
    }
};
}
