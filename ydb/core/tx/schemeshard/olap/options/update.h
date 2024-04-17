#pragma once
#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <ydb/core/tx/schemeshard/olap/common/common.h>

#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NSchemeShard {

    class TOlapOptionsUpdate {
    private:
        YDB_ACCESSOR(bool, SchemeNeedActualization, false);
        YDB_ACCESSOR_DEF(std::optional<bool>, ExternalGuaranteeExclusivePK);
    public:
        bool Parse(const NKikimrSchemeOp::TAlterColumnTableSchema& alterRequest, IErrorCollector& /*errors*/) {
            SchemeNeedActualization = alterRequest.GetOptions().GetSchemeNeedActualization();
            if (alterRequest.GetOptions().HasExternalGuaranteeExclusivePK()) {
                ExternalGuaranteeExclusivePK = alterRequest.GetOptions().GetExternalGuaranteeExclusivePK();
            }
            return true;
        }
        void SerializeToProto(NKikimrSchemeOp::TAlterColumnTableSchema& alterRequest) const {
            alterRequest.MutableOptions()->SetSchemeNeedActualization(SchemeNeedActualization);
            if (ExternalGuaranteeExclusivePK) {
                alterRequest.MutableOptions()->SetExternalGuaranteeExclusivePK(*ExternalGuaranteeExclusivePK);
            }
            
        }
    };
}
