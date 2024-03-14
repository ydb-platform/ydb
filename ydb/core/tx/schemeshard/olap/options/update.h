#pragma once
#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <ydb/core/tx/schemeshard/olap/common/common.h>

#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NSchemeShard {

    class TOlapOptionsUpdate {
    private:
        YDB_ACCESSOR(bool, SchemeNeedActualization, false);
    public:
        bool Parse(const NKikimrSchemeOp::TAlterColumnTableSchema& alterRequest, IErrorCollector& /*errors*/) {
            SchemeNeedActualization = alterRequest.GetOptions().GetSchemeNeedActualization();
            return true;
        }
        void SerializeToProto(NKikimrSchemeOp::TAlterColumnTableSchema& alterRequest) const {
            alterRequest.MutableOptions()->SetSchemeNeedActualization(SchemeNeedActualization);
        }
    };
}
