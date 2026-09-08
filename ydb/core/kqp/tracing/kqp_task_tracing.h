#pragma once

#include <ydb/core/protos/kqp_physical.pb.h>
#include <ydb/library/actors/wilson/wilson_span.h>
#include <ydb/library/yql/dq/proto/dq_tasks.pb.h>

namespace NKikimr::NKqp {

class TTaskTraceDescription {
public:
    static TTaskTraceDescription FromStage(const NKqpProto::TKqpPhyStage& stage);
    NWilson::TArrayValue OperationsAttribute() const;
    TString Name() const;
    void Save(NYql::NDqProto::TDqTask& task) const;
    static void Annotate(NWilson::TSpan& span, const NYql::NDqProto::TDqTask& task);

private:
    ui32 Operations = 0;
};

} // namespace NKikimr::NKqp
