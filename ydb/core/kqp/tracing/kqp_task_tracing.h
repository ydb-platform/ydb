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
    TString StageName() const;
    void Save(NYql::NDqProto::TDqTask& task) const;
    static void Annotate(NWilson::TSpan& span, const NYql::NDqProto::TDqTask& task);

private:
    TString Name(TStringBuf prefix) const;
    ui32 Operations = 0;
};

void SaveTaskTraceParent(NYql::NDqProto::TDqTask& task, ui64 stageSpanId);
NWilson::TTraceId GetTaskTraceParent(const NYql::NDqProto::TDqTask& task, const NWilson::TTraceId& parent);

} // namespace NKikimr::NKqp
