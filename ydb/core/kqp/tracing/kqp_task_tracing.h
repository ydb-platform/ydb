#pragma once

#include <ydb/library/actors/wilson/wilson_span.h>

namespace NKqpProto {
class TKqpPhyStage;
} // namespace NKqpProto

namespace NYql::NDqProto {
class TDqTask;
} // namespace NYql::NDqProto

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

private:
    ui32 Operations_ = 0;
};

void SaveTaskTraceParent(NYql::NDqProto::TDqTask& task, ui64 stageSpanId);
NWilson::TTraceId GetTaskTraceParent(const NYql::NDqProto::TDqTask& task, const NWilson::TTraceId& parent);

} // namespace NKikimr::NKqp
