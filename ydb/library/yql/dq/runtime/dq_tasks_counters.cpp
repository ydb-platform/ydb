#include "dq_tasks_counters.h"

namespace NYql::NDq {

NYql::NUdf::TCounter TDqTaskCountersProvider::GetCounter(const NUdf::TStringRef& module, const NUdf::TStringRef& name, bool /* deriv */) {

    TString op = TString(module);

    TOperatorStat* opStat = OperatorStat.FindPtr(op);

    if (!opStat) {
        TString id;
        TOperatorType opType(TOperatorType::Unknown);

        if (op.StartsWith(NKikimr::NMiniKQL::Operator_Join)) {
            opType = TOperatorType::Join;
            id = op.substr(NKikimr::NMiniKQL::Operator_Join.size());
        } else if (op.StartsWith(NKikimr::NMiniKQL::Operator_Aggregation)) {
            opType = TOperatorType::Aggregation;
            id = op.substr(NKikimr::NMiniKQL::Operator_Aggregation.size());
        } else if (op.StartsWith(NKikimr::NMiniKQL::Operator_Filter)) {
            opType = TOperatorType::Filter;
            id = op.substr(NKikimr::NMiniKQL::Operator_Filter.size());
        } else {
            return NYql::NUdf::TCounter();
        }

        opStat = &OperatorStat[op];
        opStat->OperatorType = opType;
        opStat->OperatorId = id;
    };

    if (name == NKikimr::NMiniKQL::Counter_OutputRows) {
        return NYql::NUdf::TCounter(&opStat->Rows);
    }

    return NYql::NUdf::TCounter();
}

NYql::NUdf::TScopedProbe TDqTaskCountersProvider::GetScopedProbe(const NUdf::TStringRef& /* module */, const NUdf::TStringRef& /* name */) {
    return NYql::NUdf::TScopedProbe();
}

} // namespace NYql::NDq
