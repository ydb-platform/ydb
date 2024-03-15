#include "status_code_counters.h"

namespace NFq {

TStatusCodeCounters::TStatusCodeCounters(const TString& name, const ::NMonitoring::TDynamicCounterPtr& counters)
    : Name(name)
    , Counters(counters) {
    SubGroup = counters->GetSubgroup("subcomponent", Name);
}

void TStatusCodeCounters::IncByStatusCode(NYql::NDqProto::StatusIds::StatusCode statusCode) {
    auto statusCodeName = NYql::NDqProto::StatusIds::StatusCode_Name(statusCode);
    auto it = CountersByStatusCode.find(statusCode);
    if (it == CountersByStatusCode.end()) {
        it = CountersByStatusCode.insert({statusCode, SubGroup->GetCounter(statusCodeName, true)}).first;
    }
    it->second->Inc();
}

TStatusCodeCounters::~TStatusCodeCounters() {
    Counters->RemoveSubgroup("subcomponent", Name);
}

} // namespace NFq
