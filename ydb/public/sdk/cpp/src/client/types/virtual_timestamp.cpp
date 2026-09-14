#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/virtual_timestamp.h>

#include <util/stream/output.h>
#include <util/string/printf.h>

#include <tuple>

namespace NYdb::inline Dev {
namespace NScheme {

TVirtualTimestamp::TVirtualTimestamp(uint64_t planStep, uint64_t txId)
    : PlanStep(planStep)
    , TxId(txId)
{}

TVirtualTimestamp::TVirtualTimestamp(const ::Ydb::VirtualTimestamp& proto)
    : TVirtualTimestamp(proto.plan_step(), proto.tx_id())
{}

std::string TVirtualTimestamp::ToString() const {
    TString result;
    TStringOutput out(result);
    Out(out);
    return result;
}

void TVirtualTimestamp::Out(IOutputStream& out) const {
    out << "{ plan_step: " << PlanStep
      << ", tx_id: " << TxId
      << " }";
}

bool TVirtualTimestamp::operator<(const TVirtualTimestamp& rhs) const {
    return std::tie(PlanStep, TxId) < std::tie(rhs.PlanStep, rhs.TxId);
}

bool TVirtualTimestamp::operator<=(const TVirtualTimestamp& rhs) const {
    return std::tie(PlanStep, TxId) <= std::tie(rhs.PlanStep, rhs.TxId);
}

bool TVirtualTimestamp::operator>(const TVirtualTimestamp& rhs) const {
    return std::tie(PlanStep, TxId) > std::tie(rhs.PlanStep, rhs.TxId);
}

bool TVirtualTimestamp::operator>=(const TVirtualTimestamp& rhs) const {
    return std::tie(PlanStep, TxId) >= std::tie(rhs.PlanStep, rhs.TxId);
}

bool TVirtualTimestamp::operator==(const TVirtualTimestamp& rhs) const {
    return PlanStep == rhs.PlanStep && TxId == rhs.TxId;
}

bool TVirtualTimestamp::operator!=(const TVirtualTimestamp& rhs) const {
    return !(*this == rhs);
}

} // namespace NScheme
} // namespace NYdb
