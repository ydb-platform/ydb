#include "stats.h"

#include <ydb/public/api/protos/ydb_table.pb.h>

#include <util/datetime/base.h>

#include <google/protobuf/text_format.h>

namespace NYdb {
namespace NTable {

class TQueryStats::TImpl {
public:
    Ydb::TableStats::QueryStats Proto;
};

TQueryStats::TQueryStats(const Ydb::TableStats::QueryStats& proto) {
    Impl_ = std::make_shared<TImpl>();
    Impl_->Proto = proto;
}

TQueryStats::TQueryStats(Ydb::TableStats::QueryStats&& proto) {
    Impl_ = std::make_shared<TImpl>();
    Impl_->Proto = std::move(proto);
}

TString TQueryStats::ToString(bool withPlan) const {
    auto proto = Impl_->Proto;

    if (!withPlan) {
        proto.clear_query_plan();
        proto.clear_query_ast();
    }

    TString res;
    ::google::protobuf::TextFormat::PrintToString(proto, &res);
    return res;
}

TMaybe<TString> TQueryStats::GetPlan() const {
    auto proto = Impl_->Proto;

    if (proto.query_plan().empty()) {
        return {};
    }

    return proto.query_plan();
}

TDuration TQueryStats::GetTotalDuration() const {
    return TDuration::MicroSeconds(Impl_->Proto.total_duration_us());
}

TDuration TQueryStats::GetTotalCpuTime() const {
    return TDuration::MicroSeconds(Impl_->Proto.total_cpu_time_us());
}

const Ydb::TableStats::QueryStats& TQueryStats::GetProto() const {
    return Impl_->Proto;
}

} // namespace NTable
} // namespace NYdb
