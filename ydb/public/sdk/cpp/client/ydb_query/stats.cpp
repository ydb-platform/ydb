#include "stats.h"

#include <ydb/public/api/protos/ydb_table.pb.h>

#include <util/datetime/base.h>

#include <google/protobuf/text_format.h>

namespace NYdb::NQuery {

class TExecStats::TImpl {
public:
    Ydb::TableStats::QueryStats Proto;
};

TExecStats::TExecStats(const Ydb::TableStats::QueryStats& proto) {
    Impl_ = std::make_shared<TImpl>();
    Impl_->Proto = proto;
}

TExecStats::TExecStats(Ydb::TableStats::QueryStats&& proto) {
    Impl_ = std::make_shared<TImpl>();
    Impl_->Proto = std::move(proto);
}

TString TExecStats::ToString(bool withPlan) const {
    auto proto = Impl_->Proto;

    if (!withPlan) {
        proto.clear_query_plan();
        proto.clear_query_ast();
    }

    TString res;
    ::google::protobuf::TextFormat::PrintToString(proto, &res);
    return res;
}

TMaybe<TString> TExecStats::GetPlan() const {
    auto proto = Impl_->Proto;

    if (proto.query_plan().empty()) {
        return {};
    }

    return proto.query_plan();
}

TMaybe<TString> TExecStats::GetAst() const {
    auto proto = Impl_->Proto;

    if (proto.query_ast().empty()) {
        return {};
    }

    return proto.query_ast();
}

TDuration TExecStats::GetTotalDuration() const {
    return TDuration::MicroSeconds(Impl_->Proto.total_duration_us());
}

TDuration TExecStats::GetTotalCpuTime() const {
    return TDuration::MicroSeconds(Impl_->Proto.total_cpu_time_us());
}

const Ydb::TableStats::QueryStats& TExecStats::GetProto() const {
    return Impl_->Proto;
}

} // namespace NYdb::NQuery
