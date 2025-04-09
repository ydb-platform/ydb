#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/stats.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/type_switcher.h>

#include <ydb/public/api/protos/ydb_table.pb.h>

#include <util/datetime/base.h>

#include <google/protobuf/text_format.h>

namespace NYdb::inline Dev::NQuery {

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

std::string TExecStats::ToString(bool withPlan) const {
    auto proto = Impl_->Proto;

    if (!withPlan) {
        proto.clear_query_plan();
        proto.clear_query_ast();
        proto.clear_query_meta();
    }

    TStringType res;
    ::google::protobuf::TextFormat::PrintToString(proto, &res);
    return res;
}

std::optional<std::string> TExecStats::GetPlan() const {
    auto proto = Impl_->Proto;

    if (proto.query_plan().empty()) {
        return {};
    }

    return proto.query_plan();
}

std::optional<std::string> TExecStats::GetAst() const {
    auto proto = Impl_->Proto;

    if (proto.query_ast().empty()) {
        return {};
    }

    return proto.query_ast();
}

std::optional<std::string> TExecStats::GetMeta() const {
    auto proto = Impl_->Proto;

    if (proto.query_meta().empty()) {
        return {};
    }

    return proto.query_meta();
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
