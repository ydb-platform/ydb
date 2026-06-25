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

TOperationStats::TOperationStats(const Ydb::TableStats::OperationStats& proto)
    : Rows_(proto.rows())
    , Bytes_(proto.bytes())
{}

uint64_t TOperationStats::GetRows() const {
    return Rows_;
}

uint64_t TOperationStats::GetBytes() const {
    return Bytes_;
}

TTableAccessStats::TTableAccessStats(const Ydb::TableStats::TableAccessStats& proto)
    : Name_(proto.name())
    , Reads_(proto.reads())
    , Updates_(proto.updates())
    , Deletes_(proto.deletes())
    , PartitionsCount_(proto.partitions_count())
{}

const std::string& TTableAccessStats::GetName() const {
    return Name_;
}

const TOperationStats& TTableAccessStats::GetReads() const {
    return Reads_;
}

const TOperationStats& TTableAccessStats::GetUpdates() const {
    return Updates_;
}

const TOperationStats& TTableAccessStats::GetDeletes() const {
    return Deletes_;
}

uint64_t TTableAccessStats::GetPartitionsCount() const {
    return PartitionsCount_;
}

TQueryPhaseStats::TQueryPhaseStats(const Ydb::TableStats::QueryPhaseStats& proto)
    : DurationUs_(proto.duration_us())
    , CpuTimeUs_(proto.cpu_time_us())
    , AffectedShards_(proto.affected_shards())
    , LiteralPhase_(proto.literal_phase())
{
    TableAccess_.reserve(proto.table_access().size());
    for (const auto& tableAccess : proto.table_access()) {
        TableAccess_.emplace_back(tableAccess);
    }
}

uint64_t TQueryPhaseStats::GetDurationUs() const {
    return DurationUs_;
}

TDuration TQueryPhaseStats::GetDuration() const {
    return TDuration::MicroSeconds(DurationUs_);
}

uint64_t TQueryPhaseStats::GetCpuTimeUs() const {
    return CpuTimeUs_;
}

TDuration TQueryPhaseStats::GetCpuTime() const {
    return TDuration::MicroSeconds(CpuTimeUs_);
}

uint64_t TQueryPhaseStats::GetAffectedShards() const {
    return AffectedShards_;
}

bool TQueryPhaseStats::IsLiteralPhase() const {
    return LiteralPhase_;
}

const std::vector<TTableAccessStats>& TQueryPhaseStats::GetTableAccess() const {
    return TableAccess_;
}

TCompilationStats::TCompilationStats(const Ydb::TableStats::CompilationStats& proto)
    : FromCache_(proto.from_cache())
    , DurationUs_(proto.duration_us())
    , CpuTimeUs_(proto.cpu_time_us())
{}

bool TCompilationStats::IsFromCache() const {
    return FromCache_;
}

uint64_t TCompilationStats::GetDurationUs() const {
    return DurationUs_;
}

TDuration TCompilationStats::GetDuration() const {
    return TDuration::MicroSeconds(DurationUs_);
}

uint64_t TCompilationStats::GetCpuTimeUs() const {
    return CpuTimeUs_;
}

TDuration TCompilationStats::GetCpuTime() const {
    return TDuration::MicroSeconds(CpuTimeUs_);
}

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

uint64_t TExecStats::GetProcessCpuTimeUs() const {
    return Impl_->Proto.process_cpu_time_us();
}

uint64_t TExecStats::GetTotalDurationUs() const {
    return Impl_->Proto.total_duration_us();
}

uint64_t TExecStats::GetTotalCpuTimeUs() const {
    return Impl_->Proto.total_cpu_time_us();
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

TDuration TExecStats::GetProcessCpuTime() const {
    return TDuration::MicroSeconds(Impl_->Proto.process_cpu_time_us());
}

TDuration TExecStats::GetTotalDuration() const {
    return TDuration::MicroSeconds(Impl_->Proto.total_duration_us());
}

TDuration TExecStats::GetTotalCpuTime() const {
    return TDuration::MicroSeconds(Impl_->Proto.total_cpu_time_us());
}

std::vector<TQueryPhaseStats> TExecStats::GetQueryPhases() const {
    std::vector<TQueryPhaseStats> phases;
    phases.reserve(Impl_->Proto.query_phases().size());
    for (const auto& phase : Impl_->Proto.query_phases()) {
        phases.emplace_back(phase);
    }
    return phases;
}

std::optional<TCompilationStats> TExecStats::GetCompilation() const {
    const auto& proto = Impl_->Proto;
    if (!proto.Hascompilation()) {
        return {};
    }
    return TCompilationStats(proto.compilation());
}

const Ydb::TableStats::QueryStats& TExecStats::GetProto() const {
    return Impl_->Proto;
}

} // namespace NYdb::NQuery
