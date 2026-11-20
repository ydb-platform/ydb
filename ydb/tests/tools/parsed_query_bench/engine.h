#pragma once

#include "measurement.h"

#include <ydb/tests/tools/parsed_query_bench/proto/main_stages.pb.h>
#include <ydb/public/api/protos/ydb_value.pb.h>
#include <yql/essentials/ast/yql_ast.h>

#include <functional>

namespace Ydb::Query { class LogicalPlan; }

namespace NYql {
struct TKikimrConfiguration;
}

namespace NKikimr::NKqp::NParsedQueryBench {

using TParameterMap = google::protobuf::Map<TString, Ydb::TypedValue>;

struct TLogicalRoot {
    TString Plan;
    TString TypedExpression;
    TVector<std::pair<TString, TString>> Columns;
    TVector<ui32> ColumnIds;
    TVector<TString> QueryColumns;
    ui32 ResultIndex;
    ui64 PhysicalTxGroup;
    bool UnorderedResult;

    bool operator==(const TLogicalRoot&) const = default;
};

struct TLogicalResult {
    TMeasurementTime Done{};
    size_t RootCount = 0;
    TVector<TLogicalRoot> Roots;
    TVector<TString> LogicalSignatures;
};

class TEngine {
public:
    explicit TEngine(const TString& catalogPath);
    ~TEngine();

    const NYql::TKikimrConfiguration& TranslationConfig() const;
    void WithSession(std::function<void()> run) const;
    TLogicalResult LowerText(const TString& text, const TParameterMap& parameters, bool verify, TMeasurementTime* imported = nullptr, bool logicalSignature = false) const;
    TLogicalResult Lower(NYql::TAstParseResult ast, const TParameterMap& parameters, bool verify) const;
    void ExportKql(const TString& text, const TParameterMap& parameters, Ydb::Query::KqlQueryList& out) const;
    TLogicalResult LowerKql(const Ydb::Query::KqlQueryList& wire, const TParameterMap& parameters, bool verify,
        TMeasurementTime* imported) const;

    TLogicalResult LowerLogical(const Ydb::Query::LogicalPlan& wire, const TParameterMap& parameters,
        bool verify, TMeasurementTime* imported) const;

    TString Cluster = "benchmark";
    TString PathPrefix = "/Root";

private:
    struct TImpl;
    std::unique_ptr<TImpl> Impl;
};

} // namespace NKikimr::NKqp::NParsedQueryBench
