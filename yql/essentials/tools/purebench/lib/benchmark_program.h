#pragma once

#include <yql/essentials/public/langver/yql_langver.h>

#include <library/cpp/yson/node/node.h>

#include <util/datetime/base.h>
#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/stream/fwd.h>
#include <util/system/types.h>

#include <variant>

namespace NYql::NPureBench {

enum class ESyntax {
    SQL,
    PG,
};

struct TBenchmarkProgramOptions {
    ui64 GeneratorInputRows = 1000000;
    TString GenerationQuery = "select index from Input";
    TString MeasuredQuery = "select count(*) as count from Input";
    ESyntax GenerationSyntax = ESyntax::SQL;
    ESyntax MeasuredQuerySyntax = ESyntax::SQL;
    TString UdfsDirectory;
    TString LLVMSettings;
    TString BlockEngineSettings = "disable";
    TLangVersion LanguageVersion = GetMaxReleasedLangVersion();
    bool EnableCalibration = true;
};

class TQueryOutput {
public:
    TQueryOutput(NYT::TNode rowType, NYT::TNode::TListType rows);

    const NYT::TNode& GetRowType() const;
    const NYT::TNode::TListType& GetRows() const;

private:
    NYT::TNode RowType_;
    NYT::TNode Rows_;
};

TString ToYson(const TQueryOutput& result);

struct TBenchmarkPreparationResult {
    ui64 GeneratorInputBytes = 0;
    ui64 BenchmarkInputBytes = 0;
};

enum class EOutputMode {
    Discard,
    Collect,
};

struct TQueryRunStatistics {
    TDuration Elapsed;
    ui64 GeneratorInputBytes = 0;
    ui64 BenchmarkInputBytes = 0;
};

struct TMeasureQueryResult {
    TQueryRunStatistics Statistics;
    TMaybe<TQueryOutput> Output;
};

struct TCalibrationDisabled {};

using TCalibrationQueryResult = std::variant<TQueryRunStatistics, TCalibrationDisabled>;

class IBenchmarkProgram {
public:
    virtual ~IBenchmarkProgram() = default;

    // Compiles queries once; regenerates input and warms up execution on each call.
    virtual TBenchmarkPreparationResult PrepareBenchmark(IOutputStream* exprOutput) = 0;
    // Collect includes result serialization in Elapsed.
    virtual TMeasureQueryResult RunMeasureQuery(EOutputMode outputMode) = 0;
    virtual TCalibrationQueryResult RunCalibrationQuery() = 0;
};

THolder<IBenchmarkProgram> CreateBenchmarkProgram(const TBenchmarkProgramOptions& options);

} // namespace NYql::NPureBench
