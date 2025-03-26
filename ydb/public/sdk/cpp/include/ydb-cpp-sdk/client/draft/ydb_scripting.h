#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/api/protos/ydb_query.pb.h>

namespace NYdb::inline Dev {
namespace NScripting {

class TExecuteYqlResult : public TStatus {
public:
    TExecuteYqlResult(TStatus&& status, std::vector<TResultSet>&& resultSets,
        const std::optional<NTable::TQueryStats>& queryStats);

    const std::vector<TResultSet>& GetResultSets() const;
    TResultSet GetResultSet(size_t resultIndex) const;

    TResultSetParser GetResultSetParser(size_t resultIndex) const;

    const std::optional<NTable::TQueryStats>& GetStats() const;

private:
    std::vector<TResultSet> ResultSets_;
    std::optional<NTable::TQueryStats> QueryStats_;
};

class TYqlPartialResult {
public:
    TYqlPartialResult(uint32_t resultSetIndex, TResultSet&& resultSet)
        : ResultSetIndex_(resultSetIndex)
        , ResultSet_(std::move(resultSet))
    {}

    uint32_t GetResultSetIndex() const { return ResultSetIndex_; }
    const TResultSet& GetResultSet() const { return ResultSet_; }

private:
    uint32_t ResultSetIndex_;
    TResultSet ResultSet_;
};

class TYqlResultPart : public TStreamPartStatus {
public:
    bool HasPartialResult() const { return PartialResult_.has_value(); }
    const TYqlPartialResult& GetPartialResult() const { return *PartialResult_; }

    bool HasQueryStats() const { return QueryStats_.has_value(); }
    const NTable::TQueryStats& GetQueryStats() const { return *QueryStats_; }
    NTable::TQueryStats ExtractQueryStats() { return std::move(*QueryStats_); }

    TYqlResultPart(TStatus&& status)
        : TStreamPartStatus(std::move(status))
    {}

    TYqlResultPart(TStatus&& status, const std::optional<NTable::TQueryStats> &queryStats)
        : TStreamPartStatus(std::move(status))
        , QueryStats_(queryStats)
    {}

    TYqlResultPart(TStatus&& status, TYqlPartialResult&& partialResult, const std::optional<NTable::TQueryStats> &queryStats)
        : TStreamPartStatus(std::move(status))
        , PartialResult_(std::move(partialResult))
        , QueryStats_(queryStats)
    {}

private:
    std::optional<TYqlPartialResult> PartialResult_;
    std::optional<NTable::TQueryStats> QueryStats_;
};

using TAsyncYqlResultPart = NThreading::TFuture<TYqlResultPart>;

class TYqlResultPartIterator : public TStatus {
    friend class TScriptingClient;
public:
    TAsyncYqlResultPart ReadNext();
    class TReaderImpl;
private:
    TYqlResultPartIterator(
        std::shared_ptr<TReaderImpl> impl,
        TPlainStatus&& status
    );
    std::shared_ptr<TReaderImpl> ReaderImpl_;
};

class TExplainYqlResult : public TStatus {
public:
    TExplainYqlResult(TStatus&& status, const ::google::protobuf::Map<TStringType, Ydb::Type>&& types, std::string&& plan);

    std::map<std::string, TType> GetParameterTypes() const;
    const std::string& GetPlan() const;

private:
    ::google::protobuf::Map<TStringType, Ydb::Type> ParameterTypes_;
    std::string Plan_;
};

using TAsyncExecuteYqlResult = NThreading::TFuture<TExecuteYqlResult>;
using TAsyncYqlResultPartIterator = NThreading::TFuture<TYqlResultPartIterator>;
using TAsyncExplainYqlResult = NThreading::TFuture<TExplainYqlResult>;

////////////////////////////////////////////////////////////////////////////////

struct TExecuteYqlRequestSettings : public TOperationRequestSettings<TExecuteYqlRequestSettings> {
    FLUENT_SETTING_DEFAULT(Ydb::Query::Syntax, Syntax, Ydb::Query::SYNTAX_YQL_V1);
    FLUENT_SETTING_DEFAULT(NTable::ECollectQueryStatsMode, CollectQueryStats, NTable::ECollectQueryStatsMode::None);
};

enum class ExplainYqlRequestMode {
    // Parse = 1,
    Validate = 2,
    Plan = 3,
};

struct TExplainYqlRequestSettings : public TOperationRequestSettings<TExplainYqlRequestSettings> {
    FLUENT_SETTING_DEFAULT(ExplainYqlRequestMode, Mode, ExplainYqlRequestMode::Validate);
};

////////////////////////////////////////////////////////////////////////////////

class TScriptingClient {
    class TImpl;

public:
    TScriptingClient(const TDriver& driver, const TCommonClientSettings& settings = TCommonClientSettings());

    //! Returns new params builder
    TParamsBuilder GetParamsBuilder();

    TAsyncExecuteYqlResult ExecuteYqlScript(const std::string& script,
        const TExecuteYqlRequestSettings& settings = TExecuteYqlRequestSettings());

    TAsyncExecuteYqlResult ExecuteYqlScript(const std::string& script, const TParams& params,
        const TExecuteYqlRequestSettings& settings = TExecuteYqlRequestSettings());

    TAsyncExecuteYqlResult ExecuteYqlScript(const std::string& script, TParams&& params,
        const TExecuteYqlRequestSettings& settings = TExecuteYqlRequestSettings());

    TAsyncYqlResultPartIterator StreamExecuteYqlScript(const std::string& script,
        const TExecuteYqlRequestSettings& settings = TExecuteYqlRequestSettings());

    TAsyncYqlResultPartIterator StreamExecuteYqlScript(const std::string& script, const TParams& params,
        const TExecuteYqlRequestSettings& settings = TExecuteYqlRequestSettings());

    TAsyncYqlResultPartIterator StreamExecuteYqlScript(const std::string& script, TParams&& params,
        const TExecuteYqlRequestSettings& settings = TExecuteYqlRequestSettings());

    TAsyncExplainYqlResult ExplainYqlScript(const std::string& script,
        const TExplainYqlRequestSettings& settings = TExplainYqlRequestSettings());

private:
    std::shared_ptr<TImpl> Impl_;
};

} // namespace NScripting
} // namespace NYdb
