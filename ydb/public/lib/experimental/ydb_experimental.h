#pragma once

#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

namespace NYdb {
namespace NExperimental {

////////////////////////////////////////////////////////////////////////////////

class TStreamPartIterator;

using TAsyncStreamPartIterator = NThreading::TFuture<TStreamPartIterator>;

class TStreamPartStatus : public TStatus {
public:
    TStreamPartStatus(TStatus&& status);
    bool EOS() const;
};

class TStreamPart : public TStreamPartStatus {
public:
    bool HasResultSet() const { return ResultSet_.Defined(); }
    const TResultSet& GetResultSet() const { return *ResultSet_; }
    TResultSet ExtractResultSet() { return std::move(*ResultSet_); }

    bool HasProfile() const { return Profile_.Defined(); }
    const TString& GetProfile() const { return *Profile_; }
    TString ExtractProfile() { return std::move(*Profile_); }

    bool HasPlan() const { return Plan_.Defined(); }
    const TString& GetPlan() const { return *Plan_; }
    TString ExtractPlan() { return std::move(*Plan_); }

    TStreamPart(TStatus&& status)
        : TStreamPartStatus(std::move(status))
    {}

    TStreamPart(TResultSet&& resultSet, TStatus&& status)
        : TStreamPartStatus(std::move(status))
        , ResultSet_(std::move(resultSet))
    {}

    TStreamPart(TString&& profile, TStatus&& status)
        : TStreamPartStatus(std::move(status))
        , Profile_(std::move(profile))
    {}

    TStreamPart(const TMaybe<TString>& plan, TStatus&& status)
        : TStreamPartStatus(std::move(status))
        , Plan_(plan)
    {}

private:
    TMaybe<TResultSet> ResultSet_;
    TMaybe<TString> Profile_;
    TMaybe<TString> Plan_;
};

using TAsyncStreamPart = NThreading::TFuture<TStreamPart>;

class TStreamPartIterator : public TStatus {
    friend class TStreamQueryClient;
public:
    TAsyncStreamPart ReadNext();
    class TReaderImpl;
private:
    TStreamPartIterator(
        std::shared_ptr<TReaderImpl> impl,
        TPlainStatus&& status
    );
    std::shared_ptr<TReaderImpl> ReaderImpl_;
};

enum class EStreamQueryProfileMode {
    None,
    Basic,
    Full,
    Profile
};

struct TExecuteStreamQuerySettings : public TRequestSettings<TExecuteStreamQuerySettings> {
    using TSelf = TExecuteStreamQuerySettings;

    FLUENT_SETTING_DEFAULT(EStreamQueryProfileMode, ProfileMode, EStreamQueryProfileMode::None);
    FLUENT_SETTING_DEFAULT(bool, Explain, false);
};

class TStreamQueryClient {
    class TImpl;

public:
    TStreamQueryClient(const TDriver& driver, const TCommonClientSettings& settings = TCommonClientSettings());

    TParamsBuilder GetParamsBuilder();

    TAsyncStreamPartIterator ExecuteStreamQuery(const TString& query,
        const TExecuteStreamQuerySettings& settings = TExecuteStreamQuerySettings());

    TAsyncStreamPartIterator ExecuteStreamQuery(const TString& query, const TParams& params,
        const TExecuteStreamQuerySettings& settings = TExecuteStreamQuerySettings());

private:
    std::shared_ptr<TImpl> Impl_;
};

} // namespace NExperimental
} // namespace NYdb
