#pragma once

#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>
#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>

#include <util/datetime/base.h>
#include <util/generic/vector.h>

#include <optional>

namespace Ydb::Replication {
    class ConnectionParams;
    class DescribeReplicationResult;
    class DescribeReplicationResult_Stats;
}

namespace NYdb {
    class TProtoAccessor;
}

namespace NYql {
    class TIssues;
}

namespace NYdb::NReplication {

class TDescribeReplicationResult;
using TAsyncDescribeReplicationResult = NThreading::TFuture<TDescribeReplicationResult>;

struct TDescribeReplicationSettings: public TOperationRequestSettings<TDescribeReplicationSettings> {
    using TSelf = TDescribeReplicationSettings;
    FLUENT_SETTING_DEFAULT(bool, IncludeStats, false);
};

struct TStaticCredentials {
    TString User;
    TString PasswordSecretName;
};

struct TOAuthCredentials {
    TString TokenSecretName;
};

class TConnectionParams: private TCommonClientSettings {
public:
    enum class ECredentials {
        Static,
        OAuth,
    };

    explicit TConnectionParams(const Ydb::Replication::ConnectionParams& params);

    const TString& GetDiscoveryEndpoint() const;
    const TString& GetDatabase() const;
    bool GetEnableSsl() const;

    ECredentials GetCredentials() const;
    const TStaticCredentials& GetStaticCredentials() const;
    const TOAuthCredentials& GetOAuthCredentials() const;

private:
    std::variant<
        TStaticCredentials,
        TOAuthCredentials
    > Credentials_;
};

class TStats {
public:
    TStats() = default;
    TStats(const Ydb::Replication::DescribeReplicationResult_Stats& stats);

    const std::optional<TDuration>& GetLag() const;
    const std::optional<float>& GetInitialScanProgress() const;

private:
    std::optional<TDuration> Lag_;
    std::optional<float> InitialScanProgress_;
};

class TRunningState {
public:
    TRunningState() = default;
    explicit TRunningState(const TStats& stats);

    const TStats& GetStats() const;

private:
    TStats Stats_;
};

struct TDoneState {};

class TErrorState {
    class TImpl;

public:
    explicit TErrorState(NYql::TIssues&& issues);

    const NYql::TIssues& GetIssues() const;

private:
    std::shared_ptr<TImpl> Impl_;
};

class TReplicationDescription {
public:
    struct TItem {
        ui64 Id;
        TString SrcPath;
        TString DstPath;
        TStats Stats;
        std::optional<TString> SrcChangefeedName;
    };

    enum class EState {
        Running,
        Error,
        Done,
    };

    explicit TReplicationDescription(const Ydb::Replication::DescribeReplicationResult& desc);

    const TConnectionParams& GetConnectionParams() const;
    const TVector<TItem> GetItems() const;

    EState GetState() const;
    const TRunningState& GetRunningState() const;
    const TErrorState& GetErrorState() const;
    const TDoneState& GetDoneState() const;

private:
    TConnectionParams ConnectionParams_;
    TVector<TItem> Items_;
    std::variant<
        TRunningState,
        TErrorState,
        TDoneState
    > State_;
};

class TDescribeReplicationResult: public NScheme::TDescribePathResult {
    friend class NYdb::TProtoAccessor;
    const Ydb::Replication::DescribeReplicationResult& GetProto() const;

public:
    TDescribeReplicationResult(TStatus&& status, Ydb::Replication::DescribeReplicationResult&& desc);
    const TReplicationDescription& GetReplicationDescription() const;

private:
    TReplicationDescription ReplicationDescription_;
    std::unique_ptr<Ydb::Replication::DescribeReplicationResult> Proto_;
};

class TReplicationClient {
    class TImpl;

public:
    TReplicationClient(const TDriver& driver, const TCommonClientSettings& settings = TCommonClientSettings());

    TAsyncDescribeReplicationResult DescribeReplication(const TString& path,
        const TDescribeReplicationSettings& settings = TDescribeReplicationSettings());

private:
    std::shared_ptr<TImpl> Impl_;
};

} // namespace NYdb::NReplication
