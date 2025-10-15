#pragma once

#include <ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb-cpp-sdk/client/driver/driver.h>

#include <optional>

#include <util/datetime/base.h>

namespace Ydb::Replication {
    class ConnectionParams;
    class ConsistencyLevelGlobal;
    class DescribeReplicationResult;
    class DescribeReplicationResult_Stats;
    class DescribeTransferResult;
    class DescribeTransferResult_Stats;
}

namespace NYdb::inline V3 {
    class TProtoAccessor;
}

namespace NYdb::inline V3::NReplication {

class TDescribeReplicationResult;
using TAsyncDescribeReplicationResult = NThreading::TFuture<TDescribeReplicationResult>;

struct TDescribeReplicationSettings: public TOperationRequestSettings<TDescribeReplicationSettings> {
    using TSelf = TDescribeReplicationSettings;
    FLUENT_SETTING_DEFAULT(bool, IncludeStats, false);
};

struct TStaticCredentials {
    std::string User;
    std::string PasswordSecretName;
};

struct TOAuthCredentials {
    std::string TokenSecretName;
};

class TConnectionParams: private TCommonClientSettings {
public:
    enum class ECredentials {
        Static,
        OAuth,
    };

    explicit TConnectionParams(const Ydb::Replication::ConnectionParams& params);

    const std::string& GetDiscoveryEndpoint() const;
    const std::string& GetDatabase() const;
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

struct TRowConsistency {
};

class TGlobalConsistency {
public:
    explicit TGlobalConsistency(const Ydb::Replication::ConsistencyLevelGlobal& proto);

    const TDuration& GetCommitInterval() const;

private:
    TDuration CommitInterval_;
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

struct TPausedState {};

class TErrorState {
    class TImpl;

public:
    explicit TErrorState(NYdb::NIssue::TIssues&& issues);

    const NYdb::NIssue::TIssues& GetIssues() const;

private:
    std::shared_ptr<TImpl> Impl_;
};

class TReplicationDescription {
public:
    struct TItem {
        uint64_t Id;
        std::string SrcPath;
        std::string DstPath;
        TStats Stats;
        std::optional<std::string> SrcChangefeedName;
    };

    enum class EConsistencyLevel {
        Row,
        Global,
    };

    enum class EState {
        Running,
        Error,
        Done,
        Paused,
    };

    explicit TReplicationDescription(const Ydb::Replication::DescribeReplicationResult& desc);

    const TConnectionParams& GetConnectionParams() const;
    const std::vector<TItem> GetItems() const;

    EConsistencyLevel GetConsistencyLevel() const;
    const TGlobalConsistency& GetGlobalConsistency() const;

    EState GetState() const;
    const TRunningState& GetRunningState() const;
    const TErrorState& GetErrorState() const;
    const TDoneState& GetDoneState() const;
    const TPausedState& GetPausedState() const;

private:
    TConnectionParams ConnectionParams_;
    std::vector<TItem> Items_;

    std::variant<
        TRowConsistency,
        TGlobalConsistency
    > ConsistencyLevel_;

    std::variant<
        TRunningState,
        TErrorState,
        TDoneState,
        TPausedState
    > State_;
};

class TDescribeReplicationResult: public NScheme::TDescribePathResult {
    friend class NYdb::V3::TProtoAccessor;
    const Ydb::Replication::DescribeReplicationResult& GetProto() const;

public:
    TDescribeReplicationResult(TStatus&& status, Ydb::Replication::DescribeReplicationResult&& desc);
    const TReplicationDescription& GetReplicationDescription() const;

private:
    TReplicationDescription ReplicationDescription_;
    std::unique_ptr<Ydb::Replication::DescribeReplicationResult> Proto_;
};


class TDescribeTransferResult;
using TAsyncDescribeTransferResult = NThreading::TFuture<TDescribeTransferResult>;

struct TBatchingSettings {
    TDuration FlushInterval;
    std::uint64_t SizeBytes;
};

class TTransferDescription {
public:
    enum class EState {
        Running,
        Error,
        Done,
        Paused,
    };

    explicit TTransferDescription(const Ydb::Replication::DescribeTransferResult& desc);

    const TConnectionParams& GetConnectionParams() const;
    const std::string& GetSrcPath() const;
    const std::string& GetDstPath() const;
    const std::string& GetTransformationLambda() const;
    const std::string& GetConsumerName() const;
    const TBatchingSettings& GetBatchingSettings() const;

    EState GetState() const;
    const TRunningState& GetRunningState() const;
    const TErrorState& GetErrorState() const;
    const TDoneState& GetDoneState() const;
    const TPausedState& GetPausedState() const;

private:
    TConnectionParams ConnectionParams_;

    std::string SrcPath_;
    std::string DstPath_;
    std::string TransformationLambda_;
    std::string ConsumerName_;
    TBatchingSettings BatchingSettings_;

    std::variant<
        TRunningState,
        TErrorState,
        TDoneState,
        TPausedState
    > State_;
};

class TDescribeTransferResult: public NScheme::TDescribePathResult {
    friend class NYdb::TProtoAccessor;
    const Ydb::Replication::DescribeTransferResult& GetProto() const;

public:
    TDescribeTransferResult(TStatus&& status, Ydb::Replication::DescribeTransferResult&& desc);
    const TTransferDescription& GetTransferDescription() const;

private:
    TTransferDescription TransferDescription_;
    std::unique_ptr<Ydb::Replication::DescribeTransferResult> Proto_;
};

class TReplicationClient {
    class TImpl;

public:
    TReplicationClient(const TDriver& driver, const TCommonClientSettings& settings = TCommonClientSettings());

    TAsyncDescribeReplicationResult DescribeReplication(const std::string& path,
        const TDescribeReplicationSettings& settings = TDescribeReplicationSettings());

    TAsyncDescribeTransferResult DescribeTransfer(const std::string& path);

private:
    std::shared_ptr<TImpl> Impl_;
};

} // namespace NYdb::V3::NReplication
