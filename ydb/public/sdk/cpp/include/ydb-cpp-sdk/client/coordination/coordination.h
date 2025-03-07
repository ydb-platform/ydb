#pragma once

#include <ydb-cpp-sdk/client/driver/driver.h>

namespace Ydb {
namespace Coordination {
    class Config;
    class CreateNodeRequest;
    class DescribeNodeResult;
    class SemaphoreDescription;
    class SemaphoreSession;
}
}

namespace NYdb::inline Dev {

namespace NScheme {
struct TPermissions;
}

namespace NCoordination {

//! Represents result of a call with status
template<class T>
class TResult : public TStatus {
public:
    template<class TStatusArg, class... TArgs>
    explicit TResult(TStatusArg&& status, TArgs&&... args)
        : TStatus(std::forward<TStatusArg>(status))
        , Result_(std::forward<TArgs>(args)...)
    { }

    // Alow copy constructors with the same type
    TResult(TResult<T>&& rhs) = default;
    TResult(const TResult<T>& rhs) = default;

    // Don't allow accidental type conversions (even explicit)
    template<class U> TResult(TResult<U>&& rhs) = delete;
    template<class U> TResult(const TResult<U>& rhs) = delete;

    const T& GetResult() const {
        CheckStatusOk("TResult::GetResult");
        return Result_;
    }

    T&& ExtractResult() {
        CheckStatusOk("TResult::ExtractResult");
        return std::move(Result_);
    }

private:
    T Result_;
};

template<>
class TResult<void> : public TStatus {
public:
    template<class TStatusArg>
    explicit TResult(TStatusArg&& status)
        : TStatus(std::forward<TStatusArg>(status))
    { }

    // Allow copy constructors with the same type
    TResult(TResult<void>&& rhs) = default;
    TResult(const TResult<void>& rhs) = default;

    // Don't allow accidental type conversions (even explicit)
    template<class U> TResult(TResult<U>&& rhs) = delete;
    template<class U> TResult(const TResult<U>& rhs) = delete;

    void GetResult() const {
        CheckStatusOk("TResult::GetResult");
    }

    void ExtractResult() {
        CheckStatusOk("TResult::ExtractResult");
    }
};

template<class T>
using TAsyncResult = NThreading::TFuture<TResult<T>>;

////////////////////////////////////////////////////////////////////////////////

enum class EConsistencyMode {
    UNSET = 0,
    STRICT_MODE = 1,
    RELAXED_MODE = 2,
};

enum class ERateLimiterCountersMode {
    UNSET = 0,
    AGGREGATED = 1,
    DETAILED = 2,
};

//! Represents coordination node description
class TNodeDescription {
public:
    TNodeDescription(const Ydb::Coordination::DescribeNodeResult& desc);

    const std::optional<TDuration>& GetSelfCheckPeriod() const;
    const std::optional<TDuration>& GetSessionGracePeriod() const;
    EConsistencyMode GetReadConsistencyMode() const;
    EConsistencyMode GetAttachConsistencyMode() const;
    ERateLimiterCountersMode GetRateLimiterCountersMode() const;

    const std::string& GetOwner() const;
    const std::vector<NScheme::TPermissions>& GetEffectivePermissions() const;
    const Ydb::Coordination::DescribeNodeResult& GetProto() const;

    void SerializeTo(Ydb::Coordination::CreateNodeRequest& creationRequest) const;

private:
    struct TImpl;
    std::shared_ptr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

//! Represents session owning or waiting on a semaphore
class TSemaphoreSession {
public:
    TSemaphoreSession();

    TSemaphoreSession(const Ydb::Coordination::SemaphoreSession& desc);

    uint64_t GetOrderId() const { return OrderId_; }
    uint64_t GetSessionId() const { return SessionId_; }
    TDuration GetTimeout() const { return Timeout_; }
    uint64_t GetCount() const { return Count_; }
    const std::string& GetData() const { return Data_; }

private:
    uint64_t OrderId_;
    uint64_t SessionId_;
    TDuration Timeout_;
    uint64_t Count_;
    std::string Data_;
};

////////////////////////////////////////////////////////////////////////////////

//! Represents semaphore description
class TSemaphoreDescription {
public:
    TSemaphoreDescription();

    TSemaphoreDescription(const Ydb::Coordination::SemaphoreDescription& desc);

    const std::string& GetName() const { return Name_; }
    const std::string& GetData() const { return Data_; }
    uint64_t GetCount() const { return Count_; }
    uint64_t GetLimit() const { return Limit_; }
    const std::vector<TSemaphoreSession>& GetOwners() const { return Owners_; }
    const std::vector<TSemaphoreSession>& GetWaiters() const { return Waiters_; }
    bool IsEphemeral() const { return IsEphemeral_; }

private:
    std::string Name_;
    std::string Data_;
    uint64_t Count_;
    uint64_t Limit_;
    std::vector<TSemaphoreSession> Owners_;
    std::vector<TSemaphoreSession> Waiters_;
    bool IsEphemeral_;
};

////////////////////////////////////////////////////////////////////////////////

using TDescribeNodeResult = TResult<TNodeDescription>;

using TAsyncDescribeNodeResult = NThreading::TFuture<TDescribeNodeResult>;

using TDescribeSemaphoreResult = TResult<TSemaphoreDescription>;

using TAsyncDescribeSemaphoreResult = NThreading::TFuture<TDescribeSemaphoreResult>;

////////////////////////////////////////////////////////////////////////////////

template<class TDerived>
struct TNodeSettings : public TOperationRequestSettings<TDerived> {
    using TSelf = TDerived;

    FLUENT_SETTING_OPTIONAL(TDuration, SelfCheckPeriod);

    FLUENT_SETTING_OPTIONAL(TDuration, SessionGracePeriod);

    FLUENT_SETTING_DEFAULT(EConsistencyMode, ReadConsistencyMode, EConsistencyMode::UNSET);

    FLUENT_SETTING_DEFAULT(EConsistencyMode, AttachConsistencyMode, EConsistencyMode::UNSET);

    FLUENT_SETTING_DEFAULT(ERateLimiterCountersMode, RateLimiterCountersMode, ERateLimiterCountersMode::UNSET);
};

struct TCreateNodeSettings : public TNodeSettings<TCreateNodeSettings> {
    TCreateNodeSettings() = default;
    TCreateNodeSettings(const Ydb::Coordination::Config& config);
};
struct TAlterNodeSettings : public TNodeSettings<TAlterNodeSettings> { };
struct TDropNodeSettings : public TOperationRequestSettings<TDropNodeSettings> { };
struct TDescribeNodeSettings : public TOperationRequestSettings<TDescribeNodeSettings> { };

////////////////////////////////////////////////////////////////////////////////

class TSession;

using TSessionResult = TResult<TSession>;

using TAsyncSessionResult = NThreading::TFuture<TSessionResult>;

////////////////////////////////////////////////////////////////////////////////

enum class ESessionState {
    ATTACHED,
    DETACHED,
    EXPIRED,
};

enum class EConnectionState {
    CONNECTING,
    ATTACHING,
    CONNECTED,
    DISCONNECTED,
    STOPPED,
};

////////////////////////////////////////////////////////////////////////////////

struct TSessionSettings : public TRequestSettings<TSessionSettings> {
    using TSelf = TSessionSettings;
    using TStateCallback = std::function<void(ESessionState)>;
    using TStoppedCallback = std::function<void()>;

    FLUENT_SETTING(std::string, Description);

    FLUENT_SETTING(TStateCallback, OnStateChanged);

    FLUENT_SETTING(TStoppedCallback, OnStopped);

    FLUENT_SETTING_DEFAULT(TDuration, Timeout, TDuration::Seconds(5));

    FLUENT_SETTING_DEFAULT(TDuration, ReconnectBackoffDelay, TDuration::MilliSeconds(250));

    FLUENT_SETTING_DEFAULT(double, ReconnectBackoffMultiplier, 2.0);

    FLUENT_SETTING_DEFAULT(double, ReconnectSessionTimeoutMultiplier, 2.0);

    FLUENT_SETTING_DEFAULT(TDuration, ConnectTimeout, TDuration::Zero());
};

////////////////////////////////////////////////////////////////////////////////

struct TAcquireSemaphoreSettings {
    using TSelf = TAcquireSemaphoreSettings;
    using TAcceptedCallback = std::function<void()>;

    FLUENT_SETTING(std::string, Data);

    FLUENT_SETTING(TAcceptedCallback, OnAccepted);

    FLUENT_SETTING_DEFAULT(uint64_t, Count, 0);

    FLUENT_SETTING_DEFAULT(TDuration, Timeout, TDuration::Max());

    FLUENT_SETTING_FLAG(Ephemeral);

    FLUENT_SETTING_FLAG_ALIAS(Shared, Count, uint64_t(1));

    FLUENT_SETTING_FLAG_ALIAS(Exclusive, Count, uint64_t(-1));
};

////////////////////////////////////////////////////////////////////////////////

struct TDescribeSemaphoreSettings {
    using TSelf = TDescribeSemaphoreSettings;
    using TChangedCallback = std::function<void(bool)>;

    FLUENT_SETTING(TChangedCallback, OnChanged);

    FLUENT_SETTING_FLAG(WatchData);

    FLUENT_SETTING_FLAG(WatchOwners);

    FLUENT_SETTING_FLAG(IncludeOwners);

    FLUENT_SETTING_FLAG(IncludeWaiters);
};

////////////////////////////////////////////////////////////////////////////////

class TClient {
public:
    TClient(const TDriver& driver, const TCommonClientSettings& settings = TCommonClientSettings());

    ~TClient();

    TAsyncSessionResult StartSession(const std::string& path,
        const TSessionSettings& settings = TSessionSettings());

    TAsyncStatus CreateNode(const std::string& path,
        const TCreateNodeSettings& settings = TCreateNodeSettings());

    TAsyncStatus AlterNode(const std::string& path,
        const TAlterNodeSettings& settings = TAlterNodeSettings());

    TAsyncStatus DropNode(const std::string& path,
        const TDropNodeSettings& settings = TDropNodeSettings());

    TAsyncDescribeNodeResult DescribeNode(const std::string& path,
        const TDescribeNodeSettings& settings = TDescribeNodeSettings());

private:
    class TImpl;
    std::shared_ptr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

class TSessionContext;

class TSession {
    friend class TSessionContext;

public:
    TSession() = default;

    explicit operator bool() const {
        return bool(Impl_);
    }

    uint64_t GetSessionId();

    ESessionState GetSessionState();

    EConnectionState GetConnectionState();

    TAsyncResult<void> Close();

    TAsyncResult<void> Ping();

    TAsyncResult<void> Reconnect();

    TAsyncResult<bool> AcquireSemaphore(const std::string& name,
        const TAcquireSemaphoreSettings& settings);

    TAsyncResult<bool> ReleaseSemaphore(const std::string& name);

    TAsyncDescribeSemaphoreResult DescribeSemaphore(const std::string& name,
        const TDescribeSemaphoreSettings& settings = TDescribeSemaphoreSettings());

    TAsyncResult<void> CreateSemaphore(const std::string& name,
        uint64_t limit, const std::string& data = std::string());

    TAsyncResult<void> UpdateSemaphore(const std::string& name,
        const std::string& data);

    TAsyncResult<void> DeleteSemaphore(const std::string& name,
        bool force = false);

private:
    explicit TSession(TSessionContext* context);

private:
    class TImpl;
    std::shared_ptr<TImpl> Impl_;
};

}
}
