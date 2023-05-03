#pragma once

#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>

#include <util/generic/maybe.h>

namespace Ydb {
namespace Coordination {
    class DescribeNodeResult;
    class SemaphoreSession;
    class SemaphoreDescription;
}
}

namespace NYdb {

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

    const TMaybe<TDuration>& GetSelfCheckPeriod() const;
    const TMaybe<TDuration>& GetSessionGracePeriod() const;
    EConsistencyMode GetReadConsistencyMode() const;
    EConsistencyMode GetAttachConsistencyMode() const;
    ERateLimiterCountersMode GetRateLimiterCountersMode() const;

    const TString& GetOwner() const;
    const TVector<NScheme::TPermissions>& GetEffectivePermissions() const;
    const Ydb::Coordination::DescribeNodeResult& GetProto() const;

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

    ui64 GetOrderId() const { return OrderId_; }
    ui64 GetSessionId() const { return SessionId_; }
    TDuration GetTimeout() const { return Timeout_; }
    ui64 GetCount() const { return Count_; }
    const TString& GetData() const { return Data_; }

private:
    ui64 OrderId_;
    ui64 SessionId_;
    TDuration Timeout_;
    ui64 Count_;
    TString Data_;
};

////////////////////////////////////////////////////////////////////////////////

//! Represents semaphore description
class TSemaphoreDescription {
public:
    TSemaphoreDescription();

    TSemaphoreDescription(const Ydb::Coordination::SemaphoreDescription& desc);

    const TString& GetName() const { return Name_; }
    const TString& GetData() const { return Data_; }
    ui64 GetCount() const { return Count_; }
    ui64 GetLimit() const { return Limit_; }
    const TVector<TSemaphoreSession>& GetOwners() const { return Owners_; }
    const TVector<TSemaphoreSession>& GetWaiters() const { return Waiters_; }
    bool IsEphemeral() const { return IsEphemeral_; }

private:
    TString Name_;
    TString Data_;
    ui64 Count_;
    ui64 Limit_;
    TVector<TSemaphoreSession> Owners_;
    TVector<TSemaphoreSession> Waiters_;
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

struct TCreateNodeSettings : public TNodeSettings<TCreateNodeSettings> { };
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

    FLUENT_SETTING(TString, Description);

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

    FLUENT_SETTING(TString, Data);

    FLUENT_SETTING(TAcceptedCallback, OnAccepted);

    FLUENT_SETTING_DEFAULT(ui64, Count, 0);

    FLUENT_SETTING_DEFAULT(TDuration, Timeout, TDuration::Max());

    FLUENT_SETTING_FLAG(Ephemeral);

    FLUENT_SETTING_FLAG_ALIAS(Shared, Count, ui64(1));

    FLUENT_SETTING_FLAG_ALIAS(Exclusive, Count, ui64(-1));
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

    TAsyncSessionResult StartSession(const TString& path,
        const TSessionSettings& settings = TSessionSettings());

    TAsyncStatus CreateNode(const TString& path,
        const TCreateNodeSettings& settings = TCreateNodeSettings());

    TAsyncStatus AlterNode(const TString& path,
        const TAlterNodeSettings& settings = TAlterNodeSettings());

    TAsyncStatus DropNode(const TString& path,
        const TDropNodeSettings& settings = TDropNodeSettings());

    TAsyncDescribeNodeResult DescribeNode(const TString& path,
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

    ui64 GetSessionId();

    ESessionState GetSessionState();

    EConnectionState GetConnectionState();

    TAsyncResult<void> Close();

    TAsyncResult<void> Ping();

    TAsyncResult<void> Reconnect();

    TAsyncResult<bool> AcquireSemaphore(const TString& name,
        const TAcquireSemaphoreSettings& settings);

    TAsyncResult<bool> ReleaseSemaphore(const TString& name);

    TAsyncDescribeSemaphoreResult DescribeSemaphore(const TString& name,
        const TDescribeSemaphoreSettings& settings = TDescribeSemaphoreSettings());

    TAsyncResult<void> CreateSemaphore(const TString& name,
        ui64 limit, const TString& data = TString());

    TAsyncResult<void> UpdateSemaphore(const TString& name,
        const TString& data);

    TAsyncResult<void> DeleteSemaphore(const TString& name,
        bool force = false);

private:
    explicit TSession(TSessionContext* context);

private:
    class TImpl;
    std::shared_ptr<TImpl> Impl_;
};

}
}
