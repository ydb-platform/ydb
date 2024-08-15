#include "schema.h"

#include <ydb/core/base/path.h>
#include <ydb/core/fq/libs/events/events.h>
#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>

#include <util/string/builder.h>
#include <util/system/defaults.h>

#define LOG_IMPL(level, logRecordStream)                                \
    LOG_LOG_S(::NActors::TActivationContext::AsActorContext(), ::NActors::NLog:: Y_CAT(PRI_, level), this->LogComponent, logRecordStream);

#define SCHEMA_LOG_DEBUG(logRecordStream) LOG_IMPL(DEBUG, logRecordStream)
#define SCHEMA_LOG_INFO(logRecordStream) LOG_IMPL(INFO, logRecordStream)
#define SCHEMA_LOG_WARN(logRecordStream) LOG_IMPL(WARN, logRecordStream)
#define SCHEMA_LOG_ERROR(logRecordStream) LOG_IMPL(ERROR, logRecordStream)

namespace NFq {

namespace {

struct TEvPrivate {
    // Event ids
    enum EEv : ui32 {
        EvBegin = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),

        EvCreateSessionResult = EvBegin,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");

    // Events
    struct TEvCreateSessionResult : public NActors::TEventLocal<TEvCreateSessionResult, EvCreateSessionResult> {
        explicit TEvCreateSessionResult(
            NYdb::NTable::TCreateSessionResult result)
            : Result(std::move(result))
        {
        }

        NYdb::NTable::TCreateSessionResult Result;
    };
};

template <class TResponseEvent>
class TSchemaActorBase : public NActors::TActorBootstrapped<TSchemaActorBase<TResponseEvent>> {
public:
    TSchemaActorBase(
        NActors::TActorId parent,
        ui64 logComponent,
        TYdbConnectionPtr connection,
        TYdbSdkRetryPolicy::TPtr retryPolicy,
        ui64 cookie)
        : Parent(parent)
        , LogComponent(logComponent)
        , Connection(std::move(connection))
        , RetryPolicy(std::move(retryPolicy))
        , Cookie(cookie)
    {
    }

    virtual void Init() {
    }

    void Bootstrap() {
        Init();
        SCHEMA_LOG_DEBUG("Run " << GetActionName() << " " << GetEntityName() << " actor");
        this->Become(&TSchemaActorBase<TResponseEvent>::StateFunc);
        CallAndSubscribe();
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TResponseEvent, Handle)
        hFunc(TEvPrivate::TEvCreateSessionResult, Handle)
        hFunc(NActors::TEvents::TEvWakeup, Handle)
    );

    void Handle(NActors::TEvents::TEvWakeup::TPtr&) {
        CallAndSubscribe();
    }

    virtual void Handle(TEvPrivate::TEvCreateSessionResult::TPtr&) {
    }

    virtual void Handle(typename TResponseEvent::TPtr&) {
    }

    template <class TEventPtr>
    [[nodiscard]] bool ScheduleNextAttempt(TEventPtr& ev) {
        if (!RetryState) {
            RetryState = RetryPolicy->CreateRetryState();
            if (!RetryState) {
                return false;
            }
        }

        auto delayMaybe = RetryState->GetNextRetryDelay(ev->Get()->Result);
        if (!delayMaybe) {
            return false;
        }

        SCHEMA_LOG_DEBUG("Schedule next attempt, delay " << delayMaybe->ToString());
        this->Schedule(*delayMaybe, new NActors::TEvents::TEvWakeup());
        return true;
    }

    virtual NYdb::TAsyncStatus CallYdbSdk() = 0;

    virtual void CallAndSubscribe() {
        SCHEMA_LOG_DEBUG("Call " << GetActionName() << " " << GetEntityName());
        CallYdbSdk().Subscribe(
            [actorId = this->SelfId(), actorSystem = NActors::TActivationContext::ActorSystem()](const NYdb::TAsyncStatus& result) {
                actorSystem->Send(actorId, new TResponseEvent(result.GetValue()));
            }
        );
    }

    void ReplyStatusAndDie(const NYdb::TStatus& status) {
        SCHEMA_LOG_DEBUG("Reply for " << GetActionName() << " " << GetEntityName() << ": " << status.GetIssues().ToOneLineString());
        if (Parent) {
            this->Send(Parent, new TResponseEvent(status), 0, Cookie);
        }
        this->PassAway();
    }

    template <class TEventPtr>
    void ReplyAndDie(TEventPtr& ev) {
        return ReplyStatusAndDie(ev->Get()->Result);
    }

    virtual TString GetEntityName() const = 0;

    virtual TStringBuf GetActionName() const = 0; // "create" or "delete"

protected:
    const NActors::TActorId Parent;
    const ui64 LogComponent;
    const TYdbConnectionPtr Connection;
    const TYdbSdkRetryPolicy::TPtr RetryPolicy;
    const ui64 Cookie;
    TYdbSdkRetryPolicy::IRetryState::TPtr RetryState;
    TMaybe<NYdb::NTable::TSession> Session;
};

class TCreateActorBase : public TSchemaActorBase<TEvents::TEvSchemaCreated> {
public:
    using TSchemaActorBase::TSchemaActorBase;

protected:
    void Handle(TEvents::TEvSchemaCreated::TPtr& ev) override {
        if (IsTableCreated(ev->Get()->Result)) {
            SCHEMA_LOG_DEBUG("Successfully created " << GetEntityName());
            ReplyAndDie(ev);
            return;
        }

        SCHEMA_LOG_ERROR("Create " << GetEntityName() << " error: " << ev->Get()->Result.GetStatus() << " " << ev->Get()->Result.GetIssues().ToOneLineString());

        if (!ScheduleNextAttempt(ev)) {
            ReplyAndDie(ev);
        }
    }

    TStringBuf GetActionName() const override {
        return "create";
    }
};

class TDeleteActorBase : public TSchemaActorBase<TEvents::TEvSchemaDeleted> {
public:
    using TSchemaActorBase::TSchemaActorBase;

protected:
    void Handle(TEvents::TEvSchemaDeleted::TPtr& ev) override {
        if (IsTableDeleted(ev->Get()->Result)) {
            SCHEMA_LOG_DEBUG("Successfully deleted " << GetEntityName());
            ReplyAndDie(ev);
            return;
        }

        SCHEMA_LOG_ERROR("Delete " << GetEntityName() << " error: " << ev->Get()->Result.GetStatus() << " " << ev->Get()->Result.GetIssues().ToOneLineString());

        if (!ScheduleNextAttempt(ev)) {
            ReplyAndDie(ev);
        }
    }

    TStringBuf GetActionName() const override {
        return "delete";
    }
};

class TUpdateActorBase : public TSchemaActorBase<TEvents::TEvSchemaUpdated> {
public:
    using TSchemaActorBase::TSchemaActorBase;

protected:
    void Handle(TEvents::TEvSchemaUpdated::TPtr& ev) override {
        if (ev->Get()->Result.IsSuccess()) {
            SCHEMA_LOG_DEBUG("Successfully updated " << GetEntityName());
            ReplyAndDie(ev);
            return;
        }

        SCHEMA_LOG_ERROR("Update " << GetEntityName() << " error: " << ev->Get()->Result.GetStatus() << " " << ev->Get()->Result.GetIssues().ToOneLineString());

        if (!ScheduleNextAttempt(ev)) {
            ReplyAndDie(ev);
        }
    }

    TStringBuf GetActionName() const override {
        return "update";
    }
};

template <class TRequestDesc, class TBase = TCreateActorBase>
class TRecursiveCreateActorBase : public TBase {
public:
    using TBase::TBase;

protected:
    void Init() override {
        FillRequestsPath(RequestsPath);
        Y_ABORT_UNLESS(!RequestsPath.empty());
        TriedPaths.resize(RequestsPath.size());
        CurrentRequest = RequestsPath.size() - 1;
        TBase::Init();
    }

    NYdb::TAsyncStatus CallYdbSdk() override final {
        return CallYdbSdk(RequestsPath[CurrentRequest]);
    }

    virtual bool OnCurrentRequestChanged() { // extra validation
        return true;
    }

    void Handle(TEvents::TEvSchemaCreated::TPtr& ev) override {
        if (CurrentRequest < RequestsPath.size() - 1 && IsTableCreated(ev->Get()->Result)) {
            SCHEMA_LOG_DEBUG("Successfully created " << GetEntityName(RequestsPath[CurrentRequest]));
            ++CurrentRequest;
            if (!OnCurrentRequestChanged()) {
                return;
            }
            this->RetryState = nullptr;
            this->CallAndSubscribe();
            return;
        }
        if (CurrentRequest > 0 && IsPathDoesNotExistError(ev->Get()->Result)) {
            if (!TriedPaths[CurrentRequest - 1]) { // Defence from cycles.
                --CurrentRequest;
                if (!OnCurrentRequestChanged()) {
                    return;
                }
                TriedPaths[CurrentRequest] = true;
                this->RetryState = nullptr;
                SCHEMA_LOG_DEBUG("Trying to recursively create " << GetEntityName(RequestsPath[CurrentRequest]));
                this->CallAndSubscribe();
                return;
            }
        }
        TBase::Handle(ev);
    }

    TString GetEntityName() const override {
        return GetEntityName(RequestsPath[CurrentRequest]);
    }

    virtual TString GetEntityName(const TRequestDesc& desc) const = 0;

    virtual void FillRequestsPath(std::vector<TRequestDesc>& desc) = 0;
    virtual NYdb::TAsyncStatus CallYdbSdk(const TRequestDesc& req) = 0;

protected:
    std::vector<TRequestDesc> RequestsPath; // From parent to leaf. // Must be filled during actor's bootstrap.
    std::vector<bool> TriedPaths;
    size_t CurrentRequest = 0;
};

class TCreateActorBaseWithSession : public TCreateActorBase {
public:
    using TCreateActorBase::TCreateActorBase;

protected:
    using TCreateActorBase::Handle;

    void Handle(TEvPrivate::TEvCreateSessionResult::TPtr& ev) override {
        if (ev->Get()->Result.IsSuccess()) {
            SCHEMA_LOG_DEBUG("Create " << GetEntityName() << ". Create session OK");
            Session = ev->Get()->Result.GetSession();
            CallAndSubscribe();
        } else {
            SCHEMA_LOG_WARN("Create " << GetEntityName() << ". Create session error: " << ev->Get()->Result.GetIssues().ToOneLineString());
            if (!ScheduleNextAttempt(ev)) {
                ReplyAndDie(ev);
            }
        }
    }

    void CreateSession() {
        Connection->TableClient.GetSession().Subscribe(
            [actorId = SelfId(), actorSystem = NActors::TActivationContext::ActorSystem()](const NYdb::NTable::TAsyncCreateSessionResult& result) {
                actorSystem->Send(actorId, new TEvPrivate::TEvCreateSessionResult(result.GetValue()));
            }
        );
    }

    void CallAndSubscribe() override {
        if (!Session) {
            CreateSession();
            return;
        }

        TCreateActorBase::CallAndSubscribe();
    }

    void PassAway() override {
        TCreateActorBase::PassAway();
        if (Session) {
            Session->Close();
        }
    }

protected:
    TMaybe<NYdb::NTable::TSession> Session;
};

class TCreateTableActor : public TCreateActorBaseWithSession {
public:
    TCreateTableActor(
        NActors::TActorId parent,
        ui64 logComponent,
        TYdbConnectionPtr connection,
        const TString& tablePath,
        const NYdb::NTable::TTableDescription& tableDesc,
        TYdbSdkRetryPolicy::TPtr retryPolicy,
        ui64 cookie)
        : TCreateActorBaseWithSession(parent, logComponent, std::move(connection), std::move(retryPolicy), cookie)
        , TablePath(tablePath)
        , TableDesc(tableDesc)
    {
    }

private:
    TString GetEntityName() const override {
        return TStringBuilder() << "table \"" << TablePath << "\"";
    }

    NYdb::TAsyncStatus CallYdbSdk() override {
        SCHEMA_LOG_DEBUG("Call create table \"" << TablePath << "\"");
        return Session->CreateTable(TablePath, NYdb::NTable::TTableDescription(TableDesc));
    }

private:
    const TString TablePath;
    const NYdb::NTable::TTableDescription TableDesc;
};

class TCreateDirectoryActor : public TCreateActorBase {
public:
    TCreateDirectoryActor(
        NActors::TActorId parent,
        ui64 logComponent,
        TYdbConnectionPtr connection,
        const TString& directoryPath,
        TYdbSdkRetryPolicy::TPtr retryPolicy,
        ui64 cookie)
        : TCreateActorBase(parent, logComponent, std::move(connection), std::move(retryPolicy), cookie)
        , DirectoryPath(directoryPath)
    {
    }

private:
    TString GetEntityName() const override {
        return TStringBuilder() << "directory \"" << DirectoryPath << "\"";
    }

    NYdb::TAsyncStatus CallYdbSdk() override {
        SCHEMA_LOG_DEBUG("Call create directory \"" << DirectoryPath << "\"");
        return Connection->SchemeClient.MakeDirectory(DirectoryPath);
    }

private:
    const TString DirectoryPath;
};

class TCreateCoordinationNodeActor : public TCreateActorBase {
public:
    TCreateCoordinationNodeActor(
        NActors::TActorId parent,
        ui64 logComponent,
        TYdbConnectionPtr connection,
        const TString& coordinationNodePath,
        TYdbSdkRetryPolicy::TPtr retryPolicy,
        ui64 cookie)
        : TCreateActorBase(parent, logComponent, std::move(connection), std::move(retryPolicy), cookie)
        , CoordinationNodePath(coordinationNodePath)
    {
    }

private:
    TString GetEntityName() const override {
        return TStringBuilder() << "coordination node \"" << CoordinationNodePath << "\"";
    }

    NYdb::TAsyncStatus CallYdbSdk() override {
        SCHEMA_LOG_DEBUG("Call create coordination node \"" << CoordinationNodePath << "\"");
        return Connection->CoordinationClient.CreateNode(CoordinationNodePath);
    }

private:
    const TString CoordinationNodePath;
};

struct TCreateRateLimiterResourceRequestDesc {
    TString Path;
    TMaybe<double> Limit;
};

class TCreateRateLimiterResourceActor : public TRecursiveCreateActorBase<TCreateRateLimiterResourceRequestDesc> {
public:
    TCreateRateLimiterResourceActor(
        NActors::TActorId parent,
        ui64 logComponent,
        TYdbConnectionPtr connection,
        const TString& coordinationNodePath,
        const TString& resourcePath,
        const std::vector<TMaybe<double>>& limits, // limits from the very parent resource to the leaf
        TYdbSdkRetryPolicy::TPtr retryPolicy,
        ui64 cookie)
        : TRecursiveCreateActorBase<TCreateRateLimiterResourceRequestDesc>(
            parent,
            logComponent,
            std::move(connection),
            std::move(retryPolicy),
            cookie)
        , CoordinationNodePath(coordinationNodePath)
        , ResourcePath(resourcePath)
        , Limits(limits)
    {
    }

private:
    TString GetEntityName(const TCreateRateLimiterResourceRequestDesc& req) const override {
        return TStringBuilder() << "rate limiter resource \"" << req.Path << "\"";
    }

    bool OnCurrentRequestChanged() override {
        if (CurrentRequest == 0 && !Limits[0]) {
            SCHEMA_LOG_WARN("Create " << TRecursiveCreateActorBase<TCreateRateLimiterResourceRequestDesc>::GetEntityName() << ". Attempt to create rate limiter resource root without limit");
            NYql::TIssues issues;
            issues.AddIssue(TStringBuilder() << "Internal error: attempt to create rate limiter resource root \"" << RequestsPath[0].Path << "\" without limit");
            NYdb::TStatus status(NYdb::EStatus::INTERNAL_ERROR, std::move(issues));
            ReplyStatusAndDie(status);
            return false;
        }
        return true;
    }

    void FillRequestsPath(std::vector<TCreateRateLimiterResourceRequestDesc>& desc) override {
        std::vector<TString> components = NKikimr::SplitPath(ResourcePath);
        desc.resize(components.size());
        Y_ABORT_UNLESS(components.size() == Limits.size(),
            "Components count and limits size don't match. Components: %lu. Limits: %lu. ResourcePath: \"%s\"",
            components.size(), Limits.size(), ResourcePath.c_str());
        for (size_t i = 0; i < components.size(); ++i) {
            desc[i].Limit = Limits[i];
            desc[i].Path = NKikimr::JoinPath({components.begin(), components.begin() + i + 1});
        }
    }

    NYdb::TAsyncStatus CallYdbSdk(const TCreateRateLimiterResourceRequestDesc& req) override {
        return Connection->RateLimiterClient.CreateResource(CoordinationNodePath, req.Path, NYdb::NRateLimiter::TCreateResourceSettings().MaxUnitsPerSecond(req.Limit));
    }

private:
    const TString CoordinationNodePath;
    const TString ResourcePath;
    std::vector<TMaybe<double>> Limits;
};

class TDeleteRateLimiterResourceActor : public TDeleteActorBase {
public:
    TDeleteRateLimiterResourceActor(
        NActors::TActorId parent,
        ui64 logComponent,
        TYdbConnectionPtr connection,
        const TString& coordinationNodePath,
        const TString& resourcePath,
        TYdbSdkRetryPolicy::TPtr retryPolicy,
        ui64 cookie)
        : TDeleteActorBase(parent, logComponent, std::move(connection), std::move(retryPolicy), cookie)
        , CoordinationNodePath(coordinationNodePath)
        , ResourcePath(resourcePath)
    {
    }

private:
    TString GetEntityName() const override {
        return TStringBuilder() << "rate limiter resource \"" << ResourcePath << "\"";
    }

    NYdb::TAsyncStatus CallYdbSdk() override {
        return Connection->RateLimiterClient.DropResource(CoordinationNodePath, ResourcePath);
    }

private:
    const TString CoordinationNodePath;
    const TString ResourcePath;
};

class TUpdateRateLimiterResourceActor : public TUpdateActorBase {
public:
    TUpdateRateLimiterResourceActor(
        NActors::TActorId parent,
        ui64 logComponent,
        TYdbConnectionPtr connection,
        const TString& coordinationNodePath,
        const TString& resourcePath,
        TMaybe<ui64> limit,
        TYdbSdkRetryPolicy::TPtr retryPolicy,
        ui64 cookie)
        : TUpdateActorBase(parent, logComponent, std::move(connection), std::move(retryPolicy), cookie)
        , CoordinationNodePath(coordinationNodePath)
        , ResourcePath(resourcePath)
        , Limit(limit)
    {
    }

private:
    TString GetEntityName() const override {
        return TStringBuilder() << "rate limiter resource \"" << ResourcePath << "\"";
    }

    NYdb::TAsyncStatus CallYdbSdk() override {
        return Connection->RateLimiterClient.AlterResource(CoordinationNodePath, ResourcePath, NYdb::NRateLimiter::TAlterResourceSettings().MaxUnitsPerSecond(Limit));
    }

private:
    const TString CoordinationNodePath;
    const TString ResourcePath;
    const TMaybe<ui64> Limit;
};

} // namespace

bool IsPathDoesNotExistError(const NYdb::TStatus& status) {
    if (status.GetStatus() == NYdb::EStatus::NOT_FOUND) {
        return true;
    }
    if (status.GetStatus() == NYdb::EStatus::BAD_REQUEST) {
        const TString issuesText = status.GetIssues().ToString();
        if (issuesText.Contains("doesn't exist") || issuesText.Contains("does not exist")) {
            return true;
        }
    }
    return false;
}

NActors::IActor* MakeCreateTableActor(
    NActors::TActorId parent,
    ui64 logComponent,
    TYdbConnectionPtr connection,
    const TString& tablePath,
    const NYdb::NTable::TTableDescription& tableDesc,
    TYdbSdkRetryPolicy::TPtr retryPolicy,
    ui64 cookie)
{
    return new TCreateTableActor(
        parent,
        logComponent,
        std::move(connection),
        tablePath,
        tableDesc,
        std::move(retryPolicy),
        cookie
    );
}

NActors::IActor* MakeCreateDirectoryActor(
    NActors::TActorId parent,
    ui64 logComponent,
    TYdbConnectionPtr connection,
    const TString& directoryPath,
    TYdbSdkRetryPolicy::TPtr retryPolicy,
    ui64 cookie)
{
    return new TCreateDirectoryActor(
        parent,
        logComponent,
        std::move(connection),
        directoryPath,
        std::move(retryPolicy),
        cookie
    );
}

NActors::IActor* MakeCreateCoordinationNodeActor(
    NActors::TActorId parent,
    ui64 logComponent,
    TYdbConnectionPtr connection,
    const TString& coordinationNodePath,
    TYdbSdkRetryPolicy::TPtr retryPolicy,
    ui64 cookie)
{
    return new TCreateCoordinationNodeActor(
        parent,
        logComponent,
        std::move(connection),
        coordinationNodePath,
        std::move(retryPolicy),
        cookie
    );
}

NActors::IActor* MakeCreateRateLimiterResourceActor(
    NActors::TActorId parent,
    ui64 logComponent,
    TYdbConnectionPtr connection,
    const TString& coordinationNodePath,
    const TString& resourcePath,
    const std::vector<TMaybe<double>>& limits, // limits from the very parent resource to the leaf
    TYdbSdkRetryPolicy::TPtr retryPolicy,
    ui64 cookie)
{
    return new TCreateRateLimiterResourceActor(
        parent,
        logComponent,
        std::move(connection),
        coordinationNodePath,
        resourcePath,
        limits,
        std::move(retryPolicy),
        cookie
    );
}

NActors::IActor* MakeDeleteRateLimiterResourceActor(
    NActors::TActorId parent,
    ui64 logComponent,
    TYdbConnectionPtr connection,
    const TString& coordinationNodePath,
    const TString& resourcePath,
    TYdbSdkRetryPolicy::TPtr retryPolicy,
    ui64 cookie)
{
    return new TDeleteRateLimiterResourceActor(
        parent,
        logComponent,
        std::move(connection),
        coordinationNodePath,
        resourcePath,
        std::move(retryPolicy),
        cookie
    );
}

NActors::IActor* MakeUpdateRateLimiterResourceActor(
    NActors::TActorId parent,
    ui64 logComponent,
    TYdbConnectionPtr connection,
    const TString& coordinationNodePath,
    const TString& resourcePath,
    TMaybe<double> limit,
    TYdbSdkRetryPolicy::TPtr retryPolicy,
    ui64 cookie)
{
    return new TUpdateRateLimiterResourceActor(
        parent,
        logComponent,
        std::move(connection),
        coordinationNodePath,
        resourcePath,
        limit,
        std::move(retryPolicy),
        cookie
    );
}

} // namespace NFq
