#include "create_schema.h"

#include <ydb/core/yq/libs/events/events.h>
#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/log.h>

#include <util/string/builder.h>
#include <util/system/defaults.h>

#define LOG_IMPL(level, logRecordStream)                                \
    LOG_LOG_S(::NActors::TActivationContext::AsActorContext(), ::NActors::NLog:: Y_CAT(PRI_, level), LogComponent, logRecordStream);

#define SCHEMA_LOG_DEBUG(logRecordStream) LOG_IMPL(DEBUG, logRecordStream)
#define SCHEMA_LOG_INFO(logRecordStream) LOG_IMPL(INFO, logRecordStream)
#define SCHEMA_LOG_WARN(logRecordStream) LOG_IMPL(WARN, logRecordStream)
#define SCHEMA_LOG_ERROR(logRecordStream) LOG_IMPL(ERROR, logRecordStream)

namespace NYq {

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

} // namespace

class TCreateActorBase : public NActors::TActorBootstrapped<TCreateActorBase> {
public:
    TCreateActorBase(
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

    void Bootstrap() {
        Become(&TCreateActorBase::StateFunc);
        SCHEMA_LOG_DEBUG("Run create " << GetEntityName() << " actor");
        CallCreate();
    }

protected:
    STRICT_STFUNC(StateFunc,
        hFunc(TEvents::TEvSchemaCreated, Handle)
        hFunc(TEvPrivate::TEvCreateSessionResult, Handle)
        hFunc(NActors::TEvents::TEvWakeup, Handle)
    );

    virtual TString GetEntityName() const = 0;

    void Handle(TEvents::TEvSchemaCreated::TPtr& ev) {
        if (IsTableCreated(ev->Get()->Result)) {
            SCHEMA_LOG_DEBUG("Successfully created " << GetEntityName());
            ReplyAndDie(ev);
            return;
        }

        SCHEMA_LOG_ERROR("Create " << GetEntityName() << " error: " << ev->Get()->Result.GetIssues().ToOneLineString());

        if (!ScheduleNextAttempt(ev)) {
            ReplyAndDie(ev);
        }
    }

    void Handle(NActors::TEvents::TEvWakeup::TPtr&) {
        CallCreate();
    }

    virtual void Handle(TEvPrivate::TEvCreateSessionResult::TPtr&) {
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

        Schedule(*delayMaybe, new NActors::TEvents::TEvWakeup());
        return true;
    }

    virtual void CallCreate() {
        CallYdbSdk().Subscribe(
            [actorId = SelfId(), actorSystem = NActors::TActivationContext::ActorSystem()](const NYdb::TAsyncStatus& result) {
                actorSystem->Send(actorId, new TEvents::TEvSchemaCreated(result.GetValue()));
            }
        );
    }

    virtual NYdb::TAsyncStatus CallYdbSdk() = 0;

    template <class TEventPtr>
    void ReplyAndDie(TEventPtr& ev) {
        SCHEMA_LOG_DEBUG("Create " << GetEntityName() << ". Reply: " << ev->Get()->Result.GetIssues().ToOneLineString());
        if (Parent) {
            Send(Parent, new TEvents::TEvSchemaCreated(ev->Get()->Result), Cookie);
        }
        PassAway();
    }

protected:
    const NActors::TActorId Parent;
    const ui64 LogComponent;
    const TYdbConnectionPtr Connection;
    const TYdbSdkRetryPolicy::TPtr RetryPolicy;
    const ui64 Cookie;
    TYdbSdkRetryPolicy::IRetryState::TPtr RetryState;
    TMaybe<NYdb::NTable::TSession> Session;
};

class TCreateActorBaseWithSession : public TCreateActorBase {
public:
    using TCreateActorBase::TCreateActorBase;

protected:
    void Handle(TEvPrivate::TEvCreateSessionResult::TPtr& ev) override {
        if (ev->Get()->Result.IsSuccess()) {
            SCHEMA_LOG_DEBUG("Create " << GetEntityName() << ". Create session OK");
            Session = ev->Get()->Result.GetSession();
            CallCreate();
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

    void CallCreate() override {
        if (!Session) {
            CreateSession();
            return;
        }

        TCreateActorBase::CallCreate();
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

} // namespace NYq
