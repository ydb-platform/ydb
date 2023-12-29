#pragma once

#include <ydb/library/db_pool/db_pool.h>

#include <ydb/core/fq/libs/common/debug_info.h>
#include <ydb/core/fq/libs/config/yq_issue.h>
#include <ydb/core/fq/libs/events/events.h>
#include <ydb/core/fq/libs/exceptions/exceptions.h>
#include <ydb/core/fq/libs/db_schema/db_schema.h>

namespace NFq {

using namespace NThreading;
using namespace NYdb;
using namespace NYdb::NTable;

class TDbExecutable {
public:
    using TPtr = std::shared_ptr<TDbExecutable>;

    TDbExecutable(bool collectDebugInfo = false) {
        if (collectDebugInfo) {
            DebugInfo = std::make_shared<TDebugInfo>();
        }
    }

    virtual TAsyncStatus Execute(NYdb::NTable::TSession& session) = 0;
    void Throw(const TString& message);

    NDbPool::TDbPool::TPtr DbPool;
    std::weak_ptr<TDbExecutable> SelfHolder;
    NYql::TIssues Issues;
    NYql::TIssues InternalIssues;
    TDebugInfoPtr DebugInfo;
    TString TablePathPrefix;
};

template <typename TProto>
void ParseProto(TDbExecutable& executable, TProto& proto, TResultSetParser& parser, const TString& columnName) {
    if (!proto.ParseFromString(*parser.ColumnParser(columnName).GetOptionalString()))
    {
        executable.Throw(Sprintf("Error parsing proto message %s", proto.GetTypeName().c_str()));
    }
}

inline TAsyncStatus Exec(NDbPool::TDbPool::TPtr dbPool, TDbExecutable::TPtr executable,
                                                                    const TString& tablePathPrefix) {
    executable->DbPool = dbPool;
    executable->SelfHolder = executable;
    executable->TablePathPrefix = tablePathPrefix;
    return ExecDbRequest(dbPool, [=](NYdb::NTable::TSession& session) {
        return executable->Execute(session);
    });
}

/*
 * 1. TDbExecuter must be created like this:
 *
 *    TDbExecutable::TPtr executable;
 *    auto& executer = TDbExecuter<...>::Create(executable);
 *
 * 2. Template param is state struct. It's lifecycle matches executer. It's expected to keep all state
 *    between async db calls.
 *
 * 3. TDbExecutable::Read adds read operation to DB access pipeline, Write adds write operation (w/o result
 *    processing). All calls are serialized. There is no concurrency issues in access TDbExecutable::State
 *    from callbacks.
 *
 * 4. Final callback (passed in TDbExecutable::Process) is expected to be called from AS thread, so actor
 *    must implement TEvents::TEvCallback handler, f.e.:
 *
 *    hFunc(TEvents::TEvCallback, [](TEvents::TEvCallback::TPtr& ev) { ev->Get()->Callback(); } );
 *
 *    it is safe to access TDbExecutable::State and actor class members from final callback w/o concurrency
 *    issus as well
 *
 * 5. Whole pipeline is retried automatically. If TDbExecutable::State needs to be cleaned up from failed run,
 *    pass handler as stateInitCallback to TDbExecutable ctor.
 */

template <typename TState>
class TDbExecuter : public TDbExecutable {

public:
    using TCallback = std::function<void(TDbExecuter<TState>&)>;
    using TBuildCallback = std::function<void(TDbExecuter<TState>&, TSqlQueryBuilder&)>;
    using TResultCallback = std::function<void(TDbExecuter<TState>&, const TVector<NYdb::TResultSet>&)>;

private:
    struct TExecStep {
        TBuildCallback BuildCallback;
        TResultCallback ResultCallback;
        TCallback ProcessCallback;
        TString Name;
        bool Commit = false;
    };
    std::vector<TExecStep> Steps;
    ui32 CurrentStepIndex = 0;
    ui32 InsertStepIndex = 0;
    NActors::TActorId HandlerActorId;
    TMaybe<TTransaction> Transaction;
    NActors::TActorSystem* ActorSystem = nullptr;
    TCallback HandlerCallback;
    TCallback StateInitCallback;
    bool SkipStep_ = false;

protected:
    TDbExecuter(bool collectDebugInfo, std::function<void(TDbExecuter<TState>&)> stateInitCallback)
        : TDbExecutable(collectDebugInfo), StateInitCallback(stateInitCallback) {
    }

    TDbExecuter(bool collectDebugInfo)
        : TDbExecutable(collectDebugInfo) {
            StateInitCallback = [](TDbExecuter<TState>& executer) { executer.State = TState{}; };
    }

    TDbExecuter(const TDbExecuter& other) = delete;

public:
    virtual ~TDbExecuter() {
    };

    static TDbExecuter& Create(TDbExecutable::TPtr& holder, bool collectDebugInfo = false) {
        auto executer = new TDbExecuter(collectDebugInfo);
        holder.reset(executer);
        return *executer;
    };

    static TDbExecuter& Create(TDbExecutable::TPtr& holder, bool collectDebugInfo, std::function<void(TDbExecuter<TState>&)> stateInitCallback) {
        auto executer = new TDbExecuter(collectDebugInfo, stateInitCallback);
        holder.reset(executer);
        return *executer;
    };

    void SkipStep() {
        SkipStep_ = true;
    }

    static std::shared_ptr<TDbExecuter> Lock(const std::weak_ptr<TDbExecutable>& self) {
        auto lock = self.lock();
        return std::static_pointer_cast<TDbExecuter>(lock);
    }

    TAsyncStatus NextStep(NYdb::NTable::TSession session) {

        if (CurrentStepIndex == Steps.size()) {
            if (Transaction) {
                return Transaction->Commit()
                .Apply([selfHolder=SelfHolder, session=session](const TFuture<TCommitTransactionResult>& future) {
                    auto self = Lock(selfHolder);
                    if (!self) {
                        return MakeFuture(TStatus{EStatus::INTERNAL_ERROR, NYql::TIssues{NYql::TIssue{"self has been deleted"}}});
                    }
                    TCommitTransactionResult result = future.GetValue();
                    auto status = static_cast<TStatus>(result);
                    if (!status.IsSuccess()) {
                        return MakeFuture(status);
                    } else {
                        self->Transaction.Clear();
                        return self->NextStep(session);
                    }
                });
            }
            if (HandlerActorId != NActors::TActorId{}) {
                auto holder = Lock(SelfHolder);
                if (holder) {
                    ActorSystem->Send(HandlerActorId, new TEvents::TEvCallback([holder=holder, handlerCallback=HandlerCallback]() {
                        handlerCallback(*holder);
                    }));
                }
            }
            return MakeFuture(TStatus{EStatus::SUCCESS, NYql::TIssues{}});
        } else {
            TSqlQueryBuilder builder(TablePathPrefix, Steps[CurrentStepIndex].Name);
            SkipStep_ = false;
            Steps[CurrentStepIndex].BuildCallback(*this, builder);

            if (SkipStep_) { // TODO Refactor this
                this->CurrentStepIndex++;
                return this->NextStep(session);
            }

            const auto query = builder.Build();
            auto transaction = Transaction ? TTxControl::Tx(*Transaction) : TTxControl::BeginTx(TTxSettings::SerializableRW());
            if (Steps[CurrentStepIndex].Commit) {
                transaction = transaction.CommitTx();
            }

            return session.ExecuteDataQuery(query.Sql, transaction, query.Params, NYdb::NTable::TExecDataQuerySettings().KeepInQueryCache(true))
            .Apply([selfHolder=SelfHolder, session=session](const TFuture<TDataQueryResult>& future) {
                auto self = Lock(selfHolder);
                if (!self) {
                    return MakeFuture(TStatus{EStatus::INTERNAL_ERROR, NYql::TIssues{NYql::TIssue{"self has been deleted"}}});
                }

                NYdb::NTable::TDataQueryResult result = future.GetValue();
                auto status = static_cast<TStatus>(result);

                if (status.GetStatus() == EStatus::SCHEME_ERROR) { // retry if table does not exist
                    self->Transaction.Clear();
                    return MakeFuture(TStatus{EStatus::UNAVAILABLE, NYql::TIssues{status.GetIssues()}});
                }
                if (!status.IsSuccess()) {
                    self->Transaction.Clear();
                    return MakeFuture(status);
                }

                if (self->Steps[self->CurrentStepIndex].Commit) {
                    self->Transaction.Clear();
                } else if (!self->Transaction) {
                    self->Transaction = result.GetTransaction();
                }

                if (self->Steps[self->CurrentStepIndex].ResultCallback) {
                    try {
                        self->Steps[self->CurrentStepIndex].ResultCallback(*self, result.GetResultSets());
                    } catch (const TCodeLineException& exception) {
                        NYql::TIssue issue = MakeErrorIssue(exception.Code, exception.GetRawMessage());
                        self->Issues.AddIssue(issue);
                        NYql::TIssue internalIssue = MakeErrorIssue(exception.Code, CurrentExceptionMessage());
                        self->InternalIssues.AddIssue(internalIssue);
                    } catch (const std::exception& exception) {
                        NYql::TIssue issue = MakeErrorIssue(TIssuesIds::INTERNAL_ERROR, exception.what());
                        self->Issues.AddIssue(issue);
                        NYql::TIssue internalIssue = MakeErrorIssue(TIssuesIds::INTERNAL_ERROR, CurrentExceptionMessage());
                        self->InternalIssues.AddIssue(internalIssue);
                    } catch (...) {
                        NYql::TIssue issue = MakeErrorIssue(TIssuesIds::INTERNAL_ERROR, CurrentExceptionMessage());
                        self->Issues.AddIssue(issue);
                        NYql::TIssue internalIssue = MakeErrorIssue(TIssuesIds::INTERNAL_ERROR, CurrentExceptionMessage());
                        self->InternalIssues.AddIssue(internalIssue);
                    }
                }

                self->CurrentStepIndex++;
                return self->NextStep(session);
            });
        }
    }

    TAsyncStatus Execute(NYdb::NTable::TSession& session) override {
        CurrentStepIndex = 0;
        if (StateInitCallback) {
            StateInitCallback(*this);
        }
        return NextStep(session);
    }

    TDbExecuter& Read(
        TBuildCallback buildCallback
        , TResultCallback resultCallback
        , const TString& Name = "DefaultReadName"
        , bool commit = false
    ) {
        Steps.emplace(Steps.begin() + InsertStepIndex, TExecStep{buildCallback, resultCallback, nullptr, Name, commit});
        InsertStepIndex++;
        return *this;
    }

    TDbExecuter& Write(
        TBuildCallback buildCallback
        , const TString& Name = "DefaultWriteName"
        , bool commit = false
    ) {
        return Read(buildCallback, nullptr, Name, commit);
    }

    void Process(
        NActors::TActorId actorId
        , TCallback handlerCallback
    ) {
        Y_ABORT_UNLESS(HandlerActorId == NActors::TActorId{}, "Handler must be empty");
        ActorSystem = NActors::TActivationContext::ActorSystem();
        HandlerActorId = actorId;
        HandlerCallback = handlerCallback;
    }

    TState State;
};

} /* NFq */
