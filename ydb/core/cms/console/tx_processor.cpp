#include "tx_processor.h"

#define YDB_LOG_THIS_FILE_COMPONENT Service

namespace NKikimr::NConsole {

TTxProcessor::TTxProcessor(ITxExecutor &executor,
                           const TString &name,
                           ui32 service,
                           TTxProcessor::TPtr parent,
                           bool temporary)
    : Executor(executor)
    , Name(name)
    , Service(service)
    , State(EState::LOCKED_BY_PARENT)
    , Temporary(temporary)
    , ActiveTx(nullptr)
    , Parent(parent.Get())
{
    Y_ABORT_UNLESS(!Temporary || Parent);
    if (!Parent || Parent->State == EState::LOCKED_BY_CHILDREN)
        State = EState::ACTIVE;
    LogPrefix = Sprintf("TTxProcessor(%s) ", Name.data());
}

TTxProcessor::TPtr TTxProcessor::GetSubProcessor(const TString &name,
                                                 const TActorContext &ctx,
                                                 bool temporary,
                                                 ui32 service)
{
    auto it = SubProcessors.find(name);
    if (it != SubProcessors.end())
        return it->second;

    YDB_LOG_TRACE_CTX(ctx, "Creating sub-processor",
        {"logPrefix", LogPrefix},
        {"name", name});

    TTxProcessor::TPtr subProcessor = new TTxProcessor(Executor,
                                                       name,
                                                       service ? service : Service,
                                                       this,
                                                       temporary);
    SubProcessors.emplace(name, subProcessor);

    if (!ActiveTx && State == EState::ACTIVE)
        ActivateChildren(ctx);

    return subProcessor;
}

void TTxProcessor::ProcessTx(ITransaction *tx,
                             const TActorContext &ctx)
{
    YDB_LOG_TRACE_CTX(ctx, "Enqueue tx",
        {"logPrefix", LogPrefix});

    TxQueue.push_back(THolder<ITransaction>(tx));
    ProcessNextTx(ctx);
}

void TTxProcessor::TxCompleted(ITransaction *tx,
                               const TActorContext &ctx)
{
    YDB_LOG_TRACE_CTX(ctx, "Completed tx",
        {"logPrefix", LogPrefix});

    Y_ABORT_UNLESS(tx == ActiveTx);
    ActiveTx = nullptr;

    if (CheckTemporary(ctx))
        return;

    if (State == EState::LOCKING)
        CheckLocks(ctx);
    else {
        Y_ABORT_UNLESS(State == EState::ACTIVE);
        ProcessNextTx(ctx);
    }
}

void TTxProcessor::RemoveSubProcessor(TTxProcessor::TPtr sub,
                                      const TActorContext &ctx)
{
    YDB_LOG_TRACE_CTX(ctx, "Removing sub-processor",
        {"logPrefix", LogPrefix},
        {"subName", sub->Name});

    Y_ABORT_UNLESS(SubProcessors.contains(sub->Name));
    SubProcessors.erase(sub->Name);

    if (CheckTemporary(ctx))
        return;

    if (SubProcessors.empty() && State == EState::LOCKED_BY_CHILDREN)
        Activate(ctx);
    else
        CheckActivation(ctx);
    CheckLocks(ctx);
}

void TTxProcessor::Clear()
{
    SubProcessors.clear();
    TxQueue.clear();
}

void TTxProcessor::Activate(const TActorContext &ctx)
{
    YDB_LOG_TRACE_CTX(ctx, "Is now active",
        {"logPrefix", LogPrefix});

    State = EState::ACTIVE;
    ProcessNextTx(ctx);
}

void TTxProcessor::ActivateChildren(const TActorContext &ctx)
{
    Y_ABORT_UNLESS(State == EState::ACTIVE);
    if (SubProcessors.empty())
        return;

    YDB_LOG_TRACE_CTX(ctx, "Is now locked by children",
        {"logPrefix", LogPrefix});

    State = EState::LOCKED_BY_CHILDREN;
    for (auto it = SubProcessors.begin(); it != SubProcessors.end(); ) {
        auto cur = it++;
        cur->second->Start(ctx);
    }
}

bool TTxProcessor::Lock(const TActorContext &ctx)
{
    if (State == EState::LOCKING)
        return false;

    if (State == EState::LOCKED_BY_PARENT)
        return true;

    YDB_LOG_TRACE_CTX(ctx, "Is now locking",
        {"logPrefix", LogPrefix});

    State = EState::LOCKING;

    if (ActiveTx)
        return false;

    bool res = true;
    for (auto &pr : SubProcessors)
        res = res && pr.second->Lock(ctx);

    if (res) {
        YDB_LOG_TRACE_CTX(ctx, "Is now locked by parent",
            {"logPrefix", LogPrefix});

        State = EState::LOCKED_BY_PARENT;
        return true;
    }

    return false;
}

void TTxProcessor::TryToLockChildren(const TActorContext &ctx)
{
    Y_ABORT_UNLESS(State == EState::LOCKED_BY_CHILDREN);
    YDB_LOG_TRACE_CTX(ctx, "Trying to lock children",
        {"logPrefix", LogPrefix});

    bool res = true;
    for (auto &pr : SubProcessors)
        if (!pr.second->Lock(ctx))
            res = false;
    if (res)
        Activate(ctx);
}

void TTxProcessor::Start(const TActorContext &ctx)
{
    if (State == EState::LOCKED_BY_PARENT)
        Activate(ctx);
    else {
        Y_ABORT_UNLESS(State == EState::LOCKED_BY_CHILDREN);
        CheckActivation(ctx);
    }
}

void TTxProcessor::CheckActivation(const TActorContext &ctx)
{
    if (TxQueue.empty())
        return;

    if (State != EState::LOCKED_BY_CHILDREN)
        return;

    for (auto &pr : SubProcessors)
        if (pr.second->State != EState::LOCKED_BY_PARENT)
            return;

    Activate(ctx);
}

void TTxProcessor::CheckLocks(const TActorContext &ctx)
{
    if (State != EState::LOCKING)
        return;

    if (ActiveTx)
        return;

    for (auto &pr : SubProcessors)
        if (pr.second->State != EState::LOCKED_BY_PARENT)
            return;

    YDB_LOG_TRACE_CTX(ctx, "Is now locked by parent",
        {"logPrefix", LogPrefix});

    State = EState::LOCKED_BY_PARENT;
    if (Parent)
        Parent->Start(ctx);
}

bool TTxProcessor::CheckTemporary(const TActorContext &ctx)
{
    if (!Temporary)
        return false;

    if (ActiveTx || !TxQueue.empty() || !SubProcessors.empty())
        return false;

    YDB_LOG_TRACE_CTX(ctx, "Unlink from parent",
        {"logPrefix", LogPrefix});

    Parent->RemoveSubProcessor(this, ctx);
    return true;
}

void TTxProcessor::ProcessNextTx(const TActorContext &ctx)
{
    if (ActiveTx)
        return;

    if (State == EState::LOCKED_BY_CHILDREN) {
        // In case of success it will process next tx by its own.
        TryToLockChildren(ctx);
        return;
    }

    if (State != EState::ACTIVE)
        return;

    if (State == EState::ACTIVE && TxQueue.empty()) {
        ActivateChildren(ctx);
        return;
    }

    ActiveTx = TxQueue.front().Release();
    TxQueue.pop_front();

    YDB_LOG_TRACE_CTX(ctx, "Starts new tx",
        {"logPrefix", LogPrefix});

    Y_ABORT_UNLESS(ActiveTx);
    Executor.Execute(ActiveTx, ctx);
}

} // namespace NKikimr::NConsole
