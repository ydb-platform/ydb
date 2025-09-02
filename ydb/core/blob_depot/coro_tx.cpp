#include "coro_tx.h"

#include <util/system/sanitizers.h>
#include <util/system/type_name.h>
#include <util/system/info.h>
#include <util/system/protect.h>

namespace NKikimr::NBlobDepot {

    enum class EOutcome {
        UNSET,
        FINISH_TX,
        RESTART_TX,
        RUN_SUCCESSOR_TX,
        END_CORO
    };

    static const size_t PageSize = NSystemInfo::GetPageSize();

    static size_t AlignStackSize(size_t size) {
        size += PageSize - (size & PageSize - 1) & PageSize - 1;
#ifndef NDEBUG
        size += PageSize;
#endif
        return size;
    }

    class TCoroTx::TContext
        : public ITrampoLine
        , public TContextBase
    {
        TMappedAllocation Stack;
        TExceptionSafeContext Context;
        TExceptionSafeContext *BackContext = nullptr;

        EOutcome Outcome = EOutcome::UNSET;

        TTokens Tokens;
        std::function<void(TContextBase&)> Body;

        bool Finished = false;
        bool Aborted = false;

    public:
        TContext(TTokens&& tokens, std::function<void(TContextBase&)>&& body)
            : Stack(AlignStackSize(65536))
            , Context({this, TArrayRef(Stack.Begin(), Stack.End())})
            , Tokens(std::move(tokens))
            , Body(std::move(body))
        {
#if !defined(NDEBUG)
            ProtectMemory(STACK_GROW_DOWN ? Stack.Begin() : Stack.End() - PageSize, PageSize, EProtectMemoryMode::PM_NONE);
#endif
        }

        ~TContext() {
            if (!Finished) {
                Aborted = true;
                Resume(nullptr);
            }
        }

        EOutcome Resume(NTabletFlatExecutor::TTransactionContext *txc) {
            Outcome = EOutcome::UNSET;

            NTabletFlatExecutor::TTransactionContext *prevTxC = std::exchange(TxContext, txc);
            Y_ABORT_UNLESS(!prevTxC);

            TExceptionSafeContext returnContext;
            Y_ABORT_UNLESS(!BackContext);
            BackContext = &returnContext;
            returnContext.SwitchTo(&Context);
            Y_ABORT_UNLESS(BackContext == &returnContext);
            BackContext = nullptr;

            prevTxC = std::exchange(TxContext, nullptr);
            Y_ABORT_UNLESS(prevTxC == txc);

            Y_ABORT_UNLESS(Outcome != EOutcome::UNSET);
            return Outcome;
        }

        void Return(EOutcome outcome) {
            Y_ABORT_UNLESS(Outcome == EOutcome::UNSET);
            Outcome = outcome;
            Y_ABORT_UNLESS(BackContext);
            Context.SwitchTo(BackContext);
            if (IsExpired()) {
                throw TExDead();
            }
        }

    private:
        bool IsExpired() const {
            if (Aborted) {
                return true;
            }
            for (auto& token : Tokens) {
                if (token.expired()) {
                    return true;
                }
            }
            return false;
        }

        void DoRun() override {
            if (!IsExpired()) {
                try {
                    Body(*this);
                } catch (const TExDead&) {
                    // just do nothing
                }
            }
            Finished = true;
            Return(EOutcome::END_CORO);
        }
    };

    TCoroTx::TCoroTx(TBlobDepot *self, TTokens&& tokens, std::function<void(TCoroTx::TContextBase&)> body)
        : TTransactionBase(self)
        , Context(std::make_unique<TContext>(std::move(tokens), std::move(body)))
    {}

    TCoroTx::TCoroTx(TCoroTx& predecessor)
        : TTransactionBase(predecessor.Self)
        , Context(std::move(predecessor.Context))
    {}

    TCoroTx::~TCoroTx()
    {}

    bool TCoroTx::Execute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext&) {
        Y_ABORT_UNLESS(Context);
        const EOutcome outcome = Context->Resume(&txc);

        switch (outcome) {
            case EOutcome::FINISH_TX:
                return true;

            case EOutcome::RESTART_TX:
                return false;

            default:
                Y_ABORT();
        }
    }

    void TCoroTx::Complete(const TActorContext&) {
        Y_ABORT_UNLESS(Context);
        const EOutcome outcome = Context->Resume(nullptr);

        switch (outcome) {
            case EOutcome::RUN_SUCCESSOR_TX:
                Self->Execute(std::make_unique<TCoroTx>(*this));
                break;

            case EOutcome::END_CORO:
                break;

            default:
                Y_ABORT();
        }
    }

    NTabletFlatExecutor::TTransactionContext *TCoroTx::TContextBase::GetTxc() {
        Y_ABORT_UNLESS(TxContext);
        return TxContext;
    }

    void TCoroTx::TContextBase::FinishTx() {
        static_cast<TContext*>(this)->Return(EOutcome::FINISH_TX);
    }

    void TCoroTx::TContextBase::RestartTx() {
        static_cast<TContext*>(this)->Return(EOutcome::RESTART_TX);
    }

    void TCoroTx::TContextBase::RunSuccessorTx() {
        static_cast<TContext*>(this)->Return(EOutcome::RUN_SUCCESSOR_TX);
    }

} // NKikimr::NBlobDepot
