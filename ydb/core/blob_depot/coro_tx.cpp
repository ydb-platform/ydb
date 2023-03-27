#include "coro_tx.h"

namespace NKikimr::NBlobDepot {

    thread_local TCoroTx *TCoroTx::Current = nullptr;

    enum class EOutcome {
        UNSET,
        FINISH_TX,
        RESTART_TX,
        RUN_SUCCESSOR_TX,
        END_CORO
    };

#ifndef NDEBUG
    static constexpr ui64 StackSentinel = 0x8E0CDBFD41F04520;
    static constexpr size_t NumStackSentinels = 8;
#endif

    class TCoroTx::TContext : public ITrampoLine {
        TMappedAllocation Stack;
        TExceptionSafeContext Context;
        TExceptionSafeContext *BackContext = nullptr;

        EOutcome Outcome = EOutcome::UNSET;

        TTokens Tokens;
        std::function<void()> Body;

        bool Finished = false;

    public:
        TContext(TTokens&& tokens, std::function<void()>&& body)
            : Stack(65536)
            , Context({this, TArrayRef(Stack.Begin(), Stack.End())})
            , Tokens(std::move(tokens))
            , Body(std::move(body))
        {
#ifndef NDEBUG
            char *p;
#   if STACK_GROW_DOWN
            p = Stack.Begin();
#   else
            p = Stack.End() - sizeof(StackSentinel) * NumStackSentinels;
#   endif
            for (size_t i = 0; i < NumStackSentinels; ++i) {
                memcpy(p + i * sizeof(StackSentinel), &StackSentinel, sizeof(StackSentinel));
            }
#endif
        }

        ~TContext() {
            if (!Finished) {
                Finished = true;
                Resume();
            }
        }

        EOutcome Resume() {
            Outcome = EOutcome::UNSET;

            TExceptionSafeContext returnContext;
            Y_VERIFY(!BackContext);
            BackContext = &returnContext;
            returnContext.SwitchTo(&Context);
            Y_VERIFY(BackContext == &returnContext);
            BackContext = nullptr;

            // validate stack
#ifndef NDEBUG
            char *p;
#   if STACK_GROW_DOWN
            p = Stack.Begin();
#   else
            p = Stack.End() - sizeof(StackSentinel) * NumStackSentinels;
#   endif
            for (size_t i = 0; i < NumStackSentinels; ++i) {
                ui64 temp;
                memcpy(&temp, p + i * sizeof(StackSentinel), sizeof(StackSentinel));
                Y_VERIFY(StackSentinel == temp);
            }
#endif

            Y_VERIFY(Outcome != EOutcome::UNSET);
            return Outcome;
        }

        void Return(EOutcome outcome) {
            Y_VERIFY(Outcome == EOutcome::UNSET);
            Outcome = outcome;
            Y_VERIFY(BackContext);
            Context.SwitchTo(BackContext);
            if (IsExpired() || Finished) {
                throw TExDead();
            }
        }

    private:
        bool IsExpired() const {
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
                    Body();
                } catch (const TExDead&) {
                    // just do nothing
                }
            }
            Finished = true;
            Return(EOutcome::END_CORO);
        }
    };

    TCoroTx::TCoroTx(TBlobDepot *self, TTokens&& tokens, std::function<void()> body)
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
        // prepare environment
        Y_VERIFY(TxContext == nullptr && Current == nullptr);
        TxContext = &txc;
        Current = this;

        Y_VERIFY(Context);
        const EOutcome outcome = Context->Resume();

        // clear environment back
        Y_VERIFY(TxContext == &txc && Current == this);
        TxContext = nullptr;
        Current = nullptr;

        switch (outcome) {
            case EOutcome::FINISH_TX:
                return true;

            case EOutcome::RESTART_TX:
                return false;

            default:
                Y_FAIL();
        }
    }

    void TCoroTx::Complete(const TActorContext&) {
        // prepare environment
        Y_VERIFY(TxContext == nullptr && Current == nullptr);
        Current = this;

        Y_VERIFY(Context);
        const EOutcome outcome = Context->Resume();

        // clear environment back
        Y_VERIFY(TxContext == nullptr && Current == this);
        Current = nullptr;

        switch (outcome) {
            case EOutcome::RUN_SUCCESSOR_TX:
                Self->Execute(std::make_unique<TCoroTx>(*this));
                break;

            case EOutcome::END_CORO:
                break;

            default:
                Y_FAIL();
        }
    }

    TCoroTx *TCoroTx::CurrentTx() {
        return Current;
    }

    NTabletFlatExecutor::TTransactionContext *TCoroTx::GetTxc() {
        Y_VERIFY(Current->TxContext);
        return Current->TxContext;
    }

    void TCoroTx::FinishTx() {
        Y_VERIFY(Current);
        Current->Context->Return(EOutcome::FINISH_TX);
    }

    void TCoroTx::RestartTx() {
        Y_VERIFY(Current);
        Current->Context->Return(EOutcome::RESTART_TX);
    }

    void TCoroTx::RunSuccessorTx() {
        Y_VERIFY(Current);
        Current->Context->Return(EOutcome::RUN_SUCCESSOR_TX);
    }

} // NKikimr::NBlobDepot
