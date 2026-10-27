#include "program.h"

#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_pack.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <util/stream/str.h>
#include <deque>
#include <algorithm>

namespace NKikimr::NMiniKQL::NAsync {
namespace {

struct TStep {
    TInstruction Instruction;
    IComputationNode* Fragment = nullptr;
    std::vector<IComputationExternalNode*> Arguments;
    TType* Type = nullptr;
};
struct TFrame {
    std::shared_ptr<TMemoryReservation> Reservation;
    NUdf::TUnboxedValue Row;
    std::vector<NUdf::TUnboxedValue> Values;
    std::vector<bool> Done;
    std::vector<ui32> Stack;
    std::optional<TOperationId> Operation;
    bool AdmissionWait = false;
    ui64 AdmissionTurn = 0;
};

class TAsyncMap final: public TMutableComputationNode<TAsyncMap> {
    using TBase = TMutableComputationNode<TAsyncMap>;

public:
    TAsyncMap(TComputationMutables& mutables, IComputationNode* source, TProgram program, std::vector<TStep> steps)
        : TBase(mutables)
        , Source_(source)
        , Program_(std::move(program))
        , Steps_(std::move(steps))
    {
    }

    class TStream final: public TComputationValue<TStream> {
    public:
        TStream(TMemoryUsageInfo* memory, TComputationContext& ctx, NUdf::TUnboxedValue source, const TAsyncMap& map)
            : TComputationValue<TStream>(memory)
            , Ctx_(ctx)
            , Services_(ctx.AsyncServices)
            , Source_(std::move(source))
            , Map_(map)
        {
        }
        ~TStream() override {
            for (auto& frame : Frames_) {
                if (frame.Operation) {
                    Services_->CancelAndForget(*frame.Operation);
                }
            }
        }

    private:
        bool Runnable(const TFrame& frame) const {
            if (frame.Stack.empty()) {
                return false;
            }
            if (frame.Operation) {
                return Services_->IsReady(*frame.Operation);
            }
            return !frame.AdmissionWait || frame.AdmissionTurn != Services_->GetTurn();
        }

        void Step(TFrame& frame) {
            const ui32 index = frame.Stack.back();
            const auto& step = Map_.Steps_[index];
            const auto& ins = step.Instruction;
            auto finish = [&](NUdf::TUnboxedValue value) {
                frame.Values[index] = std::move(value);
                frame.Done[index] = true;
                frame.Stack.pop_back();
            };
            if (frame.Done[index]) {
                frame.Stack.pop_back();
                return;
            }
            if (ins.Kind == EInstruction::Input) {
                finish(frame.Row);
                return;
            }
            if (ins.Kind == EInstruction::Branch) {
                const auto condition = ins.Dependencies[0];
                if (!frame.Done[condition]) {
                    frame.Stack.push_back(condition);
                    return;
                }
                // Optional NULL condition chooses neither branch. Evaluate the
                // original If fragment to retain MiniKQL optional semantics.
                const auto& cond = frame.Values[condition];
                const bool empty = Map_.Steps_[condition].Type->IsOptional() && !cond;
                const ui32 branch = !empty && cond.Get<bool>() ? 1 : 2;
                if (!empty && !frame.Done[ins.Dependencies[branch]]) {
                    frame.Stack.push_back(ins.Dependencies[branch]);
                    return;
                }
                for (ui32 i = 0; i != 3; ++i) {
                    step.Arguments[i]->SetValue(Ctx_, NUdf::TUnboxedValue(frame.Values[ins.Dependencies[i]]));
                }
                finish(step.Fragment->GetValue(Ctx_));
                return;
            }
            if (ins.Kind == EInstruction::Unwrap) {
                const auto optional = ins.Dependencies[0];
                const auto message = ins.Dependencies[1];
                if (!frame.Done[optional]) {
                    frame.Stack.push_back(optional);
                    return;
                }
                // The diagnostic is lazy, including any external calls in it.
                if (!frame.Values[optional] && !frame.Done[message]) {
                    frame.Stack.push_back(message);
                    return;
                }
                step.Arguments[0]->SetValue(Ctx_, NUdf::TUnboxedValue(frame.Values[optional]));
                step.Arguments[1]->SetValue(Ctx_, NUdf::TUnboxedValue(frame.Values[message]));
                // Preserve the original optional representation and error position.
                finish(step.Fragment->GetValue(Ctx_));
                return;
            }
            for (auto dependency : ins.Dependencies) {
                if (!frame.Done[dependency]) {
                    frame.Stack.push_back(dependency);
                    return;
                }
            }
            if (ins.Kind == EInstruction::Sync) {
                for (size_t i = 0; i < ins.Dependencies.size(); ++i) {
                    step.Arguments[i]->SetValue(Ctx_, NUdf::TUnboxedValue(frame.Values[ins.Dependencies[i]]));
                }
                finish(step.Fragment->GetValue(Ctx_));
                return;
            }
            if (frame.Operation) {
                auto result = Services_->TakeResult(*frame.Operation);
                MKQL_ENSURE(result, "Resuming an async operation without a result");
                frame.Operation.reset();
                // Error payloads may contain secrets; do not interpolate them.
                MKQL_ENSURE(result->Outcome == EOutcome::Success, "Async operation failed, outcome=" << static_cast<int>(result->Outcome) << ", code=" << static_cast<int>(result->Error));
                TValuePacker packer(false, step.Type);
                finish(packer.Unpack(result->Payload, Ctx_.HolderFactory));
                return;
            }
            const auto input = ins.Dependencies[0];
            TValuePacker packer(false, Map_.Steps_[input].Type);
            TRequest request{.Operation = ins.Operation,
                             .Payload = TString(packer.Pack(frame.Values[input])),
                             .MaxResponseBytes = ins.MaxResponseBytes};
            auto submitted = Services_->Submit(ins.Provider, std::move(request));
            switch (submitted.Status) {
                case EAdmission::Accepted:
                    frame.Operation = submitted.Id;
                    frame.AdmissionWait = false;
                    return;
                case EAdmission::ResourceLimit:
                    frame.AdmissionWait = true;
                    frame.AdmissionTurn = Services_->GetTurn();
                    return;
                case EAdmission::OperationsLimit:
                case EAdmission::BytesLimit:
                    // Retry on a new owner turn, including cleanup credit
                    // wakeups. Never spend the remaining quantum self-polling.
                    MKQL_ENSURE(Services_->GetStats().TrackedOperations, "Async request cannot fit the operation/payload limits");
                    frame.AdmissionWait = true;
                    frame.AdmissionTurn = Services_->GetTurn();
                    return;
                default:
                    MKQL_ENSURE(false, "Async operation admission failed: " << static_cast<int>(submitted.Status));
            }
        }

        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) final {
            MKQL_ENSURE(!Failed_, "AsyncMap execution has failed");
            try {
                Services_->AccountAllocatorBytes(Ctx_.HolderFactory.GetMemoryUsed());
                auto status = FetchImpl(result);
                Services_->AccountAllocatorBytes(Ctx_.HolderFactory.GetMemoryUsed());
                if (status == NUdf::EFetchStatus::Yield) {
                    Services_->Flush();
                }
                return status;
            } catch (...) {
                Failed_ = true;
                for (auto& frame : Frames_) {
                    if (frame.Operation) {
                        Services_->CancelAndForget(*frame.Operation);
                    }
                }
                Frames_.clear();
                SourceReservation_.reset();
                throw;
            }
        }

        NUdf::EFetchStatus FetchImpl(NUdf::TUnboxedValue& result) {
            if (SourceWaitTurn_ != Services_->GetTurn()) {
                SourceWaiting_ = false;
            }
            while (true) {
                if (!Frames_.empty() && Frames_.front().Stack.empty()) {
                    result = std::move(Frames_.front().Values[Map_.Program_.Root]);
                    Frames_.pop_front();
                    if (FrameCursor_) {
                        --FrameCursor_;
                    }
                    return NUdf::EFetchStatus::Ok;
                }
                if (Frames_.empty() && Finished_) {
                    return NUdf::EFetchStatus::Finish;
                }
                bool didWork = false;
                // A bounded window admits independent rows while earlier rows
                // wait. Output order is input order; state lives in each frame.
                for (size_t visited = 0; visited < Frames_.size(); ++visited) {
                    FrameCursor_ %= Frames_.size();
                    auto& frame = Frames_[FrameCursor_];
                    if (!Runnable(frame)) {
                        FrameCursor_ = (FrameCursor_ + 1) % Frames_.size();
                        continue;
                    }
                    if (!Services_->TryStep()) {
                        Services_->RequestRerun();
                        return NUdf::EFetchStatus::Yield;
                    }
                    Step(frame);
                    Services_->AccountAllocatorBytes(Ctx_.HolderFactory.GetMemoryUsed());
                    FrameCursor_ = (FrameCursor_ + 1) % Frames_.size();
                    didWork = true;
                }
                const bool runnable = std::any_of(Frames_.begin(), Frames_.end(), [&](const auto& frame) { return Runnable(frame); });
                const ui64 frameBytes = sizeof(TFrame) + 64 + Map_.Steps_.size() * (sizeof(NUdf::TUnboxedValue) + sizeof(ui32) + 1);
                if (!runnable && !Finished_ && !SourceWaiting_ && Frames_.size() < Map_.Program_.MaxFrames && !SourceReservation_) {
                    // Reserve the destination before starting a possibly nested
                    // Fetch, and retain it across Yield. Source completion must
                    // not need fresh quota to consume its own ready results.
                    SourceReservation_ = Services_->TryReserveFrame(frameBytes);
                    if (!SourceReservation_) {
                        MKQL_ENSURE(Services_->CanWaitForMemory(), "Async source continuation cannot fit resource limits");
                        SourceWaiting_ = true;
                        SourceWaitTurn_ = Services_->GetTurn();
                    }
                }
                if (!runnable && !Finished_ && !SourceWaiting_ && Frames_.size() < Map_.Program_.MaxFrames) {
                    if (!SourceFetchCharged_ && !Services_->TryStep()) {
                        Services_->RequestRerun();
                        return NUdf::EFetchStatus::Yield;
                    }
                    // A nested source may exhaust the shared quantum and Yield.
                    // Keep this charge until that fetch produces a row or EOF;
                    // charging again on resume could starve the inner operator.
                    SourceFetchCharged_ = true;
                    NUdf::TUnboxedValue row;
                    switch (Source_.Fetch(row)) {
                        case NUdf::EFetchStatus::Finish:
                            SourceReservation_.reset();
                            SourceFetchCharged_ = false;
                            Finished_ = true;
                            break;
                        case NUdf::EFetchStatus::Yield:
                            SourceWaiting_ = true;
                            SourceWaitTurn_ = Services_->GetTurn();
                            break;
                        case NUdf::EFetchStatus::Ok: {
                            SourceFetchCharged_ = false;
                            TFrame frame;
                            frame.Reservation = std::move(SourceReservation_);
                            frame.Stack.reserve(Map_.Steps_.size());
                            frame.Row = std::move(row);
                            frame.Values.resize(Map_.Steps_.size());
                            frame.Done.resize(Map_.Steps_.size());
                            frame.Stack.push_back(Map_.Program_.Root);
                            Frames_.push_back(std::move(frame));
                            break;
                        }
                    }
                    didWork = true;
                }
                if (!didWork) {
                    const bool admissionBlocked = std::any_of(Frames_.begin(), Frames_.end(), [](const auto& frame) { return frame.AdmissionWait; });
                    MKQL_ENSURE(!admissionBlocked || Services_->CanWaitForMemory(), "Async continuation cannot fit resource limits");
                    return NUdf::EFetchStatus::Yield;
                }
            }
        }

        ui32 GetTraverseCount() const final {
            return 1;
        }
        NUdf::TUnboxedValue GetTraverseItem(ui32) const final {
            return Source_;
        }
        NUdf::TUnboxedValue Save() const final {
            MKQL_ENSURE(false, "AsyncMap checkpoint is not supported until D4");
        }
        void Load(const NUdf::TStringRef&) final {
            MKQL_ENSURE(false, "AsyncMap checkpoint is not supported until D4");
        }

        TComputationContext& Ctx_;
        const std::shared_ptr<TExecutionServices> Services_;
        NUdf::TUnboxedValue Source_;
        const TAsyncMap& Map_;
        std::deque<TFrame> Frames_;
        std::shared_ptr<TMemoryReservation> SourceReservation_;
        size_t FrameCursor_ = 0;
        bool Failed_ = false;
        bool Finished_ = false;
        bool SourceWaiting_ = false;
        bool SourceFetchCharged_ = false;
        ui64 SourceWaitTurn_ = 0;
    };

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        MKQL_ENSURE(ctx.AsyncServices, "AsyncMap requires task-owned async execution services");
        MKQL_ENSURE(!ctx.ExecuteLLVM, "AsyncMap requires interpreted execution in D2");
        for (const auto& step : Steps_) {
            if (step.Instruction.Kind == EInstruction::Invoke) {
                MKQL_ENSURE(ctx.AsyncServices->HasProvider(step.Instruction.Provider), "Missing async provider: " << step.Instruction.Provider);
            }
        }
        return ctx.HolderFactory.Create<TStream>(ctx, Source_->GetValue(ctx), *this);
    }

private:
    void RegisterDependencies() const final {
        DependsOn(Source_);
        for (const auto& step : Steps_) {
            for (auto* arg : step.Arguments) {
                Own(arg);
            }
            if (step.Instruction.Kind == EInstruction::Input || step.Instruction.Kind == EInstruction::Invoke) {
                Own(static_cast<IComputationExternalNode*>(step.Fragment));
            } else {
                DependsOn(step.Fragment);
            }
        }
    }
    IComputationNode* const Source_;
    const TProgram Program_;
    const std::vector<TStep> Steps_;
};

} // namespace

IComputationNode* WrapAsyncMap(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() >= 3 && callable.GetType()->GetReturnType()->IsStream(), "Invalid AsyncMap");
    const auto data = AS_VALUE(TDataLiteral, callable.GetInput(1))->AsValue().AsStringRef();
    const TString metadata(data);
    TStringInput input(metadata);
    TProgram program;
    Load(&input, program);
    MKQL_ENSURE(program.Version == 2 && program.MaxFrames && program.MaxFrames <= 1024 && program.Root < program.Instructions.size(), "Invalid async program header");
    std::vector<TStep> steps;
    for (ui32 i = 0; i < program.Instructions.size(); ++i) {
        const auto& ins = program.Instructions[i];
        MKQL_ENSURE(ins.Fragment >= 2 && ins.Fragment < callable.GetInputsCount(), "Invalid async fragment");
        for (auto dependency : ins.Dependencies) {
            MKQL_ENSURE(dependency < i, "Async program must be acyclic");
        }
        MKQL_ENSURE(ins.Kind <= EInstruction::Unwrap, "Invalid async instruction");
        MKQL_ENSURE(ins.Kind != EInstruction::Branch || ins.Dependencies.size() == 3, "Invalid async branch");
        MKQL_ENSURE(ins.Kind != EInstruction::Invoke || ins.Dependencies.size() == 1, "Invalid async invocation");
        MKQL_ENSURE(ins.Kind != EInstruction::Unwrap || ins.Dependencies.size() == 2, "Invalid async unwrap");
        MKQL_ENSURE((ins.Kind != EInstruction::Sync && ins.Kind != EInstruction::Branch && ins.Kind != EInstruction::Unwrap) || ins.Arguments.size() == ins.Dependencies.size(), "Invalid async fragment arguments");
        const bool placeholder = ins.Kind == EInstruction::Input || ins.Kind == EInstruction::Invoke;
        IComputationNode* fragment = placeholder ? LocateExternalNode(ctx.NodeLocator, callable, ins.Fragment)
                                                 : LocateNode(ctx.NodeLocator, callable, ins.Fragment);
        TStep step{.Instruction = ins, .Fragment = fragment, .Type = callable.GetInput(ins.Fragment).GetStaticType()};
        for (auto argument : ins.Arguments) {
            MKQL_ENSURE(argument >= 2 && argument < callable.GetInputsCount(), "Invalid async argument");
            step.Arguments.push_back(LocateExternalNode(ctx.NodeLocator, callable, argument));
        }
        steps.push_back(std::move(step));
    }
    ctx.Mutables.HasAsyncComputation = true;
    return new TAsyncMap(ctx.Mutables, LocateNode(ctx.NodeLocator, callable, 0), std::move(program), std::move(steps));
}

} // namespace NKikimr::NMiniKQL::NAsync
