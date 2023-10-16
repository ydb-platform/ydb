#include "dq_yt_rpc_reader.h"
#include "dq_yt_rpc_helpers.h"

#include "yt/cpp/mapreduce/common/helpers.h"

#include <yt/yt/library/auth/auth.h>

#include <yt/yt/client/api/client.h>

#include <yt/yt/client/api/rpc_proxy/client_impl.h>
#include <yt/yt/client/api/rpc_proxy/config.h>
#include <yt/yt/client/api/rpc_proxy/connection.h>
#include <yt/yt/client/api/rpc_proxy/row_stream.h>

namespace NYql::NDqs {

using namespace NKikimr::NMiniKQL;

namespace {
TStatKey RPCReaderAwaitingStallTime("Job_RPCReaderAwaitingStallTime", true);
}
#ifdef RPC_PRINT_TIME
int cnt = 0;
auto beg = std::chrono::steady_clock::now();
std::mutex mtx;
void print_add(int x) {
    std::lock_guard l(mtx);
    Cerr << (cnt += x) << " (" << std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - beg).count() << "ms gone from process start)\n";
}
#endif

TParallelFileInputState::TParallelFileInputState(const TMkqlIOSpecs& spec,
    const NKikimr::NMiniKQL::THolderFactory& holderFactory,
    size_t blockSize,
    size_t inflight,
    std::unique_ptr<TSettingsHolder>&& settings)
    : InnerState_(new TInnerState (settings->RawInputs.size()))
    , StateByReader_(settings->RawInputs.size())
    , Spec_(&spec)
    , HolderFactory_(holderFactory)
    , RawInputs_(std::move(settings->RawInputs))
    , BlockSize_(blockSize)
    , Inflight_(inflight)
    , TimerAwaiting_(RPCReaderAwaitingStallTime, 100)
    , OriginalIndexes_(std::move(settings->OriginalIndexes))
    , Settings_(std::move(settings))
{
#ifdef RPC_PRINT_TIME
    print_add(1);
#endif
    YQL_ENSURE(RawInputs_.size() == OriginalIndexes_.size());
    MkqlReader_.SetSpecs(*Spec_, HolderFactory_);
    Valid_ = NextValue();
}

size_t TParallelFileInputState::GetTableIndex() const {
    return OriginalIndexes_[CurrentInput_];
}

size_t TParallelFileInputState::GetRecordIndex() const {
    Y_ABORT("Not implemented");
    return CurrentRecord_; // returns 1-based index
}

void TParallelFileInputState::SetNextBlockCallback(std::function<void()> cb) {
    MkqlReader_.SetNextBlockCallback(cb);
    OnNextBlockCallback_ = std::move(cb);
}

bool TParallelFileInputState::IsValid() const {
    return Valid_;
}

NUdf::TUnboxedValue TParallelFileInputState::GetCurrent() {
    return CurrentValue_;
}

void TParallelFileInputState::Next() {
    Valid_ = NextValue();
}

void TParallelFileInputState::Finish() {
    TimerAwaiting_.Report(Spec_->JobStats_);
    MkqlReader_.Finish();
}

bool TParallelFileInputState::RunNext() {
    while (true) {
        size_t InputIdx = 0;
        {
            std::lock_guard lock(InnerState_->Lock);
            if (InnerState_->CurrentInputIdx == RawInputs_.size() && InnerState_->CurrentInflight == 0 && InnerState_->Results.empty() && !MkqlReader_.IsValid()) {
                return false;
            }

            if (InnerState_->CurrentInflight >= Inflight_ || InnerState_->Results.size() >= Inflight_) {
                break;
            }

            while (InnerState_->CurrentInputIdx < InnerState_->IsInputDone.size() && InnerState_->IsInputDone[InnerState_->CurrentInputIdx]) {
                ++InnerState_->CurrentInputIdx;
            }

            if (InnerState_->CurrentInputIdx == InnerState_->IsInputDone.size()) {
                break;
            }
            InputIdx = InnerState_->CurrentInputIdx;
            ++InnerState_->CurrentInputIdx;
            ++InnerState_->CurrentInflight;
        }

        RawInputs_[InputIdx]->Read().SubscribeUnique(BIND([state = InnerState_, inputIdx = InputIdx
#ifdef RPC_PRINT_TIME
        , startAt = std::chrono::steady_clock::now()
#endif
        ](NYT::TErrorOr<NYT::TSharedRef>&& res_){
#ifdef RPC_PRINT_TIME
            auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - startAt).count();
            if (elapsed > 1'000'000) {
                Cerr << (TStringBuilder() "Warn: request took: " << elapsed << " mcs)\n");
            }
#endif
            if (!res_.IsOK()) {
                std::lock_guard lock(state->Lock);
                state->Error = std::move(res_);
                --state->CurrentInflight;
                state->WaitPromise.TrySet();
                return;
            }
            auto block = std::move(res_.Value());
            NYT::NApi::NRpcProxy::NProto::TRowsetDescriptor descriptor;
            NYT::NApi::NRpcProxy::NProto::TRowsetStatistics statistics;

            auto CurrentPayload_ = std::move(NYT::NApi::NRpcProxy::DeserializeRowStreamBlockEnvelope(block, &descriptor, &statistics));
            std::lock_guard lock(state->Lock);
            state->CurrentInputIdx = std::min(state->CurrentInputIdx, inputIdx);
            --state->CurrentInflight;
            // skip no-skiff data
            if (descriptor.rowset_format() != NYT::NApi::NRpcProxy::NProto::RF_FORMAT) {
                state->WaitPromise.TrySet();
                return;
            }

            if (CurrentPayload_.Empty()) {
                state->IsInputDone[inputIdx] = 1;
                state->WaitPromise.TrySet();
                return;
            }
            state->Results.emplace(std::move(TResult{inputIdx, std::move(CurrentPayload_)}));
            state->WaitPromise.TrySet();
        }));
    }
    return true;
}

bool TParallelFileInputState::NextValue() {
    for (;;) {
        if (!RunNext()) {
            Y_ENSURE(InnerState_->Results.empty());
#ifdef RPC_PRINT_TIME
        print_add(-1);
#endif
            return false;
        }
        if (MkqlReader_.IsValid()) {
            CurrentValue_ = std::move(MkqlReader_.GetRow());
            if (!Spec_->InputGroups.empty()) {
                CurrentValue_ = HolderFactory_.CreateVariantHolder(CurrentValue_.Release(), Spec_->InputGroups.at(OriginalIndexes_[CurrentInput_]));
            }
            MkqlReader_.Next();
            return true;
        }
        if (MkqlReader_.GetRowIndexUnchecked()) {
            StateByReader_[CurrentInput_].CurrentRow = *MkqlReader_.GetRowIndexUnchecked() - 1;
        }
        StateByReader_[CurrentInput_].CurrentRange = MkqlReader_.GetRangeIndexUnchecked();
        bool needWait = false;
        {
            std::lock_guard lock(InnerState_->Lock);
            needWait = InnerState_->Results.empty();
        }
        if (needWait) {
            auto guard = Guard(TimerAwaiting_);
            YQL_ENSURE(NYT::NConcurrency::WaitFor(InnerState_->WaitPromise.ToFuture()).IsOK());
        }
        TResult result;
        {
            std::lock_guard lock(InnerState_->Lock);
            if (!InnerState_->Error.IsOK()) {
                InnerState_->Error.ThrowOnError();
            }
            if (InnerState_->Results.empty()) {
                continue;
            }
            result = std::move(InnerState_->Results.front());
            InnerState_->Results.pop();
            InnerState_->WaitPromise = NYT::NewPromise<void>();
        }
        CurrentInput_ = result.Input_;
        CurrentReader_ = MakeIntrusive<TPayloadRPCReader>(std::move(result.Value_));
        MkqlReader_.SetReader(*CurrentReader_, 1, BlockSize_, ui32(OriginalIndexes_[CurrentInput_]), true, StateByReader_[CurrentInput_].CurrentRow, StateByReader_[CurrentInput_].CurrentRange);
        MkqlReader_.Next();
    }
}

void TDqYtReadWrapperRPC::MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state) const {
    auto settings = CreateInputStreams(false, Token, ClusterName, Timeout, Inflight > 1, Tables, SamplingSpec);
    state = ctx.HolderFactory.Create<TDqYtReadWrapperBase<TDqYtReadWrapperRPC, TParallelFileInputState>::TState>(Specs, ctx.HolderFactory, 4_MB, Inflight, std::move(settings));
}
}
