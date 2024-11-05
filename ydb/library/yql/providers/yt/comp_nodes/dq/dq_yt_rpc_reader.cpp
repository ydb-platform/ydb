#include "dq_yt_rpc_reader.h"
#include "dq_yt_rpc_helpers.h"

#include <ydb/library/yql/utils/failure_injector/failure_injector.h>

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
    : InnerState_(new TInnerState (settings->Requests.size(), inflight))
    , StateByReader_(settings->Requests.size())
    , Spec_(&spec)
    , HolderFactory_(holderFactory)
    , BlockSize_(blockSize)
    , TimerAwaiting_(RPCReaderAwaitingStallTime, 100)
    , OriginalIndexes_(std::move(settings->OriginalIndexes))
    , Settings_(std::move(settings))
{
#ifdef RPC_PRINT_TIME
    print_add(1);
#endif
    YQL_ENSURE(Settings_->Requests.size() == OriginalIndexes_.size());
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

void TParallelFileInputState::CheckError() const {
    if (!InnerState_->Error.IsOK()) {
        Cerr << "YT RPC Reader exception:\n" << InnerState_->Error.GetMessage();
        InnerState_->Error.ThrowOnError();
    }
}

// Call when reader created, or when previous batch was successfully decoded
void TParallelFileInputState::TInnerState::InputReady(size_t idx) {
    std::lock_guard guard(Lock);
    ReadyReaders.emplace(idx);
    --ReadersInflight;
}

TMaybe<size_t> TParallelFileInputState::TInnerState::GetFreeReader() {
    std::lock_guard guard(Lock);
    if (ReadersInflight >= Inflight) {
        return {};
    }
    if (ReadyReaders.size()) {
        size_t res = ReadyReaders.front();
        ReadyReaders.pop();
        ++ReadersInflight;
        return res;
    }
    if (NextFreeReaderIdx >= RawInputs.size()) {
        return {};
    }
    ++ReadersInflight;
    return NextFreeReaderIdx++;
}

void TParallelFileInputState::TInnerState::InputDone() {
    std::lock_guard guard(Lock);
    --ReadersInflight;
}

bool TParallelFileInputState::TInnerState::AllReadersDone() {
    std::lock_guard guard(Lock);
    return NextFreeReaderIdx == RawInputs.size() && ReadyReaders.empty() && !ReadersInflight;
}

namespace {

void ReadCallback(NYT::TErrorOr<NYT::TSharedRef>&& res_, std::shared_ptr<TParallelFileInputState::TInnerState> state, size_t inputIdx) {
    TFailureInjector::Reach("dq_rpc_reader_read_err_when_empty", [&res_] {
        res_ = NYT::TErrorOr<NYT::TSharedRef>(NYT::TError("Failure_On_Read_Callback"));
    });

    if (!res_.IsOK()) {
        std::lock_guard lock(state->Lock);
        state->Error = std::move(res_);
        state->WaitPromise.TrySet();
        return;
    }

    auto block = std::move(res_.Value());
    NYT::NApi::NRpcProxy::NProto::TRowsetDescriptor descriptor;
    NYT::NApi::NRpcProxy::NProto::TRowsetStatistics statistics;

    auto CurrentPayload_ = std::move(NYT::NApi::NRpcProxy::DeserializeRowStreamBlockEnvelope(block, &descriptor, &statistics));
    // skip no-skiff data
    if (descriptor.rowset_format() != NYT::NApi::NRpcProxy::NProto::RF_FORMAT) {
        state->WaitPromise.TrySet();
        return;
    }

    if (CurrentPayload_.Empty()) {
        state->WaitPromise.TrySet();
        state->InputDone();
        return;
    }
    std::lock_guard lock(state->Lock);

    state->Results.emplace(std::move(TParallelFileInputState::TResult{inputIdx, std::move(CurrentPayload_)}));
    state->WaitPromise.TrySet();
}

void ExecuteRead(size_t inputIdx, std::shared_ptr<TParallelFileInputState::TInnerState> state) {
    state->RawInputs[inputIdx]->Read().SubscribeUnique(BIND([state, inputIdx](NYT::TErrorOr<NYT::TSharedRef>&& res_){
        ReadCallback(std::move(res_), state, inputIdx);
    }));
}
}

void TParallelFileInputState::RunNext() {
    while (auto idx = InnerState_->GetFreeReader()) {
        auto inputIdx = *idx;
        if (!InnerState_->RawInputs[inputIdx]) {
            CreateInputStream(Settings_->Requests[inputIdx]).SubscribeUnique(BIND([state = InnerState_, inputIdx](NYT::TErrorOr<NYT::NConcurrency::IAsyncZeroCopyInputStreamPtr>&& stream) {
                if (!stream.IsOK()) {
                    state->Error = stream;
                    return;
                }
                state->RawInputs[inputIdx] = std::move(stream.Value());
                ExecuteRead(inputIdx, state);
            }));
            continue;
        }
        ExecuteRead(inputIdx, InnerState_);
    }
}

bool TParallelFileInputState::NextValue() {
    for (;;) {
        if (!InnerState_->AllReadersDone()) {
            RunNext();
        }
        if (MkqlReader_.IsValid()) {
            CurrentValue_ = std::move(MkqlReader_.GetRow());
            if (!Spec_->InputGroups.empty()) {
                CurrentValue_ = HolderFactory_.CreateVariantHolder(CurrentValue_.Release(), Spec_->InputGroups.at(OriginalIndexes_[CurrentInput_]));
            }
            MkqlReader_.Next();
            return true;
        }
        if (!Settings_->Requests.empty()) {
            if (MkqlReader_.GetRowIndexUnchecked()) {
                StateByReader_[CurrentInput_].CurrentRow = *MkqlReader_.GetRowIndexUnchecked() - 1;
            }
            StateByReader_[CurrentInput_].CurrentRange = MkqlReader_.GetRangeIndexUnchecked();
        }
        bool needWait = false;
        {
            std::lock_guard lock(InnerState_->Lock);
            needWait = InnerState_->Results.empty();
        }
        if (needWait && InnerState_->AllReadersDone()) {
            CheckError();
            return false;
        }
        if (needWait) {
            auto guard = Guard(TimerAwaiting_);
            YQL_ENSURE(NYT::NConcurrency::WaitFor(InnerState_->WaitPromise.ToFuture()).IsOK());
        }
        TResult result;
        {
            std::lock_guard lock(InnerState_->Lock);
            CheckError();
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
        InnerState_->InputReady(CurrentInput_);
    }
}

void TDqYtReadWrapperRPC::MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state) const {
    auto settings = CreateInputStreams(false, Token, ClusterName, Timeout, Inflight > 1, Tables, SamplingSpec);
    state = ctx.HolderFactory.Create<TDqYtReadWrapperBase<TDqYtReadWrapperRPC, TParallelFileInputState>::TState>(Specs, ctx.HolderFactory, 4_MB, Inflight, std::move(settings));
}
}
