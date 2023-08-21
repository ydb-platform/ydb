#include "dq_yt_rpc_reader.h"

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
NYT::NYPath::TRichYPath ConvertYPathFromOld(const NYT::TRichYPath& richYPath) {
    NYT::NYPath::TRichYPath tableYPath(richYPath.Path_);
    const auto& rngs = richYPath.GetRanges();
    if (rngs) {
        TVector<NYT::NChunkClient::TReadRange> ranges;
        for (const auto& rng: *rngs) {
            auto& range = ranges.emplace_back();
            if (rng.LowerLimit_.Offset_) {
                range.LowerLimit().SetOffset(*rng.LowerLimit_.Offset_);
            }

            if (rng.LowerLimit_.TabletIndex_) {
                range.LowerLimit().SetTabletIndex(*rng.LowerLimit_.TabletIndex_);
            }

            if (*rng.LowerLimit_.RowIndex_) {
                range.LowerLimit().SetRowIndex(*rng.LowerLimit_.RowIndex_);
            }

            if (rng.UpperLimit_.Offset_) {
                range.UpperLimit().SetOffset(*rng.UpperLimit_.Offset_);
            }

            if (rng.UpperLimit_.TabletIndex_) {
                range.UpperLimit().SetTabletIndex(*rng.UpperLimit_.TabletIndex_);
            }

            if (*rng.UpperLimit_.RowIndex_) {
                range.UpperLimit().SetRowIndex(*rng.UpperLimit_.RowIndex_);
            }
        }
        tableYPath.SetRanges(std::move(ranges));
    }

    if (richYPath.Columns_) {
        tableYPath.SetColumns(richYPath.Columns_->Parts_);
    }

    return tableYPath;
}

class TFakeRPCReader : public NYT::TRawTableReader {
public:
    TFakeRPCReader(NYT::TSharedRef&& payload) : Payload_(std::move(payload)), PayloadStream_(Payload_.Begin(), Payload_.Size()) {}

    bool Retry(const TMaybe<ui32>& rangeIndex, const TMaybe<ui64>& rowIndex) override {
        Y_UNUSED(rangeIndex);
        Y_UNUSED(rowIndex);
        return false;
    }

    void ResetRetries() override {

    }

    bool HasRangeIndices() const override {
        return true;
    };

    size_t DoRead(void* buf, size_t len) override {
        if (!PayloadStream_.Exhausted()) {
            return PayloadStream_.Read(buf, len);
        }
        return 0;
    };

    virtual ~TFakeRPCReader() override {
    }
private:
    NYT::TSharedRef Payload_;
    TMemoryInput PayloadStream_;
};
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

struct TSettingsHolder {
    NYT::NApi::IConnectionPtr Connection;
    NYT::TIntrusivePtr<NYT::NApi::NRpcProxy::TClient> Client;
};

TParallelFileInputState::TParallelFileInputState(const TMkqlIOSpecs& spec,
    const NKikimr::NMiniKQL::THolderFactory& holderFactory,
    TVector<NYT::NConcurrency::IAsyncZeroCopyInputStreamPtr>&& rawInputs,
    size_t blockSize,
    size_t inflight,
    std::unique_ptr<TSettingsHolder>&& settings)
    : InnerState_(new TInnerState (rawInputs.size()))
    , StateByReader_(rawInputs.size())
    , Spec_(&spec)
    , HolderFactory_(holderFactory)
    , RawInputs_(std::move(rawInputs))
    , BlockSize_(blockSize)
    , Inflight_(inflight)
    , Settings_(std::move(settings))
    , TimerAwaiting_(RPCReaderAwaitingStallTime, 100)
{
#ifdef RPC_PRINT_TIME
    print_add(1);
#endif
    YQL_ENSURE(Spec_->Inputs.size() == RawInputs_.size());
    MkqlReader_.SetSpecs(*Spec_, HolderFactory_);
    Valid_ = NextValue();
}

size_t TParallelFileInputState::GetTableIndex() const {
    return CurrentInput_;
}

size_t TParallelFileInputState::GetRecordIndex() const {
    Y_FAIL("Not implemented");
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
                CurrentValue_ = HolderFactory_.CreateVariantHolder(CurrentValue_.Release(), Spec_->InputGroups.at(CurrentInput_));
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
        CurrentReader_ = MakeIntrusive<TFakeRPCReader>(std::move(result.Value_));
        MkqlReader_.SetReader(*CurrentReader_, 1, BlockSize_, ui32(CurrentInput_), true, StateByReader_[CurrentInput_].CurrentRow, StateByReader_[CurrentInput_].CurrentRange);
        MkqlReader_.Next();
    }
}

void TDqYtReadWrapperRPC::MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state) const {
    auto connectionConfig = NYT::New<NYT::NApi::NRpcProxy::TConnectionConfig>();
    connectionConfig->ClusterUrl = ClusterName;
    connectionConfig->DefaultTotalStreamingTimeout = TDuration::MilliSeconds(Timeout);
    auto connection = CreateConnection(connectionConfig);
    auto clientOptions = NYT::NApi::TClientOptions();

    if (Token) {
        clientOptions.Token = Token;
    }

    auto client = DynamicPointerCast<NYT::NApi::NRpcProxy::TClient>(connection->CreateClient(clientOptions));
    Y_VERIFY(client);
    auto apiServiceProxy = client->CreateApiServiceProxy();

    TVector<NYT::TFuture<NYT::NConcurrency::IAsyncZeroCopyInputStreamPtr>> waitFor;

    for (auto [richYPath, format]: Tables) {
        auto request = apiServiceProxy.ReadTable();
        client->InitStreamingRequest(*request);
        request->ClientAttachmentsStreamingParameters().ReadTimeout = TDuration::MilliSeconds(Timeout);

        TString ppath;
        auto tableYPath = ConvertYPathFromOld(richYPath);

        NYT::NYPath::ToProto(&ppath, tableYPath);
        request->set_path(ppath);
        request->set_desired_rowset_format(NYT::NApi::NRpcProxy::NProto::ERowsetFormat::RF_FORMAT);

        request->set_enable_table_index(true);
        request->set_enable_range_index(true);
        request->set_enable_row_index(true);
        request->set_unordered(Inflight > 1);

        // https://a.yandex-team.ru/arcadia/yt/yt_proto/yt/client/api/rpc_proxy/proto/api_service.proto?rev=r11519304#L2338
        if (!SamplingSpec.IsUndefined()) {
            TStringStream ss;
            SamplingSpec.Save(&ss);
            request->set_config(ss.Str());
        }

        if (richYPath.TransactionId_) {
            ui32* dw = richYPath.TransactionId_->dw;
            // P. S. No proper way to convert it
            request->mutable_transactional_options()->mutable_transaction_id()->set_first((ui64)dw[3] | (ui64(dw[2]) << 32));
            request->mutable_transactional_options()->mutable_transaction_id()->set_second((ui64)dw[1] | (ui64(dw[0]) << 32));
        }

        // Get skiff format yson string
        TStringStream fmt;
        format.Config.Save(&fmt);
        request->set_format(fmt.Str());

        waitFor.emplace_back(std::move(CreateRpcClientInputStream(std::move(request)).ApplyUnique(BIND([](NYT::NConcurrency::IAsyncZeroCopyInputStreamPtr&& stream) {
            // first packet contains meta, skip it
            return stream->Read().ApplyUnique(BIND([stream = std::move(stream)](NYT::TSharedRef&&) {
                return std::move(stream);
            }));
        }))));
    }

    TVector<NYT::NConcurrency::IAsyncZeroCopyInputStreamPtr> rawInputs;
    NYT::NConcurrency::WaitFor(NYT::AllSucceeded(waitFor)).ValueOrThrow().swap(rawInputs);
    state = ctx.HolderFactory.Create<TDqYtReadWrapperBase<TDqYtReadWrapperRPC, TParallelFileInputState>::TState>(Specs, ctx.HolderFactory, std::move(rawInputs), 4_MB, Inflight, std::make_unique<TSettingsHolder>(TSettingsHolder{std::move(connection), std::move(client)}));
}
}
