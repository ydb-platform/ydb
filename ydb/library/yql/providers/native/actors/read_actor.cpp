#include <ydb/library/yql/providers/native/read_stream.h>
#include "callback_mailbox.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/scheduler_cookie.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/public/udf/arrow/memory_pool.h>
#include <yql/essentials/utils/yql_panic.h>

#include <arrow/api.h>

#include <algorithm>
#include <cstring>
#include <optional>

namespace NYql::NNative {
namespace {

using namespace NActors;
using namespace NDq;

std::shared_ptr<arrow::ArrayData> CopyToTaskAllocator(const std::shared_ptr<arrow::ArrayData>& input) {
    auto copy = input->Copy();
    for (auto& buffer : copy->buffers) {
        if (!buffer) {
            continue;
        }
        auto allocated = arrow::AllocateBuffer(buffer->size(), NUdf::GetYqlMemoryPool());
        YQL_ENSURE(allocated.ok(), "Native source could not allocate an output block");
        auto target = std::move(allocated).ValueOrDie();
        if (buffer->size()) {
            std::memcpy(target->mutable_data(), buffer->data(), buffer->size());
        }
        buffer = std::move(target);
    }
    for (auto& child : copy->child_data) {
        child = CopyToTaskAllocator(child);
    }
    if (copy->dictionary) {
        copy->dictionary = CopyToTaskAllocator(copy->dictionary);
    }
    return copy;
}

class TNativeReadActor final : public TActorBootstrapped<TNativeReadActor>, public IDqComputeActorAsyncInput {
    struct TEvRead : TEventLocal<TEvRead, EventSpaceBegin(TEvents::ES_PRIVATE)> {
        explicit TEvRead(TReadResult result) : Result(std::move(result)) {}
        TReadResult Result;
    };

public:
    TNativeReadActor(TReadStreamFactory factory, TReadActorSettings settings, IDqAsyncIoFactory::TSourceArguments&& args)
        : Factory_(std::move(factory))
        , Settings_(std::move(settings))
        , InputIndex_(args.InputIndex)
        , ComputeActorId_(args.ComputeActorId)
        , HolderFactory_(args.HolderFactory)
        , ValidationMode_(args.DatumValidationMode)
    {
        IngressStats_.Level = args.StatsLevel;
        auto names = Settings_.Columns;
        names.emplace_back(BlockLengthColumnName);
        std::sort(names.begin(), names.end());
        for (const auto& name : Settings_.Columns) {
            ColumnPositions_.push_back(std::lower_bound(names.begin(), names.end(), name) - names.begin());
        }
        LengthPosition_ = std::lower_bound(names.begin(), names.end(), TString(BlockLengthColumnName)) - names.begin();
    }

    void Bootstrap() {
        Become(&TNativeReadActor::StateFunc);
        Mailbox_ = std::make_shared<TCallbackMailbox>(TActivationContext::ActorSystem(), SelfId());
        Context_.Deadline = TActivationContext::Now() + Settings_.Timeout;
        Context_.MaxBatchBytes = Settings_.MaxBatchBytes;
        Context_.Cancellation = Cancellation_.Token();
        Context_.Cancellation.SetDeadline(Context_.Deadline);
        Initialized_ = true;
        if (Context_.Deadline <= TActivationContext::Now()) {
            Fail("Native source read deadline exceeded");
            return;
        }
        DeadlineTimer_.Reset(ISchedulerCookie::Make2Way());
        Schedule(Context_.Deadline, new TEvents::TEvWakeup(DeadlineTag), DeadlineTimer_.Get());
        Notify();
    }

    ~TNativeReadActor() override {
        if (Mailbox_) {
            Mailbox_->Detach();
        }
        CloseOperation();
    }

    static constexpr char ActorName[] = "NATIVE_READ_ACTOR";

    STRICT_STFUNC(StateFunc,
        hFunc(TEvRead, Handle);
        hFunc(TEvents::TEvWakeup, Handle);
        cFunc(TEvents::TEvPoison::EventType, PassAway);
    )

    i64 GetAsyncInputData(NKikimr::NMiniKQL::TUnboxedValueBatch& buffer, TMaybe<TInstant>&,
                         bool& finished, i64 freeSpace) override {
        finished = Finished_ && !Ready_;
        Demand_ = freeSpace > 0;
        // The CA may poll synchronously before our Bootstrap event.
        if (!Initialized_ || Stopping_ || Failed_ || !Demand_ || finished) {
            return 0;
        }
        if (TActivationContext::Now() >= Context_.Deadline) {
            Fail("Native source read deadline exceeded");
            return 0;
        }
        YQL_ENSURE(!buffer.IsWide(), "Native read expects a stream of block structs");
        ui64 bytes = 0;
        if (Ready_) {
            const auto& batch = *Ready_->Batch;
            NUdf::TUnboxedValue* items = nullptr;
            auto value = HolderFactory_.CreateDirectArrayHolder(Settings_.Columns.size() + 1, items);
            for (size_t i = 0; i < ColumnPositions_.size(); ++i) {
                // Output buffers belong to the task allocator so operators retaining
                // many batches remain subject to their memory quota. The
                // buffered SDK part is size checked, but has no RM reservation.
                items[ColumnPositions_[i]] = HolderFactory_.CreateArrowBlock(
                    arrow::Datum(arrow::MakeArray(CopyToTaskAllocator(batch.column_data(i)))), ValidationMode_);
            }
            items[LengthPosition_] = HolderFactory_.CreateArrowBlock(
                arrow::Datum(std::make_shared<arrow::UInt64Scalar>(batch.num_rows())), ValidationMode_);
            buffer.emplace_back(std::move(value));
            bytes = Ready_->Bytes;
            Delivered_ = Delivered_ || batch.num_rows() != 0;
            IngressStats_.Bytes += bytes;
            IngressStats_.Rows += batch.num_rows();
            ++IngressStats_.Chunks;
            Ready_.reset();
            Demand_ = freeSpace > static_cast<i64>(bytes);
        }
        Pull();
        return bytes;
    }

    void PassAway() override {
        Stopping_ = true;
        CloseOperation();
        // Future callbacks own only the shared mailbox; Detach prevents access
        // to the actor system after this actor is destroyed.
        TActorBootstrapped::PassAway();
    }

    void SaveState(const NDqProto::TCheckpoint&, TSourceState&) override {}
    void LoadState(const TSourceState&) override {}
    void CommitState(const NDqProto::TCheckpoint&) override {}
    ui64 GetInputIndex() const override { return InputIndex_; }
    const TDqAsyncStats& GetIngressStats() const override { return IngressStats_; }

private:
    void CloseStream() {
        if (Stream_) {
            Stream_->Cancel();
            Stream_.reset();
        }
    }

    void CloseOperation() {
        if (IngressStats_.CurrentPauseTs) {
            IngressStats_.Resume();
        }
        Cancellation_.Cancel();
        DeadlineTimer_.Detach();
        RetryTimer_.Detach();
        Ready_.reset();
        CloseStream();
        Factory_ = {};
    }

    void Notify() {
        Send(ComputeActorId_, new TEvNewAsyncInputDataArrived(InputIndex_));
    }

    void Fail(const TString& message) {
        if (Failed_ || Stopping_) {
            return;
        }
        Failed_ = true;
        CloseOperation();
        Send(ComputeActorId_, new TEvAsyncInputError(InputIndex_, TIssues{TIssue(message)}, NDqProto::StatusIds::EXTERNAL_ERROR));
    }

    // At most one outstanding Next and one ready batch. SDK parsing and the
    // buffered input part are not reserved against the resource manager in this stage.
    void Pull() {
        if (!Demand_ || InFlight_ || Ready_ || RetryPending_ || Finished_ || Failed_ || Stopping_) {
            return;
        }
        if (TActivationContext::Now() >= Context_.Deadline) {
            Fail("Native source read deadline exceeded");
            return;
        }
        try {
            if (!Stream_) {
                Stream_ = Factory_(Context_);
            }
            InFlight_ = true;
            IngressStats_.TryPause();
            Stream_->Next().Subscribe([mailbox = Mailbox_](const auto& future) {
                TReadResult result;
                try {
                    result = future.GetValue();
                } catch (...) {
                    result.Error = "Native source read failed unexpectedly";
                }
                mailbox->Send(new TEvRead(std::move(result)));
            });
        } catch (...) {
            InFlight_ = false;
            Fail("Native source could not start the read");
        }
    }

    void Handle(TEvRead::TPtr& ev) {
        InFlight_ = false;
        if (Failed_ || Stopping_) {
            return;
        }
        IngressStats_.Resume();
        auto result = std::move(ev->Get()->Result);
        if (result.Error) {
            Ready_.reset();
            CloseStream();
            // A retry opens a new snapshot and is allowed only before any row was
            // delivered. Late SDK callbacks may still be releasing the old stream;
            // they cannot complete its Next twice or access this actor directly.
            if (result.Retryable && !Delivered_ && Retries_ < Settings_.MaxRetries &&
                TActivationContext::Now() < Context_.Deadline) {
                ++Retries_;
                RetryPending_ = true;
                RetryTimer_.Reset(ISchedulerCookie::Make2Way());
                Schedule(TDuration::MilliSeconds(100 * (1u << Retries_)), new TEvents::TEvWakeup(RetryTag), RetryTimer_.Get());
                return;
            }
            Fail(result.Error);
            return;
        }
        if (result.Batch && result.Batch->num_rows()) {
            const auto limit = result.Batch->num_rows() == 1
                ? std::max(Settings_.MaxBatchBytes, Settings_.MaxRowBytes) : Settings_.MaxBatchBytes;
            if (result.Bytes > limit || result.Batch->num_columns() != static_cast<int>(Settings_.Columns.size())) {
                Fail("Native source returned a batch exceeding its contract");
                return;
            }
            Ready_ = std::move(result);
            Notify();
        } else if (result.Finished) {
            Finished_ = true;
            CloseOperation();
            Notify();
        } else {
            Pull();
        }
    }

    void Handle(TEvents::TEvWakeup::TPtr& ev) {
        if (Stopping_ || Failed_ || Finished_) {
            return;
        }
        if (ev->Get()->Tag == DeadlineTag) {
            DeadlineTimer_.Detach();
            Fail("Native source read deadline exceeded");
        } else {
            RetryTimer_.Detach();
            RetryPending_ = false;
            Pull();
        }
    }

    static constexpr ui64 DeadlineTag = 1;
    static constexpr ui64 RetryTag = 2;
    TReadStreamFactory Factory_;
    const TReadActorSettings Settings_;
    TReadContext Context_;
    NThreading::TCancellationTokenSource Cancellation_;
    const ui64 InputIndex_;
    const TActorId ComputeActorId_;
    const NKikimr::NMiniKQL::THolderFactory& HolderFactory_;
    const EDatumValidationMode ValidationMode_;
    TVector<size_t> ColumnPositions_;
    size_t LengthPosition_ = 0;
    TDqAsyncStats IngressStats_;
    std::shared_ptr<TCallbackMailbox> Mailbox_;
    std::shared_ptr<IReadStream> Stream_;
    std::optional<TReadResult> Ready_;
    TSchedulerCookieHolder DeadlineTimer_;
    TSchedulerCookieHolder RetryTimer_;
    ui32 Retries_ = 0;
    bool Initialized_ = false;
    bool InFlight_ = false;
    bool Demand_ = false;
    bool Delivered_ = false;
    bool RetryPending_ = false;
    bool Finished_ = false;
    bool Failed_ = false;
    bool Stopping_ = false;
};

} // namespace

std::pair<NDq::IDqComputeActorAsyncInput*, NActors::IActor*> CreateNativeReadActor(
    TReadStreamFactory factory, TReadActorSettings settings, NDq::IDqAsyncIoFactory::TSourceArguments&& args) {
    auto* actor = new TNativeReadActor(std::move(factory), std::move(settings), std::move(args));
    return {actor, actor};
}

} // namespace NYql::NNative
