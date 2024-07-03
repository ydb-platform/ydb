#include "spec.h"

#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_custom_list.h>
#include <ydb/library/yql/public/udf/arrow/udf_arrow_helpers.h>
#include <ydb/library/yql/utils/yql_panic.h>

using namespace NYql::NPureCalc;
using namespace NKikimr::NUdf;
using namespace NKikimr::NMiniKQL;

using IArrowIStream = typename TInputSpecTraits<TArrowInputSpec>::IInputStream;
using InputItemType = typename TInputSpecTraits<TArrowInputSpec>::TInputItemType;
using OutputItemType = typename TOutputSpecTraits<TArrowOutputSpec>::TOutputItemType;
using PullListReturnType = typename TOutputSpecTraits<TArrowOutputSpec>::TPullListReturnType;
using PullStreamReturnType = typename TOutputSpecTraits<TArrowOutputSpec>::TPullStreamReturnType;
using ConsumerType = typename TInputSpecTraits<TArrowInputSpec>::TConsumerType;

namespace {

template <typename T>
inline TVector<THolder<T>> VectorFromHolder(THolder<T> holder) {
    TVector<THolder<T>> result;
    result.push_back(std::move(holder));
    return result;
}


class TArrowIStreamImpl : public IArrowIStream {
private:
    IArrowIStream* Underlying_;
    // If we own Underlying_, than Owned_ == Underlying_;
    // otherwise Owned_ is nullptr.
    THolder<IArrowIStream> Owned_;

    TArrowIStreamImpl(IArrowIStream* underlying, THolder<IArrowIStream> owned)
        : Underlying_(underlying)
        , Owned_(std::move(owned))
    {
    }

public:
    TArrowIStreamImpl(THolder<IArrowIStream> stream)
        : TArrowIStreamImpl(stream.Get(), nullptr)
    {
        Owned_ = std::move(stream);
    }

    TArrowIStreamImpl(IArrowIStream* stream)
        : TArrowIStreamImpl(stream, nullptr)
    {
    }

    InputItemType Fetch() {
        return Underlying_->Fetch();
    }
};


/**
 * Converts input Datums to unboxed values.
 */
class TArrowInputConverter {
protected:
    IWorker* Worker_;
    const THolderFactory& Factory_;
    const NYT::TNode& Schema_;

public:
    explicit TArrowInputConverter(
        const TArrowInputSpec& inputSpec,
        ui32 index,
        IWorker* worker
    )
        : Worker_(worker)
        , Factory_(Worker_->GetGraph().GetHolderFactory())
        , Schema_(inputSpec.GetSchema(index))
    {
    }

    void DoConvert(arrow::compute::ExecBatch* batch, TUnboxedValue& result) {
        ui64 nvalues = Schema_.Size();
        Y_ENSURE(nvalues == static_cast<size_t>(batch->num_values()));

        TUnboxedValue* datums = nullptr;
        result = Factory_.CreateDirectArrayHolder(nvalues, datums);
        for (ui64 i = 0; i < nvalues; i++) {
            datums[i] = Factory_.CreateArrowBlock(std::move(batch->values[i]));
        }
    }
};


/**
 * Converts unboxed values to output Datums (single-output program case).
 */
class TArrowOutputConverter {
protected:
    IWorker* Worker_;
    const THolderFactory& Factory_;
    const NYT::TNode& Schema_;
    THolder<arrow::compute::ExecBatch> Batch_;

public:
    explicit TArrowOutputConverter(
        const TArrowOutputSpec& outputSpec,
        IWorker* worker
    )
        : Worker_(worker)
        , Factory_(worker->GetGraph().GetHolderFactory())
        , Schema_(outputSpec.GetSchema())
    {
        Batch_.Reset(new arrow::compute::ExecBatch);
    }

    OutputItemType DoConvert(TUnboxedValue value) {
        OutputItemType batch = Batch_.Get();
        ui64 nvalues = Schema_.Size();
        TVector<arrow::Datum> datums(nvalues);
        for (ui32 i = 0; i < nvalues; i++) {
            datums[i] = TArrowBlock::From(value.GetElement(i)).GetDatum();
        }
        *batch = ARROW_RESULT(arrow::compute::ExecBatch::Make(datums));
        return batch;
    }
};


/**
 * List (or, better, stream) of unboxed values.
 * Used as an input value in pull workers.
 */
class TArrowListValue final: public TCustomListValue {
private:
    mutable bool HasIterator_ = false;
    THolder<IArrowIStream> Underlying_;
    IWorker* Worker_;
    TArrowInputConverter Converter_;
    TScopedAlloc& ScopedAlloc_;

public:
    TArrowListValue(
        TMemoryUsageInfo* memInfo,
        const TArrowInputSpec& inputSpec,
        ui32 index,
        THolder<IArrowIStream> underlying,
        IWorker* worker
    )
      : TCustomListValue(memInfo)
      , Underlying_(std::move(underlying))
      , Worker_(worker)
      , Converter_(inputSpec, index, Worker_)
      , ScopedAlloc_(Worker_->GetScopedAlloc())
    {
    }

    ~TArrowListValue() override {
        {
            // This list value stored in the worker's computation graph and
            // destroyed upon the computation graph's destruction. This brings
            // us to an interesting situation: scoped alloc is acquired, worker
            // and computation graph are half-way destroyed, and now it's our
            // turn to die. The problem is, the underlying stream may own
            // another worker. This happens when chaining programs. Now, to
            // destroy that worker correctly, we need to release our scoped
            // alloc (because that worker has its own computation graph and
            // scoped alloc).
            // By the way, note that we shouldn't interact with the worker here
            // because worker is in the middle of its own destruction. So we're
            // using our own reference to the scoped alloc. That reference is
            // alive because scoped alloc destroyed after computation graph.
            auto unguard = Unguard(ScopedAlloc_);
            Underlying_.Destroy();
        }
    }

    TUnboxedValue GetListIterator() const override {
        YQL_ENSURE(!HasIterator_, "Only one pass over input is supported");
        HasIterator_ = true;
        return TUnboxedValuePod(const_cast<TArrowListValue*>(this));
    }

    bool Next(TUnboxedValue& result) override {
        arrow::compute::ExecBatch* batch;
        {
            auto unguard = Unguard(ScopedAlloc_);
            batch = Underlying_->Fetch();
        }

        if (!batch) {
            return false;
        }

        Converter_.DoConvert(batch, result);
        return true;
    }

    EFetchStatus Fetch(TUnboxedValue& result) override {
        if (Next(result)) {
            return EFetchStatus::Ok;
        } else {
            return EFetchStatus::Finish;
        }
    }
};


/**
 * Arrow input stream for unboxed value lists.
 */
class TArrowListImpl final: public IStream<OutputItemType> {
protected:
    TWorkerHolder<IPullListWorker> WorkerHolder_;
    TArrowOutputConverter Converter_;

public:
    explicit TArrowListImpl(
        const TArrowOutputSpec& outputSpec,
        TWorkerHolder<IPullListWorker> worker
    )
        : WorkerHolder_(std::move(worker))
        , Converter_(outputSpec, WorkerHolder_.Get())
    {
    }

    OutputItemType Fetch() override {
        TBindTerminator bind(WorkerHolder_->GetGraph().GetTerminator());

        with_lock(WorkerHolder_->GetScopedAlloc()) {
            TUnboxedValue value;

            if (!WorkerHolder_->GetOutputIterator().Next(value)) {
                return TOutputSpecTraits<TArrowOutputSpec>::StreamSentinel;
            }

            return Converter_.DoConvert(value);
        }
    }
};


/**
 * Arrow input stream for unboxed value streams.
 */
class TArrowStreamImpl final: public IStream<OutputItemType> {
protected:
    TWorkerHolder<IPullStreamWorker> WorkerHolder_;
    TArrowOutputConverter Converter_;

public:
    explicit TArrowStreamImpl(const TArrowOutputSpec& outputSpec, TWorkerHolder<IPullStreamWorker> worker)
        : WorkerHolder_(std::move(worker))
        , Converter_(outputSpec, WorkerHolder_.Get())
    {
    }

    OutputItemType Fetch() override {
        TBindTerminator bind(WorkerHolder_->GetGraph().GetTerminator());

        with_lock(WorkerHolder_->GetScopedAlloc()) {
            TUnboxedValue value;

            auto status = WorkerHolder_->GetOutput().Fetch(value);
            YQL_ENSURE(status != EFetchStatus::Yield, "Yield is not supported in pull mode");

            if (status == EFetchStatus::Finish) {
                return TOutputSpecTraits<TArrowOutputSpec>::StreamSentinel;
            }

            return Converter_.DoConvert(value);
        }
    }
};


/**
 * Consumer which converts Datums to unboxed values and relays them to the
 * worker. Used as a return value of the push processor's Process function.
 */
class TArrowConsumerImpl final: public IConsumer<arrow::compute::ExecBatch*> {
private:
    TWorkerHolder<IPushStreamWorker> WorkerHolder_;
    TArrowInputConverter Converter_;

public:
    explicit TArrowConsumerImpl(
        const TArrowInputSpec& inputSpec,
        TWorkerHolder<IPushStreamWorker> worker
    )
        : TArrowConsumerImpl(inputSpec, 0, std::move(worker))
    {
    }

    explicit TArrowConsumerImpl(
        const TArrowInputSpec& inputSpec,
        ui32 index,
        TWorkerHolder<IPushStreamWorker> worker
    )
        : WorkerHolder_(std::move(worker))
        , Converter_(inputSpec, index, WorkerHolder_.Get())
    {
    }

    void OnObject(arrow::compute::ExecBatch* batch) override {
        TBindTerminator bind(WorkerHolder_->GetGraph().GetTerminator());

        with_lock(WorkerHolder_->GetScopedAlloc()) {
            TUnboxedValue result;
            Converter_.DoConvert(batch, result);
            WorkerHolder_->Push(std::move(result));
        }
    }

    void OnFinish() override {
        TBindTerminator bind(WorkerHolder_->GetGraph().GetTerminator());

        with_lock(WorkerHolder_->GetScopedAlloc()) {
            WorkerHolder_->OnFinish();
        }
    }
};


/**
 * Push relay used to convert generated unboxed value to a Datum and push it to
 * the user's consumer.
 */
class TArrowPushRelayImpl: public IConsumer<const TUnboxedValue*> {
private:
    THolder<IConsumer<OutputItemType>> Underlying_;
    IWorker* Worker_;
    TArrowOutputConverter Converter_;

public:
    TArrowPushRelayImpl(
        const TArrowOutputSpec& outputSpec,
        IPushStreamWorker* worker,
        THolder<IConsumer<OutputItemType>> underlying
    )
        : Underlying_(std::move(underlying))
        , Worker_(worker)
        , Converter_(outputSpec, Worker_)
    {
    }

    // XXX: If you've read a comment in the TArrowListValue's destructor, you
    // may be wondering why don't we do the same trick here. Well, that's
    // because in push mode, consumer is destroyed before acquiring scoped alloc
    // and destroying computation graph.

    void OnObject(const TUnboxedValue* value) override {
        OutputItemType message = Converter_.DoConvert(*value);
        auto unguard = Unguard(Worker_->GetScopedAlloc());
        Underlying_->OnObject(message);
    }

    void OnFinish() override {
        auto unguard = Unguard(Worker_->GetScopedAlloc());
        Underlying_->OnFinish();
    }
};


template <typename TWorker>
void PrepareWorkerImpl(const TArrowInputSpec& inputSpec, TWorker* worker,
    TVector<THolder<TArrowIStreamImpl>>&& streams
) {
    YQL_ENSURE(worker->GetInputsCount() == streams.size(),
        "number of input streams should match number of inputs provided by spec");

    with_lock(worker->GetScopedAlloc()) {
        auto& holderFactory = worker->GetGraph().GetHolderFactory();
        for (ui32 i = 0; i < streams.size(); i++) {
            auto input = holderFactory.template Create<TArrowListValue>(
                inputSpec, i, std::move(streams[i]), worker);
            worker->SetInput(std::move(input), i);
        }
    }
}

} // namespace


TArrowInputSpec::TArrowInputSpec(const TVector<NYT::TNode>& schemas)
    : Schemas_(schemas)
{
}

const TVector<NYT::TNode>& TArrowInputSpec::GetSchemas() const {
    return Schemas_;
}

const NYT::TNode& TArrowInputSpec::GetSchema(ui32 index) const {
    return Schemas_[index];
}

void TInputSpecTraits<TArrowInputSpec>::PreparePullListWorker(
    const TArrowInputSpec& inputSpec, IPullListWorker* worker,
    IArrowIStream* stream
) {
    TInputSpecTraits<TArrowInputSpec>::PreparePullListWorker(
        inputSpec, worker, TVector<IArrowIStream*>({stream}));
}

void TInputSpecTraits<TArrowInputSpec>::PreparePullListWorker(
    const TArrowInputSpec& inputSpec, IPullListWorker* worker,
    const TVector<IArrowIStream*>& streams
) {
    TVector<THolder<TArrowIStreamImpl>> wrappers;
    for (ui32 i = 0; i < streams.size(); i++) {
        wrappers.push_back(MakeHolder<TArrowIStreamImpl>(streams[i]));
    }
    PrepareWorkerImpl(inputSpec, worker, std::move(wrappers));
}

void TInputSpecTraits<TArrowInputSpec>::PreparePullListWorker(
    const TArrowInputSpec& inputSpec, IPullListWorker* worker,
    THolder<IArrowIStream> stream
) {
    TInputSpecTraits<TArrowInputSpec>::PreparePullListWorker(inputSpec, worker,
        VectorFromHolder<IArrowIStream>(std::move(stream)));
}

void TInputSpecTraits<TArrowInputSpec>::PreparePullListWorker(
    const TArrowInputSpec& inputSpec, IPullListWorker* worker,
    TVector<THolder<IArrowIStream>>&& streams
) {
    TVector<THolder<TArrowIStreamImpl>> wrappers;
    for (ui32 i = 0; i < streams.size(); i++) {
        wrappers.push_back(MakeHolder<TArrowIStreamImpl>(std::move(streams[i])));
    }
    PrepareWorkerImpl(inputSpec, worker, std::move(wrappers));
}


void TInputSpecTraits<TArrowInputSpec>::PreparePullStreamWorker(
    const TArrowInputSpec& inputSpec, IPullStreamWorker* worker,
    IArrowIStream* stream
) {
    TInputSpecTraits<TArrowInputSpec>::PreparePullStreamWorker(
        inputSpec, worker, TVector<IArrowIStream*>({stream}));
}

void TInputSpecTraits<TArrowInputSpec>::PreparePullStreamWorker(
    const TArrowInputSpec& inputSpec, IPullStreamWorker* worker,
    const TVector<IArrowIStream*>& streams
) {
    TVector<THolder<TArrowIStreamImpl>> wrappers;
    for (ui32 i = 0; i < streams.size(); i++) {
        wrappers.push_back(MakeHolder<TArrowIStreamImpl>(streams[i]));
    }
    PrepareWorkerImpl(inputSpec, worker, std::move(wrappers));
}

void TInputSpecTraits<TArrowInputSpec>::PreparePullStreamWorker(
    const TArrowInputSpec& inputSpec, IPullStreamWorker* worker,
    THolder<IArrowIStream> stream
) {
    TInputSpecTraits<TArrowInputSpec>::PreparePullStreamWorker(
        inputSpec, worker, VectorFromHolder<IArrowIStream>(std::move(stream)));
}

void TInputSpecTraits<TArrowInputSpec>::PreparePullStreamWorker(
    const TArrowInputSpec& inputSpec, IPullStreamWorker* worker,
    TVector<THolder<IArrowIStream>>&& streams
) {
    TVector<THolder<TArrowIStreamImpl>> wrappers;
    for (ui32 i = 0; i < streams.size(); i++) {
        wrappers.push_back(MakeHolder<TArrowIStreamImpl>(std::move(streams[i])));
    }
    PrepareWorkerImpl(inputSpec, worker, std::move(wrappers));
}


ConsumerType TInputSpecTraits<TArrowInputSpec>::MakeConsumer(
    const TArrowInputSpec& inputSpec, TWorkerHolder<IPushStreamWorker> worker
) {
    return MakeHolder<TArrowConsumerImpl>(inputSpec, std::move(worker));
}


TArrowOutputSpec::TArrowOutputSpec(const NYT::TNode& schema)
    : Schema_(schema)
{
}

const NYT::TNode& TArrowOutputSpec::GetSchema() const {
    return Schema_;
}


PullListReturnType TOutputSpecTraits<TArrowOutputSpec>::ConvertPullListWorkerToOutputType(
    const TArrowOutputSpec& outputSpec, TWorkerHolder<IPullListWorker> worker
) {
    return MakeHolder<TArrowListImpl>(outputSpec, std::move(worker));
}

PullStreamReturnType TOutputSpecTraits<TArrowOutputSpec>::ConvertPullStreamWorkerToOutputType(
    const TArrowOutputSpec& outputSpec, TWorkerHolder<IPullStreamWorker> worker
) {
    return MakeHolder<TArrowStreamImpl>(outputSpec, std::move(worker));
}

void TOutputSpecTraits<TArrowOutputSpec>::SetConsumerToWorker(
    const TArrowOutputSpec& outputSpec, IPushStreamWorker* worker,
    THolder<IConsumer<TOutputItemType>> consumer
) {
    worker->SetConsumer(MakeHolder<TArrowPushRelayImpl>(outputSpec, worker, std::move(consumer)));
}
