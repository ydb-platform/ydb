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

namespace {

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

public:
    explicit TArrowInputConverter(
        const TArrowInputSpec& inputSpec,
        IWorker* worker
    )
        : Worker_(worker)
        , Factory_(Worker_->GetGraph().GetHolderFactory())
    {
        Y_UNUSED(inputSpec);
    }

    void DoConvert(arrow::compute::ExecBatch* batch, TUnboxedValue& result) {
        ui64 nvalues = batch->num_values();
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
        THolder<IArrowIStream> underlying,
        IWorker* worker
    )
      : TCustomListValue(memInfo)
      , Underlying_(std::move(underlying))
      , Worker_(worker)
      , Converter_(inputSpec, Worker_)
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

} // namespace


TArrowInputSpec::TArrowInputSpec(const TVector<NYT::TNode>& schemas)
    : Schemas_(schemas)
{
}

const TVector<NYT::TNode>& TArrowInputSpec::GetSchemas() const {
    return Schemas_;
}


void TInputSpecTraits<TArrowInputSpec>::PreparePullListWorker(
    const TArrowInputSpec& inputSpec, IPullListWorker* worker,
    IArrowIStream* stream
) {
    with_lock(worker->GetScopedAlloc()) {
        worker->SetInput(worker->GetGraph().GetHolderFactory()
            .Create<TArrowListValue>(inputSpec, MakeHolder<TArrowIStreamImpl>(stream), worker), 0);
    }
}

void TInputSpecTraits<TArrowInputSpec>::PreparePullListWorker(
    const TArrowInputSpec& inputSpec, IPullListWorker* worker,
    THolder<IArrowIStream> stream
) {
    with_lock(worker->GetScopedAlloc()) {
        worker->SetInput(worker->GetGraph().GetHolderFactory()
            .Create<TArrowListValue>(inputSpec, std::move(stream), worker), 0);
    }
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
