#include "purecalc.h"

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_custom_list.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/utils/yql_panic.h>

namespace NYdb::NTopic::NPurecalc {

namespace {

using namespace NYql::NUdf;
using namespace NKikimr::NMiniKQL;

constexpr const char* DataFieldName = "_data";
constexpr const char* OffsetFieldName = "_offset";

constexpr const size_t FieldCount = 2; // Change it when change fields

struct FieldPositions {
    ui64 Data = 0;
    ui64 Offset = 0;
};


NYT::TNode CreateTypeNode(const TString& fieldType) {
    return NYT::TNode::CreateList()
        .Add("DataType")
        .Add(fieldType);
}

void AddField(NYT::TNode& node, const TString& fieldName, const TString& fieldType) {
    node.Add(
        NYT::TNode::CreateList()
            .Add(fieldName)
            .Add(CreateTypeNode(fieldType))
    );
}


NYT::TNode CreateMessageScheme() {
    auto structMembers = NYT::TNode::CreateList();
    AddField(structMembers, DataFieldName, "String");
    AddField(structMembers, OffsetFieldName, "Uint64");

    return NYT::TNode::CreateList()
        .Add("StructType")
        .Add(std::move(structMembers));
}

static const TVector<NYT::TNode> InputSchema{ CreateMessageScheme() };

struct TMessageWrapper {
    const TMessage& Message;

    NYql::NUdf::TUnboxedValuePod GetData() const {
        return NKikimr::NMiniKQL::MakeString(Message.Data);
    }

    NYql::NUdf::TUnboxedValuePod GetOffset() const {
        return NYql::NUdf::TUnboxedValuePod(Message.Offset);
    }
};

class TInputConverter {
protected:
    IWorker* Worker_;
    TPlainContainerCache Cache_;
    FieldPositions Position;

public:
    explicit TInputConverter(IWorker* worker)
        : Worker_(worker)
    {
        const TStructType* structType = worker->GetInputType();
        const ui64 count = structType->GetMembersCount();
 
        for (ui64 i = 0; i < count; ++i) { 
            const auto name = structType->GetMemberName(i);
            if (name == DataFieldName) {
                Position.Data = i;
            } else if (name == OffsetFieldName) {
                Position.Offset = i;
            }
        }
    }

public:
    void DoConvert(const TMessage* message, TUnboxedValue& result) {
        auto& holderFactory = Worker_->GetGraph().GetHolderFactory();
        TUnboxedValue* items = nullptr;
        result = Cache_.NewArray(holderFactory, static_cast<ui32>(FieldCount), items);

        TMessageWrapper wrap {*message};
        items[Position.Data] = wrap.GetData();
        items[Position.Offset] = wrap.GetOffset();
    }

    void ClearCache() {
        Cache_.Clear();
    }
};

/**
 * List (or, better, stream) of unboxed values. Used as an input value in pull workers.
 */
class TMessageListValue final: public TCustomListValue {
private:
    mutable bool HasIterator_ = false;
    THolder<IStream<TMessage*>> Underlying_;
    TInputConverter Converter;
    IWorker* Worker_;
    TScopedAlloc& ScopedAlloc_;

public:
    TMessageListValue(
        TMemoryUsageInfo* memInfo,
        const TMessageInputSpec& /*inputSpec*/,
        THolder<IStream<TMessage*>> underlying,
        IWorker* worker
    )
        : TCustomListValue(memInfo)
        , Underlying_(std::move(underlying))
        , Converter(worker)
        , Worker_(worker)
        , ScopedAlloc_(Worker_->GetScopedAlloc())
    {
    }

    ~TMessageListValue() override {
        {
            // This list value stored in the worker's computation graph and destroyed upon the computation
            // graph's destruction. This brings us to an interesting situation: scoped alloc is acquired,
            // worker and computation graph are half-way destroyed, and now it's our turn to die. The problem is,
            // the underlying stream may own another worker. This happens when chaining programs. Now, to destroy
            // that worker correctly, we need to release our scoped alloc (because that worker has its own
            // computation graph and scoped alloc).
            // By the way, note that we shouldn't interact with the worker here because worker is in the middle of
            // its own destruction. So we're using our own reference to the scoped alloc. That reference is alive
            // because scoped alloc destroyed after computation graph.
            auto unguard = Unguard(ScopedAlloc_);
            Underlying_.Destroy();
        }
    }

public:
    TUnboxedValue GetListIterator() const override {
        YQL_ENSURE(!HasIterator_, "Only one pass over input is supported");
        HasIterator_ = true;
        return TUnboxedValuePod(const_cast<TMessageListValue*>(this));
    }

    bool Next(TUnboxedValue& result) override {
        const TMessage* message;
        {
            auto unguard = Unguard(ScopedAlloc_);
            message = Underlying_->Fetch();
        }

        if (!message) {
            return false;
        }

        Converter.DoConvert(message, result);

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

class TMessageConsumerImpl final: public IConsumer<TMessage*> {
private:
    TWorkerHolder<IPushStreamWorker> WorkerHolder;
    TInputConverter Converter;

public:
    TMessageConsumerImpl(
        const TMessageInputSpec& /*inputSpec*/,
        TWorkerHolder<IPushStreamWorker> worker
    )
        : WorkerHolder(std::move(worker))
        , Converter(WorkerHolder.Get())
    {
    }

    ~TMessageConsumerImpl() override {
        with_lock(WorkerHolder->GetScopedAlloc()) {
            Converter.ClearCache();
        }
    }

public:
    void OnObject(TMessage* message) override {
        TBindTerminator bind(WorkerHolder->GetGraph().GetTerminator());

        with_lock(WorkerHolder->GetScopedAlloc()) {
            Y_DEFER {
                // Clear cache after each object because
                // values allocated on another allocator and should be released
                Converter.ClearCache();
                WorkerHolder->Invalidate();
            };

            TUnboxedValue result;
            Converter.DoConvert(message, result);
            WorkerHolder->Push(std::move(result));
        }
    }

    void OnFinish() override {
        TBindTerminator bind(WorkerHolder->GetGraph().GetTerminator());

        with_lock(WorkerHolder->GetScopedAlloc()) {
            WorkerHolder->OnFinish();
        }
    }
};

} // namespace

const TVector<NYT::TNode>& TMessageInputSpec::GetSchemas() const {
    return InputSchema;
}

} // namespace NYdb::NTopic::NPurecalc

namespace NYql::NPureCalc {

using namespace NYdb::NTopic::NPurecalc;

using ConsumerType = TInputSpecTraits<TMessageInputSpec>::TConsumerType;

void TInputSpecTraits<TMessageInputSpec>::PreparePullStreamWorker(
    const TMessageInputSpec& inputSpec,
    IPullStreamWorker* worker,
    THolder<IStream<TMessage*>> stream
) {
    with_lock(worker->GetScopedAlloc()) {
        worker->SetInput(
            worker->GetGraph().GetHolderFactory().Create<TMessageListValue>(inputSpec, std::move(stream), worker), 0);
    }
}

void TInputSpecTraits<TMessageInputSpec>::PreparePullListWorker(
    const TMessageInputSpec& inputSpec,
    IPullListWorker* worker,
    THolder<IStream<TMessage*>> stream
) {
    with_lock(worker->GetScopedAlloc()) {
        worker->SetInput(
            worker->GetGraph().GetHolderFactory().Create<TMessageListValue>(inputSpec, std::move(stream), worker), 0);
    }
}

ConsumerType TInputSpecTraits<TMessageInputSpec>::MakeConsumer(
    const TMessageInputSpec& inputSpec,
    TWorkerHolder<IPushStreamWorker> worker
) {
    return MakeHolder<TMessageConsumerImpl>(inputSpec, std::move(worker));
}

} // namespace NYql::NPureCalc
