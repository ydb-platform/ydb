#include "purecalc.h"

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_custom_list.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/utils/yql_panic.h>

namespace NKikimr::NReplication::NTransfer {

namespace {

using namespace NYql::NUdf;
using namespace NKikimr::NMiniKQL;

constexpr const char* AttributesFieldName = "_attributes";
constexpr const char* CreateTimestampFieldName = "_create_timestamp";
constexpr const char* DataFieldName = "_data";
constexpr const char* KeyFieldName = "_key";
constexpr const char* MessageGroupIdFieldName = "_message_group_id";
constexpr const char* OffsetFieldName = "_offset";
constexpr const char* PartitionFieldName = "_partition";
constexpr const char* ProducerIdFieldName = "_producer_id";
constexpr const char* SeqNoFieldName = "_seq_no";
constexpr const char* WriteTimestampFieldName = "_write_timestamp";

constexpr const size_t FieldCount = 10; // Change it when change fields


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

void AddOptionalField(NYT::TNode& node, const TString& fieldName, const TString& fieldType) {
    node.Add(
        NYT::TNode::CreateList()
            .Add(fieldName)
            .Add(NYT::TNode::CreateList()
                .Add("OptionalType")
                .Add(CreateTypeNode(fieldType))
            )
    );
}

void AddAttributeField(NYT::TNode& node) {
    node.Add(
        NYT::TNode::CreateList()
            .Add(AttributesFieldName)
            .Add(NYT::TNode::CreateList()
                .Add("DictType")
                .Add(CreateTypeNode("String"))
                .Add(CreateTypeNode("String"))
            )
    );
}


NYT::TNode CreateMessageScheme() {
    auto structMembers = NYT::TNode::CreateList();

    AddAttributeField(structMembers);
    AddField(structMembers, CreateTimestampFieldName, "Timestamp");
    AddField(structMembers, DataFieldName, "String");
    AddOptionalField(structMembers, KeyFieldName, "String");
    AddField(structMembers, MessageGroupIdFieldName, "String");
    AddField(structMembers, OffsetFieldName, "Uint64");
    AddField(structMembers, PartitionFieldName, "Uint32");
    AddField(structMembers, ProducerIdFieldName, "String");
    AddField(structMembers, SeqNoFieldName, "Uint64");
    AddField(structMembers, WriteTimestampFieldName, "Timestamp");

    return NYT::TNode::CreateList()
        .Add("StructType")
        .Add(std::move(structMembers));
}

static const TVector<NYT::TNode> InputSchema{ CreateMessageScheme() };

struct TMessageWrapper {
    const TMessage& Message;

    NYql::NUdf::TUnboxedValuePod GetAttributes(const THolderFactory& nodeFactory, const TTypeEnvironment& typeEnv) const {
        auto type = TDictType::Create(
                TDataType::Create(NUdf::TDataType<char*>::Id, typeEnv),
                TDataType::Create(NUdf::TDataType<char*>::Id, typeEnv),
                typeEnv
            );

        TKeyTypes keyTypes;
        bool isTuple;
        bool encoded;
        bool useIHash;
        GetDictionaryKeyTypes(type->GetKeyType(), keyTypes, isTuple, encoded, useIHash);

        return nodeFactory.CreateDirectHashedDictHolder([&](TValuesDictHashMap& map) {
                const auto& m = Message.Message.GetMessageMeta();
                if (m) {
                    for (auto& [k, v] : m->Fields) {
                        map.emplace(NKikimr::NMiniKQL::MakeString(k), NKikimr::NMiniKQL::MakeString(v));
                    }
                }
            },
            keyTypes, false, true, nullptr, nullptr, nullptr);
    }

    NYql::NUdf::TUnboxedValuePod GetCreateTimestamp() const {
        return NYql::NUdf::TUnboxedValuePod(Message.Message.GetCreateTime().MicroSeconds());
    }

    NYql::NUdf::TUnboxedValuePod GetData() const {
        return NKikimr::NMiniKQL::MakeString(Message.Message.GetData());
    }

    NYql::NUdf::TUnboxedValuePod GetKey() const {
        const auto& m = Message.Message.GetMessageMeta();
        if (m) {
            const auto* result = FindIf(m->Fields.begin(), m->Fields.end(), [](const auto& v) {
                return v.first == "__key";
            });

            if (result != m->Fields.end()) {
                return NKikimr::NMiniKQL::MakeString(result->second).MakeOptional();
            }
        }

        return NYql::NUdf::TUnboxedValuePod();
    }

    NYql::NUdf::TUnboxedValuePod GetMessageGroupId() const {
        return NKikimr::NMiniKQL::MakeString(Message.Message.GetMessageGroupId());
    }

    NYql::NUdf::TUnboxedValuePod GetOffset() const {
        return NYql::NUdf::TUnboxedValuePod(Message.Message.GetOffset());
    }

    NYql::NUdf::TUnboxedValuePod GetPartition() const {
        return NYql::NUdf::TUnboxedValuePod(Message.PartitionId);
    }

    NYql::NUdf::TUnboxedValuePod GetProducerId() const {
        return NKikimr::NMiniKQL::MakeString(Message.Message.GetProducerId());
    }

    NYql::NUdf::TUnboxedValuePod GetSeqNo() const {
        return NYql::NUdf::TUnboxedValuePod(Message.Message.GetSeqNo());
    }

    NYql::NUdf::TUnboxedValuePod GetWriteTimestamp() const {
        return NYql::NUdf::TUnboxedValuePod(Message.Message.GetWriteTime().MicroSeconds());
    }
};

class TInputConverter {
protected:
    IWorker* Worker_;
    TPlainContainerCache Cache_;

public:
    explicit TInputConverter(IWorker* worker)
        : Worker_(worker)
    {
    }

public:
    void DoConvert(const TMessage* message, TUnboxedValue& result) {
        auto& holderFactory = Worker_->GetGraph().GetHolderFactory();
        auto& typeEnv = Worker_->GetGraph().GetContext().TypeEnv;
        TUnboxedValue* items = nullptr;
        result = Cache_.NewArray(holderFactory, static_cast<ui32>(FieldCount), items);

        TMessageWrapper wrap {*message};
        // lex order by field name
        items[0] = wrap.GetAttributes(holderFactory, typeEnv);
        items[1] = wrap.GetCreateTimestamp();
        items[2] = wrap.GetData();
        items[3] = wrap.GetKey();
        items[4] = wrap.GetMessageGroupId();
        items[5] = wrap.GetOffset();
        items[6] = wrap.GetPartition();
        items[7] = wrap.GetProducerId();
        items[8] = wrap.GetSeqNo();
        items[9] = wrap.GetWriteTimestamp();
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

} // namespace NKikimr::NReplication::NTransfer

namespace NYql::NPureCalc {

using namespace NKikimr::NReplication::NTransfer;

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
