#include "mkql_bridge.h"

#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_node_serialization.h>
#include <yql/essentials/minikql/mkql_terminator.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/minikql/mkql_utils.h>
#include <yql/essentials/minikql/runtime_settings/runtime_settings.h>

#include <library/cpp/containers/stack_vector/stack_vec.h>

#include <util/generic/yexception.h>
#include <util/string/builder.h>
#include <util/stream/output.h>

namespace NKikimr::NMiniKQL {

namespace {

template <typename TDerived>
class TBridgeProxyNode: public TComputationValue<TDerived> {
public:
    TBridgeProxyNode(TMemoryUsageInfo* memInfo, TIntrusivePtr<TBridgeChannel> channel, TBridgeNodeId nodeId)
        : TComputationValue<TDerived>(memInfo)
        , Channel_(std::move(channel))
        , NodeId_(nodeId)
    {
    }

    ~TBridgeProxyNode() override {
        try {
            Channel_->UnrefNodeRemote(NodeId_);
        } catch (...) {
            Cerr << "Bridge: failed to unref remote node " << NodeId_ << " during cleanup: " << CurrentExceptionMessage() << Endl;
        }
    }

protected:
    const TIntrusivePtr<TBridgeChannel> Channel_;
    const TBridgeNodeId NodeId_;
};

template <typename TDerived>
class TBridgeProxyOutgoingNode: public TBridgeProxyNode<TDerived> {
public:
    TBridgeProxyOutgoingNode(TMemoryUsageInfo* memInfo, TIntrusivePtr<TBridgeChannel> channel, TBridgeNamespaceId ownerNamespace, TBridgeNodeId nodeId)
        : TBridgeProxyNode<TDerived>(memInfo, std::move(channel), nodeId)
    {
        this->Channel_->RegisterOutgoingProxy(this, ownerNamespace, this->NodeId_);
    }

    ~TBridgeProxyOutgoingNode() override {
        this->Channel_->UnregisterOutgoingProxy(this);
    }
};

class TBridgeProxyListIterator: public TBridgeProxyNode<TBridgeProxyListIterator> {
    using TBase = TBridgeProxyNode<TBridgeProxyListIterator>;

public:
    TBridgeProxyListIterator(TMemoryUsageInfo* memInfo, TIntrusivePtr<TBridgeChannel> channel, TBridgeNodeId nodeId, const TType* itemType)
        : TBase(memInfo, std::move(channel), nodeId)
        , ItemType_(itemType)
    {
    }

private:
    bool Next(NUdf::TUnboxedValue& value) final {
        return Channel_->NextIteratorRemote(NodeId_, ItemType_, value);
    }

    bool Skip() final {
        NUdf::TUnboxedValue dummy;
        return Channel_->NextIteratorRemote(NodeId_, ItemType_, dummy);
    }

    const TType* const ItemType_;
};

class TBridgeProxyList: public TBridgeProxyOutgoingNode<TBridgeProxyList> {
    using TBase = TBridgeProxyOutgoingNode<TBridgeProxyList>;

public:
    TBridgeProxyList(TMemoryUsageInfo* memInfo, TIntrusivePtr<TBridgeChannel> channel, TBridgeNamespaceId ownerNamespace, TBridgeNodeId nodeId, const TType* itemType)
        : TBase(memInfo, std::move(channel), ownerNamespace, nodeId)
        , ItemType_(itemType)
    {
    }

private:
    // Not "fast" in the intended sense: GetListLength() below is a real wire
    // round trip, not a free O(1) read.
    bool HasFastListLength() const final {
        return false;
    }

    const NUdf::TUnboxedValue* GetElements() const final {
        return nullptr;
    }

    NUdf::TUnboxedValue GetListIterator() const final {
        const auto iterNodeId = Channel_->MakeIteratorRemote(NodeId_);
        return NUdf::TUnboxedValuePod(new TBridgeProxyListIterator(GetMemInfo(), Channel_, iterNodeId, ItemType_));
    }

    ui64 GetListLength() const final {
        return Channel_->ListLengthRemote(NodeId_);
    }

    ui64 GetEstimatedListLength() const final {
        return Channel_->ListEstimatedLengthRemote(NodeId_);
    }

    bool HasListItems() const final {
        return Channel_->ListHasItemsRemote(NodeId_);
    }

    const TType* const ItemType_;
};

class TBridgeProxyDictIterator: public TBridgeProxyNode<TBridgeProxyDictIterator> {
    using TBase = TBridgeProxyNode<TBridgeProxyDictIterator>;

public:
    TBridgeProxyDictIterator(TMemoryUsageInfo* memInfo, TIntrusivePtr<TBridgeChannel> channel, TBridgeNodeId nodeId, const TType* keyType, const TType* payloadType)
        : TBase(memInfo, std::move(channel), nodeId)
        , KeyType_(keyType)
        , PayloadType_(payloadType)
    {
    }

private:
    bool NextPair(NUdf::TUnboxedValue& key, NUdf::TUnboxedValue& payload) final {
        return Channel_->NextDictIteratorRemote(NodeId_, KeyType_, PayloadType_, key, payload);
    }

    bool Skip() final {
        NUdf::TUnboxedValue key;
        NUdf::TUnboxedValue payload;
        return Channel_->NextDictIteratorRemote(NodeId_, KeyType_, PayloadType_, key, payload);
    }

    const TType* const KeyType_;
    const TType* const PayloadType_;
};

class TBridgeProxyDict: public TBridgeProxyOutgoingNode<TBridgeProxyDict> {
    using TBase = TBridgeProxyOutgoingNode<TBridgeProxyDict>;

public:
    TBridgeProxyDict(TMemoryUsageInfo* memInfo, TIntrusivePtr<TBridgeChannel> channel, TBridgeNamespaceId ownerNamespace, TBridgeNodeId nodeId, const TType* keyType, const TType* payloadType)
        : TBase(memInfo, std::move(channel), ownerNamespace, nodeId)
        , KeyType_(keyType)
        , PayloadType_(payloadType)
    {
    }

private:
    NUdf::TUnboxedValue GetDictIterator() const final {
        const auto iterNodeId = Channel_->MakeDictIteratorRemote(NodeId_);
        return NUdf::TUnboxedValuePod(new TBridgeProxyDictIterator(GetMemInfo(), Channel_, iterNodeId, KeyType_, PayloadType_));
    }

    NUdf::TUnboxedValue GetKeysIterator() const final {
        const auto iterNodeId = Channel_->MakeKeysIteratorRemote(NodeId_);
        return NUdf::TUnboxedValuePod(new TBridgeProxyListIterator(GetMemInfo(), Channel_, iterNodeId, KeyType_));
    }

    NUdf::TUnboxedValue GetPayloadsIterator() const final {
        const auto iterNodeId = Channel_->MakePayloadsIteratorRemote(NodeId_);
        return NUdf::TUnboxedValuePod(new TBridgeProxyListIterator(GetMemInfo(), Channel_, iterNodeId, PayloadType_));
    }

    ui64 GetDictLength() const final {
        return Channel_->DictLengthRemote(NodeId_);
    }

    bool HasDictItems() const final {
        return Channel_->DictHasItemsRemote(NodeId_);
    }

    bool Contains(const NUdf::TUnboxedValuePod& key) const final {
        return Channel_->DictContainsRemote(NodeId_, KeyType_, key);
    }

    NUdf::TUnboxedValue Lookup(const NUdf::TUnboxedValuePod& key) const final {
        return Channel_->DictLookupRemote(NodeId_, KeyType_, PayloadType_, key);
    }

    const TType* const KeyType_;
    const TType* const PayloadType_;
};

class TBridgeProxyResource: public TBridgeProxyOutgoingNode<TBridgeProxyResource> {
    using TBase = TBridgeProxyOutgoingNode<TBridgeProxyResource>;

public:
    TBridgeProxyResource(TMemoryUsageInfo* memInfo, TIntrusivePtr<TBridgeChannel> channel, TBridgeNamespaceId ownerNamespace, TBridgeNodeId nodeId, const TResourceType* type)
        : TBase(memInfo, std::move(channel), ownerNamespace, nodeId)
        , Type_(type)
    {
    }

private:
    NUdf::TStringRef GetResourceTag() const final {
        const auto tag = Type_->GetTag();
        return NUdf::TStringRef(tag.data(), static_cast<ui32>(tag.size()));
    }

    void* GetResource() final {
        ythrow TBridgeException() << "Bridge: cannot dereference resource '" << Type_->GetTag()
                                  << "' across the bridge boundary, its raw pointer is only valid on the owning side";
    }

    const TResourceType* const Type_;
};

class TBridgeProxyCallable: public TBridgeProxyOutgoingNode<TBridgeProxyCallable> {
    using TBase = TBridgeProxyOutgoingNode<TBridgeProxyCallable>;

public:
    TBridgeProxyCallable(TMemoryUsageInfo* memInfo, TIntrusivePtr<TBridgeChannel> channel, TBridgeNamespaceId ownerNamespace, TBridgeNodeId nodeId, const TCallableType* funcType)
        : TBase(memInfo, std::move(channel), ownerNamespace, nodeId)
        , FuncType_(funcType)
    {
    }

private:
    NUdf::TUnboxedValue Run(const NUdf::IValueBuilder* /*valueBuilder*/, const NUdf::TUnboxedValuePod* args) const final {
        return Channel_->RunRemote(NodeId_, FuncType_, args);
    }

    const TCallableType* const FuncType_;
};

} // namespace

TBridgeChannel::TBridgeChannel(IInputStream& in, IOutputStream& out, const THolderFactory& holderFactory, const NUdf::IValueBuilder* valueBuilder,
                               TBridgeNamespaceId ownNamespace, TBridgeNamespaceId peerNamespace,
                               const IFunctionRegistry* workerFunctionRegistry, const TTypeEnvironment* workerEnv,
                               NYql::TRuntimeSettings::TConstPtr workerRuntimeSettings)
    : In_(in)
    , Out_(out)
    , HolderFactory_(holderFactory)
    , ValueBuilder_(valueBuilder)
    , OwnNamespace_(ownNamespace)
    , PeerNamespace_(peerNamespace)
    , WorkerFunctionRegistry_(workerFunctionRegistry)
    , WorkerEnv_(workerEnv)
    , WorkerRuntimeSettings_(std::move(workerRuntimeSettings))
{
}

TBridgeChannel::~TBridgeChannel() = default;

NUdf::TUnboxedValue TBridgeChannel::ResolveFunction(const TBridgeUdfSpec& spec, const TCallableType* funcType) {
    if (ModuleName_.empty()) {
        ModuleName_ = TString(ModuleName(spec.FunctionName));
    }
    WriteRequestHeader(Out_, EBridgeCommand::ResolveFunction);
    WriteBytes(Out_, spec.FunctionName);
    WriteBytes(Out_, spec.TypeConfig);
    WriteBytes(Out_, spec.SerializedUserType);
    WriteUi32(Out_, spec.LangVer);
    Out_.Flush();
    WaitForResponse();
    const auto nodeId = ReadNodeId(In_);
    return NUdf::TUnboxedValuePod(new TBridgeProxyCallable(&HolderFactory_.GetMemInfo(), this, PeerNamespace_, nodeId, funcType));
}

TString TBridgeChannel::WithModule(TStringBuf message) const {
    if (ModuleName_.empty()) {
        return TString(message);
    }
    return TStringBuilder() << message << " (module: " << ModuleName_ << ")";
}

void TBridgeChannel::ServeForever() {
    const TOnlyThrowingBindTerminator terminator;
    for (;;) {
        EBridgeFrameKind kind;
        try {
            kind = ReadFrameHeader(In_);
        } catch (...) {
            // The peer closed its end of the pipe/process -- normal shutdown.
            return;
        }
        if (kind != EBridgeFrameKind::Request) {
            ythrow TBridgeException() << "Bridge: worker expected a Request frame";
        }
        ServeOneRequest();
    }
}

void TBridgeChannel::WaitForResponse() {
    for (;;) {
        EBridgeFrameKind kind;
        try {
            kind = ReadFrameHeader(In_);
        } catch (...) {
            MKQLTerminate(WithModule("Bridge: worker process died unexpectedly").c_str());
        }
        if (kind == EBridgeFrameKind::Response) {
            return;
        }
        if (kind == EBridgeFrameKind::Error) {
            ythrow TBridgeException() << ReadErrorMessage(In_);
        }
        if (kind == EBridgeFrameKind::TerminateError) {
            const auto message = ReadErrorMessage(In_);
            MKQLTerminate(message.c_str());
        }
        ServeOneRequest();
    }
}

void TBridgeChannel::ServeOneRequest() {
    const auto command = ReadCommand(In_);
    try {
        Dispatch(command);
    } catch (const TTerminateException& e) {
        WriteFrameHeader(Out_, EBridgeFrameKind::TerminateError);
        WriteErrorMessage(Out_, e.what());
        Out_.Flush();
    } catch (...) {
        WriteFrameHeader(Out_, EBridgeFrameKind::Error);
        WriteErrorMessage(Out_, WithModule(CurrentExceptionMessage()));
        Out_.Flush();
    }
}

void TBridgeChannel::Dispatch(EBridgeCommand command) {
    switch (command) {
        case EBridgeCommand::Run: {
            const auto funcNodeId = ReadNodeId(In_);
            auto& node = GetNode(funcNodeId, EBridgeNodeKind::Callable);
            const auto funcType = static_cast<const TCallableType*>(node.Type);
            const ui32 argsCount = ReadUi32(In_);
            MKQL_ENSURE(argsCount == funcType->GetArgumentsCount(), "Bridge: RUN argument count mismatch");
            TSmallVec<NUdf::TUnboxedValue> args(argsCount);
            for (ui32 i = 0; i < argsCount; ++i) {
                args[i] = DecodeValue(funcType->GetArgumentType(i));
            }
            auto result = node.Value.Run(ValueBuilder_, args.data());
            WriteFrameHeader(Out_, EBridgeFrameKind::Response);
            EncodeValue(funcType->GetReturnType(), result);
            Out_.Flush();
            break;
        }
        case EBridgeCommand::MakeIterator: {
            const auto listNodeId = ReadNodeId(In_);
            auto& node = GetNode(listNodeId, EBridgeNodeKind::List);
            const auto itemType = static_cast<const TListType*>(node.Type)->GetItemType();
            auto iter = node.Value.GetListIterator();
            const auto iterNodeId = RegisterNode(EBridgeNodeKind::ListIterator, itemType, std::move(iter));
            WriteFrameHeader(Out_, EBridgeFrameKind::Response);
            WriteNodeId(Out_, iterNodeId);
            Out_.Flush();
            break;
        }
        case EBridgeCommand::NextIterator: {
            const auto iterNodeId = ReadNodeId(In_);
            auto& node = GetNode(iterNodeId, EBridgeNodeKind::ListIterator);
            NUdf::TUnboxedValue item;
            const bool has = node.Value.Next(item);
            WriteFrameHeader(Out_, EBridgeFrameKind::Response);
            WriteBool(Out_, has);
            if (has) {
                EncodeValue(node.Type, item);
            }
            Out_.Flush();
            break;
        }
        case EBridgeCommand::ListLength: {
            const auto listNodeId = ReadNodeId(In_);
            auto& node = GetNode(listNodeId, EBridgeNodeKind::List);
            const ui64 length = node.Value.GetListLength();
            WriteFrameHeader(Out_, EBridgeFrameKind::Response);
            WriteUi64(Out_, length);
            Out_.Flush();
            break;
        }
        case EBridgeCommand::ListEstimatedLength: {
            const auto listNodeId = ReadNodeId(In_);
            auto& node = GetNode(listNodeId, EBridgeNodeKind::List);
            const ui64 length = node.Value.GetEstimatedListLength();
            WriteFrameHeader(Out_, EBridgeFrameKind::Response);
            WriteUi64(Out_, length);
            Out_.Flush();
            break;
        }
        case EBridgeCommand::ListHasItems: {
            const auto listNodeId = ReadNodeId(In_);
            auto& node = GetNode(listNodeId, EBridgeNodeKind::List);
            const bool has = node.Value.HasListItems();
            WriteFrameHeader(Out_, EBridgeFrameKind::Response);
            WriteBool(Out_, has);
            Out_.Flush();
            break;
        }
        case EBridgeCommand::MakeDictIterator: {
            const auto dictNodeId = ReadNodeId(In_);
            auto& node = GetNode(dictNodeId, EBridgeNodeKind::Dict);
            auto iter = node.Value.GetDictIterator();
            const auto iterNodeId = RegisterNode(EBridgeNodeKind::DictIterator, node.Type, std::move(iter));
            WriteFrameHeader(Out_, EBridgeFrameKind::Response);
            WriteNodeId(Out_, iterNodeId);
            Out_.Flush();
            break;
        }
        case EBridgeCommand::NextDictIterator: {
            const auto iterNodeId = ReadNodeId(In_);
            auto& node = GetNode(iterNodeId, EBridgeNodeKind::DictIterator);
            const auto dictType = static_cast<const TDictType*>(node.Type);
            NUdf::TUnboxedValue key;
            NUdf::TUnboxedValue payload;
            const bool has = node.Value.NextPair(key, payload);
            WriteFrameHeader(Out_, EBridgeFrameKind::Response);
            WriteBool(Out_, has);
            if (has) {
                EncodeValue(dictType->GetKeyType(), key);
                EncodeValue(dictType->GetPayloadType(), payload);
            }
            Out_.Flush();
            break;
        }
        case EBridgeCommand::MakeKeysIterator: {
            const auto dictNodeId = ReadNodeId(In_);
            auto& node = GetNode(dictNodeId, EBridgeNodeKind::Dict);
            const auto keyType = static_cast<const TDictType*>(node.Type)->GetKeyType();
            auto iter = node.Value.GetKeysIterator();
            const auto iterNodeId = RegisterNode(EBridgeNodeKind::ListIterator, keyType, std::move(iter));
            WriteFrameHeader(Out_, EBridgeFrameKind::Response);
            WriteNodeId(Out_, iterNodeId);
            Out_.Flush();
            break;
        }
        case EBridgeCommand::MakePayloadsIterator: {
            const auto dictNodeId = ReadNodeId(In_);
            auto& node = GetNode(dictNodeId, EBridgeNodeKind::Dict);
            const auto payloadType = static_cast<const TDictType*>(node.Type)->GetPayloadType();
            auto iter = node.Value.GetPayloadsIterator();
            const auto iterNodeId = RegisterNode(EBridgeNodeKind::ListIterator, payloadType, std::move(iter));
            WriteFrameHeader(Out_, EBridgeFrameKind::Response);
            WriteNodeId(Out_, iterNodeId);
            Out_.Flush();
            break;
        }
        case EBridgeCommand::DictLength: {
            const auto dictNodeId = ReadNodeId(In_);
            auto& node = GetNode(dictNodeId, EBridgeNodeKind::Dict);
            const ui64 length = node.Value.GetDictLength();
            WriteFrameHeader(Out_, EBridgeFrameKind::Response);
            WriteUi64(Out_, length);
            Out_.Flush();
            break;
        }
        case EBridgeCommand::DictHasItems: {
            const auto dictNodeId = ReadNodeId(In_);
            auto& node = GetNode(dictNodeId, EBridgeNodeKind::Dict);
            const bool has = node.Value.HasDictItems();
            WriteFrameHeader(Out_, EBridgeFrameKind::Response);
            WriteBool(Out_, has);
            Out_.Flush();
            break;
        }
        case EBridgeCommand::DictContains: {
            const auto dictNodeId = ReadNodeId(In_);
            auto& node = GetNode(dictNodeId, EBridgeNodeKind::Dict);
            const auto dictType = static_cast<const TDictType*>(node.Type);
            const auto key = DecodeValue(dictType->GetKeyType());
            const bool has = node.Value.Contains(key);
            WriteFrameHeader(Out_, EBridgeFrameKind::Response);
            WriteBool(Out_, has);
            Out_.Flush();
            break;
        }
        case EBridgeCommand::DictLookup: {
            const auto dictNodeId = ReadNodeId(In_);
            auto& node = GetNode(dictNodeId, EBridgeNodeKind::Dict);
            const auto dictType = static_cast<const TDictType*>(node.Type);
            const auto key = DecodeValue(dictType->GetKeyType());
            auto payload = node.Value.Lookup(key);
            const bool has = static_cast<bool>(payload);
            WriteFrameHeader(Out_, EBridgeFrameKind::Response);
            WriteBool(Out_, has);
            if (has) {
                EncodeValue(dictType->GetPayloadType(), payload.GetOptionalValue());
            }
            Out_.Flush();
            break;
        }
        case EBridgeCommand::UnrefNode: {
            const auto nodeId = ReadNodeId(In_);
            Nodes_.erase(nodeId);
            WriteFrameHeader(Out_, EBridgeFrameKind::Response);
            Out_.Flush();
            break;
        }
        case EBridgeCommand::ResolveFunction: {
            MKQL_ENSURE(WorkerFunctionRegistry_ && WorkerEnv_ && WorkerRuntimeSettings_, "Bridge: this channel cannot resolve functions (not a worker-side channel)");
            TBridgeUdfSpec spec;
            spec.FunctionName = ReadBytes(In_);
            if (ModuleName_.empty()) {
                ModuleName_ = TString(ModuleName(spec.FunctionName));
            }
            spec.TypeConfig = ReadBytes(In_);
            spec.SerializedUserType = ReadBytes(In_);
            spec.LangVer = ReadUi32(In_);
            const TCallableType* funcType = nullptr;
            auto value = ResolveBridgeFunction(*WorkerFunctionRegistry_, spec, *WorkerEnv_, *WorkerRuntimeSettings_, &funcType);
            const auto nodeId = RegisterNode(EBridgeNodeKind::Callable, funcType, std::move(value));
            WriteFrameHeader(Out_, EBridgeFrameKind::Response);
            WriteNodeId(Out_, nodeId);
            Out_.Flush();
            break;
        }
    }
}

TBridgeChannel::TNode& TBridgeChannel::GetNode(TBridgeNodeId nodeId, EBridgeNodeKind expectedKind) {
    const auto it = Nodes_.find(nodeId);
    if (it == Nodes_.end()) {
        ythrow TBridgeException() << "Bridge: unknown node id " << nodeId;
    }
    if (it->second.Kind != expectedKind) {
        ythrow TBridgeException() << "Bridge: node " << nodeId << " has an unexpected kind";
    }
    return it->second;
}

TBridgeNodeId TBridgeChannel::RegisterNode(EBridgeNodeKind kind, const TType* type, NUdf::TUnboxedValue&& value) {
    const TBridgeNodeId nodeId(NextNodeId_++);
    Nodes_.emplace(nodeId, TNode{.Kind = kind, .Type = type, .Value = std::move(value)});
    return nodeId;
}

size_t TBridgeChannel::DebugNodeTableSize() const {
    return Nodes_.size();
}

NUdf::TUnboxedValue TBridgeChannel::RunRemote(TBridgeNodeId funcNodeId, const TCallableType* funcType, const NUdf::TUnboxedValuePod* args) {
    WriteRequestHeader(Out_, EBridgeCommand::Run);
    WriteNodeId(Out_, funcNodeId);
    const ui32 argsCount = funcType->GetArgumentsCount();
    WriteUi32(Out_, argsCount);
    for (ui32 i = 0; i < argsCount; ++i) {
        EncodeValue(funcType->GetArgumentType(i), args[i]);
    }
    Out_.Flush();
    WaitForResponse();
    return DecodeValue(funcType->GetReturnType());
}

TBridgeNodeId TBridgeChannel::MakeIteratorRemote(TBridgeNodeId listNodeId) {
    WriteRequestHeader(Out_, EBridgeCommand::MakeIterator);
    WriteNodeId(Out_, listNodeId);
    Out_.Flush();
    WaitForResponse();
    return ReadNodeId(In_);
}

bool TBridgeChannel::NextIteratorRemote(TBridgeNodeId iterNodeId, const TType* itemType, NUdf::TUnboxedValue& result) {
    WriteRequestHeader(Out_, EBridgeCommand::NextIterator);
    WriteNodeId(Out_, iterNodeId);
    Out_.Flush();
    WaitForResponse();
    const bool has = ReadBool(In_);
    if (has) {
        result = DecodeValue(itemType);
    }
    return has;
}

ui64 TBridgeChannel::ListLengthRemote(TBridgeNodeId listNodeId) {
    WriteRequestHeader(Out_, EBridgeCommand::ListLength);
    WriteNodeId(Out_, listNodeId);
    Out_.Flush();
    WaitForResponse();
    return ReadUi64(In_);
}

ui64 TBridgeChannel::ListEstimatedLengthRemote(TBridgeNodeId listNodeId) {
    WriteRequestHeader(Out_, EBridgeCommand::ListEstimatedLength);
    WriteNodeId(Out_, listNodeId);
    Out_.Flush();
    WaitForResponse();
    return ReadUi64(In_);
}

bool TBridgeChannel::ListHasItemsRemote(TBridgeNodeId listNodeId) {
    WriteRequestHeader(Out_, EBridgeCommand::ListHasItems);
    WriteNodeId(Out_, listNodeId);
    Out_.Flush();
    WaitForResponse();
    return ReadBool(In_);
}

TBridgeNodeId TBridgeChannel::MakeDictIteratorRemote(TBridgeNodeId dictNodeId) {
    WriteRequestHeader(Out_, EBridgeCommand::MakeDictIterator);
    WriteNodeId(Out_, dictNodeId);
    Out_.Flush();
    WaitForResponse();
    return ReadNodeId(In_);
}

bool TBridgeChannel::NextDictIteratorRemote(TBridgeNodeId iterNodeId, const TType* keyType, const TType* payloadType, NUdf::TUnboxedValue& key, NUdf::TUnboxedValue& payload) {
    WriteRequestHeader(Out_, EBridgeCommand::NextDictIterator);
    WriteNodeId(Out_, iterNodeId);
    Out_.Flush();
    WaitForResponse();
    const bool has = ReadBool(In_);
    if (has) {
        key = DecodeValue(keyType);
        payload = DecodeValue(payloadType);
    }
    return has;
}

TBridgeNodeId TBridgeChannel::MakeKeysIteratorRemote(TBridgeNodeId dictNodeId) {
    WriteRequestHeader(Out_, EBridgeCommand::MakeKeysIterator);
    WriteNodeId(Out_, dictNodeId);
    Out_.Flush();
    WaitForResponse();
    return ReadNodeId(In_);
}

TBridgeNodeId TBridgeChannel::MakePayloadsIteratorRemote(TBridgeNodeId dictNodeId) {
    WriteRequestHeader(Out_, EBridgeCommand::MakePayloadsIterator);
    WriteNodeId(Out_, dictNodeId);
    Out_.Flush();
    WaitForResponse();
    return ReadNodeId(In_);
}

ui64 TBridgeChannel::DictLengthRemote(TBridgeNodeId dictNodeId) {
    WriteRequestHeader(Out_, EBridgeCommand::DictLength);
    WriteNodeId(Out_, dictNodeId);
    Out_.Flush();
    WaitForResponse();
    return ReadUi64(In_);
}

bool TBridgeChannel::DictHasItemsRemote(TBridgeNodeId dictNodeId) {
    WriteRequestHeader(Out_, EBridgeCommand::DictHasItems);
    WriteNodeId(Out_, dictNodeId);
    Out_.Flush();
    WaitForResponse();
    return ReadBool(In_);
}

bool TBridgeChannel::DictContainsRemote(TBridgeNodeId dictNodeId, const TType* keyType, const NUdf::TUnboxedValuePod& key) {
    WriteRequestHeader(Out_, EBridgeCommand::DictContains);
    WriteNodeId(Out_, dictNodeId);
    EncodeValue(keyType, key);
    Out_.Flush();
    WaitForResponse();
    return ReadBool(In_);
}

NUdf::TUnboxedValue TBridgeChannel::DictLookupRemote(TBridgeNodeId dictNodeId, const TType* keyType, const TType* payloadType, const NUdf::TUnboxedValuePod& key) {
    WriteRequestHeader(Out_, EBridgeCommand::DictLookup);
    WriteNodeId(Out_, dictNodeId);
    EncodeValue(keyType, key);
    Out_.Flush();
    WaitForResponse();
    const bool has = ReadBool(In_);
    if (!has) {
        return NUdf::TUnboxedValue();
    }
    return DecodeValue(payloadType).Release().MakeOptional();
}

void TBridgeChannel::UnrefNodeRemote(TBridgeNodeId nodeId) {
    WriteRequestHeader(Out_, EBridgeCommand::UnrefNode);
    WriteNodeId(Out_, nodeId);
    Out_.Flush();
    WaitForResponse();
}

bool TBridgeChannel::IsPlainType(const TType* type) {
    const auto it = PlainCache_.find(type);
    if (it != PlainCache_.end()) {
        return it->second;
    }
    const bool result = ComputeIsPlainType(type);
    PlainCache_.emplace(type, result);
    return result;
}

bool TBridgeChannel::ComputeIsPlainType(const TType* type) {
    switch (type->GetKind()) {
        case TType::EKind::Void:
        case TType::EKind::Null:
        case TType::EKind::EmptyList:
        case TType::EKind::EmptyDict:
        case TType::EKind::Data:
        case TType::EKind::Pg:
            return true;

        case TType::EKind::Optional:
            return IsPlainType(static_cast<const TOptionalType*>(type)->GetItemType());

        case TType::EKind::Tagged:
            return IsPlainType(static_cast<const TTaggedType*>(type)->GetBaseType());

        case TType::EKind::Tuple: {
            const auto tupleType = static_cast<const TTupleType*>(type);
            for (ui32 i = 0; i < tupleType->GetElementsCount(); ++i) {
                if (!IsPlainType(tupleType->GetElementType(i))) {
                    return false;
                }
            }
            return true;
        }

        case TType::EKind::Struct: {
            const auto structType = static_cast<const TStructType*>(type);
            for (ui32 i = 0; i < structType->GetMembersCount(); ++i) {
                if (!IsPlainType(structType->GetMemberType(i))) {
                    return false;
                }
            }
            return true;
        }

        case TType::EKind::Variant: {
            const auto variantType = static_cast<const TVariantType*>(type);
            for (ui32 i = 0; i < variantType->GetAlternativesCount(); ++i) {
                if (!IsPlainType(variantType->GetAlternativeType(i))) {
                    return false;
                }
            }
            return true;
        }

        default:
            // List, Dict, Resource, Callable, Stream, Flow, Any, Type, Multi,
            // Linear, Block, ReservedKind -- always node-proxied or unsupported.
            return false;
    }
}

TValuePackerGeneric<true>& TBridgeChannel::GetPacker(const TType* type) {
    const auto it = Packers_.find(type);
    if (it != Packers_.end()) {
        return *it->second;
    }
    auto packer = MakeHolder<TValuePackerGeneric<true>>(/* stable */ false, type);
    auto& ref = *packer;
    Packers_.emplace(type, std::move(packer));
    return ref;
}

void TBridgeChannel::RegisterOutgoingProxy(const void* proxy, TBridgeNamespaceId nodeNamespace, TBridgeNodeId nodeId) {
    OutgoingProxies_.emplace(proxy, TRemoteNodeRef{.Namespace = nodeNamespace, .NodeId = nodeId});
}

void TBridgeChannel::UnregisterOutgoingProxy(const void* proxy) {
    OutgoingProxies_.erase(proxy);
}

bool TBridgeChannel::TryReuseNode(const NUdf::TUnboxedValuePod& value, TBridgeNamespaceId& outNamespace, TBridgeNodeId& outNodeId) const {
    if (!value.IsBoxed()) {
        return false;
    }
    const auto it = OutgoingProxies_.find(value.AsBoxed().Get());
    if (it == OutgoingProxies_.end()) {
        return false;
    }
    outNamespace = it->second.Namespace;
    outNodeId = it->second.NodeId;
    return true;
}

void TBridgeChannel::EncodeValue(const TType* type, const NUdf::TUnboxedValuePod& value) {
    if (IsPlainType(type)) {
        WriteBytes(Out_, GetPacker(type).Pack(value));
        return;
    }

    switch (type->GetKind()) {
        case TType::EKind::Optional: {
            const auto itemType = static_cast<const TOptionalType*>(type)->GetItemType();
            const bool present = static_cast<bool>(value);
            WriteBool(Out_, present);
            if (present) {
                EncodeValue(itemType, value.GetOptionalValue());
            }
            break;
        }

        case TType::EKind::Tuple: {
            const auto tupleType = static_cast<const TTupleType*>(type);
            for (ui32 i = 0; i < tupleType->GetElementsCount(); ++i) {
                EncodeValue(tupleType->GetElementType(i), value.GetElement(i));
            }
            break;
        }

        case TType::EKind::Struct: {
            const auto structType = static_cast<const TStructType*>(type);
            for (ui32 i = 0; i < structType->GetMembersCount(); ++i) {
                EncodeValue(structType->GetMemberType(i), value.GetElement(i));
            }
            break;
        }

        case TType::EKind::Variant: {
            const auto variantType = static_cast<const TVariantType*>(type);
            const ui32 index = value.GetVariantIndex();
            WriteUi32(Out_, index);
            EncodeValue(variantType->GetAlternativeType(index), value.GetVariantItem());
            break;
        }

        case TType::EKind::Tagged:
            EncodeValue(static_cast<const TTaggedType*>(type)->GetBaseType(), value);
            break;

        case TType::EKind::List: {
            TBridgeNamespaceId ns;
            TBridgeNodeId nodeId;
            if (!TryReuseNode(value, ns, nodeId)) {
                ns = OwnNamespace_;
                nodeId = RegisterNode(EBridgeNodeKind::List, type, NUdf::TUnboxedValue(value));
            }
            WriteUi64(Out_, *ns);
            WriteNodeId(Out_, nodeId);
            break;
        }

        case TType::EKind::Dict: {
            TBridgeNamespaceId ns;
            TBridgeNodeId nodeId;
            if (!TryReuseNode(value, ns, nodeId)) {
                ns = OwnNamespace_;
                nodeId = RegisterNode(EBridgeNodeKind::Dict, type, NUdf::TUnboxedValue(value));
            }
            WriteUi64(Out_, *ns);
            WriteNodeId(Out_, nodeId);
            break;
        }

        case TType::EKind::Resource: {
            TBridgeNamespaceId ns;
            TBridgeNodeId nodeId;
            if (!TryReuseNode(value, ns, nodeId)) {
                ns = OwnNamespace_;
                nodeId = RegisterNode(EBridgeNodeKind::Resource, type, NUdf::TUnboxedValue(value));
            }
            WriteUi64(Out_, *ns);
            WriteNodeId(Out_, nodeId);
            break;
        }

        case TType::EKind::Callable: {
            TBridgeNamespaceId ns;
            TBridgeNodeId nodeId;
            if (!TryReuseNode(value, ns, nodeId)) {
                ns = OwnNamespace_;
                nodeId = RegisterNode(EBridgeNodeKind::Callable, type, NUdf::TUnboxedValue(value));
            }
            WriteUi64(Out_, *ns);
            WriteNodeId(Out_, nodeId);
            break;
        }

        default:
            ythrow TBridgeException() << "Bridge: unsupported type in value: " << type->GetKindAsStr();
    }
}

NUdf::TUnboxedValue TBridgeChannel::DecodeValue(const TType* type) {
    if (IsPlainType(type)) {
        return GetPacker(type).Unpack(ReadBytes(In_), HolderFactory_);
    }

    switch (type->GetKind()) {
        case TType::EKind::Optional: {
            const auto itemType = static_cast<const TOptionalType*>(type)->GetItemType();
            if (!ReadBool(In_)) {
                return NUdf::TUnboxedValue();
            }
            return DecodeValue(itemType).Release().MakeOptional();
        }

        case TType::EKind::Tuple: {
            const auto tupleType = static_cast<const TTupleType*>(type);
            NUdf::TUnboxedValue* items = nullptr;
            auto result = ValueBuilder_->NewArray(tupleType->GetElementsCount(), items);
            for (ui32 i = 0; i < tupleType->GetElementsCount(); ++i) {
                items[i] = DecodeValue(tupleType->GetElementType(i));
            }
            return result;
        }

        case TType::EKind::Struct: {
            const auto structType = static_cast<const TStructType*>(type);
            NUdf::TUnboxedValue* items = nullptr;
            auto result = ValueBuilder_->NewArray(structType->GetMembersCount(), items);
            for (ui32 i = 0; i < structType->GetMembersCount(); ++i) {
                items[i] = DecodeValue(structType->GetMemberType(i));
            }
            return result;
        }

        case TType::EKind::Variant: {
            const auto variantType = static_cast<const TVariantType*>(type);
            const ui32 index = ReadUi32(In_);
            auto item = DecodeValue(variantType->GetAlternativeType(index));
            return ValueBuilder_->NewVariant(index, std::move(item));
        }

        case TType::EKind::Tagged:
            return DecodeValue(static_cast<const TTaggedType*>(type)->GetBaseType());

        case TType::EKind::List: {
            const TBridgeNamespaceId ns(ReadUi64(In_));
            const auto nodeId = ReadNodeId(In_);
            if (ns == OwnNamespace_) {
                return GetNode(nodeId, EBridgeNodeKind::List).Value;
            }
            const auto itemType = static_cast<const TListType*>(type)->GetItemType();
            return NUdf::TUnboxedValuePod(new TBridgeProxyList(&HolderFactory_.GetMemInfo(), this, ns, nodeId, itemType));
        }

        case TType::EKind::Dict: {
            const TBridgeNamespaceId ns(ReadUi64(In_));
            const auto nodeId = ReadNodeId(In_);
            if (ns == OwnNamespace_) {
                return GetNode(nodeId, EBridgeNodeKind::Dict).Value;
            }
            const auto dictType = static_cast<const TDictType*>(type);
            return NUdf::TUnboxedValuePod(new TBridgeProxyDict(&HolderFactory_.GetMemInfo(), this, ns, nodeId, dictType->GetKeyType(), dictType->GetPayloadType()));
        }

        case TType::EKind::Resource: {
            const TBridgeNamespaceId ns(ReadUi64(In_));
            const auto nodeId = ReadNodeId(In_);
            if (ns == OwnNamespace_) {
                return GetNode(nodeId, EBridgeNodeKind::Resource).Value;
            }
            return NUdf::TUnboxedValuePod(new TBridgeProxyResource(&HolderFactory_.GetMemInfo(), this, ns, nodeId, static_cast<const TResourceType*>(type)));
        }

        case TType::EKind::Callable: {
            const TBridgeNamespaceId ns(ReadUi64(In_));
            const auto nodeId = ReadNodeId(In_);
            if (ns == OwnNamespace_) {
                return GetNode(nodeId, EBridgeNodeKind::Callable).Value;
            }
            return NUdf::TUnboxedValuePod(new TBridgeProxyCallable(&HolderFactory_.GetMemInfo(), this, ns, nodeId, static_cast<const TCallableType*>(type)));
        }

        default:
            ythrow TBridgeException() << "Bridge: unsupported type in value: " << type->GetKindAsStr();
    }
}

TBridgeWorkerSetup CreateBridgeWorkerSetup(const IFunctionRegistry& functionRegistry) {
    TBridgeWorkerSetup setup;
    setup.Alloc = MakeHolder<TScopedAlloc>(__LOCATION__);
    setup.Env = MakeHolder<TTypeEnvironment>(*setup.Alloc);
    setup.MemInfo = MakeHolder<TMemoryUsageInfo>("UdfBridgeWorker");
    setup.HolderFactory = MakeHolder<THolderFactory>(setup.Alloc->Ref(), *setup.MemInfo, &functionRegistry);
    setup.ValueBuilder = MakeHolder<TDefaultValueBuilder>(*setup.HolderFactory, NUdf::EValidatePolicy::Exception);
    setup.CalleePosition = MakeHolder<const NUdf::TSourcePosition*>(nullptr);
    setup.ValueBuilder->SetCalleePositionHolder(*setup.CalleePosition);
    return setup;
}

NUdf::TUnboxedValue ResolveBridgeFunction(
    const IFunctionRegistry& functionRegistry,
    const TBridgeUdfSpec& spec,
    const TTypeEnvironment& env,
    const NYql::TRuntimeSettings& runtimeSettings,
    const TCallableType** outFuncType) {
    TType* userType = nullptr;
    if (spec.SerializedUserType) {
        userType = static_cast<TType*>(DeserializeNode(spec.SerializedUserType, env));
    }

    NUdf::ITypeInfoHelper::TPtr typeInfoHelper(new TTypeInfoHelper);
    TFunctionTypeInfo funcInfo;
    const auto status = functionRegistry.FindFunctionTypeInfo(
        spec.LangVer, runtimeSettings, env, typeInfoHelper, /* countersProvider */ nullptr,
        spec.FunctionName, userType, spec.TypeConfig, /* flags */ 0, NUdf::TSourcePosition(),
        /* secureParamsProvider */ nullptr, /* logProvider */ nullptr, &funcInfo);
    if (!status.IsOk()) {
        ythrow TBridgeException() << "Bridge: failed to resolve UDF " << spec.FunctionName << ": " << status.GetError();
    }
    if (!funcInfo.Implementation) {
        ythrow TBridgeException() << "Bridge: UDF implementation is not set for function " << spec.FunctionName;
    }

    *outFuncType = AS_TYPE(TCallableType, funcInfo.FunctionType);
    return NUdf::TUnboxedValue(NUdf::TUnboxedValuePod(funcInfo.Implementation.Release()));
}

void WrapCallableWithBridge(
    const TCallableType* callableType,
    NUdf::TUnboxedValue& callable,
    NUdf::EBridgeMode mode,
    TIntrusivePtr<TBridgeChannel> channel,
    const TBridgeUdfSpec& spec) {
    if (mode == NUdf::EBridgeMode::None) {
        return;
    }

    Y_ENSURE(channel, "Bridge: a channel is required for a non-None bridge mode");
    callable = channel->ResolveFunction(spec, callableType);
}

} // namespace NKikimr::NMiniKQL
