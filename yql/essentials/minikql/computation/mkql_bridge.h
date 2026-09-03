#pragma once

#include "mkql_bridge_protocol.h"
#include "mkql_computation_node_holders.h"
#include "mkql_computation_node_pack.h"
#include "mkql_value_builder.h"

#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/mkql_bridge_mode.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/runtime_settings/runtime_settings.h>
#include <yql/essentials/public/langver/yql_langver.h>
#include <yql/essentials/public/udf/udf_value.h>

#include <util/generic/hash.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>

namespace NKikimr::NMiniKQL {

enum class EBridgeNodeKind: ui8 {
    Unknown,
    List,
    ListIterator,
    Dict,
    DictIterator,
    Resource,
    Callable,
};

struct TBridgeUdfSpec {
    TString FunctionName;
    TString TypeConfig;
    TString SerializedUserType;
    NYql::TLangVersion LangVer = NYql::UnknownLangVersion;
};

class TBridgeChannel: public TThrRefBase {
public:
    TBridgeChannel(IInputStream& in, IOutputStream& out, const THolderFactory& holderFactory, const NUdf::IValueBuilder* valueBuilder,
                   TBridgeNamespaceId ownNamespace, TBridgeNamespaceId peerNamespace,
                   const IFunctionRegistry* workerFunctionRegistry, const TTypeEnvironment* workerEnv,
                   NYql::TRuntimeSettings::TConstPtr workerRuntimeSettings);
    ~TBridgeChannel() override;

    NUdf::TUnboxedValue ResolveFunction(const TBridgeUdfSpec& spec, const TCallableType* funcType);

    void ServeForever();

    NUdf::TUnboxedValue RunRemote(TBridgeNodeId funcNodeId, const TCallableType* funcType, const NUdf::TUnboxedValuePod* args);
    TBridgeNodeId MakeIteratorRemote(TBridgeNodeId listNodeId);
    bool NextIteratorRemote(TBridgeNodeId iterNodeId, const TType* itemType, NUdf::TUnboxedValue& result);
    ui64 ListLengthRemote(TBridgeNodeId listNodeId);
    ui64 ListEstimatedLengthRemote(TBridgeNodeId listNodeId);
    bool ListHasItemsRemote(TBridgeNodeId listNodeId);
    TBridgeNodeId MakeDictIteratorRemote(TBridgeNodeId dictNodeId);
    bool NextDictIteratorRemote(TBridgeNodeId iterNodeId, const TType* keyType, const TType* payloadType, NUdf::TUnboxedValue& key, NUdf::TUnboxedValue& payload);
    TBridgeNodeId MakeKeysIteratorRemote(TBridgeNodeId dictNodeId);
    TBridgeNodeId MakePayloadsIteratorRemote(TBridgeNodeId dictNodeId);
    ui64 DictLengthRemote(TBridgeNodeId dictNodeId);
    bool DictHasItemsRemote(TBridgeNodeId dictNodeId);
    bool DictContainsRemote(TBridgeNodeId dictNodeId, const TType* keyType, const NUdf::TUnboxedValuePod& key);
    NUdf::TUnboxedValue DictLookupRemote(TBridgeNodeId dictNodeId, const TType* keyType, const TType* payloadType, const NUdf::TUnboxedValuePod& key);
    void UnrefNodeRemote(TBridgeNodeId nodeId);

    // Every TBridgeProxy{List,Dict,Resource,Callable} registers/unregisters
    // itself here (by its own raw address) for the span of its lifetime --
    // see TryReuseNode below for why: this is a plain, RTTI-free identity
    // lookup, not a type check. `nodeNamespace`/`nodeId` are the *original*
    // reference the proxy was decoded from (see TBridgeNamespaceId).
    void RegisterOutgoingProxy(const void* proxy, TBridgeNamespaceId nodeNamespace, TBridgeNodeId nodeId);
    void UnregisterOutgoingProxy(const void* proxy);

    // Test-only: number of live entries in this side's node table, to assert
    // there's no leak once every proxy has been released.
    size_t DebugNodeTableSize() const;

private:
    struct TNode {
        EBridgeNodeKind Kind = EBridgeNodeKind::Unknown;
        const TType* Type = nullptr;
        NUdf::TUnboxedValue Value;
    };

    void WaitForResponse();
    void ServeOneRequest();
    void Dispatch(EBridgeCommand command);

    void EncodeValue(const TType* type, const NUdf::TUnboxedValuePod& value);
    NUdf::TUnboxedValue DecodeValue(const TType* type);

    // True if `value` is already one of our own proxies -- in that case
    // `outNamespace`/`outNodeId` are the *original* (namespace, node id) it
    // was built from, to be forwarded as-is instead of registering a new
    // node here (which would otherwise assign it a fresh id in our own
    // namespace, hiding its true owner from whoever decodes it next -- see
    // TBridgeNamespaceId's own comment in mkql_bridge_protocol.h).
    bool TryReuseNode(const NUdf::TUnboxedValuePod& value, TBridgeNamespaceId& outNamespace, TBridgeNodeId& outNodeId) const;

    bool IsPlainType(const TType* type);
    bool ComputeIsPlainType(const TType* type);
    TValuePackerGeneric<true>& GetPacker(const TType* type);

    TNode& GetNode(TBridgeNodeId nodeId, EBridgeNodeKind expectedKind);
    TBridgeNodeId RegisterNode(EBridgeNodeKind kind, const TType* type, NUdf::TUnboxedValue&& value);

    struct TRemoteNodeRef {
        TBridgeNamespaceId Namespace;
        TBridgeNodeId NodeId;
    };

    IInputStream& In_;
    IOutputStream& Out_;
    const THolderFactory& HolderFactory_;
    const NUdf::IValueBuilder* const ValueBuilder_;
    const TBridgeNamespaceId OwnNamespace_;
    const TBridgeNamespaceId PeerNamespace_;
    const IFunctionRegistry* const WorkerFunctionRegistry_;
    const TTypeEnvironment* const WorkerEnv_;
    const NYql::TRuntimeSettings::TConstPtr WorkerRuntimeSettings_;

    THashMap<TBridgeNodeId, TNode> Nodes_;
    // Plain ui64, not TBridgeNodeId, since it needs to be incremented -- the
    // strong alias deliberately doesn't support arithmetic. Wrapped at the
    // point each fresh id is handed out (see RegisterNode).
    ui64 NextNodeId_ = 0;

    // Keyed by the proxy's own address (see RegisterOutgoingProxy above).
    THashMap<const void*, TRemoteNodeRef> OutgoingProxies_;

    THashMap<const TType*, bool> PlainCache_;
    THashMap<const TType*, THolder<TValuePackerGeneric<true>>> Packers_;
};

struct TBridgeWorkerSetup {
    THolder<TScopedAlloc> Alloc;
    THolder<TTypeEnvironment> Env;
    THolder<TMemoryUsageInfo> MemInfo;
    THolder<THolderFactory> HolderFactory;
    THolder<TDefaultValueBuilder> ValueBuilder;
    THolder<const NUdf::TSourcePosition*> CalleePosition;
};

TBridgeWorkerSetup CreateBridgeWorkerSetup(const IFunctionRegistry& functionRegistry);

NUdf::TUnboxedValue ResolveBridgeFunction(
    const IFunctionRegistry& functionRegistry,
    const TBridgeUdfSpec& spec,
    const TTypeEnvironment& env,
    const NYql::TRuntimeSettings& runtimeSettings,
    const TCallableType** outFuncType);

void WrapCallableWithBridge(
    const TCallableType* callableType,
    NUdf::TUnboxedValue& callable,
    NUdf::EBridgeMode mode,
    TIntrusivePtr<TBridgeChannel> channel,
    const TBridgeUdfSpec& spec);

} // namespace NKikimr::NMiniKQL
