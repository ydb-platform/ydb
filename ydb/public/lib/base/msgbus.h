#pragma once
#include "defs.h"
#include "msgbus_status.h"
#include <ydb/core/protos/msgbus.pb.h>
#include <ydb/core/protos/msgbus_kv.pb.h>
#include <ydb/core/protos/msgbus_pq.pb.h>
#include <ydb/library/actors/core/actor.h>
#include <library/cpp/messagebus/ybus.h>
#include <library/cpp/messagebus/protobuf/ybusbuf.h>
#include <library/cpp/messagebus/session_config.h>
#include <library/cpp/messagebus/queue_config.h>

namespace NKikimr {
namespace NMsgBusProxy {

enum {
    MTYPE_CLIENT_REQUEST = 10401,
    MTYPE_CLIENT_RESPONSE = 10402,
    MTYPE_CLIENT_FAKE_CONFIGDUMMY = 10403,
    MTYPE_CLIENT_INSPECT = 10404,
    MTYPE_CLIENT_SCHEME_INITROOT = 10405,
    MTYPE_CLIENT_SCHEME_NAVIGATE = 10407,
    MTYPE_CLIENT_TYPES_REQUEST = 10408,
    MTYPE_CLIENT_TYPES_RESPONSE = 10409,
    MTYPE_CLIENT_CREATE_TABLET_POOL = 10410, // deprecated
    MTYPE_CLIENT_RESOLVE_JOB_RUNNER = 10411, // deprecated
    MTYPE_CLIENT_RUN_JOB = 10412, // deprecated
    MTYPE_CLIENT_JOB_RESPONSE = 10413, // deprecated
    MTYPE_CLIENT_REQUEST_JOB_EXECUTION = 10414, // deprecated
    MTYPE_CLIENT_DELAYED_JOB_LAUNCH = 10415, // deprecated
    MTYPE_CLIENT_JOB_EXECUTION_STATUS = 10416, // deprecated
    MTYPE_CLIENT_REQUEST_JOB_INFORMATION = 10417, // deprecated
    MTYPE_CLIENT_DATASHARD_SET_CONFIG = 10418, // deprecated
    MTYPE_CLIENT_DESTROY_JOB = 10419, // deprecated
    MTYPE_CLIENT_COMPACTION_BROKER_SET_CONFIG = 10420, // deprecated
    MTYPE_CLIENT_OLD_HIVE_CREATE_TABLET = 10421, // deprecated
    MTYPE_CLIENT_HIVE_CREATE_TABLET_RESULT = 10422, // deprecated
    MTYPE_CLIENT_OLD_LOCAL_ENUMERATE_TABLETS = 10423, // deprecated
    MTYPE_CLIENT_LOCAL_ENUMERATE_TABLETS_RESULT = 10424, // deprecated
    MTYPE_CLIENT_OLD_KEYVALUE = 10425, // deprecated
    MTYPE_CLIENT_KEYVALUE_RESPONSE = 10426, // deprecated
    /*MTYPE_CLIENT_MESSAGE_BUS_TRACE*/ MTYPE_CLIENT_DEPRECATED_10427 = 10427,
    /*MTYPE_CLIENT_MESSAGE_BUS_TRACE_STATUS*/ MTYPE_CLIENT_DEPRECATED_10428 = 10428,
    MTYPE_CLIENT_TABLET_KILL_REQUEST = 10429,
    MTYPE_CLIENT_TABLET_STATE_REQUEST = 10430,
    MTYPE_CLIENT_LOCAL_MINIKQL = 10431,
    MTYPE_CLIENT_FLAT_TX_REQUEST = 10432,
    MTYPE_CLIENT_FLAT_TX_STATUS_REQUEST = 10434,
    MTYPE_CLIENT_OLD_FLAT_DESCRIBE_REQUEST = 10435, // deprecated
    MTYPE_CLIENT_OLD_FLAT_DESCRIBE_RESPONSE = 10436, // deprecated
    MTYPE_CLIENT_CREATE_TABLET = 10437,
    MTYPE_CLIENT_DIRECT_REQUEST_JOB_EXECUTION_STATUS = 10440, // deprecated
    MTYPE_CLIENT_PERSQUEUE = 10441,
    MTYPE_CLIENT_DB_SCHEMA = 10443,
    MTYPE_CLIENT_DB_OPERATION = 10444,
    MTYPE_CLIENT_DB_RESPONSE = 10445,
    MTYPE_CLIENT_HIVE_CREATE_TABLET = 10446,
    MTYPE_CLIENT_LOCAL_ENUMERATE_TABLETS = 10447,
    MTYPE_CLIENT_KEYVALUE = 10448,
    MTYPE_CLIENT_DB_BATCH = 10449,
    MTYPE_CLIENT_FLAT_DESCRIBE_REQUEST = 10450,
    MTYPE_CLIENT_LOCAL_SCHEME_TX = 10453,
    MTYPE_CLIENT_DB_QUERY = 10456,
    MTYPE_CLIENT_TABLET_COUNTERS_REQUEST = 10457,
    MTYPE_CLIENT_CANCEL_BACKUP = 10458,
    MTYPE_CLIENT_BLOB_STORAGE_CONFIG_REQUEST = 10459,
    MTYPE_CLIENT_DRAIN_NODE = 10460,
    MTYPE_CLIENT_FILL_NODE = 10461,
    MTYPE_CLIENT_RESOLVE_NODE = 10462,
    MTYPE_CLIENT_NODE_REGISTRATION_REQUEST = 10463,
    MTYPE_CLIENT_NODE_REGISTRATION_RESPONSE = 10464,
    MTYPE_CLIENT_CMS_REQUEST = 10465,
    MTYPE_CLIENT_CMS_RESPONSE = 10466,
    MTYPE_CLIENT_RESOURCE_BROKER_SET_CONFIG = 10467,
    MTYPE_CLIENT_CHOOSE_PROXY = 10468,
    MTYPE_CLIENT_SQS_REQUEST = 10469,
    MTYPE_CLIENT_SQS_RESPONSE = 10470,
    MTYPE_CLIENT_DEPRECATED_10471 = 10471,
    MTYPE_CLIENT_STREAM_REQUEST = 10472,
    MTYPE_CLIENT_S3_LISTING_REQUEST = 10474,
    MTYPE_CLIENT_S3_LISTING_RESPONSE = 10475,
    MTYPE_CLIENT_INTERCONNECT_DEBUG = 10476,
    MTYPE_CLIENT_CONSOLE_REQUEST = 10477,
    MTYPE_CLIENT_CONSOLE_RESPONSE = 10478,
    MTYPE_CLIENT_TENANT_SLOT_BROKER_REQUEST = 10479,
    MTYPE_CLIENT_TENANT_SLOT_BROKER_RESPONSE = 10480,
    MTYPE_CLIENT_TEST_SHARD_CONTROL = 10481,
    MTYPE_CLIENT_DS_LOAD_REQUEST = 10482, // deprecated
    MTYPE_CLIENT_DS_LOAD_RESPONSE = 10483, // deprecated
    /*MTYPE_CLIENT_LOGIN_REQUEST*/ MTYPE_CLIENT_DEPRECATED_10484 = 10484,
};

template <typename InstanceType, class TBufferRecord, int MType>
struct TBusMessage : NBus::TBusBufferMessage<TBufferRecord, MType> { NBus::TBusBufferBase* New() override { return new InstanceType; } };

// we had to define structs instead of typedef because we need to create real C++ type, so we could use RTTI names later in protocol registration
struct TBusRequest : TBusMessage<TBusRequest, NKikimrClient::TRequest, MTYPE_CLIENT_REQUEST> {};
struct TBusResponse : TBusMessage<TBusResponse, NKikimrClient::TResponse, MTYPE_CLIENT_RESPONSE> {};
struct TBusFakeConfigDummy : TBusMessage<TBusFakeConfigDummy, NKikimrClient::TFakeConfigDummy, MTYPE_CLIENT_FAKE_CONFIGDUMMY> {};
struct TBusSchemeInitRoot : TBusMessage<TBusSchemeInitRoot, NKikimrClient::TSchemeInitRoot, MTYPE_CLIENT_SCHEME_INITROOT> {};
struct TBusTypesRequest : TBusMessage<TBusTypesRequest, NKikimrClient::TTypeMetadataRequest, MTYPE_CLIENT_TYPES_REQUEST> {};
struct TBusTypesResponse : TBusMessage<TBusTypesResponse, NKikimrClient::TTypeMetadataResponse, MTYPE_CLIENT_TYPES_RESPONSE> {};
struct TBusHiveCreateTablet : TBusMessage<TBusHiveCreateTablet, NKikimrClient::THiveCreateTablet, MTYPE_CLIENT_HIVE_CREATE_TABLET> {};
struct TBusOldHiveCreateTablet : TBusMessage<TBusOldHiveCreateTablet, NKikimrClient::THiveCreateTablet, MTYPE_CLIENT_OLD_HIVE_CREATE_TABLET> {};
struct TBusHiveCreateTabletResult : TBusMessage<TBusHiveCreateTabletResult, NKikimrClient::THiveCreateTabletResult, MTYPE_CLIENT_HIVE_CREATE_TABLET_RESULT> {};
struct TBusLocalEnumerateTablets : TBusMessage<TBusLocalEnumerateTablets, NKikimrClient::TLocalEnumerateTablets, MTYPE_CLIENT_LOCAL_ENUMERATE_TABLETS> {};
struct TBusOldLocalEnumerateTablets : TBusMessage<TBusOldLocalEnumerateTablets, NKikimrClient::TLocalEnumerateTablets, MTYPE_CLIENT_OLD_LOCAL_ENUMERATE_TABLETS> {};
struct TBusLocalEnumerateTabletsResult : TBusMessage<TBusLocalEnumerateTabletsResult, NKikimrClient::TLocalEnumerateTabletsResult, MTYPE_CLIENT_LOCAL_ENUMERATE_TABLETS_RESULT> {};
struct TBusKeyValue : TBusMessage<TBusKeyValue, NKikimrClient::TKeyValueRequest, MTYPE_CLIENT_KEYVALUE> {};
struct TBusOldKeyValue : TBusMessage<TBusOldKeyValue, NKikimrClient::TKeyValueRequest, MTYPE_CLIENT_OLD_KEYVALUE> {};
struct TBusKeyValueResponse : TBusMessage<TBusKeyValueResponse, NKikimrClient::TKeyValueResponse, MTYPE_CLIENT_KEYVALUE_RESPONSE> {};
struct TBusPersQueue : TBusMessage<TBusPersQueue, NKikimrClient::TPersQueueRequest, MTYPE_CLIENT_PERSQUEUE> {};
struct TBusTabletKillRequest : TBusMessage<TBusTabletKillRequest, NKikimrClient::TTabletKillRequest, MTYPE_CLIENT_TABLET_KILL_REQUEST> {};
struct TBusTabletStateRequest : TBusMessage<TBusTabletStateRequest, NKikimrClient::TTabletStateRequest, MTYPE_CLIENT_TABLET_STATE_REQUEST> {};
struct TBusTabletCountersRequest : TBusMessage<TBusTabletCountersRequest, NKikimrClient::TTabletCountersRequest, MTYPE_CLIENT_TABLET_COUNTERS_REQUEST> {};
struct TBusTabletLocalMKQL : TBusMessage<TBusTabletLocalMKQL, NKikimrClient::TLocalMKQL, MTYPE_CLIENT_LOCAL_MINIKQL> {};
struct TBusTabletLocalSchemeTx : TBusMessage<TBusTabletLocalSchemeTx, NKikimrClient::TLocalSchemeTx, MTYPE_CLIENT_LOCAL_SCHEME_TX> {};
struct TBusSchemeOperation : TBusMessage<TBusSchemeOperation, NKikimrClient::TSchemeOperation, MTYPE_CLIENT_FLAT_TX_REQUEST> {};
struct TBusSchemeOperationStatus : TBusMessage<TBusSchemeOperationStatus, NKikimrClient::TSchemeOperationStatus, MTYPE_CLIENT_FLAT_TX_STATUS_REQUEST> {};
struct TBusSchemeDescribe : TBusMessage<TBusSchemeDescribe, NKikimrClient::TSchemeDescribe, MTYPE_CLIENT_FLAT_DESCRIBE_REQUEST> {};
struct TBusOldFlatDescribeRequest : TBusMessage<TBusOldFlatDescribeRequest, NKikimrClient::TSchemeDescribe, MTYPE_CLIENT_OLD_FLAT_DESCRIBE_REQUEST> {};
struct TBusOldFlatDescribeResponse : TBusMessage<TBusOldFlatDescribeResponse, NKikimrClient::TFlatDescribeResponse, MTYPE_CLIENT_OLD_FLAT_DESCRIBE_RESPONSE> {};
struct TBusBlobStorageConfigRequest : TBusMessage<TBusBlobStorageConfigRequest, NKikimrClient::TBlobStorageConfigRequest, MTYPE_CLIENT_BLOB_STORAGE_CONFIG_REQUEST> {};
struct TBusDrainNode : TBusMessage<TBusDrainNode, NKikimrClient::TDrainNodeRequest, MTYPE_CLIENT_DRAIN_NODE> {};
struct TBusFillNode : TBusMessage<TBusFillNode, NKikimrClient::TFillNodeRequest, MTYPE_CLIENT_FILL_NODE> {};
struct TBusResolveNode : TBusMessage<TBusResolveNode, NKikimrClient::TResolveNodeRequest, MTYPE_CLIENT_RESOLVE_NODE> {};
struct TBusNodeRegistrationRequest : TBusMessage<TBusNodeRegistrationRequest, NKikimrClient::TNodeRegistrationRequest, MTYPE_CLIENT_NODE_REGISTRATION_REQUEST> {};
struct TBusNodeRegistrationResponse : TBusMessage<TBusNodeRegistrationResponse, NKikimrClient::TNodeRegistrationResponse, MTYPE_CLIENT_NODE_REGISTRATION_RESPONSE> {};
struct TBusCmsRequest : TBusMessage<TBusCmsRequest, NKikimrClient::TCmsRequest, MTYPE_CLIENT_CMS_REQUEST> {};
struct TBusCmsResponse : TBusMessage<TBusCmsResponse, NKikimrClient::TCmsResponse, MTYPE_CLIENT_CMS_RESPONSE> {};
struct TBusChooseProxy : TBusMessage<TBusChooseProxy, NKikimrClient::TChooseProxyRequest, MTYPE_CLIENT_CHOOSE_PROXY> {};
struct TBusSqsRequest : TBusMessage<TBusSqsRequest, NKikimrClient::TSqsRequest, MTYPE_CLIENT_SQS_REQUEST> {};
struct TBusSqsResponse : TBusMessage<TBusSqsResponse, NKikimrClient::TSqsResponse, MTYPE_CLIENT_SQS_RESPONSE> {};
struct TBusStreamRequest : TBusMessage<TBusStreamRequest, NKikimrClient::TRequest, MTYPE_CLIENT_STREAM_REQUEST> {};
struct TBusInterconnectDebug : TBusMessage<TBusInterconnectDebug, NKikimrClient::TInterconnectDebug, MTYPE_CLIENT_INTERCONNECT_DEBUG> {};
struct TBusConsoleRequest : TBusMessage<TBusConsoleRequest, NKikimrClient::TConsoleRequest, MTYPE_CLIENT_CONSOLE_REQUEST> {};
struct TBusConsoleResponse : TBusMessage<TBusConsoleResponse, NKikimrClient::TConsoleResponse, MTYPE_CLIENT_CONSOLE_RESPONSE> {};
struct TBusTestShardControlRequest : TBusMessage<TBusTestShardControlRequest, NKikimrClient::TTestShardControlRequest, MTYPE_CLIENT_TEST_SHARD_CONTROL> {};

class TBusResponseStatus : public TBusResponse {
public:
    TBusResponseStatus(EResponseStatus status, const TString& text = TString())
    {
        Record.SetStatus(status);
        if (text) {
            Record.SetErrorReason(text);
        }
    }
};

class TProtocol : public NBus::TBusBufferProtocol {
protected:
    THashMap<TString, NBus::TBusBufferBase*> NameToType;

public:
    static TString TrimMessageName(const TString& name) {
        std::size_t pos = name.rfind(':');
        if (pos != TString::npos && pos + 5 <= name.size() && name.substr(pos + 1, 4) == "TBus")
            return name.substr(pos + 5); // removing leading 'TBus' letters
        return name;
    }

    template <typename BusMessageType>
    void RegisterType(BusMessageType* message) {
        NameToType[TrimMessageName(TypeName<BusMessageType>())] = message;
        NBus::TBusBufferProtocol::RegisterType(message);
    }

    TAutoPtr<NBus::TBusBufferBase> NewMessage(const TString& name) const {
        auto it = NameToType.find(name);
        if (it != NameToType.end()) {
            return it->second->New();
        }
        return nullptr;
    }

    TProtocol(int port)
        : NBus::TBusBufferProtocol("CLI_MB", port)
    {
        RegisterType(new TBusRequest);
        RegisterType(new TBusResponse);
        RegisterType(new TBusFakeConfigDummy);
        RegisterType(new TBusSchemeInitRoot);
        RegisterType(new TBusTypesRequest);
        RegisterType(new TBusTypesResponse);
        RegisterType(new TBusHiveCreateTablet);
        RegisterType(new TBusOldHiveCreateTablet);
        RegisterType(new TBusHiveCreateTabletResult);
        RegisterType(new TBusLocalEnumerateTablets);
        RegisterType(new TBusOldLocalEnumerateTablets);
        RegisterType(new TBusLocalEnumerateTabletsResult);
        RegisterType(new TBusKeyValue);
        RegisterType(new TBusOldKeyValue);
        RegisterType(new TBusKeyValueResponse);
        RegisterType(new TBusPersQueue);
        RegisterType(new TBusTabletKillRequest);
        RegisterType(new TBusTabletStateRequest);
        RegisterType(new TBusTabletCountersRequest);
        RegisterType(new TBusTabletLocalMKQL);
        RegisterType(new TBusTabletLocalSchemeTx);
        RegisterType(new TBusSchemeOperation);
        RegisterType(new TBusSchemeOperationStatus);
        RegisterType(new TBusSchemeDescribe);
        RegisterType(new TBusOldFlatDescribeRequest);
        RegisterType(new TBusOldFlatDescribeResponse);
        RegisterType(new TBusBlobStorageConfigRequest);
        RegisterType(new TBusDrainNode);
        RegisterType(new TBusFillNode);
        RegisterType(new TBusResolveNode);
        RegisterType(new TBusNodeRegistrationRequest);
        RegisterType(new TBusNodeRegistrationResponse);
        RegisterType(new TBusCmsRequest);
        RegisterType(new TBusCmsResponse);
        RegisterType(new TBusChooseProxy);
        RegisterType(new TBusStreamRequest);
        RegisterType(new TBusInterconnectDebug);
        RegisterType(new TBusConsoleRequest);
        RegisterType(new TBusConsoleResponse);
        RegisterType(new TBusTestShardControlRequest);
    }

    const static ui32 DefaultPort = 2134;
};

class IMessageBusServer {
public:
    virtual NActors::IActor* CreateProxy() = 0;
    virtual NActors::IActor* CreateMessageBusTraceService() = 0;
    virtual ~IMessageBusServer() {}
};

class IPersQueueGetReadSessionsInfoWorkerFactory;

IMessageBusServer* CreateMsgBusServer(
    NBus::TBusMessageQueue *queue,
    const NBus::TBusServerSessionConfig &config,
    ui32 bindPort = TProtocol::DefaultPort
);

inline NActors::TActorId CreateMsgBusProxyId() {
    return NActors::TActorId(0, "MsgBusProxy");
}

}
}
