#include "msgbus_server.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/scheme/scheme_type_registry.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/core/client/metadata/types_metadata.h>
#include <ydb/core/client/metadata/functions_metadata.h>

#include <util/digest/numeric.h>


namespace NKikimr {
namespace NMsgBusProxy {

class TMessageBusGetTypes : public TActorBootstrapped<TMessageBusGetTypes>, public TMessageBusSessionIdentHolder {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::MSGBUS_COMMON;
    }

    TMessageBusGetTypes(TBusMessageContext &msg, TMaybe<ui64> etag)
        : TMessageBusSessionIdentHolder(msg)
        , Etag(etag)
    {}

    void Bootstrap(const TActorContext& ctx) {
        const auto* typeRegistry = AppData(ctx)->TypeRegistry;
        const auto& functionRegistry = AppData(ctx)->FunctionRegistry->GetBuiltins();
        ui64 typeEtag = typeRegistry->GetMetadataEtag();
        ui64 functionEtag = functionRegistry->GetMetadataEtag();

        ui64 currentEtag = CombineHashes(typeEtag, functionEtag);

        auto reply = new TBusTypesResponse();
        reply->Record.SetStatus(MSTATUS_OK);
        if (!Etag.Defined() || *Etag.Get() != currentEtag) {
            reply->Record.SetETag(currentEtag);
            SerializeMetadata(typeRegistry->GetTypeMetadataRegistry(), reply->Record.MutableTypeMetadata());
            SerializeMetadata(*functionRegistry, reply->Record.MutableFunctionMetadata());
        }

        SendReplyMove(reply);
        return Die(ctx);
    }

private:
    const TMaybe<ui64> Etag;
};

IActor* CreateMessageBusGetTypes(TBusMessageContext &msg) {
    const auto &record = static_cast<TBusTypesRequest *>(msg.GetMessage())->Record;

    const TMaybe<ui64> etag = record.HasETag() ? record.GetETag() : TMaybe<ui64>();
    return new TMessageBusGetTypes(msg, etag);
}
}
}
