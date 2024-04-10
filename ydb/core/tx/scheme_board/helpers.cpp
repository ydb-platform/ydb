#include "helpers.h"

#include <ydb/core/util/pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/subdomains.pb.h>

#include <util/string/join.h>

#include <contrib/libs/protobuf/src/google/protobuf/util/json_util.h>

namespace NKikimr {
namespace NSchemeBoard {

TActorId MakeInterconnectProxyId(const ui32 nodeId) {
    return TActivationContext::InterconnectProxy(nodeId);
}

// NKikimrScheme::TEvDescribeSchemeResult
//

ui64 GetPathVersion(const NKikimrScheme::TEvDescribeSchemeResult& record) {
    if (!record.HasPathDescription()) {
        return 0;
    }

    const auto& pathDescription = record.GetPathDescription();
    if (!pathDescription.HasSelf()) {
        return 0;
    }

    const auto& self = pathDescription.GetSelf();
    if (!self.HasPathVersion()) {
        return 0;
    }

    return self.GetPathVersion();
}

// Object path-id is determined from TDescribeSchemeResult with backward compatibility support.
// DescribeSchemeResult.PathOwnerId should be used.
// DescribeSchemeResult.PathDescription.Self.SchemeshardId is deprecated.
// DescribeSchemeResult.PathOwnerId takes preference.
TPathId GetPathId(const NKikimrScheme::TEvDescribeSchemeResult &record) {
    if (record.HasPathId() && record.HasPathOwnerId()) {
        return TPathId(record.GetPathOwnerId(), record.GetPathId());
    }

    // for compatibility look deeper if PathOwnerId hasn't been set
    if (!record.HasPathDescription()) {
        return TPathId();
    }

    const auto& pathDescription = record.GetPathDescription();
    if (!pathDescription.HasSelf()) {
        return TPathId();
    }

    const auto& self = pathDescription.GetSelf();

    return TPathId(self.GetSchemeshardId(), self.GetPathId());
}

TDomainId GetDomainId(const NKikimrScheme::TEvDescribeSchemeResult &record) {
    if (!record.HasPathDescription()) {
        return TDomainId();
    }

    const auto& pathDescription = record.GetPathDescription();
    if (!pathDescription.HasDomainDescription()) {
        return TDomainId();
    }

    const auto& domainKey = pathDescription.GetDomainDescription().GetDomainKey();

    return TDomainId(domainKey.GetSchemeShard(), domainKey.GetPathId());
}

TSet<ui64> GetAbandonedSchemeShardIds(const NKikimrScheme::TEvDescribeSchemeResult &record) {
    if (!record.HasPathDescription()) {
        return {};
    }

    const auto& pathDescription = record.GetPathDescription();
    return TSet<ui64>(
        pathDescription.GetAbandonedTenantsSchemeShards().begin(),
        pathDescription.GetAbandonedTenantsSchemeShards().end()
    );
}

// NKikimrSchemeBoard::TEvUpdate
//

ui64 GetPathVersion(const NKikimrSchemeBoard::TEvUpdate& record) {
    return record.GetPathDirEntryPathVersion();
}

TSet<ui64> GetAbandonedSchemeShardIds(const NKikimrSchemeBoard::TEvUpdate& record) {
    return TSet<ui64>(
        record.GetPathAbandonedTenantsSchemeShards().begin(),
        record.GetPathAbandonedTenantsSchemeShards().end()
    );
}

// NKikimrSchemeBoard::TEvNotify
//

ui64 GetPathVersion(const NKikimrSchemeBoard::TEvNotify& record) {
    return record.GetVersion();
}

NSchemeBoard::TDomainId GetDomainId(const NKikimrSchemeBoard::TEvNotify& record) {
    return PathIdFromPathId(record.GetPathSubdomainPathId());
}

TSet<ui64> GetAbandonedSchemeShardIds(const NKikimrSchemeBoard::TEvNotify& record) {
    return TSet<ui64>(
        record.GetPathAbandonedTenantsSchemeShards().begin(),
        record.GetPathAbandonedTenantsSchemeShards().end()
    );
}

// Assorted methods
//
TIntrusivePtr<TEventSerializedData> SerializeEvent(IEventBase* ev) {
    TAllocChunkSerializer serializer;
    Y_ABORT_UNLESS(ev->SerializeToArcadiaStream(&serializer));
    return serializer.Release(ev->CreateSerializationInfo());
}

void MultiSend(const TVector<const TActorId*>& recipients, const TActorId& sender, TAutoPtr<IEventBase> ev, ui32 flags, ui64 cookie) {
    auto buffer = SerializeEvent(ev.Get());
    for (const TActorId* recipient : recipients) {
        TlsActivationContext->Send(new IEventHandle(
            ev->Type(), flags, *recipient, sender, buffer, cookie
        ));
    }
}

// Work with TEvDescribeSchemeResult and its parts
//

TString SerializeDescribeSchemeResult(const NKikimrScheme::TEvDescribeSchemeResult& proto) {
    TString serialized;
    Y_PROTOBUF_SUPPRESS_NODISCARD proto.SerializeToString(&serialized);
    return serialized;
}

TString SerializeDescribeSchemeResult(const TString& preSerializedPart, const NKikimrScheme::TEvDescribeSchemeResult& protoPart) {
    return Join("", preSerializedPart, SerializeDescribeSchemeResult(protoPart));
}

NKikimrScheme::TEvDescribeSchemeResult DeserializeDescribeSchemeResult(const TString& serialized) {
    NKikimrScheme::TEvDescribeSchemeResult result;
    Y_ABORT_UNLESS(ParseFromStringNoSizeLimit(result, serialized));
    return result;
}

NKikimrScheme::TEvDescribeSchemeResult* DeserializeDescribeSchemeResult(const TString& serialized, google::protobuf::Arena* arena) {
    auto* proto = google::protobuf::Arena::CreateMessage<NKikimrScheme::TEvDescribeSchemeResult>(arena);
    Y_ABORT_UNLESS(ParseFromStringNoSizeLimit(*proto, serialized));
    return proto;
}

TString JsonFromDescribeSchemeResult(const TString& serialized) {
    using namespace google::protobuf::util;

    google::protobuf::Arena arena;
    const auto* proto = DeserializeDescribeSchemeResult(serialized, &arena);

    JsonPrintOptions opts;
    opts.preserve_proto_field_names = true;

    TString json;
    MessageToJsonString(*proto, &json, opts);

    return json;
}

} // NSchemeBoard
} // NKikimr
