#include "helpers.h"

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <google/protobuf/wire_format_lite.h>

#include <library/cpp/actors/core/executor_thread.h>
#include <library/cpp/actors/core/interconnect.h>

#include <util/stream/format.h>

namespace NKikimr {
namespace NSchemeBoard {

TActorId MakeInterconnectProxyId(const ui32 nodeId) {
    return TActivationContext::InterconnectProxy(nodeId);
}

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

TIntrusivePtr<TEventSerializedData> SerializeEvent(IEventBase* ev) {
    TAllocChunkSerializer serializer;
    Y_VERIFY(ev->SerializeToArcadiaStream(&serializer));
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

TString PreSerializedProtoField(TString data, int fieldNo) {
    using CodedOutputStream = google::protobuf::io::CodedOutputStream;
    using StringOutputStream = google::protobuf::io::StringOutputStream;
    using WireFormat = google::protobuf::internal::WireFormatLite;

    TString key;
    {
        StringOutputStream stream(&key);
        CodedOutputStream output(&stream);
        WireFormat::WriteTag(fieldNo, WireFormat::WireType::WIRETYPE_LENGTH_DELIMITED, &output);
        output.WriteVarint32(data.size());
    }

    data.prepend(key);
    return data;
}

} // NSchemeBoard
} // NKikimr
