#pragma once
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/grpc/client/grpc_client_low.h>

namespace NNebiusCloud {

template <typename TEv, ui32 TEventType, typename TProtoMessage>
struct TEvGrpcProtoRequest : NActors::TEventLocal<TEv, TEventType> {
    TProtoMessage Request;
    TString Token;
    TString RequestId;
    std::unordered_map<TString, TString> Headers;
};

template <typename TEv, ui32 TEventType, typename TProtoMessage>
struct TEvGrpcProtoResponse : NActors::TEventLocal<TEv, TEventType> {
    THolder<NActors::IEventHandle> Request;
    TProtoMessage Response;
    NYdbGrpc::TGrpcStatus Status;
};

}
