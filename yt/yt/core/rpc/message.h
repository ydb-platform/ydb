#pragma once

#include "public.h"

#include <yt/yt_proto/yt/core/rpc/proto/rpc.pb.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM_WITH_UNDERLYING_TYPE(EMessageType, ui32,
    ((Unknown)              (         0))
    ((Request)              (0x69637072)) // rpci
    ((RequestCancelation)   (0x63637072)) // rpcc
    ((Response)             (0x6f637072)) // rpco
    ((StreamingPayload)     (0x70637072)) // rpcp
    ((StreamingFeedback)    (0x66637072)) // rpcf
);

////////////////////////////////////////////////////////////////////////////////

TSharedRefArray CreateRequestMessage(
    const NProto::TRequestHeader& header,
    TSharedRef body,
    const std::vector<TSharedRef>& attachments);

TSharedRefArray CreateRequestMessage(
    const NProto::TRequestHeader& header,
    const TSharedRefArray& data);

TSharedRefArray CreateRequestCancelationMessage(
    const NProto::TRequestCancelationHeader& header);

TSharedRefArray CreateResponseMessage(
    const NProto::TResponseHeader& header,
    TSharedRef body,
    const std::vector<TSharedRef>& attachments);

TSharedRefArray CreateResponseMessage(
    const ::google::protobuf::MessageLite& body,
    const std::vector<TSharedRef>& attachments = std::vector<TSharedRef>());

TSharedRefArray CreateErrorResponseMessage(
    const NProto::TResponseHeader& header);

TSharedRefArray CreateErrorResponseMessage(
    TRequestId requestId,
    const TError& error);

TSharedRefArray CreateErrorResponseMessage(
    const TError& error);

TSharedRefArray CreateStreamingPayloadMessage(
    const NProto::TStreamingPayloadHeader& header,
    const std::vector<TSharedRef>& attachments);

TSharedRefArray CreateStreamingFeedbackMessage(
    const NProto::TStreamingFeedbackHeader& header);

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NProto::TStreamingParameters* protoParameters,
    const TStreamingParameters& parameters);

void FromProto(
    TStreamingParameters* parameters,
    const NProto::TStreamingParameters& protoParameters);

////////////////////////////////////////////////////////////////////////////////

EMessageType GetMessageType(const TSharedRefArray& message);

[[nodiscard]] bool TryParseRequestHeader(
    const TSharedRefArray& message,
    NProto::TRequestHeader* header);

[[nodiscard]] TSharedRefArray SetRequestHeader(
    const TSharedRefArray& message,
    const NProto::TRequestHeader& header);

[[nodiscard]] bool TryParseResponseHeader(
    const TSharedRefArray& message,
    NProto::TResponseHeader* header);

[[nodiscard]] TSharedRefArray SetResponseHeader(
    const TSharedRefArray& message,
    const NProto::TResponseHeader& header);

void MergeRequestHeaderExtensions(
    NProto::TRequestHeader* to,
    const NProto::TRequestHeader& from);

[[nodiscard]] bool TryParseRequestCancelationHeader(
    const TSharedRefArray& message,
    NProto::TRequestCancelationHeader* header);

[[nodiscard]] bool TryParseStreamingPayloadHeader(
    const TSharedRefArray& message,
    NProto::TStreamingPayloadHeader * header);

[[nodiscard]] bool TryParseStreamingFeedbackHeader(
    const TSharedRefArray& message,
    NProto::TStreamingFeedbackHeader* header);

i64 GetMessageHeaderSize(const TSharedRefArray& message);
i64 GetMessageBodySize(const TSharedRefArray& message);
int GetMessageAttachmentCount(const TSharedRefArray& message);
i64 GetTotalMessageAttachmentSize(const TSharedRefArray& message);

TError CheckBusMessageLimits(const TSharedRefArray& message);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
