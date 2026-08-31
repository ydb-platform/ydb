#pragma once

#include "events.h"

#include <ydb/core/persqueue/writer/writer.h>
#include <ydb/library/actors/wilson/wilson_span.h>

#include <util/generic/deque.h>
#include <util/generic/ptr.h>

#include <optional>

namespace NKikimr::NPQ::NDataplane::NWrite {

struct TWriteRequestInfo : public TSimpleRefCount<TWriteRequestInfo> {
    using TPtr = TIntrusivePtr<TWriteRequestInfo>;

    struct TUserWriteRequest {
        TVector<TWriteSessionMessage> Messages;
        std::optional<std::pair<TString, TString>> Tx;
        std::optional<TDeferredPublishWriterOpts> DeferredPublish;
        ui64 UserRequestByteSize = 0;
    };

    explicit TWriteRequestInfo(ui64 cookie, NWilson::TSpan span);

    void StartQuotaSpan();
    void SetSpanParamRequestedQuota();
    std::pair<TString, TString> GetTransactionId() const;
    std::optional<TDeferredPublishWriterOpts> GetDeferredPublishOpts() const;

    TDeque<TUserWriteRequest> UserWriteRequests;
    THolder<TEvPartitionWriter::TEvWriteRequest> PartitionWriteRequest;
    ui64 Cookie;
    ui64 ByteSize;
    ui64 RequiredQuota;
    NWilson::TSpan QuotaSpan;
    NWilson::TSpan Span;
};

} // namespace NKikimr::NPQ::NDataplane::NWrite
