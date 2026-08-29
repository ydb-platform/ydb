#include "write_request_info.h"

#include <ydb/core/persqueue/deferred_publish/constants.h>
#include <ydb/library/wilson_ids/wilson.h>

namespace NKikimr::NPQ::NDataplane::NWrite {

TWriteRequestInfo::TWriteRequestInfo(ui64 cookie, NWilson::TSpan span)
    : PartitionWriteRequest(new TEvPartitionWriter::TEvWriteRequest(cookie))
    , Cookie(cookie)
    , ByteSize(0)
    , RequiredQuota(0)
    , Span(std::move(span))
{
}

void TWriteRequestInfo::StartQuotaSpan() {
    QuotaSpan = NWilson::TSpan(TWilsonTopic::TopicDetailed, Span.GetTraceId(), "RequestQuota");
}

void TWriteRequestInfo::SetSpanParamRequestedQuota() {
    QuotaSpan.Attribute("quota", static_cast<i64>(RequiredQuota));
}

std::pair<TString, TString> TWriteRequestInfo::GetTransactionId() const {
    AFL_ENSURE(!UserWriteRequests.empty());
    if (const auto& tx = UserWriteRequests.front().Tx) {
        return *tx;
    }
    if (UserWriteRequests.front().DeferredPublish) {
        return {"", NDeferredPublish::MakeDeferredPublishWriterKey(
            UserWriteRequests.front().DeferredPublish->IntPublicationId)};
    }
    return {"", ""};
}

std::optional<TDeferredPublishWriterOpts> TWriteRequestInfo::GetDeferredPublishOpts() const {
    AFL_ENSURE(!UserWriteRequests.empty());
    return UserWriteRequests.front().DeferredPublish;
}

} // namespace NKikimr::NPQ::NDataplane::NWrite
