#include <ydb/public/sdk/cpp/src/library/grpc/client/grpc_client_low.h>

#include <util/datetime/base.h>
#include <util/generic/maybe.h>

#include <ydb/core/grpc_caching/request_data.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/system/yassert.h>

namespace {

TString SmallToken(FuzzedDataProvider& provider) {
    TString value = provider.ConsumeRandomLengthString(provider.ConsumeIntegralInRange<size_t>(0, 64));
    for (char& ch : value) {
        const ui8 raw = static_cast<ui8>(ch);
        ch = char('a' + raw % 26);
    }
    return value;
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    FuzzedDataProvider provider(data, size);

    NKikimr::TRequestIdData requestId;
    if (provider.ConsumeBool()) {
        requestId.RequestId = SmallToken(provider);
    }

    TMaybe<NKikimr::TRequestIdData> cached;
    if (provider.ConsumeBool()) {
        cached.ConstructInPlace();
        cached->RequestId = SmallToken(provider);
    }

    const TString original = requestId.RequestId;
    requestId.FillCachedRequestData(cached);
    if (original.empty()) {
        if (cached.Defined()) {
            Y_ABORT_UNLESS(requestId.RequestId == cached->RequestId);
        } else {
            Y_ABORT_UNLESS(!requestId.RequestId.empty());
        }
    } else {
        Y_ABORT_UNLESS(requestId.RequestId == original);
    }

    const auto requestMeta = requestId.FillCallMeta();
    Y_ABORT_UNLESS(requestMeta.Aux.size() == 1);
    Y_ABORT_UNLESS(requestMeta.Aux[0].first == NKikimr::REQUEST_ID_METADATA_NAME);
    Y_ABORT_UNLESS(requestMeta.Aux[0].second == requestId.RequestId);

    NKikimr::TTicketData ticket;
    ticket.Ticket = SmallToken(provider);
    ticket.FillCachedRequestData(Nothing());
    const auto ticketMeta = ticket.FillCallMeta();
    Y_ABORT_UNLESS(ticketMeta.Aux.size() == 1);
    Y_ABORT_UNLESS(ticketMeta.Aux[0].first == "authorization");
    Y_ABORT_UNLESS(ticketMeta.Aux[0].second == "Bearer " + ticket.Ticket);

    return 0;
}
