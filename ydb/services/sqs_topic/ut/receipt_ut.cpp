#include <ydb/core/http_proxy/sqs_serialization.h>
#include <ydb/services/sqs_topic/receipt.h>

#include <library/cpp/cgiparam/cgiparam.h>
#include <library/cpp/string_utils/quote/quote.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/string/builder.h>

#include <optional>

using namespace NKikimr::NSqsTopic::V1;

namespace {

    // CGIEscape, applied by the query-protocol parser, rewrites '+' and '='.
    bool HasCgiSpecial(const TString& receipt) {
        return receipt.find('+') != TString::npos || receipt.find('=') != TString::npos;
    }

    std::optional<NKikimr::NPQ::NMLP::TMessageId> FindMessageId(bool wantsCgiSpecial) {
        for (ui32 partition = 0; partition < 4; ++partition) {
            for (ui64 offset = 0; offset < 20000; ++offset) {
                const NKikimr::NPQ::NMLP::TMessageId id{partition, offset};
                if (HasCgiSpecial(SerializeReceipt(id)) == wantsCgiSpecial) {
                    return id;
                }
            }
        }
        return std::nullopt;
    }

    // Same path as a form-urlencoded DeleteMessage: the proxy URL-decodes the body,
    // then DeserializeXml CGI-escapes ReceiptHandle before the service decodes it.
    void ExpectDeleteRoundTrip(const NKikimr::NPQ::NMLP::TMessageId& id) {
        const TString receipt = SerializeReceipt(id);
        const auto direct = DeserializeReceipt(receipt);
        UNIT_ASSERT_C(direct.has_value(), direct.error());
        UNIT_ASSERT_VALUES_EQUAL(direct->PartitionId, id.PartitionId);
        UNIT_ASSERT_VALUES_EQUAL(direct->Offset, id.Offset);

        const TString body = TStringBuilder()
            << "Action=DeleteMessage"
            << "&QueueUrl=" << CGIEscapeRet("https://sqs.example/123/queue")
            << "&ReceiptHandle=" << CGIEscapeRet(receipt);
        TCgiParameters cgi(body);

        Ydb::Ymq::V1::DeleteMessageRequest request;
        NKikimr::NHttpProxy::NSQS::DeserializeXml(request, cgi);

        const auto decoded = DeserializeReceipt(request.receipt_handle());
        UNIT_ASSERT_C(decoded.has_value(),
            "receipt '" << receipt << "' was parsed as '" << request.receipt_handle()
            << "': " << decoded.error());
        UNIT_ASSERT_VALUES_EQUAL(decoded->PartitionId, id.PartitionId);
        UNIT_ASSERT_VALUES_EQUAL(decoded->Offset, id.Offset);
    }

} // namespace

Y_UNIT_TEST_SUITE(SqsTopicReceiptHandle) {
    Y_UNIT_TEST(QueryProtocolKeepsCgiSafeReceipt) {
        const auto id = FindMessageId(false);
        UNIT_ASSERT_C(id.has_value(), "no receipt without '+' or '='");
        ExpectDeleteRoundTrip(*id);
    }

    Y_UNIT_TEST(QueryProtocolKeepsReceiptWithPlusAndEquals) {
        std::optional<NKikimr::NPQ::NMLP::TMessageId> id;
        for (ui32 partition = 0; partition < 4 && !id; ++partition) {
            for (ui64 offset = 0; offset < 20000; ++offset) {
                const NKikimr::NPQ::NMLP::TMessageId candidate{partition, offset};
                const TString receipt = SerializeReceipt(candidate);
                if (receipt.find('+') != TString::npos && receipt.find('=') != TString::npos) {
                    id = candidate;
                    break;
                }
            }
        }
        UNIT_ASSERT_C(id.has_value(), "no receipt containing both '+' and '='");
        ExpectDeleteRoundTrip(*id);
    }
}
