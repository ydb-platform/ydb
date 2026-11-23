#include <ydb/core/http_proxy/sqs_serialization.h>

#include <library/cpp/cgiparam/cgiparam.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/string/builder.h>

#include <array>

namespace NKikimr::NHttpProxy::NSQS {
namespace {

struct TReceiptCase {
    TStringBuf Encoded;
    TStringBuf Decoded;
};

// Query clients percent-encode the form body. The protobuf must contain the
// original opaque receipt, including standard Base64 characters used by MLP.
constexpr std::array<TReceiptCase, 6> Receipts{{
    {"CAAQ%2BLoG", "CAAQ+LoG"},
    {"CAAQ%2BAE%3D", "CAAQ+AE="},
    {"CAAQ%2FAE%3D", "CAAQ/AE="},
    {"CAAQAA%3D%3D", "CAAQAA=="},
    {"legacy-url_safe-handle", "legacy-url_safe-handle"},
    {"opaque%252Bhandle", "opaque%2Bhandle"},
}};

template <typename TRequest>
void CheckSingleReceipt() {
    for (const auto& receipt : Receipts) {
        const TCgiParameters form(TStringBuilder()
            << "QueueUrl=test-queue&VisibilityTimeout=30&ReceiptHandle=" << receipt.Encoded);
        TRequest request;
        DeserializeXml(request, form);
        UNIT_ASSERT_VALUES_EQUAL(request.receipt_handle(), receipt.Decoded);
        UNIT_ASSERT_VALUES_EQUAL(request.queue_url(), "test-queue");
    }
}

template <typename TRequest>
void CheckBatchReceipts(TStringBuf prefix) {
    TStringBuilder body;
    body << "QueueUrl=test-queue";
    for (size_t i = 0; i < Receipts.size(); ++i) {
        body << '&' << prefix << '.' << i + 1 << ".Id=id-" << i
             << '&' << prefix << '.' << i + 1 << ".ReceiptHandle=" << Receipts[i].Encoded
             << '&' << prefix << '.' << i + 1 << ".VisibilityTimeout=30";
    }
    const TCgiParameters form(body);
    TRequest request;
    DeserializeXml(request, form);
    UNIT_ASSERT_VALUES_EQUAL(request.entries_size(), Receipts.size());
    for (size_t i = 0; i < Receipts.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(request.entries(i).id(), TStringBuilder() << "id-" << i);
        UNIT_ASSERT_VALUES_EQUAL(request.entries(i).receipt_handle(), Receipts[i].Decoded);
    }
}

} // namespace

Y_UNIT_TEST_SUITE(SqsXmlReceipt) {
    Y_UNIT_TEST(DeleteMessagePreservesReceipt) {
        CheckSingleReceipt<Ydb::Ymq::V1::DeleteMessageRequest>();
    }

    Y_UNIT_TEST(DeleteMessageBatchPreservesReceipts) {
        CheckBatchReceipts<Ydb::Ymq::V1::DeleteMessageBatchRequest>("DeleteMessageBatchRequestEntry");
    }

    Y_UNIT_TEST(ChangeMessageVisibilityPreservesReceipt) {
        CheckSingleReceipt<Ydb::Ymq::V1::ChangeMessageVisibilityRequest>();
    }

    Y_UNIT_TEST(ChangeMessageVisibilityBatchPreservesReceipts) {
        CheckBatchReceipts<Ydb::Ymq::V1::ChangeMessageVisibilityBatchRequest>("ChangeMessageVisibilityBatchRequestEntry");
    }
}

} // namespace NKikimr::NHttpProxy::NSQS
