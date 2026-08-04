#include <ydb/core/ymq/base/helpers.h>
#include <util/generic/string.h>

using namespace NKikimr::NSQS;

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size == 0) {
        return 0;
    }

    try {
        const TString receiptHandle(reinterpret_cast<const char*>(data), size);
        TReceipt receipt = DecodeReceiptHandle(receiptHandle);
        (void)receipt;
    } catch (...) {
    }

    return 0;
}
