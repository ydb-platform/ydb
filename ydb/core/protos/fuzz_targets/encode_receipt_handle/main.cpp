#include <ydb/core/ymq/base/helpers.h>
#include <cstring>

using namespace NKikimr::NSQS;

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    try {
        TReceipt receipt;
        ui64 offset = 0;
        const size_t offsetBytes = size < sizeof(offset) ? size : sizeof(offset);
        std::memcpy(&offset, data, offsetBytes);
        receipt.SetOffset(offset);

        if (size > offsetBytes) {
            receipt.SetMessageGroupId(TString(
                reinterpret_cast<const char*>(data + offsetBytes),
                size - offsetBytes
            ));
        }

        TString encoded = EncodeReceiptHandle(receipt);
        (void)encoded;
    } catch (...) {
    }

    return 0;
}
