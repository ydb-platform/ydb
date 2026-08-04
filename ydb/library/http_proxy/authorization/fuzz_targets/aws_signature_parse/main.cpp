#include <ydb/library/http_proxy/authorization/signature.h>
#include <util/generic/string.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    TString input((const char*)data, size);
    try {
        NKikimr::NSQS::TAwsRequestSignV4 sign(input);
        if (!sign.Empty()) {
            auto canonical = sign.GetCanonicalRequest();
            (void)canonical;
            auto stringToSign = sign.GetStringToSign();
            (void)stringToSign;
            auto parsedSig = sign.GetParsedSignature();
            (void)parsedSig;
            auto timestamp = sign.GetSigningTimestamp();
            (void)timestamp;
            auto region = sign.GetRegion();
            (void)region;
            auto keyId = sign.GetAccessKeyId();
            (void)keyId;
        }
    } catch (...) {}
    return 0;
}
