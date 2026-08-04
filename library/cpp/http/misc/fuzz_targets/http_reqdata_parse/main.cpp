#include <library/cpp/http/misc/httpdate.h>
#include <library/cpp/http/misc/httpreqdata.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

namespace {

void FuzzRequestData(FuzzedDataProvider& fdp) {
    const TString rawRequest = fdp.ConsumeRandomLengthString(2048);
    const TString extraQuery = fdp.ConsumeRandomLengthString(256);
    const TString headerName = fdp.ConsumeRandomLengthString(64);
    const TString headerValue = fdp.ConsumeRandomLengthString(256);
    const TString remoteAddr = fdp.ConsumeRandomLengthString(128);
    const TString hostHeader = fdp.ConsumeRemainingBytesAsString();

    TServerRequestData requestData;
    (void)requestData.Parse(rawRequest);
    requestData.Scan();

    if (!extraQuery.empty()) {
        requestData.AppendQueryString(extraQuery);
        requestData.Scan();
    }

    if (!headerName.empty()) {
        requestData.AddHeader(headerName, headerValue);
    }
    if (!hostHeader.empty()) {
        requestData.AddHeader("Host", hostHeader);
    }
    if (!remoteAddr.empty()) {
        requestData.SetRemoteAddr(remoteAddr);
        (void)requestData.RemoteAddr();
    }

    (void)requestData.ServerName();
    (void)requestData.ServerPort();
    (void)requestData.ScriptName();
    (void)requestData.Query();
    (void)requestData.OrigQuery();
    (void)requestData.GetCurPage();
    (void)requestData.HeadersIn();
    (void)requestData.HeadersCount();
    (void)requestData.RequestBeginTime();

    if (!headerName.empty()) {
        (void)requestData.HeaderIn(headerName);
        try {
            (void)requestData.HeaderInOrEmpty(headerName);
        } catch (...) {
        }
    }

    const size_t headerCount = requestData.HeadersCount();
    for (size_t i = 0; i < headerCount && i < 8; ++i) {
        (void)requestData.HeaderByIndex(i);
    }

    const time_t parsedDate = parse_http_date(rawRequest);
    if (parsedDate != BAD_DATE) {
        char buffer[64];
        (void)format_http_date(buffer, sizeof(buffer), parsedDate);
        (void)FormatHttpDate(parsedDate);
    }

    requestData.Clear();
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    try {
        FuzzedDataProvider fdp(data, size);
        FuzzRequestData(fdp);
    } catch (...) {
    }

    return 0;
}
