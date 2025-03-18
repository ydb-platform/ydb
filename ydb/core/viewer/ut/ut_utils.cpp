#include "ut_utils.h"

namespace NKikimr::NViewerTests {

void WaitForHttpReady(TKeepAliveHttpClient& client) {
    for (int retries = 0;; ++retries) {
        UNIT_ASSERT(retries < 100);
        TStringStream responseStream;
        const TKeepAliveHttpClient::THttpCode statusCode = client.DoGet("/viewer/simple_counter?max_counter=1&period=100", &responseStream);
        const TString response = responseStream.ReadAll();
        if (statusCode == HTTP_OK) {
            break;
        }
    }
}

} // namespace NKikimr::NViewerTests

