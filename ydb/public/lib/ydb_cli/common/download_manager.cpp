#include "download_manager.h"

#include <library/cpp/http/simple/http_client.h>
#include <library/cpp/string_utils/url/url.h>

#include <util/stream/file.h>
#include <util/string/ascii.h>
#include <util/string/builder.h>

namespace NYdb {
namespace NConsoleClient {

namespace {

// Stream wrapper that tracks write progress and calls callback
class TProgressOutputStream : public IOutputStream {
public:
    TProgressOutputStream(IOutputStream* output, ui64 totalSize, TDownloadProgressCallback callback)
        : Output(output)
        , TotalSize(totalSize)
        , Callback(std::move(callback))
        , LastProgressUpdate(TInstant::Now())
    {
    }

    ui64 GetWrittenBytes() const {
        return WrittenBytes;
    }

private:
    void DoWrite(const void* buf, size_t len) override {
        Output->Write(buf, len);
        WrittenBytes += len;
        UpdateProgress();
    }

    void DoFlush() override {
        Output->Flush();
    }

    void UpdateProgress() {
        if (!Callback) {
            return;
        }

        TInstant now = TInstant::Now();
        TDuration elapsed = now - LastProgressUpdate;

        // Update progress at most 30 times per second
        if (elapsed < TDuration::MilliSeconds(33) && WrittenBytes < TotalSize) {
            return;
        }
        LastProgressUpdate = now;

        Callback(WrittenBytes, TotalSize);
    }

    IOutputStream* Output;
    ui64 TotalSize;
    TDownloadProgressCallback Callback;
    ui64 WrittenBytes = 0;
    TInstant LastProgressUpdate;
};

} // namespace

TDownloadResult DownloadFile(
    const TString& url,
    const TString& destinationPath,
    TDownloadProgressCallback progressCallback,
    TDuration connectTimeout,
    TDuration socketTimeout)
{
    TDownloadResult result;

    try {
        // Parse URL to extract scheme, host, port, and path
        TStringBuf scheme;
        TStringBuf host;
        ui16 port = 0;

        GetSchemeHostAndPort(url, scheme, host, port);

        TStringBuf pathAndQuery = GetPathAndQuery(url);
        if (pathAndQuery.empty()) {
            pathAndQuery = "/";
        }

        // Determine if HTTPS
        bool isHttps = scheme.StartsWith("https");
        if (port == 0) {
            port = isHttps ? 443 : 80;
        }

        // Build host string with scheme for TKeepAliveHttpClient
        TString hostWithScheme = TStringBuilder() << scheme << host;

        TKeepAliveHttpClient client(hostWithScheme, port, socketTimeout, connectTimeout, false);
        if (isHttps) {
            // Disable verification for simplicity, as we're downloading from trusted storage
            client.DisableVerificationForHttps();
        }

        // First, do a HEAD request to get Content-Length
        THttpHeaders responseHeaders;
        ui64 contentLength = 0;

        try {
            unsigned code = client.DoRequest(
                "HEAD",
                pathAndQuery,
                {},
                nullptr,
                {},
                &responseHeaders);

            if (code >= 200 && code < 300) {
                for (const auto& header : responseHeaders) {
                    if (AsciiEqualsIgnoreCase(header.Name(), "Content-Length")) {
                        contentLength = FromString<ui64>(header.Value());
                        break;
                    }
                }
            }
        } catch (...) {
            // HEAD request failed, continue without content length
            contentLength = 0;
        }

        // Reset connection after HEAD request
        client.ResetConnection();

        // Now do the actual GET request
        TFileOutput fileOutput(destinationPath);

        if (progressCallback) {
            TProgressOutputStream progressOutput(&fileOutput, contentLength, progressCallback);

            unsigned code = client.DoGet(
                pathAndQuery,
                &progressOutput,
                {},
                nullptr);

            if (code < 200 || code >= 300) {
                result.ErrorMessage = TStringBuilder() << "HTTP error: " << code;
                return result;
            }

            result.BytesDownloaded = progressOutput.GetWrittenBytes();
        } else {
            unsigned code = client.DoGet(
                pathAndQuery,
                &fileOutput,
                {},
                nullptr);

            if (code < 200 || code >= 300) {
                result.ErrorMessage = TStringBuilder() << "HTTP error: " << code;
                return result;
            }
        }

        fileOutput.Finish();
        result.Success = true;

    } catch (const THttpRequestException& e) {
        result.ErrorMessage = TStringBuilder()
            << "HTTP request failed: " << e.what()
            << " (status code: " << e.GetStatusCode() << ")";
    } catch (const yexception& e) {
        result.ErrorMessage = TStringBuilder() << "Download failed: " << e.what();
    } catch (const std::exception& e) {
        result.ErrorMessage = TStringBuilder() << "Download failed: " << e.what();
    }

    return result;
}

} // namespace NConsoleClient
} // namespace NYdb
