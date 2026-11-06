#pragma once

#include "backup_mock.h"

#include <library/cpp/http/server/http.h>
#include <library/cpp/cgiparam/cgiparam.h>

#include <atomic>
#include <functional>
#include <memory>

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr {
namespace NWrappers {
namespace NTestHelpers {

class TS3Mock: public TBackupMock, public THttpServer::ICallBack {
public:
    struct TSettings {
        THttpServer::TOptions HttpOptions;
        bool CorruptETags;
        bool RejectUploadParts;
        TString PartialReadPath;
        ui32 PartialReadFailures;
        std::function<TString(TStringBuf method, TStringBuf path)> ErrorResponse;

        TSettings();
        explicit TSettings(ui16 port);

        TSettings& WithHttpOptions(const THttpServer::TOptions& opts);
        TSettings& WithCorruptETags(bool value);
        TSettings& WithRejectUploadParts(bool value);
        TSettings& WithPartialReadFailure(TString path, ui32 count = 1);

    }; // TSettings

    // The objects of the mock and what it does with them, shared by the HTTP server and by the storage operator that
    // serves the mock inside an actor system (see ServeInActorSystem). Paths are "/bucket/key", as in an HTTP request
    // without virtual addressing.
    class TStorage {
    public:
        enum class EStatus {
            Ok,
            NotFound,       // Error is the S3 error code: NoSuchKey, NoSuchUpload
            BadRequest,     // Error is the message
        };

        struct TResult {
            EStatus Status = EStatus::Ok;
            TString Error;
            TString Body;       // what a read returns
            TString ETag;
            ui64 ObjectSize = 0;
            std::pair<ui32, ui32> Range;    // of a read
            TString UploadId;

            static TResult NotFound(TString code);
            static TResult BadRequest(TString message);
        };

        explicit TStorage(const TSettings& settings, THashMap<TString, TString>&& data = {});

        static bool TryParseRange(TStringBuf str, std::pair<ui32, ui32>& range);

        // range: a "bytes=a-b" header value, if any; head: no body
        TResult Read(TStringBuf path, const TString* range, bool head) const;
        TVector<std::pair<TString, size_t>> List(TStringBuf bucketName, const TString& prefix) const;
        TResult Put(TStringBuf path, TString content);
        TResult CreateMultipartUpload(TStringBuf path);
        bool HasMultipartUpload(TStringBuf path, const TString& uploadId) const;
        TResult UploadPart(TStringBuf path, const TString& uploadId, const TString& partNumber, TString content);
        // source: "/bucket/key", sourceRange: a "bytes=a-b" value, both as in x-amz-copy-source(-range)
        TResult UploadPartCopy(TStringBuf path, const TString& uploadId, const TString& partNumber,
            const TString& source, const TString* sourceRange);
        // etags by part number, starting from part 1; empty for a part the request does not name
        TResult CompleteMultipartUpload(TStringBuf path, const TString& uploadId, const TVector<TString>& etags);
        TResult AbortMultipartUpload(TStringBuf path, const TString& uploadId);
        TResult Delete(TStringBuf path);

        bool ShouldFailPartialRead(TStringBuf path);

        const TSettings Settings;
        THashMap<TString, TString> Data;
        std::atomic<ui32> PartialReadFailuresLeft;
        std::atomic<ui32> PartialReadRequestCount = 0;
        std::atomic<ui32> PartialReadFailureCount = 0;

    private:
        TResult PreparePart(TStringBuf path, const TString& uploadId, const TString& partNumber, TString*& partSlot);

        int NextUploadId = 1;
        THashMap<std::pair<TString, TString>, TVector<TString>> MultipartUploads;
    }; // TStorage

private:
    class TRequest: public TRequestReplier {
        enum class EMethod {
            NotImplemented,
            Head,
            Get,
            Put,
            Post,
            Patch,
            Delete,
        };

        static EMethod ParseMethod(const char* str);

        bool HttpBadRequest(const TReplyParams& params, const TString& error = {});
        bool HttpNotFound(const TReplyParams& params, const TString& errorCode = {});
        bool HttpNotImplemented(const TReplyParams& params);
        bool HttpError(const TReplyParams& params, const TStorage::TResult& result);
        void MaybeContinue(const TReplyParams& params);
        bool HttpServeRead(const TReplyParams& params, EMethod method, const TStringBuf path);
        bool HttpServeList(const TReplyParams& params, TStringBuf bucketName, const TString& prefix);
        bool HttpServeWrite(const TReplyParams& params, TStringBuf path, const TCgiParameters& queryParams);
        bool HttpServeAction(const TReplyParams& params, EMethod method, TStringBuf path, const TCgiParameters& queryParams);

    public:
        explicit TRequest(TS3Mock* parent);

        bool DoReply(const TReplyParams& params) override;

    private:
        TS3Mock* const Parent;

    }; // TRequest

public:
    explicit TS3Mock(const TSettings& settings = {});
    explicit TS3Mock(THashMap<TString, TString>&& data, const TSettings& settings = {});
    explicit TS3Mock(const THashMap<TString, TString>& data, const TSettings& settings = {});
    ~TS3Mock();

    TClientRequest* CreateClient() override;
    // Starts the HTTP server and registers the mock as the storage of its endpoint, localhost:<port>, for
    // ServeInActorSystem
    bool Start();
    const char* GetError();

    const THashMap<TString, TString>& GetData() const override { return Storage->Data; }
    THashMap<TString, TString>& GetData() override { return Storage->Data; }

    ui32 GetPartialReadRequestCount() const { return Storage->PartialReadRequestCount.load(); }
    ui32 GetPartialReadFailureCount() const { return Storage->PartialReadFailureCount.load(); }
    // Requests that came over HTTP, not through ServeInActorSystem
    ui32 GetHttpRequestCount() const { return HttpRequestCount.load(); }

    // From now on, S3 settings whose endpoint is that of a started mock are served by the mock inside the actor
    // system of the caller: requests are answered as events of that actor system, with no HTTP and no threads of
    // their own, so a test actor runtime sees the whole exchange and can simulate time through it. Endpoints of no
    // started mock keep going over HTTP. For the whole process.
    static void ServeInActorSystem();

private:
    std::shared_ptr<TStorage> Storage;
    std::atomic<ui32> HttpRequestCount = 0;
    TVector<TString> RegisteredEndpoints;
    THttpServer HttpServer;

}; // TS3Mock

} // NTestHelpers
} // NWrappers
} // NKikimr
