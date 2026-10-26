#include "s3_mock.h"
#include "s3_mock_storage.h"

#include <ydb/core/wrappers/abstract.h>

#include <library/cpp/digest/md5/md5.h>
#include <library/cpp/xml/document/xml-document.h>

#include <util/generic/singleton.h>
#include <util/generic/xrange.h>
#include <util/random/shuffle.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/join.h>
#include <util/string/printf.h>
#include <util/system/mutex.h>

#include <mutex>

namespace NKikimr {
namespace NWrappers {
namespace NTestHelpers {

namespace {

// Started mocks by endpoint, for ServeInActorSystem
struct TStartedMocks {
    TMutex Mutex;
    THashMap<TString, std::weak_ptr<TS3Mock::TStorage>> Storages;

    std::shared_ptr<TS3Mock::TStorage> Find(TStringBuf endpoint) {
        if (!endpoint.SkipPrefix("http://")) {
            endpoint.SkipPrefix("https://");
        }
        TGuard<TMutex> guard(Mutex);
        const auto it = Storages.find(endpoint);
        return it == Storages.end() ? nullptr : it->second.lock();
    }
};

TStartedMocks& StartedMocks() {
    return *Singleton<TStartedMocks>();
}

} // anonymous

TS3Mock::TSettings::TSettings()
    : CorruptETags(false)
    , RejectUploadParts(false)
    , PartialReadFailures(0)
{
}

TS3Mock::TSettings::TSettings(ui16 port)
    : HttpOptions(THttpServer::TOptions(port).SetThreads(1))
    , CorruptETags(false)
    , RejectUploadParts(false)
    , PartialReadFailures(0)
{
}

TS3Mock::TSettings& TS3Mock::TSettings::WithHttpOptions(const THttpServer::TOptions& opts) {
    HttpOptions = opts;
    return *this;
}

TS3Mock::TSettings& TS3Mock::TSettings::WithCorruptETags(bool value) {
    CorruptETags = value;
    return *this;
}

TS3Mock::TSettings& TS3Mock::TSettings::WithRejectUploadParts(bool value) {
    RejectUploadParts = value;
    return *this;
}

TS3Mock::TSettings& TS3Mock::TSettings::WithPartialReadFailure(TString path, ui32 count) {
    PartialReadPath = std::move(path);
    PartialReadFailures = count;
    return *this;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TStorage
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

TS3Mock::TStorage::TResult TS3Mock::TStorage::TResult::NotFound(TString code) {
    TResult result;
    result.Status = EStatus::NotFound;
    result.Error = std::move(code);
    return result;
}

TS3Mock::TStorage::TResult TS3Mock::TStorage::TResult::BadRequest(TString message) {
    TResult result;
    result.Status = EStatus::BadRequest;
    result.Error = std::move(message);
    return result;
}

TS3Mock::TStorage::TStorage(const TSettings& settings, THashMap<TString, TString>&& data)
    : Settings(settings)
    , Data(std::move(data))
    , PartialReadFailuresLeft(settings.PartialReadFailures)
{
}

bool TS3Mock::TStorage::TryParseRange(TStringBuf str, std::pair<ui32, ui32>& range) {
    if (!str.SkipPrefix("bytes=")) {
        return false;
    }

    ui32 start;
    if (!TryFromString(str.NextTok('-'), start)) {
        return false;
    }

    ui32 end;
    if (!TryFromString(str, end)) {
        return false;
    }

    range = std::make_pair(start, end);
    return true;
}

TS3Mock::TStorage::TResult TS3Mock::TStorage::Read(TStringBuf path, const TString* rangeValue, bool head) const {
    const auto it = Data.find(path);
    if (it == Data.end()) {
        return TResult::NotFound("NoSuchKey");
    }

    const TStringBuf content = it->second;
    Cerr << "S3_MOCK::HttpServeRead: " << path << " / " << content.size() << Endl;
    std::pair<ui32, ui32> range(0, content.size() ? content.size() - 1 : 0);
    if (rangeValue) {
        if (!TryParseRange(*rangeValue, range)) {
            return TResult::BadRequest("Invalid range");
        }
        if (range.first > range.second) {
            return TResult::BadRequest("Invalid range borders (from > to)");
        }
    }
    if (range.second >= content.size() && content.size()) {
        return TResult::BadRequest("Invalid range right border");
    }

    TResult result;
    result.ETag = MD5::Data(content);
    if (Settings.CorruptETags) {
        ShuffleRange(result.ETag);
    }
    result.ObjectSize = content.size();
    result.Range = range;
    if (!head) {
        result.Body = TString(content.SubStr(range.first, range.second - range.first + 1));
    }
    return result;
}

TVector<std::pair<TString, size_t>> TS3Mock::TStorage::List(TStringBuf bucketName, const TString& prefix) const {
    Cerr << "S3_MOCK::HttpServeList: " << prefix << Endl;
    TVector<std::pair<TString, size_t>> objects;
    TString bucketPrefix(bucketName);
    if (!bucketPrefix.EndsWith('/')) {
        bucketPrefix += '/';
    }
    const TString keyPrefix = TStringBuilder() << bucketPrefix << prefix;
    for (const auto& [key, value] : Data) {
        if (key.StartsWith(keyPrefix)) {
            objects.emplace_back(key.substr(bucketPrefix.size()), value.size());
        }
    }
    return objects;
}

TS3Mock::TStorage::TResult TS3Mock::TStorage::Put(TStringBuf path, TString content) {
    TResult result;
    result.ETag = MD5::Data(content);
    Data[path] = std::move(content);
    return result;
}

TS3Mock::TStorage::TResult TS3Mock::TStorage::CreateMultipartUpload(TStringBuf path) {
    TResult result;
    result.UploadId = ToString(NextUploadId++);
    MultipartUploads[std::make_pair(TString(path), result.UploadId)] = {};
    return result;
}

bool TS3Mock::TStorage::HasMultipartUpload(TStringBuf path, const TString& uploadId) const {
    return MultipartUploads.contains(std::make_pair(TString(path), uploadId));
}

// The checks a part goes through before its content is taken, in the order of the HTTP mock; on success, the part
// slot is in partSlot
TS3Mock::TStorage::TResult TS3Mock::TStorage::PreparePart(TStringBuf path, const TString& uploadId,
    const TString& partNumberStr, TString*& partSlot)
{
    auto it = MultipartUploads.find(std::make_pair(TString(path), uploadId));
    if (it == MultipartUploads.end()) {
        return TResult::NotFound("NoSuchUpload");
    }

    size_t partNumber = 0;
    if (!TryFromString(partNumberStr, partNumber) || !partNumber) {
        return TResult::BadRequest("Invalid partNumber");
    }

    if (Settings.RejectUploadParts) {
        return TResult::BadRequest("Reject");
    }

    auto& parts = it->second;
    if (parts.size() < partNumber) {
        parts.resize(partNumber);
    }
    partSlot = &parts[partNumber - 1];
    return {};
}

TS3Mock::TStorage::TResult TS3Mock::TStorage::UploadPart(TStringBuf path, const TString& uploadId,
    const TString& partNumber, TString content)
{
    TString* part = nullptr;
    if (auto result = PreparePart(path, uploadId, partNumber, part); result.Status != EStatus::Ok) {
        return result;
    }
    TResult result;
    result.ETag = MD5::Data(content);
    *part = std::move(content);
    return result;
}

TS3Mock::TStorage::TResult TS3Mock::TStorage::UploadPartCopy(TStringBuf path, const TString& uploadId,
    const TString& partNumber, const TString& sourcePath, const TString* sourceRange)
{
    TString* part = nullptr;
    if (auto result = PreparePart(path, uploadId, partNumber, part); result.Status != EStatus::Ok) {
        return result;
    }

    if (!sourceRange) {
        return TResult::BadRequest("Invalid range");
    }

    std::pair<ui32, ui32> range(0, 0);
    if (!TryParseRange(*sourceRange, range)) {
        return TResult::BadRequest("Invalid range");
    }

    const auto* source = Data.FindPtr(sourcePath);
    if (!source) {
        return TResult::BadRequest("Invalid source");
    }

    if (source->size() < range.first + range.second) {
        return TResult::BadRequest("Range is outside of source");
    }

    *part = source->substr(range.first, range.second);
    TResult result;
    result.ETag = MD5::Data(*part);
    return result;
}

TS3Mock::TStorage::TResult TS3Mock::TStorage::CompleteMultipartUpload(TStringBuf path, const TString& uploadId,
    const TVector<TString>& etags)
{
    auto it = MultipartUploads.find(std::make_pair(TString(path), uploadId));
    if (it == MultipartUploads.end()) {
        return TResult::NotFound("NoSuchUpload");
    }

    if (etags.size() != it->second.size()) {
        return TResult::BadRequest("<Error><Code>InvalidPart</Code><Message>Invalid part count</Message></Error>");
    }

    for (auto i : xrange(etags.size())) {
        if (etags.at(i) != MD5::Data(it->second.at(i))) {
            return TResult::BadRequest("<Error><Code>InvalidPart</Code><Message>Invalid part</Message></Error>");
        }
    }

    auto& object = Data[path];
    object = JoinSeq("", it->second);
    MultipartUploads.erase(it);

    TResult result;
    result.ETag = MD5::Data(object);
    return result;
}

TS3Mock::TStorage::TResult TS3Mock::TStorage::AbortMultipartUpload(TStringBuf path, const TString& uploadId) {
    auto it = MultipartUploads.find(std::make_pair(TString(path), uploadId));
    if (it == MultipartUploads.end()) {
        return TResult::NotFound("NoSuchUpload");
    }
    MultipartUploads.erase(it);
    return {};
}

TS3Mock::TStorage::TResult TS3Mock::TStorage::Delete(TStringBuf path) {
    if (!Data.contains(path)) {
        return TResult::BadRequest({});
    }
    Data.erase(path);
    return {};
}

bool TS3Mock::TStorage::ShouldFailPartialRead(TStringBuf path) {
    if (path != Settings.PartialReadPath) {
        return false;
    }

    ++PartialReadRequestCount;

    ui32 failuresLeft = PartialReadFailuresLeft.load();
    while (failuresLeft) {
        if (PartialReadFailuresLeft.compare_exchange_weak(failuresLeft, failuresLeft - 1)) {
            ++PartialReadFailureCount;
            return true;
        }
    }
    return false;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// HTTP
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

TS3Mock::TRequest::EMethod TS3Mock::TRequest::ParseMethod(const char* str) {
    if (strnicmp(str, "HEAD", 4) == 0) {
        return EMethod::Head;
    } else if (strnicmp(str, "GET", 3) == 0) {
        return EMethod::Get;
    } else if (strnicmp(str, "PUT", 3) == 0) {
        return EMethod::Put;
    } else if (strnicmp(str, "POST", 4) == 0) {
        return EMethod::Post;
    } else if (strnicmp(str, "PATCH", 5) == 0) {
        return EMethod::Patch;
    } else if (strnicmp(str, "DELETE", 6) == 0) {
        return EMethod::Delete;
    }
    return EMethod::NotImplemented;
}

bool TS3Mock::TRequest::HttpBadRequest(const TReplyParams& params, const TString& error) {
    Cerr << "S3_MOCK::HttpBadRequest: " << error << Endl;
    params.Output << "HTTP/1.1 400 Bad request\r\n\r\n";
    params.Output << error;
    return true;
}

bool TS3Mock::TRequest::HttpNotFound(const TReplyParams& params, const TString& errorCode) {
    params.Output << "HTTP/1.1 404 Not found\r\n\r\n";
    params.Output << Sprintf(R"(
        <?xml version="1.0" encoding="UTF-8"?>
        <Error>
          <Code>%s</Code>
        </Error>
    )", errorCode.c_str());
    return true;
}

bool TS3Mock::TRequest::HttpNotImplemented(const TReplyParams& params) {
    params.Output << "HTTP/1.1 501 Not Implemented\r\n\r\n";
    return true;
}

bool TS3Mock::TRequest::HttpError(const TReplyParams& params, const TStorage::TResult& result) {
    Y_ABORT_UNLESS(result.Status != TStorage::EStatus::Ok);
    return result.Status == TStorage::EStatus::NotFound
        ? HttpNotFound(params, result.Error)
        : HttpBadRequest(params, result.Error);
}

void TS3Mock::TRequest::MaybeContinue(const TReplyParams& params) {
    if (params.Input.HasExpect100Continue()) {
        params.Output.SendContinue();
    }
}

bool TS3Mock::TRequest::HttpServeRead(const TReplyParams& params, EMethod method, const TStringBuf path) {
    const auto* rangeHeader = params.Input.Headers().FindHeader("Range");
    const TString rangeValue = rangeHeader ? rangeHeader->Value() : TString();
    const auto read = Parent->Storage->Read(path, rangeHeader ? &rangeValue : nullptr, method != EMethod::Get);
    if (read.Status != TStorage::EStatus::Ok) {
        return HttpError(params, read);
    }

    params.Output << (rangeHeader ? "HTTP/1.1 206 Partial Content\r\n" : "HTTP/1.1 200 Ok\r\n");
    THttpHeaders headers;
    headers.AddHeader("ETag", read.ETag);

    if (method == EMethod::Get) {
        const size_t responseSize = read.Range.second - read.Range.first + 1;
        const bool failPartialRead = Parent->Storage->ShouldFailPartialRead(path);
        headers.AddHeader("Content-Length", responseSize);
        headers.AddHeader("Content-Type", "application/octet-stream");
        if (rangeHeader) {
            headers.AddHeader("Accept-Ranges", "bytes");
            headers.AddHeader("Content-Range", "bytes " + ::ToString(read.Range.first) + "-" + ToString(read.Range.second) + "/" + ::ToString(read.ObjectSize));
        }
        if (failPartialRead) {
            params.Output.EnableKeepAlive(false);
            headers.AddHeader("Connection", "close");
        }
        headers.OutTo(&params.Output);
        params.Output << "\r\n";
        params.Output << TStringBuf(read.Body).SubStr(0, failPartialRead ? responseSize / 2 : responseSize);
        if (failPartialRead) {
            params.Output.Finish();
            return true;
        }
    } else {
        headers.AddHeader("Content-Length", read.ObjectSize);
        headers.OutTo(&params.Output);
        params.Output << "\r\n";
    }
    params.Output.Flush();

    return true;
}

TString BuildContentXML(const TString& path, size_t size) {
    return Sprintf(R"(
                <Contents>
                    <Key>%s</Key>
                    <Size>%zu</Size>
                </Contents>
        )", path.c_str(), size);
}

TString BuildContentListXML(const TVector<std::pair<TString, size_t>>& objects) {
    TString result;
    for (const auto& [path, size] : objects) {
        result += BuildContentXML(path, size);
    }
    return result;
}

TString BuildListObjectsXML(const TVector<std::pair<TString, size_t>>& objects, const TStringBuf bucketName) {
    return Sprintf(R"(<?xml version="1.0" encoding="UTF-8"?>
            <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                <Name>%s</Name>
                <IsTruncated>false</IsTruncated>
                <MaxKeys>%zu</MaxKeys>
                <KeyCount>%zu</KeyCount>
                %s
            </ListBucketResult>
        )", TString(bucketName).c_str(), objects.size(), objects.size(), BuildContentListXML(objects).c_str());
}

bool TS3Mock::TRequest::HttpServeList(const TReplyParams& params, TStringBuf bucketName, const TString& prefix) {
    params.Output << "HTTP/1.1 200 Ok\r\n";
    THttpHeaders headers;

    TString xml = BuildListObjectsXML(Parent->Storage->List(bucketName, prefix), bucketName);

    headers.AddHeader("Content-Type", "application/xml");
    headers.AddHeader("Content-Length", xml.length());
    headers.OutTo(&params.Output);

    params.Output << "\r\n";
    params.Output << xml;
    params.Output.Flush();

    return true;
}

bool TS3Mock::TRequest::HttpServeWrite(const TReplyParams& params, TStringBuf path, const TCgiParameters& queryParams) {
    TString content;
    ui64 length = 0;

    if (params.Input.GetContentLength(length)) {
        content = TString::Uninitialized(length);
        params.Input.Load(content.Detach(), length);
    } else {
        content = params.Input.ReadAll();
    }
    Cerr << "S3_MOCK::HttpServeWrite: " << path << " / " << queryParams.Print() << " / " << length << Endl;

    TStorage::TResult result;
    if (!queryParams) {
        result = Parent->Storage->Put(path, std::move(content));
    } else {
        if (!queryParams.Has("uploadId") || !queryParams.Has("partNumber")) {
            return HttpBadRequest(params, "'uploadId' & 'partNumber' must be specified");
        }

        const auto* sourceHeader = params.Input.Headers().FindHeader("x-amz-copy-source");
        if (sourceHeader) {
            const auto* sourceRangeHeader = params.Input.Headers().FindHeader("x-amz-copy-source-range");
            const TString sourceRange = sourceRangeHeader ? sourceRangeHeader->Value() : TString();
            result = Parent->Storage->UploadPartCopy(path, queryParams.Get("uploadId"), queryParams.Get("partNumber"),
                sourceHeader->Value(), sourceRangeHeader ? &sourceRange : nullptr);
            if (result.Status != TStorage::EStatus::Ok) {
                return HttpError(params, result);
            }

            params.Output << "HTTP/1.1 200 Ok\r\n";
            THttpHeaders headers;
            headers.AddHeader("x-amz-id-2", "LriYPLdmOdAiIfgSm/F1YsViT1LW94/xUQxMsF7xiEb1a0wiIOIxl+zbwZ163pt7");
            headers.AddHeader("x-amz-request-id", "0A49CE4060975EAC");
            headers.OutTo(&params.Output);
            params.Output << Sprintf(R"(
                <CopyPartResult>
                    <ETag>%s</ETag>
                    <LastModified>2011-04-11T20:34:56.000Z</LastModified>
                </CopyPartResult>
            )", result.ETag.c_str());

            params.Output.Flush();

            return true;
        }
        result = Parent->Storage->UploadPart(path, queryParams.Get("uploadId"), queryParams.Get("partNumber"), std::move(content));
    }
    if (result.Status != TStorage::EStatus::Ok) {
        return HttpError(params, result);
    }

    params.Output << "HTTP/1.1 200 Ok\r\n";
    THttpHeaders headers;
    headers.AddHeader("x-amz-id-2", "LriYPLdmOdAiIfgSm/F1YsViT1LW94/xUQxMsF7xiEb1a0wiIOIxl+zbwZ163pt7");
    headers.AddHeader("x-amz-request-id", "0A49CE4060975EAC");
    headers.AddHeader("ETag", result.ETag);
    headers.OutTo(&params.Output);
    params.Output << "\r\n";
    params.Output.Flush();

    return true;
}

bool TS3Mock::TRequest::HttpServeAction(const TReplyParams& params, EMethod method, TStringBuf path, const TCgiParameters& queryParams) {
    Cerr << "S3_MOCK::HttpServeAction: " << (ui32)method << " / " << path << " / " << queryParams.Print() << Endl;
    if (queryParams.Has("uploads")) {
        const auto result = Parent->Storage->CreateMultipartUpload(path);

        params.Output << "HTTP/1.1 200 Ok\r\n\r\n";
        params.Output << Sprintf(R"(
            <?xml version="1.0" encoding="UTF-8"?>
            <InitiateMultipartUploadResult>
              <Key>%s</Key>
              <UploadId>%s</UploadId>
            </InitiateMultipartUploadResult>
        )", TString(path.After('/')).c_str(), result.UploadId.c_str());
    } else if (queryParams.Has("uploadId")) {
        const TString& uploadId = queryParams.Get("uploadId");
        if (!Parent->Storage->HasMultipartUpload(path, uploadId)) {
            return HttpNotFound(params, "NoSuchUpload");
        }

        if (method == EMethod::Post) {
            NXml::TDocument request(params.Input.ReadAll(), NXml::TDocument::String);
            TVector<TString> etags;

            auto part = request.Root().FirstChild("Part");
            while (!part.IsNull()) {
                auto partNumberNode = part.FirstChild("PartNumber");
                if (partNumberNode.IsNull()) {
                    return HttpBadRequest(params, "Invalid request");
                }

                const ui32 partNumber = partNumberNode.Value<ui32>();
                if (etags.size() < partNumber) {
                    etags.resize(partNumber);
                }

                auto etagNode = part.FirstChild("ETag");
                if (etagNode.IsNull()) {
                    return HttpBadRequest(params, "Invalid request");
                }

                etags[partNumber - 1] = etagNode.Value<TString>();
                part = part.NextSibling("Part");
            }

            const auto result = Parent->Storage->CompleteMultipartUpload(path, uploadId, etags);
            if (result.Status != TStorage::EStatus::Ok) {
                return HttpError(params, result);
            }

            params.Output << "HTTP/1.1 200 Ok\r\n\r\n";
            params.Output << Sprintf(R"(
                <?xml version="1.0" encoding="UTF-8"?>
                <CompleteMultipartUploadResult>
                  <Key>%s</Key>
                  <ETag>%s</ETag>
                </CompleteMultipartUploadResult>
            )", TString(path.After('/')).c_str(), result.ETag.c_str());
        } else if (method == EMethod::Delete) {
            Parent->Storage->AbortMultipartUpload(path, uploadId);
            params.Output << "HTTP/1.1 204 Ok\r\n\r\n";
        } else {
            return HttpBadRequest(params);
        }
    } else if (method == EMethod::Delete && Parent->Storage->Delete(path).Status == TStorage::EStatus::Ok) {
        params.Output << "HTTP/1.1 204 Ok\r\n\r\n";
    } else {
        return HttpBadRequest(params);
    }

    return true;
}

TS3Mock::TRequest::TRequest(TS3Mock* parent)
    : Parent(parent)
{
}

bool TS3Mock::TRequest::DoReply(const TReplyParams& params) {
    ++Parent->HttpRequestCount;
    TStringBuf requestString(RequestString);
    TStringBuf methodStr = requestString.NextTok(' ');
    TStringBuf uriStr = requestString.NextTok(' ');

    Cerr << "REQUEST: " << params.Input.FirstLine() << Endl;
    Cerr << "HEADERS: " << Endl;
    for (auto&& i : params.Input.Headers()) {
        Cerr << i.Name() << ": " << i.Value() << Endl;
    }

    if (!methodStr.IsInited() || !uriStr.IsInited()) {
        return HttpBadRequest(params);
    }

    const EMethod method = ParseMethod(methodStr.data());
    TStringBuf pathStr = uriStr.NextTok('?');

    TCgiParameters queryParams;
    queryParams.ScanAddAll(uriStr);

    if (Parent->Storage->Settings.ErrorResponse) {
        if (const TString response = Parent->Storage->Settings.ErrorResponse(methodStr, pathStr)) {
            MaybeContinue(params);
            if (params.Input.HasContent()) {
                params.Input.ReadAll();
            }

            params.Output.EnableKeepAlive(false);
            params.Output << response;
            params.Output.Flush();
            return true;
        }
    }

    switch (method) {
    case EMethod::NotImplemented:
        return HttpNotImplemented(params);

    case EMethod::Head:
    case EMethod::Get:
        if (queryParams.Has("prefix")) {
            return HttpServeList(params, pathStr == "/" ? "" : pathStr, queryParams.Get("prefix"));
        } else {
            return HttpServeRead(params, method, pathStr);
        }
        break;

    case EMethod::Put:
        MaybeContinue(params);
        return HttpServeWrite(params, pathStr, queryParams);

    case EMethod::Post:
    case EMethod::Delete:
        MaybeContinue(params);
        return HttpServeAction(params, method, pathStr, queryParams);

    case EMethod::Patch:
        MaybeContinue(params);
        return HttpNotImplemented(params);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TS3Mock
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

bool TS3Mock::Start() {
    if (!HttpServer.Start()) {
        return false;
    }
    const ui16 port = Storage->Settings.HttpOptions.Port;
    TGuard<TMutex> guard(StartedMocks().Mutex);
    for (const TString& host : {"localhost", "127.0.0.1"}) {
        const TString endpoint = TStringBuilder() << host << ":" << port;
        StartedMocks().Storages[endpoint] = Storage;
        RegisteredEndpoints.push_back(endpoint);
    }
    return true;
}

const char* TS3Mock::GetError() {
    return HttpServer.GetError();
}

TS3Mock::TS3Mock(const TSettings& settings)
    : Storage(std::make_shared<TStorage>(settings))
    , HttpServer(this, settings.HttpOptions)
{
}

TS3Mock::TS3Mock(THashMap<TString, TString>&& data, const TSettings& settings)
    : Storage(std::make_shared<TStorage>(settings, std::move(data)))
    , HttpServer(this, settings.HttpOptions)
{
}

TS3Mock::TS3Mock(const THashMap<TString, TString>& data, const TSettings& settings)
    : Storage(std::make_shared<TStorage>(settings, THashMap<TString, TString>(data)))
    , HttpServer(this, settings.HttpOptions)
{
}

TS3Mock::~TS3Mock() {
    TGuard<TMutex> guard(StartedMocks().Mutex);
    for (const auto& endpoint : RegisteredEndpoints) {
        if (auto it = StartedMocks().Storages.find(endpoint); it != StartedMocks().Storages.end() && it->second.lock() == Storage) {
            StartedMocks().Storages.erase(it);
        }
    }
}

TClientRequest* TS3Mock::CreateClient() {
    return new TRequest(this);
}

void TS3Mock::ServeInActorSystem() {
    static std::once_flag once;
    std::call_once(once, [] {
        NExternalStorage::IExternalStorageConfig::SetS3EndpointOverride(
            [](const TString& endpoint, const TString& bucket, bool useVirtualAddressing) -> NExternalStorage::IExternalStorageConfig::TPtr {
                auto storage = StartedMocks().Find(endpoint);
                // Raw error responses and broken connections are what the HTTP client sees, so such mocks stay on HTTP
                if (!storage || storage->Settings.ErrorResponse || storage->Settings.PartialReadFailures) {
                    return nullptr;
                }
                return MakeS3MockStorageConfig(std::move(storage), bucket, useVirtualAddressing);
            });
    });
}

} // NTestHelpers
} // NWrappers
} // NKikimr
