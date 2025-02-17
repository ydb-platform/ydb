#include "s3_mock.h"

#include <library/cpp/digest/md5/md5.h>
#include <library/cpp/xml/document/xml-document.h>

#include <util/generic/xrange.h>
#include <util/random/shuffle.h>
#include <util/string/cast.h>
#include <util/string/join.h>
#include <util/string/printf.h>

namespace NKikimr {
namespace NWrappers {
namespace NTestHelpers {

TS3Mock::TSettings::TSettings()
    : CorruptETags(false)
    , RejectUploadParts(false)
{
}

TS3Mock::TSettings::TSettings(ui16 port)
    : HttpOptions(THttpServer::TOptions(port).SetThreads(1))
    , CorruptETags(false)
    , RejectUploadParts(false)
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

bool TS3Mock::TRequest::TryParseRange(TStringBuf str, std::pair<ui32, ui32>& range) {
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

void TS3Mock::TRequest::MaybeContinue(const TReplyParams& params) {
    if (params.Input.HasExpect100Continue()) {
        params.Output.SendContinue();
    }
}

bool TS3Mock::TRequest::HttpServeRead(const TReplyParams& params, EMethod method, const TStringBuf path) {
    const TStringBuf content = Parent->Data.at(path);
    Cerr << "S3_MOCK::HttpServeRead: " << path << " / " << content.size() << Endl;
    std::pair<ui32, ui32> range(0, content.size() ? content.size() - 1 : 0);
    const auto* rangeHeader = params.Input.Headers().FindHeader("Range");
    if (rangeHeader) {
        if (!TryParseRange(rangeHeader->Value(), range)) {
            return HttpBadRequest(params, "Invalid range");
        }
        if (range.first > range.second) {
            return HttpBadRequest(params, "Invalid range borders (from > to)");
        }
    }
    if (range.second >= content.size() && content.size()) {
        return HttpBadRequest(params, "Invalid range right border");
    }

    TString etag = MD5::Data(content);
    if (Parent->Settings.CorruptETags) {
        ShuffleRange(etag);
    }

    params.Output << "HTTP/1.1 200 Ok\r\n";
    THttpHeaders headers;
    headers.AddHeader("ETag", etag);

    if (method == EMethod::Get) {
        headers.AddHeader("Content-Length", range.second - range.first + 1);
        headers.AddHeader("Content-Type", "application/octet-stream");
        headers.OutTo(&params.Output);
        if (rangeHeader) {
            headers.AddHeader("Accept-Ranges", "bytes");
            headers.AddHeader("Content-Range", "bytes " + ::ToString(range.first) + "-" + ToString(range.second) + "/" + ::ToString(content.size()));
        }
        params.Output << "\r\n";
        params.Output << content.SubStr(range.first, range.second - range.first + 1);
    } else {
        headers.AddHeader("Content-Length", content.size());
        headers.OutTo(&params.Output);
        params.Output << "\r\n";
    }
    params.Output.Flush();

    return true;
}

bool TS3Mock::TRequest::HttpServeWrite(const TReplyParams& params, TStringBuf path, const TCgiParameters& queryParams) {
    TString content;
    ui64 length;

    if (params.Input.GetContentLength(length)) {
        content = TString::Uninitialized(length);
        params.Input.Read(content.Detach(), length);
    } else {
        content = params.Input.ReadAll();
    }
    Cerr << "S3_MOCK::HttpServeWrite: " << path << " / " << queryParams.Print() << " / " << length << Endl;

    TString etag = MD5::Data(content);

    if (!queryParams) {
        Parent->Data[path] = std::move(content);
    } else {
        if (!queryParams.Has("uploadId") || !queryParams.Has("partNumber")) {
            return HttpBadRequest(params, "'uploadId' & 'partNumber' must be specified");
        }

        auto it = Parent->MultipartUploads.find(std::make_pair(path, queryParams.Get("uploadId")));
        if (it == Parent->MultipartUploads.end()) {
            return HttpNotFound(params, "NoSuchUpload");
        }

        size_t partNumber = 0;
        if (!TryFromString(queryParams.Get("partNumber"), partNumber) || !partNumber) {
            return HttpBadRequest(params, "Invalid partNumber");
        }

        if (Parent->Settings.RejectUploadParts) {
            return HttpBadRequest(params, "Reject");
        }

        auto& parts = it->second;
        if (parts.size() < partNumber) {
            parts.resize(partNumber);
        }

        const auto* sourceHeader = params.Input.Headers().FindHeader("x-amz-copy-source");
        if (sourceHeader) {
            const auto* sourceRangeHeader = params.Input.Headers().FindHeader("x-amz-copy-source-range");
            if (!sourceRangeHeader) {
                return HttpBadRequest(params, "Invalid range");
            }

            std::pair<ui32, ui32> range(0, 0);
            if (!TryParseRange(sourceRangeHeader->Value(), range)) {
                return HttpBadRequest(params, "Invalid range");
            }

            const auto* source = Parent->Data.FindPtr(sourceHeader->Value());
            if (!source) {
                return HttpBadRequest(params, "Invalid source");
            }

            if (source->size() < range.first + range.second) {
                return HttpBadRequest(params, "Range is outside of source");
            }

            const auto part = source->substr(range.first, range.second);

            parts[partNumber - 1] = part;
            etag = MD5::Data(part);

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
            )", etag.c_str());

            params.Output.Flush();

            return true;
        } else {
            parts[partNumber - 1] = std::move(content);
        }
    }

    params.Output << "HTTP/1.1 200 Ok\r\n";
    THttpHeaders headers;
    headers.AddHeader("x-amz-id-2", "LriYPLdmOdAiIfgSm/F1YsViT1LW94/xUQxMsF7xiEb1a0wiIOIxl+zbwZ163pt7");
    headers.AddHeader("x-amz-request-id", "0A49CE4060975EAC");
    headers.AddHeader("ETag", etag);
    headers.OutTo(&params.Output);
    params.Output << "\r\n";
    params.Output.Flush();

    return true;
}

bool TS3Mock::TRequest::HttpServeAction(const TReplyParams& params, EMethod method, TStringBuf path, const TCgiParameters& queryParams) {
    Cerr << "S3_MOCK::HttpServeAction: " << (ui32)method << " / " << path << " / " << queryParams.Print() << Endl;
    if (queryParams.Has("uploads")) {
        const int uploadId = Parent->NextUploadId++;
        Parent->MultipartUploads[std::make_pair(path, ToString(uploadId))] = {};

        params.Output << "HTTP/1.1 200 Ok\r\n\r\n";
        params.Output << Sprintf(R"(
            <?xml version="1.0" encoding="UTF-8"?>
            <InitiateMultipartUploadResult>
              <Key>%s</Key>
              <UploadId>%i</UploadId>
            </InitiateMultipartUploadResult>
        )", TString(path.After('/')).c_str(), uploadId);
    } else if (queryParams.Has("uploadId")) {
        auto it = Parent->MultipartUploads.find(std::make_pair(path, queryParams.Get("uploadId")));
        if (it == Parent->MultipartUploads.end()) {
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

            if (etags.size() != it->second.size()) {
                return HttpBadRequest(params, "Invalid part count");
            }

            for (auto i : xrange(etags.size())) {
                if (etags.at(i) != MD5::Data(it->second.at(i))) {
                    return HttpBadRequest(params, "Invalid part");
                }
            }

            Parent->Data[path] = JoinSeq("", it->second);
            Parent->MultipartUploads.erase(it);

            const TString etag = MD5::Data(Parent->Data[path]);

            params.Output << "HTTP/1.1 200 Ok\r\n\r\n";
            params.Output << Sprintf(R"(
                <?xml version="1.0" encoding="UTF-8"?>
                <CompleteMultipartUploadResult>
                  <Key>%s</Key>
                  <ETag>%s</ETag>
                </CompleteMultipartUploadResult>
            )", TString(path.After('/')).c_str(), etag.c_str());
        } else if (method == EMethod::Delete) {
            Parent->MultipartUploads.erase(it);
            params.Output << "HTTP/1.1 204 Ok\r\n\r\n";
        } else {
            return HttpBadRequest(params);
        }
    } else if (method == EMethod::Delete && Parent->Data.contains(path)) {
        Parent->Data.erase(path);
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

    switch (method) {
    case EMethod::NotImplemented:
        return HttpNotImplemented(params);

    case EMethod::Head:
    case EMethod::Get:
        if (Parent->Data.contains(pathStr)) {
            return HttpServeRead(params, method, pathStr);
        } else {
            return HttpNotFound(params, "NoSuchKey");
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

bool TS3Mock::Start() {
    return HttpServer.Start();
}

const char* TS3Mock::GetError() {
    return HttpServer.GetError();
}

TS3Mock::TS3Mock(const TSettings& settings)
    : Settings(settings)
    , HttpServer(this, settings.HttpOptions)
{
}

TS3Mock::TS3Mock(THashMap<TString, TString>&& data, const TSettings& settings)
    : Settings(settings)
    , Data(std::move(data))
    , HttpServer(this, settings.HttpOptions)
{
}

TS3Mock::TS3Mock(const THashMap<TString, TString>& data, const TSettings& settings)
    : Settings(settings)
    , Data(data)
    , HttpServer(this, settings.HttpOptions)
{
}

TClientRequest* TS3Mock::CreateClient() {
    return new TRequest(this);
}

} // NTestHelpers
} // NWrappers
} // NKikimr
