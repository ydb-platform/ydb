#include "codes.h"
#include "parse.h"

#include <library/cpp/charset/codepage.h>
#include <library/cpp/http/io/stream.h>
#include <library/cpp/mime/types/mime.h>
#include <library/cpp/uri/uri.h>

#include <library/cpp/string_utils/url/url.h>
#include <util/string/vector.h>

namespace NHttpFetcher {
    namespace {
        static TString MimeTypeFromUrl(const NUri::TUri& httpUrl) {
            TStringBuf path = httpUrl.GetField(NUri::TField::FieldPath);
            size_t pos = path.find_last_of('.');
            if (pos == TStringBuf::npos) {
                return "";
            }
            // TODO (stanly) replace TString with TStringBuf
            TString ext = TString(path.substr(pos + 1));
            TString mime = mimetypeByExt(path.data());
            if (mime) {
                return mime;
            }

            if (ext == "jpg" || ext == "jpeg" || ext == "png" || ext == "gif") {
                return "image/" + ext;
            } else if (ext == "m4v" || ext == "mp4" || ext == "flv" || ext == "mpeg") {
                return "video/" + ext;
            } else if (ext == "mp3" || ext == "wav" || ext == "ogg") {
                return "audio/" + ext;
            } else if (ext == "zip" || ext == "doc" || ext == "docx" || ext == "xls" || ext == "xlsx" || ext == "pdf" || ext == "ppt") {
                return "application/" + ext;
            } else if (ext == "rar" || ext == "7z") {
                return "application/x-" + ext + "-compressed";
            } else if (ext == "exe") {
                return "application/octet-stream";
            }

            return "";
        }

        static TString MimeTypeFromUrl(const TString& url) {
            static const ui64 flags = NUri::TFeature::FeaturesRobot | NUri::TFeature::FeatureToLower;

            NUri::TUri httpUrl;
            if (httpUrl.Parse(url, flags) != NUri::TUri::ParsedOK) {
                return "";
            }

            return MimeTypeFromUrl(httpUrl);
        }

        // Extracts encoding & content-type from headers
        static void ProcessHeaders(TResult& result) {
            for (THttpHeaders::TConstIterator it = result.Headers.Begin(); it != result.Headers.End(); it++) {
                TString name = it->Name();
                name.to_lower();
                if (name == "content-type") {
                    TString value = it->Value();
                    value.to_lower();
                    size_t delimPos = value.find(';');
                    if (delimPos == TString::npos) {
                        delimPos = value.size();
                    }
                    result.MimeType = value.substr(0, delimPos);
                    size_t charsetPos = value.find("charset=");
                    if (charsetPos == TString::npos) {
                        continue;
                    }
                    delimPos = value.find(';', charsetPos + 1);
                    TString charsetStr = value.substr(charsetPos + 8,
                                                      delimPos == TString::npos ? delimPos : delimPos - charsetPos - 8);
                    ECharset charset = CharsetByName(charsetStr.data());
                    if (charset != CODES_UNSUPPORTED && charset != CODES_UNKNOWN) {
                        result.Encoding = charset;
                    }
                }
            }

            if (result.MimeType.empty() || result.MimeType == "application/octet-stream") {
                const TString& detectedMimeType = MimeTypeFromUrl(result.ResolvedUrl);
                if (detectedMimeType) {
                    result.MimeType = detectedMimeType;
                }
            }
        }

    }

    void ParseHttpResponse(TResult& result, IInputStream& is, THttpURL::EKind kind,
                           TStringBuf host, ui16 port) {
        THttpInput httpIn(&is);
        TString firstLine = httpIn.FirstLine();
        TVector<TString> params = SplitString(firstLine, " ");
        try {
            if (params.size() < 2) {
                ythrow yexception() << "failed to parse first line";
            }
            result.HttpVersion = params[0];
            result.Code = FromString(params[1]);
        } catch (const std::exception&) {
            result.Code = WRONG_HTTP_HEADER_CODE;
        }
        for (auto it = httpIn.Headers().Begin(); it < httpIn.Headers().End(); ++it) {
            const THttpInputHeader& header = *it;
            TString name = header.Name();
            name.to_lower();
            if (name == "location" && IsRedirectCode(result.Code)) {
                // TODO (stanly) use correct routine to parse location
                result.Location = header.Value();
                result.ResolvedUrl = header.Value();
                if (result.ResolvedUrl.StartsWith('/')) {
                    const bool defaultPort =
                        (kind == THttpURL::SchemeHTTP && port == 80) ||
                        (kind == THttpURL::SchemeHTTPS && port == 443);

                    result.ResolvedUrl = TString(NUri::SchemeKindToString(kind)) + "://" + host +
                                         (defaultPort ? "" : ":" + ToString(port)) +
                                         result.ResolvedUrl;
                }
            }
        }
        try {
            result.Headers = httpIn.Headers();
            result.Data = httpIn.ReadAll();
            ProcessHeaders(result);
            // TODO (stanly) try to detect mime-type by content
        } catch (const yexception& /* exception */) {
            result.Code = WRONG_HTTP_RESPONSE;
        }
    }

    void ParseHttpResponse(TResult& result, IInputStream& stream, const TString& url) {
        THttpURL::EKind kind;
        TString host;
        ui16 port;
        ParseUrl(url, kind, host, port);
        ParseHttpResponse(result, stream, kind, host, port);
    }

    void ParseUrl(const TStringBuf url, THttpURL::EKind& kind, TString& host, ui16& port) {
        using namespace NUri;

        static const int URI_PARSE_FLAGS =
            TFeature::FeatureSchemeKnown | TFeature::FeatureConvertHostIDN | TFeature::FeatureEncodeExtendedDelim | TFeature::FeatureEncodePercent;

        TUri uri;
        // Cut out url's path to speedup processing.
        if (uri.Parse(GetSchemeHostAndPort(url, false, false), URI_PARSE_FLAGS) != TUri::ParsedOK) {
            ythrow yexception() << "can't parse url: " << url;
        }

        kind = uri.GetScheme();
        host = uri.GetField(TField::FieldHost);
        port = uri.GetPort();
    }

}
