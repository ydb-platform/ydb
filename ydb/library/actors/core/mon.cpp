#include "mon.h"

#include <ydb/library/actors/protos/actors.pb.h>

#include <library/cpp/string_utils/base64/base64.h>
#include <util/generic/guid.h>

namespace NActors::NMon {

    TEvRemoteHttpInfo::TEvRemoteHttpInfo()
    {}

    TEvRemoteHttpInfo::TEvRemoteHttpInfo(const TString& query, HTTP_METHOD method)
        : Query(query)
        , Method(method)
    {}

    TEvRemoteHttpInfo::TEvRemoteHttpInfo(NActorsProto::TRemoteHttpInfo info)
        : Query(MakeSerializedQuery(info))
        , ExtendedQuery(std::make_unique<NActorsProto::TRemoteHttpInfo>(info))
    {}

    TEvRemoteHttpInfo::~TEvRemoteHttpInfo()
    {}

    TString TEvRemoteHttpInfo::MakeSerializedQuery(const NActorsProto::TRemoteHttpInfo& info) {
        TString s(1, '\0');
        const bool success = info.AppendToString(&s);
        Y_ABORT_UNLESS(success);
        return s;
    }

    TString TEvRemoteHttpInfo::PathInfo() const {
        if (ExtendedQuery) {
            return ExtendedQuery->GetPath();
        } else {
            const size_t pos = Query.find('?');
            return (pos == TString::npos) ? TString() : Query.substr(0, pos);
        }
    }

    TCgiParameters TEvRemoteHttpInfo::Cgi() const {
        if (ExtendedQuery) {
            TCgiParameters params;
            for (const auto& kv : ExtendedQuery->GetQueryParams()) {
                params.emplace(kv.GetKey(), kv.GetValue());
            }
            return params;
        } else {
            const size_t pos = Query.find('?');
            return TCgiParameters((pos == TString::npos) ? TString() : Query.substr(pos + 1));
        }
    }

    HTTP_METHOD TEvRemoteHttpInfo::GetMethod() const {
        return ExtendedQuery ? static_cast<HTTP_METHOD>(ExtendedQuery->GetMethod()) : Method;
    }

    TString TEvRemoteHttpInfo::GetHeader(TStringBuf name) const {
        if (ExtendedQuery) {
            for (const auto& header : ExtendedQuery->GetHeaders()) {
                if (AsciiEqualsIgnoreCase(header.GetName(), name)) {
                    return header.GetValue();
                }
            }
        }
        return {};
    }

    TString TEvRemoteHttpInfo::GetCookie(TStringBuf name) const {
        TString cookieHeader = GetHeader("Cookie");
        TStringBuf buf(cookieHeader);
        while (buf) {
            TStringBuf token = buf.NextTok(';');
            while (token.StartsWith(' ')) {
                token.Skip(1);
            }
            TStringBuf key = token.NextTok('=');
            if (key == name) {
                return TString(token);
            }
        }
        return {};
    }

    TEvRemoteHttpInfo* TEvRemoteHttpInfo::Load(const TEventSerializedData* bufs) {
        TString s = bufs->GetString();
        if (s.size() && s[0] == '\0') {
            TRope::TConstIterator iter = bufs->GetBeginIter();
            ui64 size = bufs->GetSize();
            iter += 1, --size; // skip '\0'
            TRopeStream stream(iter, size);

            auto res = std::make_unique<TEvRemoteHttpInfo>();
            res->Query = s;
            res->ExtendedQuery = std::make_unique<NActorsProto::TRemoteHttpInfo>();
            const bool success = res->ExtendedQuery->ParseFromZeroCopyStream(&stream);
            Y_ABORT_UNLESS(success);
            return res.release();
        } else {
            return new TEvRemoteHttpInfo(s);
        }
    }

    bool TEvRemoteHttpInfoRes::SerializeToArcadiaStream(TChunkSerializer *serializer) const {
        if (Nonce) {
            // Extended format: \0 + 4-byte nonce length + nonce + html
            TString packed;
            packed.reserve(1 + sizeof(ui32) + Nonce.size() + Html.size());
            packed.append('\0');
            ui32 nonceLen = Nonce.size();
            packed.append(reinterpret_cast<const char*>(&nonceLen), sizeof(nonceLen));
            packed.append(Nonce);
            packed.append(Html);
            return serializer->WriteString(&packed);
        }
        return serializer->WriteString(&Html);
    }

    ui32 TEvRemoteHttpInfoRes::CalculateSerializedSize() const {
        if (Nonce) {
            return 1 + sizeof(ui32) + Nonce.size() + Html.size();
        }
        return Html.size();
    }

    TEvRemoteHttpInfoRes* TEvRemoteHttpInfoRes::Load(const TEventSerializedData* bufs) {
        TString s = bufs->GetString();
        if (s.size() && s[0] == '\0') {
            TStringBuf buf(s);
            buf.Skip(1); // skip marker
            if (buf.size() >= sizeof(ui32)) {
                ui32 nonceLen;
                memcpy(&nonceLen, buf.data(), sizeof(nonceLen));
                buf.Skip(sizeof(nonceLen));
                if (buf.size() >= nonceLen) {
                    auto* res = new TEvRemoteHttpInfoRes(TString(buf.SubStr(nonceLen)));
                    res->Nonce = TString(buf.Head(nonceLen));
                    return res;
                }
            }
        }
        return new TEvRemoteHttpInfoRes(s);
    }

    TString BuildActorsLink(const TString& path, const TCgiParameters& currentParams, const std::initializer_list<std::pair<TString, TString>> newParams) {

        TCgiParameters mergedParams;

        for (auto param : currentParams) {
            mergedParams.insert(param);
        }

        for (auto param : newParams) {
            mergedParams.erase(param.first);
            if (param.second) {
                mergedParams.insert(param);
            }
        }

        if (mergedParams.empty()) {
            return path;
        } else {
            return TStringBuilder() << path << '?' << mergedParams.Print();
        }
    }

    TString GenerateCspNonce() {
        TGUID guid;
        CreateGuid(&guid);
        return Base64Encode(TStringBuf(reinterpret_cast<const char*>(&guid), sizeof(guid)));
    }

} // NActors::NMon
