#pragma once

#include "events.h"
#include "event_local.h"
#include <library/cpp/monlib/service/monservice.h>
#include <library/cpp/monlib/service/pages/mon_page.h>

namespace NActors {
    namespace NMon {
        enum {
            HttpInfo = EventSpaceBegin(NActors::TEvents::ES_MON),
            HttpInfoRes,
            RemoteHttpInfo,
            RemoteHttpInfoRes,
            RemoteJsonInfoRes,
            RemoteBinaryInfoRes,
            End
        };

        static_assert(End < EventSpaceEnd(NActors::TEvents::ES_MON), "expect End < EventSpaceEnd(NActors::TEvents::ES_MON)");

        // request info from an actor in HTML format
        struct TEvHttpInfo: public NActors::TEventLocal<TEvHttpInfo, HttpInfo> {
            TEvHttpInfo(const NMonitoring::IMonHttpRequest& request, int subReqId = 0)
                : Request(request)
                , SubRequestId(subReqId)
            {
            }

            TEvHttpInfo(const NMonitoring::IMonHttpRequest& request, const TString& userToken)
                : Request(request)
                , UserToken(userToken)
                , SubRequestId(0)
            {
            }

            const NMonitoring::IMonHttpRequest& Request;
            TString UserToken; // built and serialized
            // SubRequestId != 0 means that we assemble reply from multiple parts and SubRequestId contains this part id
            int SubRequestId;
        };

        // base class for HTTP info response
        struct IEvHttpInfoRes: public NActors::TEventLocal<IEvHttpInfoRes, HttpInfoRes> {
            enum EContentType {
                Html,
                Custom,
            };

            IEvHttpInfoRes() {
            }

            virtual ~IEvHttpInfoRes() {
            }

            virtual void Output(IOutputStream& out) const = 0;
            virtual EContentType GetContentType() const = 0;
        };

        // Ready to output HTML in TString
        struct TEvHttpInfoRes: public IEvHttpInfoRes {
            TEvHttpInfoRes(const TString& answer, int subReqId = 0, EContentType contentType = Html)
                : Answer(answer)
                , SubRequestId(subReqId)
                , ContentType(contentType)
            {
            }

            void Output(IOutputStream& out) const override {
                out << Answer;
            }

            EContentType GetContentType() const override {
                return ContentType;
            }

            const TString Answer;
            const int SubRequestId;
            const EContentType ContentType;
        };

        struct TEvRemoteHttpInfo: public NActors::TEventBase<TEvRemoteHttpInfo, RemoteHttpInfo> {
            TEvRemoteHttpInfo() = default;

            TEvRemoteHttpInfo(const TString& query, HTTP_METHOD method = HTTP_METHOD_UNDEFINED)
                : Query(query)
                , Method(method)
            {
            }

            TEvRemoteHttpInfo(NActorsProto::TRemoteHttpInfo info)
                : Query(MakeSerializedQuery(info))
                , ExtendedQuery(std::move(info))
            {}

            static TString MakeSerializedQuery(const NActorsProto::TRemoteHttpInfo& info) {
                TString s(1, '\0');
                const bool success = info.AppendToString(&s);
                Y_VERIFY(success);
                return s;
            }

            TString Query;
            HTTP_METHOD Method = HTTP_METHOD_UNDEFINED;
            std::optional<NActorsProto::TRemoteHttpInfo> ExtendedQuery;

            TString PathInfo() const {
                if (ExtendedQuery) {
                    return ExtendedQuery->GetPath();
                } else {
                    const size_t pos = Query.find('?');
                    return (pos == TString::npos) ? TString() : Query.substr(0, pos);
                }
            }

            TCgiParameters Cgi() const {
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

            HTTP_METHOD GetMethod() const {
                return ExtendedQuery ? static_cast<HTTP_METHOD>(ExtendedQuery->GetMethod()) : Method;
            }

            TString ToStringHeader() const override {
                return "TEvRemoteHttpInfo";
            }

            bool SerializeToArcadiaStream(TChunkSerializer *serializer) const override {
                return serializer->WriteString(&Query);
            }

            ui32 CalculateSerializedSize() const override {
                return Query.size();
            }

            bool IsSerializable() const override {
                return true;
            }

            static IEventBase* Load(TEventSerializedData* bufs) {
                TString s = bufs->GetString();
                if (s.size() && s[0] == '\0') {
                    TRope::TConstIterator iter = bufs->GetBeginIter();
                    ui64 size = bufs->GetSize();
                    iter += 1, --size; // skip '\0'
                    TRopeStream stream(iter, size);

                    auto res = std::make_unique<TEvRemoteHttpInfo>();
                    res->Query = s;
                    res->ExtendedQuery.emplace();
                    const bool success = res->ExtendedQuery->ParseFromZeroCopyStream(&stream);
                    Y_VERIFY(success);
                    return res.release();
                } else {
                    return new TEvRemoteHttpInfo(s);
                }
            }
        };

        struct TEvRemoteHttpInfoRes: public NActors::TEventBase<TEvRemoteHttpInfoRes, RemoteHttpInfoRes> {
            TEvRemoteHttpInfoRes() {
            }

            TEvRemoteHttpInfoRes(const TString& html)
                : Html(html)
            {
            }

            TString Html;

            TString ToStringHeader() const override {
                return "TEvRemoteHttpInfoRes";
            }

            bool SerializeToArcadiaStream(TChunkSerializer *serializer) const override {
                return serializer->WriteString(&Html);
            }

            ui32 CalculateSerializedSize() const override {
                return Html.size();
            }

            bool IsSerializable() const override {
                return true;
            }

            static IEventBase* Load(TEventSerializedData* bufs) {
                return new TEvRemoteHttpInfoRes(bufs->GetString());
            }
        };

        struct TEvRemoteJsonInfoRes: public NActors::TEventBase<TEvRemoteJsonInfoRes, RemoteJsonInfoRes> {
            TEvRemoteJsonInfoRes() {
            }

            TEvRemoteJsonInfoRes(const TString& json)
                : Json(json)
            {
            }

            TString Json;

            TString ToStringHeader() const override {
                return "TEvRemoteJsonInfoRes";
            }

            bool SerializeToArcadiaStream(TChunkSerializer *serializer) const override {
                return serializer->WriteString(&Json);
            }

            ui32 CalculateSerializedSize() const override {
                return Json.size();
            }

            bool IsSerializable() const override {
                return true;
            }

            static IEventBase* Load(TEventSerializedData* bufs) {
                return new TEvRemoteJsonInfoRes(bufs->GetString());
            }
        };

        struct TEvRemoteBinaryInfoRes: public NActors::TEventBase<TEvRemoteBinaryInfoRes, RemoteBinaryInfoRes> {
            TEvRemoteBinaryInfoRes() {
            }

            TEvRemoteBinaryInfoRes(const TString& blob)
                : Blob(blob)
            {
            }

            TString Blob;

            TString ToStringHeader() const override {
                return "TEvRemoteBinaryInfoRes";
            }

            bool SerializeToArcadiaStream(TChunkSerializer *serializer) const override {
                return serializer->WriteString(&Blob);
            }

            ui32 CalculateSerializedSize() const override {
                return Blob.size();
            }

            bool IsSerializable() const override {
                return true;
            }

            static IEventBase* Load(TEventSerializedData* bufs) {
                return new TEvRemoteBinaryInfoRes(bufs->GetString());
            }
        };

    }

}
