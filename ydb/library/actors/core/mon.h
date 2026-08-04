#pragma once

#include "events.h"
#include "event_local.h"
#include <library/cpp/monlib/service/monservice.h>
#include <library/cpp/monlib/service/pages/mon_page.h>

namespace NActorsProto {
    class TRemoteHttpInfo;
} // NActorsProto

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
            virtual TString GetNonce() const { return {}; }
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

            TString GetNonce() const override {
                return Nonce;
            }

            const TString Answer;
            const int SubRequestId;
            const EContentType ContentType;
            TString Nonce;
        };

        struct TEvRemoteHttpInfo: public NActors::TEventBase<TEvRemoteHttpInfo, RemoteHttpInfo> {
            TEvRemoteHttpInfo();
            TEvRemoteHttpInfo(const TString& query, HTTP_METHOD method = HTTP_METHOD_UNDEFINED);
            TEvRemoteHttpInfo(NActorsProto::TRemoteHttpInfo info);
            ~TEvRemoteHttpInfo();

            static TString MakeSerializedQuery(const NActorsProto::TRemoteHttpInfo& info);

            TString Query;
            HTTP_METHOD Method = HTTP_METHOD_UNDEFINED;
            std::unique_ptr<NActorsProto::TRemoteHttpInfo> ExtendedQuery;

            TString PathInfo() const;
            TCgiParameters Cgi() const;
            TString GetUserToken() const;
            HTTP_METHOD GetMethod() const;
            TString GetHeader(TStringBuf name) const;
            TString GetCookie(TStringBuf name) const;

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

            static TEvRemoteHttpInfo* Load(const TEventSerializedData* bufs);
        };

        struct TEvRemoteHttpInfoRes: public NActors::TEventBase<TEvRemoteHttpInfoRes, RemoteHttpInfoRes> {
            TEvRemoteHttpInfoRes() {
            }

            TEvRemoteHttpInfoRes(const TString& html)
                : Html(html)
            {
            }

            TString Html;
            TString Nonce;

            TString ToStringHeader() const override {
                return "TEvRemoteHttpInfoRes";
            }

            bool SerializeToArcadiaStream(TChunkSerializer *serializer) const override;
            ui32 CalculateSerializedSize() const override;

            bool IsSerializable() const override {
                return true;
            }

            static TEvRemoteHttpInfoRes* Load(const TEventSerializedData* bufs);
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

            static TEvRemoteJsonInfoRes* Load(const TEventSerializedData* bufs) {
                return new TEvRemoteJsonInfoRes(bufs->GetString());
            }
        };

        // TODO: tablet mon replies carry no HTTP status.
        //
        // TEvRemoteHttpInfoRes and TEvRemoteJsonInfoRes hold a payload and nothing else, and
        // NTabletMonitoringProxy hardcodes "HTTP/1.1 200 Ok" for both. A handler that fails can
        // therefore only report it inside the payload, and every client that looks at the status
        // code sees success. TEvRemoteBinaryInfoRes below is the escape hatch: callers hand-build
        // a whole raw HTTP response, status line included, just to be able to answer 4xx. Hive
        // does exactly that in MakeRawHttpEvent().
        //
        // The fix is to give the reply events an explicit status defaulting to 200 and let the
        // proxy emit it. Two things make it more than a one-liner:
        //   * the wire format of these events is a bare string (see Load() below), so adding a
        //     field changes serialization and needs a story for a tablet and a proxy running
        //     different versions during a rolling restart;
        //   * about 18 files construct these events, so every error path has to be revisited to
        //     decide which status it should now report.
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

            static TEvRemoteBinaryInfoRes* Load(const TEventSerializedData* bufs) {
                return new TEvRemoteBinaryInfoRes(bufs->GetString());
            }
        };


        TString BuildActorsLink(const TString& path, const TCgiParameters& currentParams, const std::initializer_list<std::pair<TString, TString>> newParams);

        TString GenerateCspNonce();

    }

}
