#include "mon_get_blob_page.h"
#include <ydb/core/base/blobstorage.h>
#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/threading/future/future.h>

namespace NKikimr {

namespace {

    class TMonGetBlobPage
        : public NMonitoring::IMonPage
    {
        struct TRequestResult {
            TLogoBlobID LogoBlobId;
            NKikimrProto::EReplyStatus Status;
            TString Buffer;
            TString DebugInfo;
            TVector<TEvBlobStorage::TEvGetResult::TPartMapItem> PartMap;
        };

        class TRequestActor
            : public TActorBootstrapped<TRequestActor>
        {
            const ui32 GroupId;
            const TLogoBlobID LogoBlobId;
            const bool CollectDebugInfo;
            NThreading::TPromise<TRequestResult> Promise;

        public:
            static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
                return NKikimrServices::TActivity::BS_GET_ACTOR;
            }

            TRequestActor(ui32 groupId, const TLogoBlobID& logoBlobId, bool collectDebugInfo,
                    NThreading::TPromise<TRequestResult> promise)
                : GroupId(groupId)
                , LogoBlobId(logoBlobId)
                , CollectDebugInfo(collectDebugInfo)
                , Promise(promise)
            {}

            void Bootstrap(const TActorContext& ctx) {
                auto query = std::make_unique<TEvBlobStorage::TEvGet>(LogoBlobId, 0, 0, TInstant::Max(),
                    NKikimrBlobStorage::AsyncRead);
                query->CollectDebugInfo = CollectDebugInfo;
                query->ReportDetailedPartMap = true;
                SendToBSProxy(ctx, GroupId, query.release());
                Become(&TRequestActor::StateFunc);
            }

            void Handle(TEvBlobStorage::TEvGetResult::TPtr& ev, const TActorContext& ctx) {
                TEvBlobStorage::TEvGetResult *msg = ev->Get();

                TRequestResult result;
                if (msg->Status != NKikimrProto::OK) {
                    result.Status = msg->Status;
                } else if (msg->ResponseSz != 1) {
                    result.Status = NKikimrProto::ERROR;
                } else {
                    result.LogoBlobId = msg->Responses[0].Id;
                    result.Status = msg->Responses[0].Status;
                    result.Buffer = msg->Responses[0].Buffer.ConvertToString();
                    result.PartMap = std::move(msg->Responses[0].PartMap);
                }
                result.DebugInfo = std::move(msg->DebugInfo);

                Promise.SetValue(result);
                Die(ctx);
            }

            STRICT_STFUNC(StateFunc,
                HFunc(TEvBlobStorage::TEvGetResult, Handle)
            )
        };

    private:
        TActorSystem *ActorSystem;

    public:
        TMonGetBlobPage(const TString& path, TActorSystem *actorSystem)
            : IMonPage(path)
            , ActorSystem(actorSystem)
        {}

        void Output(NMonitoring::IMonHttpRequest& request) override {
            IOutputStream& out = request.Output();

            // parse HTTP request
            const TCgiParameters& params = request.GetParams();

            auto generateError = [&](const TString& msg) {
                out << "HTTP/1.1 400 Bad Request\r\n"
                    << "Content-Type: text/plain\r\n"
                    << "Connection: close\r\n"
                    << "\r\n"
                    << msg << "\r\n";
            };

            ui32 groupId = 0;
            if (!params.Has("groupId")) {
                return generateError("Missing groupId parameter");
            } else if (!TryFromString(params.Get("groupId"), groupId)) {
                return generateError("Failed to parse groupId parameter -- must be an integer");
            }

            TLogoBlobID logoBlobId;
            TString errorExplanation;
            if (!params.Has("blob")) {
                return generateError("Missing blob parameter");
            } else if (!TLogoBlobID::Parse(logoBlobId, params.Get("blob"), errorExplanation)) {
                return generateError("Failed to parse blob parameter -- " + errorExplanation);
            } else if (logoBlobId != logoBlobId.FullID()) {
                return generateError("Desired blob must be full one");
            } else if (!logoBlobId || !logoBlobId.BlobSize()) {
                return generateError("Invalid blob id");
            }

            bool binary = false;
            if (params.Has("binary")) {
                int value;
                if (!TryFromString(params.Get("binary"), value) || !(value >= 0 && value <= 1)) {
                    return generateError("Failed to parse binary parameter -- must be an integer in range [0, 1]");
                } else {
                    binary = value != 0;
                }
            }

            bool collectDebugInfo = false;
            if (params.Has("debugInfo")) {
                int value;
                if (!TryFromString(params.Get("debugInfo"), value) || !(value >= 0 && value <= 1)) {
                    return generateError("Failed to parse debugInfo parameter -- must be an integer in range [0, 1]");
                } else {
                    collectDebugInfo = value != 0;
                }

                if (collectDebugInfo && binary) {
                    return generateError("debugInfo and binary are mutually exclusive parameters");
                }
            }

            // create promise & future to obtain query result
            auto promise = NThreading::NewPromise<TRequestResult>();
            auto future = promise.GetFuture();

            // register and start actor
            ActorSystem->Register(new TRequestActor(groupId, logoBlobId, collectDebugInfo, promise));

            // wait for query to complete
            future.Wait();

            // obtain result
            const TRequestResult& result = future.GetValue();

            if (binary) {
                // generate data stream depending on result status
                if (result.Status == NKikimrProto::OK) {
                    out << "HTTP/1.1 200 OK\r\n"
                        << "Content-Type: application/octet-stream\r\n"
                        << "Content-Disposition: attachment; filename=\"" << result.LogoBlobId.ToString() << "\"\r\n"
                        << "Connection: close\r\n"
                        << "\r\n";
                    out.Write(result.Buffer);
                } else {
                    if (result.Status == NKikimrProto::NODATA) {
                        out << "HTTP/1.1 204 No Content\r\n";
                    } else {
                        out << "HTTP/1.1 500 Error\r\n";
                    }
                    out << "Content-Type: application/json\r\n"
                        << "Connection: close\r\n"
                        << "\r\n";

                    out << "{\n"
                        << "  \"Status\": \"" << NKikimrProto::EReplyStatus_Name(result.Status) << "\"\n"
                        << "}\n";
                }
            } else {
                out << NMonitoring::HTTPOKHTML;

                HTML(out) {
                    out << "<!DOCTYPE html>\n";
                    HTML_TAG() {
                        HEAD() {
                            out << "<title>Blob Query</title>\n";
                            out << "<link rel='stylesheet' href='https://yastatic.net/bootstrap/3.3.1/css/bootstrap.min.css'>\n";
                            out << "<script language='javascript' type='text/javascript' src='https://yastatic.net/jquery/2.1.3/jquery.min.js'></script>\n";
                            out << "<script language='javascript' type='text/javascript' src='https://yastatic.net/bootstrap/3.3.1/js/bootstrap.min.js'></script>\n";
                            out << "<style type=\"text/css\">\n";
                            out << ".table-nonfluid { width: auto; }\n";
                            out << ".narrow-line50 {line-height: 50%}\n";
                            out << ".narrow-line60 {line-height: 60%}\n";
                            out << ".narrow-line70 {line-height: 70%}\n";
                            out << ".narrow-line80 {line-height: 80%}\n";
                            out << ".narrow-line90 {line-height: 90%}\n";
                            out << "</style>\n";
                        }
                        BODY() {
                            DIV_CLASS("panel panel-info") {
                                DIV_CLASS("panel-heading") {
                                    out << "Blob Data";
                                }
                                DIV_CLASS("panel-body") {
                                    DIV() {
                                        out << "LogoBlobId: ";
                                        STRONG() {
                                            out << logoBlobId.ToString();
                                        }
                                    }

                                    DIV() {
                                        out << "Status: ";
                                        STRONG() {
                                            out << NKikimrProto::EReplyStatus_Name(result.Status);
                                        }
                                    }

                                    if (collectDebugInfo) {
                                        DIV() {
                                            out << "Debug Info:";
                                            DIV() {
                                                out << "<pre><small>";
                                                out << result.DebugInfo;
                                                out << "</small></pre>";
                                            }
                                        }
                                    }

                                    DIV() {
                                        out << "Part Map:";
                                        DIV() {
                                            TABLE_CLASS("table table-condensed") {
                                                TABLEHEAD() {
                                                    TABLER() {
                                                        TABLEH() { out << "DiskOrderNumber"; }
                                                        TABLEH() { out << "PartIdRequested"; }
                                                        TABLEH() { out << "RequestIndex"; }
                                                        TABLEH() { out << "ResponseIndex"; }
                                                        TABLEH() { out << "PartId"; }
                                                        TABLEH() { out << "Status"; }
                                                    }
                                                }
                                                TABLEBODY() {
                                                    for (const auto& item : result.PartMap) {
                                                        auto prefix = [&] {
                                                            TABLED() { out << item.DiskOrderNumber; }
                                                            TABLED() { out << item.PartIdRequested; }
                                                            TABLED() { out << item.RequestIndex; }
                                                            TABLED() { out << item.ResponseIndex; }
                                                        };
                                                        if (item.Status) {
                                                            for (const auto& x : item.Status) {
                                                                TABLER() {
                                                                    prefix();
                                                                    TABLED() { out << x.first; }
                                                                    TABLED() { out << NKikimrProto::EReplyStatus_Name(x.second); }
                                                                }
                                                            }
                                                        } else {
                                                            TABLER() {
                                                                prefix();
                                                                TABLED() { out << "-"; }
                                                                TABLED() { out << "-"; }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }

                                    if (result.Status == NKikimrProto::OK) {
                                        DIV() {
                                            TCgiParameters params;
                                            params.InsertEscaped("blob", logoBlobId.ToString());
                                            params.InsertEscaped("groupId", ToString(groupId));
                                            params.InsertEscaped("binary", "1");
                                            out << "<a href=\"?" << params() << "\">Data</a>";
                                            DIV() {
                                                out << "<pre><small>";
                                                const TString& buffer = result.Buffer;
                                                const size_t rowSize = 64;
                                                for (size_t offset = 0; offset < buffer.size(); offset += rowSize) {
                                                    out << Sprintf("0x%06zx | ", offset);
                                                    size_t i;
                                                    for (i = 0; i < rowSize && i + offset < buffer.size(); ++i) {
                                                        out << Sprintf("%02x ", (ui8)buffer[i + offset]);
                                                    }
                                                    for (; i < rowSize; ++i) {
                                                        out << "   ";
                                                    }
                                                    out << "| ";
                                                    bool gray = false;
                                                    for (i = 0; i < rowSize && i + offset < buffer.size(); ++i) {
                                                        ui8 ch = buffer[offset + i];
                                                        if (isprint(ch)) {
                                                            if (gray) {
                                                                out << "</font>";
                                                                gray = false;
                                                            }
                                                            out << ch;
                                                        } else {
                                                            if (!gray) {
                                                                out << "<font color=\"gray\">";
                                                                gray = true;
                                                            }
                                                            out << ".";
                                                        }
                                                    }
                                                    out << "\n";
                                                }
                                                out << "</small></pre>";
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    };

} // anon

NMonitoring::IMonPage *CreateMonGetBlobPage(const TString& path, TActorSystem *actorSystem) {
    return new TMonGetBlobPage(path, actorSystem);
}

} // NKikimr
