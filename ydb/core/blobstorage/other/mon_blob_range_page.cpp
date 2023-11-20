#include "mon_blob_range_page.h"
#include <ydb/core/base/blobstorage.h>
#include <library/cpp/json/writer/json.h>
#include <library/cpp/json/writer/json_value.h>
#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/threading/future/future.h>

namespace NKikimr {

namespace {

    class TMonBlobRangeActor
        : public TActor<TMonBlobRangeActor>
    {
        ui64 LastCookie = 0;
        THashMap<ui64, std::tuple<TActorId, ui64, int, bool>> RequestsInFlight;

    public:
        TMonBlobRangeActor()
            : TActor(&TThis::StateFunc)
        {}

        void Handle(NMon::TEvHttpInfo::TPtr ev) {
            // parse HTTP request
            const TCgiParameters& params = ev->Get()->Request.GetParams();

            auto generateError = [&](const TString& msg) {
                TStringStream out;

                out << "HTTP/1.1 400 Bad Request\r\n"
                    << "Content-Type: text/plain\r\n"
                    << "Connection: close\r\n"
                    << "\r\n"
                    << msg << "\r\n";

                Send(ev->Sender, new NMon::TEvHttpInfoRes(out.Str(), ev->Get()->SubRequestId, NMon::TEvHttpInfoRes::Custom),
                    0, ev->Cookie);
            };

            ui32 groupId = 0;
            if (!params.Has("groupId")) {
                return generateError("Missing groupId parameter");
            } else if (!TryFromString(params.Get("groupId"), groupId)) {
                return generateError("Failed to parse groupId parameter -- must be an integer");
            }

            ui64 tabletId = 0;
            if (!params.Has("tabletId")) {
                return generateError("Missing tabletId parameter");
            } else if (!TryFromString(params.Get("tabletId"), tabletId)) {
                return generateError("Failed to parse tabletId parameter -- must be an integer");
            }

            ui32 json = 0;
            ui32 mustRestoreFirst = 0;
            TLogoBlobID from, to;
            TString errorExplanation;
            if (!params.Has("from")) {
                return generateError("Missing from parameter");
            } else if (!TLogoBlobID::Parse(from, params.Get("from"), errorExplanation)) {
                return generateError("Failed to parse from parameter -- " + errorExplanation);
            } else if (from != from.FullID()) {
                return generateError("Desired blob must be full one");
            }
            if (!params.Has("to")) {
                return generateError("Missing to parameter");
            } else if (!TLogoBlobID::Parse(to, params.Get("to"), errorExplanation)) {
                return generateError("Failed to parse to parameter -- " + errorExplanation);
            } else if (to != to.FullID()) {
                return generateError("Desired blob must be full one");
            }
            if (params.Has("json")) {
                if (!TryFromString(params.Get("json"), json)) {
                    return generateError("Failed to parse json parameter -- must be an integer");
                }
            }
            if (params.Has("mustRestoreFirst") && !TryFromString(params.Get("mustRestoreFirst"), mustRestoreFirst)) {
                return generateError("Failed to parse mustRestoreFirst parameter -- must be an integer");
            }

            const ui64 cookie = ++LastCookie;
            auto query = std::make_unique<TEvBlobStorage::TEvRange>(tabletId, from, to, mustRestoreFirst, TInstant::Max(), true);
            SendToBSProxy(SelfId(), groupId, query.release(), cookie);
            RequestsInFlight[cookie] = {ev->Sender, ev->Cookie, ev->Get()->SubRequestId, json};
        }

        void Handle(TEvBlobStorage::TEvRangeResult::TPtr ev) {
            const auto it = RequestsInFlight.find(ev->Cookie);
            Y_ABORT_UNLESS(it != RequestsInFlight.end());
            const auto& [sender, senderCookie, subRequestId, json] = it->second;

            auto *result = ev->Get();
            TStringStream out;

            if (json) {
                out << NMonitoring::HTTPOKJSON;

                NJson::TJsonValue root;
                root["Status"] = NKikimrProto::EReplyStatus_Name(result->Status);
                if (result->ErrorReason) {
                    root["ErrorReason"] = result->ErrorReason;
                }
                NJson::TJsonValue blobs(NJson::JSON_ARRAY);
                for (const auto& blob : result->Responses) {
                    blobs.AppendValue(blob.Id.ToString());
                }
                root["Blobs"] = blobs;

                NJsonWriter::TBuf buf;
                buf.WriteJsonValue(&root);
                out << buf.Str();
            } else {
                out << NMonitoring::HTTPOKHTML;

                HTML(out) {
                    out << "<!DOCTYPE html>\n";
                    HTML_TAG() {
                        HEAD() {
                            out << "<title>Blob Range Index Query</title>\n";
                            out << "<link rel='stylesheet' href='../static/css/bootstrap.min.css'>\n";
                            out << "<script language='javascript' type='text/javascript' src='../static/js/jquery.min.js'></script>\n";
                            out << "<script language='javascript' type='text/javascript' src='../static/js/bootstrap.min.js'></script>\n";
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
                                        out << "Status: ";
                                        STRONG() {
                                            out << NKikimrProto::EReplyStatus_Name(result->Status);
                                        }
                                    }

                                    if (result->ErrorReason) {
                                        DIV() {
                                            out << "ErrorReason: ";
                                            STRONG() {
                                                out << result->ErrorReason;
                                            }
                                        }
                                    }

                                    TABLE() {
                                        TABLEHEAD() {
                                            TABLER() {
                                                TABLEH() {
                                                    out << "LogoBlobId";
                                                }
                                            }
                                        }
                                        TABLEBODY() {
                                            for (const auto& blob : result->Responses) {
                                                TABLER() {
                                                    TABLED() {
                                                        out << blob.Id.ToString();
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
            }

            Send(sender, new NMon::TEvHttpInfoRes(out.Str(), subRequestId, NMon::TEvHttpInfoRes::Custom), 0, senderCookie);
            RequestsInFlight.erase(it);
        }

        STRICT_STFUNC(StateFunc,
            hFunc(NMon::TEvHttpInfo, Handle);
            hFunc(TEvBlobStorage::TEvRangeResult, Handle);
        )
    };

} // anon

IActor *CreateMonBlobRangeActor() {
    return new TMonBlobRangeActor;
}

} // NKikimr
