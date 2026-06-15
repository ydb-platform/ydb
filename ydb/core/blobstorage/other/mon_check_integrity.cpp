#include "mon_check_integrity.h"
#include <ydb/core/base/blobstorage.h>
#include <library/cpp/monlib/service/pages/templates.h>

namespace NKikimr {

namespace {

    class TMonCheckIntegrityActor
        : public TActor<TMonCheckIntegrityActor>
    {
        ui64 LastCookie = 0;
        // sender, senderCookie, subRequestId, groupId
        THashMap<ui64, std::tuple<TActorId, ui64, int, ui32>> RequestsInFlight;

    public:
        TMonCheckIntegrityActor()
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

                Send(ev->Sender, new NMon::TEvHttpInfoRes(out.Str(), ev->Get()->SubRequestId, NMon::TEvHttpInfoRes::Custom), 0,
                    ev->Cookie);
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

            const ui64 cookie = ++LastCookie;
            auto query = std::make_unique<TEvBlobStorage::TEvCheckIntegrity>(
                logoBlobId, TActivationContext::Now() + TDuration::Seconds(10), NKikimrBlobStorage::AsyncRead);
            SendToBSProxy(SelfId(), groupId, query.release(), cookie);
            RequestsInFlight[cookie] = {ev->Sender, ev->Cookie, ev->Get()->SubRequestId, groupId};
        }

        void Handle(TEvBlobStorage::TEvCheckIntegrityResult::TPtr ev) {
            const auto it = RequestsInFlight.find(ev->Cookie);
            Y_ABORT_UNLESS(it != RequestsInFlight.end());
            const auto& [sender, senderCookie, subRequestId, groupId] = it->second;

            TEvBlobStorage::TEvCheckIntegrityResult& msg = *ev->Get();

            TStringStream out;
            out << NMonitoring::HTTPOKHTML;

            HTML(out) {
                out << "<!DOCTYPE html>\n";
                HTML_TAG() {
                    HEAD() {
                        out << "<title>Blob Check Integrity</title>\n";
                        out << "<link rel='stylesheet' href='../static/css/bootstrap.min.css'>\n";
                        out << "<script language='javascript' type='text/javascript' src='../static/js/jquery.min.js'></script>\n";
                        out << "<script language='javascript' type='text/javascript' src='../static/js/bootstrap.min.js'></script>\n";
                        out << "<style type=\"text/css\">\n";
                        out << ".table-nonfluid { width: auto; }\n";
                        out << "</style>\n";
                    }
                    BODY() {
                        DIV_CLASS("panel panel-info") {
                            DIV_CLASS("panel-heading") {
                                out << "Blob Check Integrity";
                            }
                            DIV_CLASS("panel-body") {
                                DIV() {
                                    out << "GroupId: ";
                                    STRONG() {
                                        out << groupId;
                                    }
                                }

                                DIV() {
                                    out << "LogoBlobId: ";
                                    STRONG() {
                                        out << msg.Id.ToString();
                                    }
                                }

                                DIV() {
                                    out << "Status: ";
                                    STRONG() {
                                        out << NKikimrProto::EReplyStatus_Name(msg.Status);
                                    }
                                }

                                if (msg.ErrorReason) {
                                    DIV() {
                                        out << "ErrorReason: ";
                                        STRONG() {
                                            out << msg.ErrorReason;
                                        }
                                    }
                                }

                                DIV() {
                                    out << "Erasure: ";
                                    STRONG() {
                                        out << TBlobStorageGroupType::ErasureSpeciesName(msg.Erasure);
                                    }
                                }

                                DIV() {
                                    out << "PlacementStatus: ";
                                    STRONG() {
                                        out << TEvBlobStorage::TEvCheckIntegrityResult::PlacementStatusToString(msg.PlacementStatus);
                                    }
                                }

                                DIV() {
                                    out << "DataStatus: ";
                                    STRONG() {
                                        out << TEvBlobStorage::TEvCheckIntegrityResult::DataStatusToString(msg.DataStatus);
                                    }
                                }

                                if (msg.DataInfo) {
                                    DIV() {
                                        out << "DataInfo:";
                                        DIV() {
                                            out << "<pre><small>";
                                            for (char ch : msg.DataInfo) {
                                                switch (ch) {
                                                    case '&':  out << "&amp;"; break;
                                                    case '<':  out << "&lt;" ; break;
                                                    case '>':  out << "&gt;" ; break;
                                                    case '\'': out << "&#39;"; break;
                                                    case '"':  out << "&quot;"; break;
                                                    default:   out << ch     ; break;
                                                }
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

            Send(sender, new NMon::TEvHttpInfoRes(out.Str(), subRequestId, NMon::TEvHttpInfoRes::Custom), 0, senderCookie);
            RequestsInFlight.erase(it);
        }

        STRICT_STFUNC(StateFunc,
            hFunc(NMon::TEvHttpInfo, Handle);
            hFunc(TEvBlobStorage::TEvCheckIntegrityResult, Handle);
        )
    };

} // anon

IActor *CreateMonCheckIntegrityActor() {
    return new TMonCheckIntegrityActor;
}

} // NKikimr
