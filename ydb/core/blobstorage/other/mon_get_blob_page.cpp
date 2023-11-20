#include "mon_get_blob_page.h"
#include <ydb/core/base/blobstorage.h>
#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/threading/future/future.h>

namespace NKikimr {

namespace {

    class TMonGetBlobActor
        : public TActor<TMonGetBlobActor>
    {
        ui64 LastCookie = 0;
        THashMap<ui64, std::tuple<TActorId, ui64, int, ui32, bool, bool>> RequestsInFlight;

    public:
        TMonGetBlobActor()
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

            const ui64 cookie = ++LastCookie;
            auto query = std::make_unique<TEvBlobStorage::TEvGet>(logoBlobId, 0, 0, TInstant::Max(),
                NKikimrBlobStorage::AsyncRead);
            query->CollectDebugInfo = collectDebugInfo;
            query->ReportDetailedPartMap = true;
            SendToBSProxy(SelfId(), groupId, query.release(), cookie);
            RequestsInFlight[cookie] = {ev->Sender, ev->Cookie, ev->Get()->SubRequestId, groupId, binary, collectDebugInfo};
        }

        void Handle(TEvBlobStorage::TEvGetResult::TPtr ev) {
            const auto it = RequestsInFlight.find(ev->Cookie);
            Y_ABORT_UNLESS(it != RequestsInFlight.end());
            const auto& [sender, senderCookie, subRequestId, groupId, binary, collectDebugInfo] = it->second;

            TEvBlobStorage::TEvGetResult& msg = *ev->Get();
            if (msg.Status == NKikimrProto::OK && msg.ResponseSz != 1) {
                msg.Status = NKikimrProto::ERROR;
            }

            NKikimrProto::EReplyStatus status = msg.Status;
            TLogoBlobID id;
            TString buffer;

            if (msg.ResponseSz == 1) {
                status = msg.Responses[0].Status;
                id = msg.Responses[0].Id;
                buffer = msg.Responses[0].Buffer.ConvertToString();
            }

            TStringStream out;

            if (binary) {
                // generate data stream depending on result status
                if (status == NKikimrProto::OK) {
                    out << "HTTP/1.1 200 OK\r\n"
                        << "Content-Type: application/octet-stream\r\n"
                        << "Content-Disposition: attachment; filename=\"" << id.ToString() << "\"\r\n"
                        << "Connection: close\r\n"
                        << "\r\n";
                    out.Write(buffer);
                } else {
                    if (status == NKikimrProto::NODATA) {
                        out << "HTTP/1.1 204 No Content\r\n";
                    } else {
                        out << "HTTP/1.1 500 Error\r\n";
                    }
                    out << "Content-Type: application/json\r\n"
                        << "Connection: close\r\n"
                        << "\r\n";

                    out << "{\n"
                        << "  \"Status\": \"" << NKikimrProto::EReplyStatus_Name(status) << "\"\n"
                        << "}\n";
                }
            } else {
                out << NMonitoring::HTTPOKHTML;

                HTML(out) {
                    out << "<!DOCTYPE html>\n";
                    HTML_TAG() {
                        HEAD() {
                            out << "<title>Blob Query</title>\n";
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
                                        out << "LogoBlobId: ";
                                        STRONG() {
                                            out << id.ToString();
                                        }
                                    }

                                    DIV() {
                                        out << "Status: ";
                                        STRONG() {
                                            out << NKikimrProto::EReplyStatus_Name(status);
                                        }
                                    }

                                    if (collectDebugInfo) {
                                        DIV() {
                                            out << "Debug Info:";
                                            DIV() {
                                                out << "<pre><small>";
                                                out << msg.DebugInfo;
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
                                                    if (msg.ResponseSz == 1) {
                                                        for (const auto& item : msg.Responses[0].PartMap) {
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
                                    }

                                    if (status == NKikimrProto::OK) {
                                        DIV() {
                                            TCgiParameters params;
                                            params.InsertEscaped("blob", id.ToString());
                                            params.InsertEscaped("groupId", ToString(groupId));
                                            params.InsertEscaped("binary", "1");
                                            out << "<a href=\"?" << params() << "\">Data</a>";
                                            DIV() {
                                                out << "<pre><small>";
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

            Send(sender, new NMon::TEvHttpInfoRes(out.Str(), subRequestId, NMon::TEvHttpInfoRes::Custom), 0, senderCookie);
            RequestsInFlight.erase(it);
        }

        STRICT_STFUNC(StateFunc,
            hFunc(NMon::TEvHttpInfo, Handle);
            hFunc(TEvBlobStorage::TEvGetResult, Handle);
        )
    };

} // anon

IActor *CreateMonGetBlobActor() {
    return new TMonGetBlobActor;
}

} // NKikimr
