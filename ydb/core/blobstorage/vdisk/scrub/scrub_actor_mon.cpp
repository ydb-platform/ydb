#include "scrub_actor_impl.h"

namespace NKikimr {

    void TScrubCoroImpl::Handle(NMon::TEvHttpInfo::TPtr ev) {
        const auto& cgi = ev->Get()->Request.GetParams();
        ui32 maxUnreadableBlobs = 100;
        if (cgi.Has("maxUnreadableBlobs")) {
            TryFromString<ui32>(cgi.Get("maxUnreadableBlobs"), maxUnreadableBlobs);
        }
        Send(ev->Sender, new NMon::TEvHttpInfoRes(RenderHtml(maxUnreadableBlobs), ev->Get()->SubRequestId));
    }

    TString TScrubCoroImpl::RenderHtml(ui32 maxUnreadableBlobs) const {
        TStringStream out;
        HTML(out) {
            DIV_CLASS("panel panel-info") {
                DIV_CLASS("panel-heading") {
                    out << "Scrub";
                }
                DIV_CLASS("panel-body") {
                    out << CurrentState;
                    out << "<br>State: " << (State ? State->DebugString() : "<null>");

                    out << "<br>UnreadableBlobs";
                    TABLE_CLASS("table table-condensed") {
                        TABLEHEAD() {
                            TABLER() {
                                TABLEH() { out << "BlobId"; }
                                TABLEH() { out << "UnreadableParts"; }
                            }
                        }
                        TABLEBODY() {
                            std::vector<std::pair<TLogoBlobID, NMatrix::TVectorType>> blobs;
                            blobs.reserve(UnreadableBlobs.size());
                            for (const auto& [blobId, state] : UnreadableBlobs) {
                                blobs.emplace_back(blobId, state.UnreadableParts);
                            }
                            auto comp = [](const auto& x, const auto& y) { return x.first < y.first; };
                            std::sort(blobs.begin(), blobs.end(), comp);
                            ui32 num = 0;
                            for (const auto& [blobId, unreadableParts] : blobs) {
                                TABLER() {
                                    TABLED() {
                                        out << blobId;
                                    }
                                    TABLED() {
                                        out << unreadableParts.ToString();
                                    }
                                }
                                if (++num == maxUnreadableBlobs) {
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }
        return out.Str();
    }

} // NKikimr
