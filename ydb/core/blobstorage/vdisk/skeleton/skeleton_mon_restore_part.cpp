#include "skeleton_mon_restore_part.h"

#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <library/cpp/html/escape/escape.h>
#include <library/cpp/monlib/service/pages/templates.h>

#include <util/string/strip.h>

#include <vector>

namespace NKikimr {
namespace {

    class TMonRestorePartRequestActor : public TActorBootstrapped<TMonRestorePartRequestActor> {
        using TResult = NKikimrBlobStorage::TEvVRestorePartResult;

        struct TItem {
            TString OriginalLine;
            TLogoBlobID BlobId;
            TResult Result;
        };

        const TVDiskID SelfVDiskId;
        const TActorId NotifyId;
        const TActorId SkeletonFrontId;
        NMon::TEvHttpInfo::TPtr Ev;
        std::vector<TItem> Items;
        TInstant Deadline;
        size_t NextItem = 0;
        size_t NumInFlight = 0;
        ui32 InflightLimit = 1;

        static TResult MakeResult(NKikimrProto::EReplyStatus status, const TString& error = {}) {
            TResult result;
            result.SetStatus(status);
            result.SetResponseOrigin(TResult::MON_RESTORE_PART_REQUEST_ACTOR);
            result.SetErrorReason(error);
            return result;
        }

        void Reply(NMon::TEvHttpInfoRes* response) {
            Send(Ev->Sender, response, 0, Ev->Cookie);
            PassAway();
        }

        void ReplyText(TStringBuf httpStatus, const TString& text) {
            TStringStream out;
            out << "HTTP/1.1 " << httpStatus << "\r\nContent-Type: text/plain; charset=utf-8\r\n";
            if (httpStatus == "405 Method Not Allowed") {
                out << "Allow: GET, POST\r\n";
            }
            out << "Connection: close\r\n\r\n" << text;
            Reply(new NMon::TEvHttpInfoRes(out.Str(), Ev->Get()->SubRequestId, NMon::TEvHttpInfoRes::Custom));
        }

        void ReplyForm() {
            const auto& params = Ev->Get()->Request.GetParams();
            const TString groupId = params.Has("GroupId") ? params.Get("GroupId")
                : ToString(SelfVDiskId.GroupID.GetRawId());
            const TString nonce = NMon::GenerateCspNonce();
            TStringStream out;
            HTML(out) {
                H3_CLASS("") { out << "Restore blob parts"; }
                TAG_ATTRS(TFormC, {{"id", "restore-part-form"}, {"method", "post"}, {"class", "form-horizontal"}}) {
                    DIV_CLASS("control-group") {
                        LABEL_CLASS_FOR("control-label", "restore-part-group") { out << "GroupId"; }
                        DIV_CLASS("controls") {
                            out << "<input id=\"restore-part-group\" name=\"GroupId\" type=\"number\" min=\"0\" "
                                << "required value=\"" << NHtml::EscapeAttributeValue(groupId) << "\">";
                        }
                    }
                    DIV_CLASS("control-group") {
                        LABEL_CLASS_FOR("control-label", "restore-part-inflight") { out << "Inflight"; }
                        DIV_CLASS("controls") {
                            out << "<input id=\"restore-part-inflight\" name=\"inflight\" type=\"number\" min=\"1\" value=\"1\" required>";
                        }
                    }
                    DIV_CLASS("control-group") {
                        LABEL_CLASS_FOR("control-label", "restore-part-timeout") { out << "Batch timeout (seconds)"; }
                        DIV_CLASS("controls") {
                            out << "<input id=\"restore-part-timeout\" name=\"timeout\" type=\"number\" min=\"1\" value=\"30\" required>";
                        }
                    }
                    DIV_CLASS("control-group") {
                        LABEL_CLASS_FOR("control-label", "restore-part-blobs") { out << "BlobIds (one per line)"; }
                        DIV_CLASS("controls") {
                            out << "<textarea id=\"restore-part-blobs\" name=\"blobs\" rows=\"16\" class=\"form-control\" "
                                << "style=\"width:100%;font-family:monospace\" "
                                << "placeholder=\"[tablet:gen:step:channel:cookie:blobsize:partid]\"></textarea>";
                        }
                    }
                    DIV_CLASS("control-group") {
                        DIV_CLASS("controls") {
                            PARA() { out << "OK means a part was written; ALREADY means a valid part is already stored locally."; }
                            out << "<button type=\"submit\" class=\"btn btn-default\">Submit</button>";
                        }
                    }
                }
                out << "<pre id=\"restore-part-report\" hidden></pre>";
            }
            out << "<script nonce=\"" << nonce << "\">" << R"JS(
(() => {
    const form = document.getElementById('restore-part-form');
    const report = document.getElementById('restore-part-report');
    form.addEventListener('submit', async event => {
        event.preventDefault();
        const button = form.querySelector('button[type="submit"]');
        const url = new URL(window.location.href);
        url.searchParams.set('type', 'restorepart');
        for (const name of ['GroupId', 'timeout', 'inflight']) {
            url.searchParams.set(name, form.elements[name].value);
        }
        report.hidden = false;
        report.textContent = 'Restoring...';
        button.disabled = true;
        try {
            const response = await fetch(url, {
                method: 'POST',
                headers: {'Content-Type': 'text/plain; charset=utf-8'},
                body: form.elements.blobs.value
            });
            report.textContent = await response.text();
        } catch (error) {
            report.textContent = 'Request failed: ' + error.message;
        } finally {
            button.disabled = false;
        }
    });
})();
)JS" << "</script>";
            auto* response = new NMon::TEvHttpInfoRes(out.Str(), Ev->Get()->SubRequestId);
            response->Nonce = nonce;
            Reply(response);
        }

        void Finish() {
            TStringStream out;
            for (const auto& item : Items) {
                out << item.OriginalLine << ' ' << item.Result.ShortDebugString() << '\n';
            }
            ReplyText("200 OK", out.Str());
        }

        void FinishPending(NKikimrProto::EReplyStatus status, const TString& error) {
            for (auto& item : Items) {
                if (item.Result.GetStatus() == NKikimrProto::UNKNOWN) {
                    item.Result = MakeResult(status, error);
                }
            }
            Finish();
        }

        void SendNext() {
            if (TActivationContext::Now() >= Deadline) {
                return HandleTimeout();
            }
            while (NextItem < Items.size() && NumInFlight < InflightLimit) {
                const ui64 cookie = ++NextItem;
                auto& item = Items[cookie - 1];
                if (item.Result.GetStatus() != NKikimrProto::UNKNOWN) {
                    continue;
                }
                auto request = std::make_unique<TEvVRestorePart>();
                VDiskIDFromVDiskID(SelfVDiskId, request->Record.MutableVDiskID());
                LogoBlobIDFromLogoBlobID(item.BlobId, request->Record.MutableBlobID());
                request->Record.SetDeadline(Deadline.GetValue());
                ++NumInFlight;
                Send(SkeletonFrontId, request.release(), IEventHandle::FlagTrackDelivery, cookie);
            }
            if (NextItem == Items.size() && !NumInFlight) {
                Finish();
            }
        }

        void Complete(ui64 cookie, TResult result) {
            Items[cookie - 1].Result = std::move(result);
            --NumInFlight;
            SendNext();
        }

        void Handle(TEvVRestorePartResult::TPtr ev) {
            Complete(ev->Cookie, std::move(ev->Get()->Record));
        }

        void Handle(TEvents::TEvUndelivered::TPtr ev) {
            Complete(ev->Cookie, MakeResult(NKikimrProto::NOTREADY, "Target VDisk is unavailable"));
        }

        void HandleTimeout() {
            FinishPending(NKikimrProto::DEADLINE, "Timed out waiting for VRestorePart");
        }

        void HandlePoison() {
            FinishPending(NKikimrProto::NOTREADY, "VDisk is shutting down");
        }

        void PassAway() override {
            Send(NotifyId, new TEvents::TEvGone);
            TActorBootstrapped::PassAway();
        }

    public:
        TMonRestorePartRequestActor(const TVDiskID& selfVDiskId, const TActorId& notifyId,
                const TActorId& skeletonFrontId, NMon::TEvHttpInfo::TPtr& ev)
            : SelfVDiskId(selfVDiskId)
            , NotifyId(notifyId)
            , SkeletonFrontId(skeletonFrontId)
            , Ev(ev)
        {}

        void Bootstrap() {
            const auto& request = Ev->Get()->Request;
            if (request.GetMethod() == HTTP_METHOD_GET) {
                return ReplyForm();
            }
            if (request.GetMethod() != HTTP_METHOD_POST) {
                return ReplyText("405 Method Not Allowed", "Only GET and POST are supported\n");
            }
            const auto& params = request.GetParams();
            ui32 groupId;
            if (!TryFromString(params.Get("GroupId"), groupId)) {
                return ReplyText("400 Bad Request", "GroupId must be a decimal uint32\n");
            }
            if (groupId != SelfVDiskId.GroupID.GetRawId()) {
                return ReplyText("409 Conflict", TStringBuilder() << "GroupId mismatch: requested# " << groupId
                    << " current# " << SelfVDiskId.GroupID.GetRawId() << '\n');
            }
            ui32 timeout = 30;
            if (params.Has("timeout") && (!TryFromString(params.Get("timeout"), timeout) || !timeout)) {
                return ReplyText("400 Bad Request", "timeout must be a positive integer in seconds\n");
            }
            if (params.Has("inflight") && (!TryFromString(params.Get("inflight"), InflightLimit) || !InflightLimit)) {
                return ReplyText("400 Bad Request", "inflight must be a positive integer\n");
            }
            TStringBuf text = request.GetPostContent();
            while (text) {
                auto line = StripString(text.NextTok('\n'));
                if (!line) {
                    continue;
                }
                TItem item;
                item.OriginalLine = line;
                TString error;
                item.Result = TLogoBlobID::Parse(item.BlobId, item.OriginalLine, error)
                    ? MakeResult(NKikimrProto::UNKNOWN)
                    : MakeResult(NKikimrProto::ERROR, error);
                Items.push_back(std::move(item));
            }
            Deadline = TActivationContext::Now() + TDuration::Seconds(timeout);
            Become(&TThis::StateFunc, Deadline, new TEvents::TEvWakeup);
            SendNext();
        }

        STRICT_STFUNC(StateFunc,
            hFunc(TEvVRestorePartResult, Handle);
            hFunc(TEvents::TEvUndelivered, Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
            cFunc(TEvents::TSystem::Poison, HandlePoison);
        )
    };

} // anonymous namespace

IActor* CreateMonRestorePartRequestActor(const TVDiskID& selfVDiskId, const TActorId& notifyId,
        const TActorId& skeletonFrontId, NMon::TEvHttpInfo::TPtr& ev) {
    return new TMonRestorePartRequestActor(selfVDiskId, notifyId, skeletonFrontId, ev);
}

} // NKikimr
