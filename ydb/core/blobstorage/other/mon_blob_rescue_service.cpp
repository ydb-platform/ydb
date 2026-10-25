#include "mon_blob_rescue_service.h"

#include <ydb/library/actors/core/mon.h>

#include <library/cpp/monlib/service/pages/templates.h>

namespace NKikimr {
namespace {

    class TMonBlobRescueServiceActor : public TActor<TMonBlobRescueServiceActor> {
        void Handle(NMon::TEvHttpInfo::TPtr ev) {
            if (ev->Get()->Request.GetMethod() != HTTP_METHOD_GET) {
                Send(ev->Sender, new NMon::TEvHttpInfoRes(
                    "HTTP/1.1 405 Method Not Allowed\r\nAllow: GET\r\nConnection: close\r\n\r\n",
                    ev->Get()->SubRequestId, NMon::TEvHttpInfoRes::Custom), 0, ev->Cookie);
                return;
            }
            TStringStream str;
            HTML(str) {
                DIV_CLASS("panel panel-info") {
                    DIV_CLASS("panel-heading") {
                        str << "Blob Rescue Service";
                    }
                    DIV_CLASS("panel-body") {
                        str << R"(<form method="post" action="/restore_part" class="form-horizontal">
<div class="control-group">
<label class="control-label" for="restore-requests">VDiskId BlobId (one pair per line)</label>
<div class="controls"><textarea id="restore-requests" name="requests" rows="16"
class="form-control" style="width:100%;font-family:monospace" required
placeholder="[groupIdHex:generation:realm:domain:vdisk] [tablet:gen:step:channel:cookie:blobsize:partid]"></textarea></div>
</div>
<p>PartId 0 ensures this VDisk has at least one part. An explicit PartId restores that part.</p>
<p>OK means a part was written; ALREADY means a valid part is already stored locally.</p>
<div class="control-group"><label for="restore-inflight">Inflight</label>
<input id="restore-inflight" name="inflight" type="number" min="1" value="1" required></div>
<div class="control-group"><label for="restore-timeout">Batch timeout (seconds)</label>
<input id="restore-timeout" name="timeout" type="number" min="1" value="30" required></div>
<button type="submit" class="btn btn-default">Rescue</button>
</form>)";
                    }
                }
            }
            Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str(), ev->Get()->SubRequestId), 0, ev->Cookie);
        }

    public:
        TMonBlobRescueServiceActor()
            : TActor(&TThis::StateFunc)
        {}

        STRICT_STFUNC(StateFunc,
            hFunc(NMon::TEvHttpInfo, Handle);
        )
    };

} // anonymous namespace

IActor* CreateMonBlobRescueServiceActor() {
    return new TMonBlobRescueServiceActor;
}

} // NKikimr
