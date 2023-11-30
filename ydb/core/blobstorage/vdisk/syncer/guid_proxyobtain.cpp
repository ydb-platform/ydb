#include "guid_proxyobtain.h"
#include "guid_proxybase.h"

#include <ydb/library/actors/core/interconnect.h>

using namespace NKikimrServices;

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TObtainVDiskGuidProxy
    ////////////////////////////////////////////////////////////////////////////
    class TObtainVDiskGuidProxy : public TVDiskGuidProxyBase {
    protected:
        virtual std::unique_ptr<TEvBlobStorage::TEvVSyncGuid> GenerateRequest() override {
            return std::make_unique<TEvBlobStorage::TEvVSyncGuid>(SelfVDiskId, TargetVDiskId);
        }

        virtual void HandleReply(const TActorContext &ctx,
                                 const NKikimrBlobStorage::TEvVSyncGuidResult &record) override {
            // all checks are passed
            Y_ABORT_UNLESS(record.GetStatus() == NKikimrProto::OK);
            TVDiskID fromVDisk = VDiskIDFromVDiskID(record.GetVDiskID());
            auto guidInfo = record.GetReadInfo();
            auto guid = guidInfo.GetGuid();
            auto state = guidInfo.GetState();
            LOG_DEBUG(ctx, BS_SYNCER,
                      VDISKP(VCtx->VDiskLogPrefix, "TObtainVDiskGuidProxy: SUCCESS; vdisk# %s guid# %" PRIu64,
                            fromVDisk.ToString().data(), guid));

            ctx.Send(NotifyId, new TEvVDiskGuidObtained(fromVDisk, guid, state));

        }

    public:
        TObtainVDiskGuidProxy(TIntrusivePtr<TVDiskContext> vctx,
                              const TVDiskID &selfVDiskId,
                              const TVDiskID &targetVDiskId,
                              const TActorId &targetServiceId,
                              const TActorId &notifyId)
            : TVDiskGuidProxyBase(std::move(vctx), selfVDiskId, targetVDiskId, targetServiceId, notifyId)
        {}
    };


    ////////////////////////////////////////////////////////////////////////////
    // CreateProxyForObtainingVDiskGuid
    ////////////////////////////////////////////////////////////////////////////
    IActor* CreateProxyForObtainingVDiskGuid(TIntrusivePtr<TVDiskContext> vctx,
                                             const TVDiskID &selfVDiskId,
                                             const TVDiskID &targetVDiskId,
                                             const TActorId &targetServiceId,
                                             const TActorId &notifyId) {
        return new TObtainVDiskGuidProxy(std::move(vctx), selfVDiskId, targetVDiskId,
                                         targetServiceId, notifyId);
    }

} // NKikimr

