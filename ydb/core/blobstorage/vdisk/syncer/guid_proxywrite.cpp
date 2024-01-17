#include "guid_proxywrite.h"
#include "guid_proxybase.h"

#include <ydb/library/actors/core/interconnect.h>

using namespace NKikimrServices;

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TWriteVDiskGuidProxy
    ////////////////////////////////////////////////////////////////////////////
    class TWriteVDiskGuidProxy : public TVDiskGuidProxyBase {
    protected:
        const NKikimrBlobStorage::TSyncGuidInfo::EState State;
        const TVDiskEternalGuid Guid;

        virtual std::unique_ptr<TEvBlobStorage::TEvVSyncGuid> GenerateRequest() override {
            // write request
            return std::make_unique<TEvBlobStorage::TEvVSyncGuid>(SelfVDiskId,
                                                            TargetVDiskId,
                                                            Guid,
                                                            State);
        }

        virtual void HandleReply(const TActorContext &ctx,
                                 const NKikimrBlobStorage::TEvVSyncGuidResult &record) override {
            // all checks are passed
            Y_ABORT_UNLESS(record.GetStatus() == NKikimrProto::OK);
            ctx.Send(NotifyId, new TEvVDiskGuidWritten(TargetVDiskId, Guid, State));
        }

    public:
        TWriteVDiskGuidProxy(TIntrusivePtr<TVDiskContext> vctx,
                             const TVDiskID &selfVDiskId,
                             const TVDiskID &targetVDiskId,
                             const TActorId &targetServiceId,
                             const TActorId &notifyId,
                             NKikimrBlobStorage::TSyncGuidInfo::EState state,
                             TVDiskEternalGuid guid)
            : TVDiskGuidProxyBase(std::move(vctx), selfVDiskId, targetVDiskId, targetServiceId, notifyId)
            , State(state)
            , Guid(guid)
        {}
    };

    ////////////////////////////////////////////////////////////////////////////
    // CreateProxyForWritingVDiskGuid
    ////////////////////////////////////////////////////////////////////////////
    IActor* CreateProxyForWritingVDiskGuid(TIntrusivePtr<TVDiskContext> vctx,
                                           const TVDiskID &selfVDiskId,
                                           const TVDiskID &targetVDiskId,
                                           const TActorId &targetServiceId,
                                           const TActorId &notifyId,
                                           NKikimrBlobStorage::TSyncGuidInfo::EState state,
                                           TVDiskEternalGuid guid) {
        Y_ABORT_UNLESS(!(state == NKikimrBlobStorage::TSyncGuidInfo::Final &&
                   guid == TVDiskEternalGuid()));
        return new TWriteVDiskGuidProxy(std::move(vctx), selfVDiskId, targetVDiskId, targetServiceId,
                                        notifyId, state, guid);
    }

} // NKikimr
