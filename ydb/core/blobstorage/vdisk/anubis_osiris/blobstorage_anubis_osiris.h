#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/blobstorage_hulldefs.h>

namespace NKikimr {

    struct TAnubisOsirisPutRecoveryLogRec;

    ////////////////////////////////////////////////////////////////////////////
    // TEvAnubisOsirisPut
    ////////////////////////////////////////////////////////////////////////////
    class TEvAnubisOsirisPut :
        public TEventLocal<TEvAnubisOsirisPut, TEvBlobStorage::EvAnubisOsirisPut>
    {
    public:
        // Depending on partId of the blob we are going to resurrect or
        // to remove the blob:
        // 1. Iff PartId != 0, it means that Osiris want to resurrect this part
        //    of the LogoBlob
        // 2. Iff PartId == 0, it means that Anubis decided to remove this blob
        //    completely by adding 'Don't keep flags'
        const TLogoBlobID LogoBlobId;

        friend struct TAnubisOsirisPutRecoveryLogRec;

        // create from LogoBlob
        TEvAnubisOsirisPut(const TLogoBlobID &id)
            : LogoBlobId(id)
        {}

        // create from recovery log
        TEvAnubisOsirisPut(const TAnubisOsirisPutRecoveryLogRec &rec);

        size_t ByteSize() const {
            return sizeof(TLogoBlobID);
        }

        // is this blob written by Anubis
        bool IsAnubis() const {
            return LogoBlobId.PartId() == 0;
        }

        // is this blob written by Osiris
        bool IsOsiris() const {
            return !IsAnubis();
        }

        // return data to insert to Hull Database, we create ingress according to whether this
        // blob is Anubis or Osiris record
        THullDbInsert PrepareInsert(const TBlobStorageGroupInfo::TTopology *top,
                                    const TVDiskIdShort &vd) const {
            if (IsAnubis()) {
                Y_ABORT_UNLESS(!LogoBlobId.PartId());
                TIngress ingressDontKeep;
                ingressDontKeep.SetKeep(TIngress::IngressMode(top->GType), CollectModeDoNotKeep);
                return {LogoBlobId, ingressDontKeep};
            } else {
                Y_ABORT_UNLESS(LogoBlobId.PartId());
                auto ingressOpt = TIngress::CreateIngressWOLocal(top, vd, LogoBlobId);
                Y_ABORT_UNLESS(ingressOpt);
                TLogoBlobID genId(LogoBlobId, 0);
                return {genId, *ingressOpt};
            }
        }
    };

    ////////////////////////////////////////////////////////////////////////////
    // TEvAnubisOsirisPutResult
    ////////////////////////////////////////////////////////////////////////////
    struct TEvAnubisOsirisPutResult :
        public TEventLocal<TEvAnubisOsirisPutResult, TEvBlobStorage::EvAnubisOsirisPutResult>,
        public TEvVResultBase
    {
        const NKikimrProto::EReplyStatus Status;

        TEvAnubisOsirisPutResult(NKikimrProto::EReplyStatus status,
                                 const TInstant &now,
                                 ::NMonitoring::TDynamicCounters::TCounterPtr counterPtr,
                                 NVDiskMon::TLtcHistoPtr histoPtr)
            : TEvVResultBase(now, TInterconnectChannels::IC_BLOBSTORAGE_SMALL_MSG, counterPtr, histoPtr)
            , Status(status)
        {}
    };

    ////////////////////////////////////////////////////////////////////////////
    // TAnubisOsirisPutRecoveryLogRec
    ////////////////////////////////////////////////////////////////////////////
    struct TAnubisOsirisPutRecoveryLogRec {
        TLogoBlobID Id;

        TAnubisOsirisPutRecoveryLogRec()
            : Id()
        {}

        TAnubisOsirisPutRecoveryLogRec(const TEvAnubisOsirisPut &msg);
        TString Serialize() const;
        bool ParseFromString(const TString &data);
        bool ParseFromArray(const char* data, size_t size);
        TString ToString() const;
        void Output(IOutputStream &str) const;
    };

} // NKikimr
