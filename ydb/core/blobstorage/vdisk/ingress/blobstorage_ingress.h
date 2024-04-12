#pragma once

#include "defs.h"
#include "blobstorage_ingress_matrix.h"

#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // INGRESS -- sync state of parts
    ////////////////////////////////////////////////////////////////////////////
    enum ECollectMode {
        CollectModeDefault = 0,
        CollectModeKeep = 1,
        CollectModeDoNotKeep = 2
    };

    inline const char *CollectMode2String(int mode) {
        switch (mode) {
            case CollectModeDefault: return "Def";
            case CollectModeKeep: return "Keep";
            case CollectModeDoNotKeep: return "DoNotKeepMissed";
            case (CollectModeKeep | CollectModeDoNotKeep): return "DoNotKeep";
            default: return "Unknown";
        }
    }

    ////////////////////////////////////////////////////////////////////////////
    // TIngressCache -- precalculate some common parts to operate faster
    ////////////////////////////////////////////////////////////////////////////
    class TIngressCache;
    using TIngressCachePtr = TIntrusivePtr<TIngressCache>;

    class TIngressCache : public TThrRefBase {
    public:
        const ui32 VDiskOrderNum;
        const ui32 TotalVDisks;
        const ui32 DomainsNum;
        const ui32 DisksInDomain;
        const ui32 Handoff;
        const ui32 BarrierIngressValueMask;
        const ui32 BarrierIngressDomainMask;
        const std::shared_ptr<TBlobStorageGroupInfo::TTopology> Topology;

        static TIngressCachePtr Create(std::shared_ptr<TBlobStorageGroupInfo::TTopology> top,
                                       const TVDiskIdShort &vdisk);

    private:
        TIngressCache(ui32 vdiskOrderNum,
                      ui32 totalVDisks,
                      ui32 domainsNum,
                      ui32 disksInDomain,
                      ui32 handoff,
                      ui32 barrierIngressValueMask,
                      ui32 barrierIngressDomainMask,
                      std::shared_ptr<TBlobStorageGroupInfo::TTopology> topology);
    };


    ////////////////////////////////////////////////////////////////////////////
    // TIngress -- sync info for LogoBlobs
    ////////////////////////////////////////////////////////////////////////////
    // FIXME: make sure, we never copy local vector, even for full recovery
    // FIXME: we have to use different Ingress structures for different storage types
#pragma pack(push, 4)
    class TIngress {
    public:
        typedef std::pair<NMatrix::TVectorType, NMatrix::TVectorType> TPairOfVectors;

        enum class EMode {
            GENERIC,
            MIRROR3OF4,
        };
        static EMode IngressMode(TBlobStorageGroupType gtype);

        TIngress() = default;
        // explicit constructor
        explicit TIngress(ui64 rawData);

        bool KeepUnconditionally(EMode ingressMode) const;
        void SetKeep(EMode ingressMode, ECollectMode mode);
        int GetCollectMode(EMode ingressMode) const;
        // Returns vector of parts we have heard about, i.e. main_vec | handoff1 | ... | handoffN
        NMatrix::TVectorType PartsWeKnowAbout(TBlobStorageGroupType gtype) const;
        // Returns vector of parts we MUST have locally according to Ingress, i.e. parts we have
        // written locally or recovered after crash
        NMatrix::TVectorType PartsWeMustHaveLocally(const TBlobStorageGroupInfo::TTopology *top,
                                                const TVDiskIdShort &vdisk,
                                                const TLogoBlobID &id) const;
        // returns a pair <VectorOfPartsToMove, VectorOfPartsToDelete>
        TPairOfVectors HandoffParts(const TBlobStorageGroupInfo::TTopology *top,
                                    const TVDiskIdShort &vdisk,
                                    const TLogoBlobID &id) const;
        NMatrix::TVectorType GetVDiskHandoffVec(const TBlobStorageGroupInfo::TTopology *top,
                                           const TVDiskIdShort &vdisk,
                                           const TLogoBlobID &id) const;
        NMatrix::TVectorType GetVDiskHandoffDeletedVec(const TBlobStorageGroupInfo::TTopology *top,
                                           const TVDiskIdShort &vdisk,
                                           const TLogoBlobID &id) const;
        NMatrix::TVectorType LocalParts(TBlobStorageGroupType gtype) const;
        NMatrix::TVectorType KnownParts(TBlobStorageGroupType gtype, ui8 nodeId) const;
        // Returns main replica for this LogoBlob with PartId != 0
        static TVDiskIdShort GetMainReplica(const TBlobStorageGroupInfo::TTopology *top, const TLogoBlobID &id);
        // Make a copy of ingress w/o local bits
        TIngress CopyWithoutLocal(TBlobStorageGroupType gtype) const;
        void DeleteHandoff(const TBlobStorageGroupInfo::TTopology *top,
                           const TVDiskIdShort &vdisk,
                           const TLogoBlobID &id,
                           bool deleteLocal=false);
        TString ToString(const TBlobStorageGroupInfo::TTopology *top,
                        const TVDiskIdShort &vdisk,
                        const TLogoBlobID &id) const;

        ui64 Raw() const {
            return Data;
        }
        void Merge(const TIngress &n) {
            Merge(*this, n);
        }

        static TString PrintVDisksForLogoBlob(const TBlobStorageGroupInfo *info, const TLogoBlobID &id);
        static void Merge(TIngress &myIngress, const TIngress &newIngress);
        static bool MustKnowAboutLogoBlob(const TBlobStorageGroupInfo::TTopology *top,
                                          const TVDiskIdShort &vdisk,
                                          const TLogoBlobID &id);
        static TIngress CreateFromRepl(const TBlobStorageGroupInfo::TTopology *top,
                                       const TVDiskIdShort& vdisk,
                                       const TLogoBlobID& id,
                                       NMatrix::TVectorType recoveredParts);

        // NOTE: take into account that partId in logoblob is from 1 to SomeMax when
        //       we are talking about a concrete part; method of this class usually return
        //       NMatrix::TVectorType mask of parts starting from 0.

        // create ingress from LogoBlobID id with main or handoff ingress bits
        // AND local bits (i.e. data is present)
        static TMaybe<TIngress> CreateIngressWithLocal(
                                    const TBlobStorageGroupInfo::TTopology *top,
                                    const TVDiskIdShort &vdisk,
                                    const TLogoBlobID &id);
        // create ingress from LogoBlobID id with main or handoff ingress bits
        // AND WITHOT local bits (i.e. 'we know about id, but have no data')
        static TMaybe<TIngress> CreateIngressWOLocal(
                                    const TBlobStorageGroupInfo::TTopology *top,
                                    const TVDiskIdShort &vdisk,
                                    const TLogoBlobID &id);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // mirror-3of4 support
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    private:
        ui64 Data = 0;
        // Data layout:
        // 1. Main replics vector (TShiftedMainBitVec)              =>  TotalPartCount()
        // 2. Local presents vector (TShiftedMainBitVec)            =>  TotalPartCount()
        // 3. N vectors of handoff replicas (TShiftedHandoffBitVec) =>  TotalPartCount() * 2 * N

        // create ingress from LogoBlobID id (fills in main or handoff ingress bits
        // and local bits optionally)
        static TMaybe<TIngress> CreateIngressInternal(
                                    TBlobStorageGroupType gtype,
                                    const ui8 nodeId,           // Ingress for _this_ node
                                    const TLogoBlobID &id,      // LogoBlobID
                                    const bool setUpLocalBits); // Setup data also
    };
#pragma pack(pop)


    ////////////////////////////////////////////////////////////////////////////
    // TBarrierIngress -- sync info for Barriers (garbage collection)
    ////////////////////////////////////////////////////////////////////////////
    class TBarrierIngress {
    public:
        TBarrierIngress();
        // create ingress explicitly for this disk id
        explicit TBarrierIngress(ui8 id);
        // create ingress for myself (self vdisk id is stored in cache)
        TBarrierIngress(const TIngressCache *cache);

        bool IsQuorum(const TIngressCache *cache) const;
        TString ToString(const TIngressCache *cache) const;

        static TBarrierIngress CreateFromRaw(ui32 raw);
        static void Merge(TBarrierIngress &myIngress, const TBarrierIngress &newIngress);
        static void SetUp(TBarrierIngress *ingress, ui8 id);
        static void SetUp(TBarrierIngress *ingress, const TIngressCache *cache);
        static void CheckBlobStorageGroup(const TBlobStorageGroupInfo *info);

        ui32 Raw() const {
            return Data;
        }

    private:
        ui32 Data;
    };


    ////////////////////////////////////////////////////////////////////////////
    // We check vdisk group for correspondence with ingress facilities
    ////////////////////////////////////////////////////////////////////////////
    void IngressGlobalCheck(const TBlobStorageGroupInfo *info);


    ////////////////////////////////////////////////////////////////////////////
    // Filters: barriers (gc) and block databases should be synced throughout
    // the blob storage group, logoblobs should synced only between replicas
    // which can contain these logoblobs. Filter is used for replication, sync,
    // gc commands, etc.
    ////////////////////////////////////////////////////////////////////////////
    struct TLogoBlobFilter {
        TLogoBlobFilter(const std::shared_ptr<TBlobStorageGroupInfo::TTopology> &top,
                        const TVDiskIdShort &vdisk)
            : Top(top)
            , VDisk(vdisk)
        {}

        TLogoBlobFilter(const TLogoBlobFilter &) = default;
        TLogoBlobFilter(TLogoBlobFilter &&) = default;

        bool Check(const TLogoBlobID &id) const {
            return Top->BelongsToSubgroup(VDisk, id.Hash());
        }

        std::shared_ptr<TBlobStorageGroupInfo::TTopology> Top;
        const TVDiskIdShort VDisk;
    };

    struct TFakeFilter {
        template<typename... T>
        bool Check(T&&...) const { return true; }
    };

} // NKikimr
