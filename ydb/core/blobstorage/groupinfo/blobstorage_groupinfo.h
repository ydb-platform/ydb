#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/base/blobstorage_vdiskid.h>
#include <ydb/core/blobstorage/crypto/crypto.h>
#include <ydb/core/protos/blobstorage.pb.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/blobstorage_common.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/event_filter.h>
#include <ydb/core/protos/blobstorage_base3.pb.h>
#include <ydb/core/util/log_priority_mute_checker.h>

#include <util/str_stl.h>
#include <util/digest/numeric.h>
#include <util/generic/hash_set.h>

namespace NKikimrBlobStorage {
    class TGroupInfo;
};

namespace NActors {
    class TNodeLocation;
} // NActors

namespace NKikimr {

struct TDsProxyNodeMon;
class TSubgroupPartLayout;

namespace NBlobMapper {
    class TBasicMapper;
    class TMirror3dcMapper;
} // NBlobMapper

static constexpr ui8 MaxHandoffNodes = 6;
static constexpr ui8 MaxNodesPerBlob = 10;
static constexpr ui8 MaxTotalPartCount = 7;
static constexpr ui8 MaxVDisksInGroup = 32;

// mapper interface forward declaration
struct IBlobToDiskMapper;

// encryption key for the group
struct TEncryptionKey {
    TCypherKey Key;
    ui64 Version = 0;
    TString Id;

    TString ToString() const {
        return TStringBuilder() << "{Id# '" << EscapeC(Id) << "' Version# " << Version << "}";
    }

    explicit operator bool() const { // returns true if the key is set
        return Key.GetIsKeySet();
    }

    explicit operator const TCypherKey&() const {
        return Key;
    }
};

// current state of storage group
class TBlobStorageGroupInfo : public TThrRefBase {
public:
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // ITERATORS
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    class TFailRealmIterator;
    class TFailDomainIterator;
    class TVDiskIterator;

    template<typename TBaseIter>
    class TRangeBase;
    // all fail domains of this group info (or for specific ring)
    using TFailDomainRange = TRangeBase<TFailDomainIterator>;
    // all VDisks of this group info (or for specific ring)
    using TVDiskRange = TRangeBase<TVDiskIterator>;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // TYPES
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    using TVDiskIds = TStackVec<TVDiskID, 16>;
    using TServiceIds = TStackVec<TActorId, 16>;
    using TOrderNums = TStackVec<ui32, 16>;
    enum EBlobStateFlags {
        EBSF_DISINTEGRATED = 1, // Group is disintegrated.
        EBSF_UNRECOVERABLE = 1 << 1, // Recoverability: Blob can not be recovered. Ever.
        EBSF_RECOVERABLE = 1 << 2, // Recoverability: Blob can be recovered.
        EBSF_FULL = 1 << 3, // Integrity: All parts are present.
        EBSF_FRAGMENTARY = 1 << 4, // Integrity: There is at least one part missing FOR SURE.
        EBSF_DOUBTED = 1 << 5 // Integrity: We can't be sure, but we have seen only some parts.
    };

    enum EBlobState {
        EBS_DISINTEGRATED = EBSF_DISINTEGRATED,
        EBS_UNRECOVERABLE_FRAGMENTARY = EBSF_UNRECOVERABLE | EBSF_FRAGMENTARY,
        EBS_RECOVERABLE_FRAGMENTARY = EBSF_RECOVERABLE | EBSF_FRAGMENTARY,
        EBS_RECOVERABLE_DOUBTED = EBSF_RECOVERABLE | EBSF_DOUBTED,
        EBS_FULL = EBSF_FULL
    };

    enum EEncryptionMode {
        EEM_NONE = 0, // The plain data mode, no encryption, no MAC, no CRC at proxy level
        EEM_ENC_V1 = 1 // Encryption at proxy level, no MAC, no CRC
    };

    // INITIAL upon group creation in base and in memory
    // PROPOSE comes from the node warden when it proposes after getting INITIAL
    // INITIAL -> IN_TRANSITION in memory state from transaction start to transaction end
    // IN_TRANSITION -> IN_USE in base and in memory at transaction completion
    enum ELifeCyclePhase {
        ELCP_INITIAL = 0,
        ELCP_PROPOSE = 1,
        ELCP_IN_TRANSITION = 2,
        ELCP_IN_USE = 3,
        ELCP_KEY_CRC_ERROR = 700,
        ELCP_KEY_VERSION_ERROR = 701,
        ELCP_KEY_ID_ERROR = 702,
        ELCP_KEY_NOT_LOADED = 703,
    };

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // SUBSET HELPER CLASSES
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    template<typename TDerived>
    class TDomainSetBase;
    class TSubgroupVDisks;
    class TGroupVDisks;
    class TGroupFailDomains;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // QUORUM CHECKER HELPER CLASS
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    struct IQuorumChecker {
        virtual ~IQuorumChecker() = default;
        virtual bool CheckFailModelForSubgroup(const TSubgroupVDisks& failedSubgroupDisks) const = 0;
        virtual bool CheckFailModelForGroup(const TGroupVDisks& failedGroupDisks) const = 0;
        virtual bool CheckFailModelForGroupDomains(const TGroupFailDomains& failedDomains) const = 0;
        virtual bool CheckQuorumForSubgroup(const TSubgroupVDisks& subgroupDisks) const = 0;
        virtual bool CheckQuorumForGroup(const TGroupVDisks& groupDisks) const = 0;
        virtual bool CheckQuorumForGroupDomains(const TGroupFailDomains& domains) const = 0;
        virtual bool IsDegraded(const TGroupVDisks& failedDisks) const = 0;
        virtual bool OneStepFromDegradedOrWorse(const TGroupVDisks& failedDisks) const = 0;

        virtual EBlobState GetBlobState(const TSubgroupPartLayout& parts, const TSubgroupVDisks& failedDisks) const = 0;

        // check if we need to resurrect something; returns bit mask of parts needed for specified disk in group,
        // nth bit represents nth part; all returned parts are suitable for this particular disk
        virtual ui32 GetPartsToResurrect(const TSubgroupPartLayout& parts, ui32 idxInSubgroup) const = 0;
    };

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // TOPOLOGY
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    struct TVDiskInfo {
        // short VDisk identifier
        TVDiskIdShort VDiskIdShort;

        // order number inside this group; starting from 0 up to, but not including GetNumTotalVDisks()
        ui32 OrderNumber = 0;

        // order number of fail domain this VDisk resides in; fail domains are numbered continuously through all fail
        // realms
        ui32 FailDomainOrderNumber = 0;
    };

    struct TFailDomain {
        // VDisks composing this fail domain
        TVector<TVDiskInfo> VDisks;

        // fail domain order number; domains are numbered continuously through all fail realms
        ui32 FailDomainOrderNumber = 0;
    };

    struct TFailRealm {
        // fail domains contained in this fail realm
        TVector<TFailDomain> FailDomains;
    };

    struct TTopology {
        // group type (i.e. erasure)
        const TBlobStorageGroupType GType;
        // fail realms in this group
        TVector<TFailRealm> FailRealms;
        // total number of fail domains in this group
        ui32 TotalFailDomains = 0;
        // total number of vdisks
        ui32 TotalVDisks = 0;
        // maps blobs to disks in topology
        std::unique_ptr<IBlobToDiskMapper> BlobMapper;
        // quorum checker
        std::unique_ptr<IQuorumChecker> QuorumChecker;
        // map to quickly get (Short)VDisk id from its order number inside the group
        TVector<TVDiskIdShort> VDiskIdForOrderNumber;

        TTopology(TBlobStorageGroupType gtype);
        TTopology(TBlobStorageGroupType gtype, ui32 numFailRealms, ui32 numFailDomainsPerFailRealm, ui32 numVDisksPerFailDomain,
                bool finalize = false);
        TTopology(const TTopology&) = delete;
        TTopology &operator =(const TTopology&) = delete;
        TTopology(TTopology&&) = default;
        TTopology &operator =(TTopology&&) = default;
        ~TTopology();
        void FinalizeConstruction();

        // Check equality of 2 topologies (for some sanity checks we sometimes need to check equality of topologies)
        bool EqualityCheck(const TTopology &t);
        // obtain logical fail domain index -- through number for mirror-3-dc and per-ring for other erasures
        ui32 GetFailDomainOrderNumber(const TVDiskIdShort& vdisk) const;
        // get (Short)VDiskId by specified orderNumber (in range 0...TotalVDisks)
        TVDiskIdShort GetVDiskId(ui32 orderNumber) const;
        // Get fail domain
        const TFailDomain& GetFailDomain(const TVDiskIdShort& vdisk) const;
        const TFailDomain& GetFailDomain(ui32 failDomainOrderNumber) const;

        // check if vdisk id is a valid id for the group
        bool IsValidId(const TVDiskID &vdisk) const;
        bool IsValidId(const TVDiskIdShort &vdisk) const;
        // vdisk order number in the blobstorage group
        ui32 GetOrderNumber(const TVDiskIdShort &vdisk) const;
        // get total number of fail realms in the blobstorage group
        ui32 GetTotalFailRealmsNum() const { return FailRealms.size(); }
        // get total number of fail domains in the blobstorage group
        ui32 GetTotalFailDomainsNum() const { return TotalFailDomains; }
        // get the total number of VDisks in the blobstorage group
        ui32 GetTotalVDisksNum() const { return TotalVDisks; }
        // get number of VDisks per fail domain
        ui32 GetNumVDisksPerFailDomain() const { return FailRealms[0].FailDomains[0].VDisks.size(); }
        // get number of fail domains per fail realm
        ui32 GetNumFailDomainsPerFailRealm() const { return FailRealms[0].FailDomains.size(); }
        // get quorum checker
        const IQuorumChecker& GetQuorumChecker() const { return *QuorumChecker; }

        //////////////////////////////////////////////////////////////////////////////////////
        // IBlobToDiskMapper interface
        //////////////////////////////////////////////////////////////////////////////////////
        void PickSubgroup(ui32 hash, TBlobStorageGroupInfo::TOrderNums &orderNums) const;
        // function checks if a blob with provided hash is a replica for specific vdisk
        bool BelongsToSubgroup(const TVDiskIdShort& vdisk, ui32 hash) const;
        // function returns "vdisk" index into "vdisks" array of select replicas, i.e. it is equivalent to
        //
        // TVDiskIds ids;
        // PickSubgroup(hash, vdisk.Ring, &vdisks, nullptr);
        // return std::find(ids.begin(), ids.end(), vdisk) - ids.begin();
        //
        // It returns BlobSubgroupSize if this vdisk is not designated for provided blob
        ui32 GetIdxInSubgroup(const TVDiskIdShort& vdisk, ui32 hash) const;
        // function returns idxInSubgroup-th element of vdisks array from PickSubgroup
        TVDiskIdShort GetVDiskInSubgroup(ui32 idxInSubgroup, ui32 hash) const;

        bool IsHandoff(const TVDiskIdShort& vdisk, ui32 hash) const;


        TFailRealmIterator FailRealmsBegin() const;
        TFailRealmIterator FailRealmsEnd() const;

        TFailDomainIterator FailDomainsBegin() const;
        TFailDomainIterator FailDomainsEnd() const;

        TVDiskIterator VDisksBegin() const;
        TVDiskIterator VDisksEnd() const;

        // get all fail domains of this group info (or for specific ring)
        TFailDomainRange GetFailDomains() const;
        // get all VDisks of this group info (or for specific ring)
        TVDiskRange GetVDisks() const;

        EBlobState BlobState(ui32 effectiveReplicas, ui32 errorDomains) const;
        TString ToString() const;

    private:
        static IBlobToDiskMapper *CreateMapper(TBlobStorageGroupType gtype, const TTopology *topology);
        static IQuorumChecker *CreateQuorumChecker(const TTopology *topology);
    };

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // DYNAMIC INFO
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    struct TDynamicInfo {
        // blobstorage group id
        const TGroupId GroupId;
        // blobstorage group generation
        const ui32 GroupGeneration;
        // map to quickly get Service id (TActorId) from its order number inside TTopology
        TVector<TActorId> ServiceIdForOrderNumber;

        TDynamicInfo(TGroupId groupId, ui32 groupGen);
        TDynamicInfo(const TDynamicInfo&) = default;
        TDynamicInfo(TDynamicInfo&&) = default;
        TDynamicInfo &operator =(TDynamicInfo&&) = default;
        ~TDynamicInfo() = default;

        void PushBackActorId(const TActorId &aid) {
            ServiceIdForOrderNumber.push_back(aid);
        }
    };

    const IQuorumChecker& GetQuorumChecker() const;
    const TTopology &GetTopology() const { return *Topology; }
    const TDynamicInfo &GetDynamicInfo() const { return Dynamic; }
    EEncryptionMode GetEncryptionMode() const { return EncryptionMode; }
    ELifeCyclePhase GetLifeCyclePhase() const { return LifeCyclePhase; }
    ui64 GetGroupKeyNonce() const { return GroupKeyNonce; }
    const TCypherKey* GetCypherKey() const { return &Key; }
    std::shared_ptr<TTopology> PickTopology() const { return Topology; }

    // for testing purposes; numFailDomains = 0 automatically selects possible minimum for provided erasure; groupId=0
    // and groupGen=1 for constructed group
    explicit TBlobStorageGroupInfo(TBlobStorageGroupType gtype, ui32 numVDisksPerFailDomain = 1,
            ui32 numFailDomains = 0, ui32 numFailRealms = 1, const TVector<TActorId> *vdiskIds = nullptr,
            EEncryptionMode encryptionMode = EEM_ENC_V1, ELifeCyclePhase lifeCyclePhase = ELCP_IN_USE,
            TCypherKey key = TCypherKey((const ui8*)"TestKey", 8), TGroupId groupId = TGroupId::Zero());

    TBlobStorageGroupInfo(std::shared_ptr<TTopology> topology, TDynamicInfo&& rti, TString storagePoolName,
        TMaybe<TKikimrScopeId> acceptedScope, NPDisk::EDeviceType deviceType);

    TBlobStorageGroupInfo(TTopology&& topology, TDynamicInfo&& rti, TString storagePoolName,
        TMaybe<TKikimrScopeId> acceptedScope = {}, NPDisk::EDeviceType deviceType = NPDisk::DEVICE_TYPE_UNKNOWN);

    TBlobStorageGroupInfo(const TIntrusivePtr<TBlobStorageGroupInfo>& info, const TVDiskID& vdiskId, const TActorId& actorId);

    ~TBlobStorageGroupInfo();

    static TIntrusivePtr<TBlobStorageGroupInfo> Parse(const NKikimrBlobStorage::TGroupInfo& group,
        const TEncryptionKey *key, IOutputStream *err);

    static bool DecryptGroupKey(TBlobStorageGroupInfo::EEncryptionMode encryptionMode, const TString& mainKeyId,
        const TString& encryptedGroupKey, ui64 groupKeyNonce, const TCypherKey& tenantKey, TCypherKey *outGroupKey,
        ui32 groupId);

    TString GetStoragePoolName() const {
        return StoragePoolName ? StoragePoolName : "static";
    }

    NPDisk::EDeviceType GetDeviceType() const {
        return DeviceType;
    }

    TVDiskID CreateVDiskID(const TVDiskIdShort &id) const;

    static TString BlobStateToString(EBlobState);
    EBlobState BlobState(ui32 effectiveReplicas, ui32 errorDomains) const;
    void PickSubgroup(ui32 hash, TVDiskIds *outVDisk, TServiceIds *outServiceIds) const;
    bool BelongsToSubgroup(const TVDiskID &vdisk, ui32 hash) const;
    ui32 GetIdxInSubgroup(const TVDiskID &vdisk, ui32 hash) const;
    TVDiskID GetVDiskInSubgroup(ui32 idxInSubgroup, ui32 hash) const;

    bool IsValidId(const TVDiskID &vdisk) const;
    bool IsValidId(const TVDiskIdShort &vdisk) const;
    ui32 GetOrderNumber(const TVDiskID &vdisk) const;
    ui32 GetOrderNumber(const TVDiskIdShort &vdisk) const;

    // obtain logical fail domain index -- through number for mirror-3-dc and per-ring for other erasures
    ui32 GetFailDomainOrderNumber(const TVDiskID& vdisk) const;
    // obtain logical fail domain index -- through number for mirror-3-dc and per-ring for other erasures
    ui32 GetFailDomainOrderNumber(const TVDiskIdShort& vdisk) const;
    // get VDisk by specified orderNumber (in range 0...TotalVDisks)
    TVDiskID GetVDiskId(ui32 orderNumber) const;
    // obtain full vdisk id having just short vdisk id
    TVDiskID GetVDiskId(const TVDiskIdShort &vd) const;
    // get ActorID by specified orderNumber (in range 0...TotalVDisks)
    TActorId GetActorId(ui32 orderNumber) const;
    // get ActorID having just short vdisk id
    TActorId GetActorId(const TVDiskIdShort &vdisk) const;
    // get the total number of VDisks in this group
    ui32 GetTotalVDisksNum() const;
    // get the total number of fail domains in this group
    ui32 GetTotalFailDomainsNum() const;
    // get number of VDisks per fail domain
    ui32 GetNumVDisksPerFailDomain() const;

    TString ToString() const;

    TFailRealmIterator FailRealmsBegin() const;
    TFailRealmIterator FailRealmsEnd() const;

    TFailDomainIterator FailDomainsBegin() const;
    TFailDomainIterator FailDomainsEnd() const;

    TVDiskIterator VDisksBegin() const;
    TVDiskIterator VDisksEnd() const;

    // get all fail domains of this group info (or for specific ring)
    TFailDomainRange GetFailDomains() const;
    // get all VDisks of this group info (or for specific ring)
    TVDiskRange GetVDisks() const;
    // obtain iterator for vdisk
    TVDiskIterator FindVDisk(const TVDiskID& vdisk) const;

    bool CheckScope(const TKikimrScopeId& scopeId, const TActorContext& ctx, bool allowLocalScope) const {
        if (allowLocalScope) {
            if (scopeId == AppData(ctx)->LocalScopeId || scopeId.GetInterconnectScopeId() == TScopeId::LocallyGenerated) {
                return true;
            }
        }
        return !AcceptedScope || *AcceptedScope == scopeId || scopeId == TKikimrScopeId::DynamicTenantScopeId;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // encryption parameters handling

private:
    template<typename TBaseIter, typename TParentIter, typename TDerived>
    class TIteratorBase;

    class TRootIteratorBase;

private:
    friend class NBlobMapper::TBasicMapper;
    friend class NBlobMapper::TMirror3dcMapper;
    const TFailDomain& GetFailDomain(const TVDiskID& vdisk) const;
    const TFailDomain& GetFailDomain(ui32 failDomainOrderNumber) const;

public:
    // blobstorage group id
    const TGroupId GroupID;
    // blobstorage group generation
    const ui32 GroupGeneration;
    // erasure primarily
    const TBlobStorageGroupType Type;
    // virtual group BlobDepot tablet id
    std::optional<ui64> BlobDepotId;
    // assimilating group id
    NKikimrBlobStorage::TGroupDecommitStatus::E DecommitStatus = NKikimrBlobStorage::TGroupDecommitStatus::NONE;
    // origin of the group info content
    std::optional<NKikimrBlobStorage::TGroupInfo> Group;

    TAtomicLogPriorityMuteChecker<NLog::PRI_ERROR, NLog::PRI_DEBUG> PutErrorMuteChecker;

private:
    // group topology -- all disks inside this group
    std::shared_ptr<TTopology> Topology;
    // run type info about group (i.e. actor ids)
    TDynamicInfo Dynamic;
    // encryption mode
    EEncryptionMode EncryptionMode = EEM_NONE;
    ELifeCyclePhase LifeCyclePhase = ELCP_INITIAL;
    ui64 GroupKeyNonce = 0;
    TCypherKey Key;
    // access control
    TMaybe<TKikimrScopeId> AcceptedScope;
    TString StoragePoolName;
    NPDisk::EDeviceType DeviceType = NPDisk::DEVICE_TYPE_UNKNOWN;
};

// physical fail domain description
struct TFailDomain {
    struct TLevelIds {
        TVector<ui8> Ids;

        TLevelIds();
        bool IsEmpty() const;
        bool operator==(const TLevelIds& other) const;
        bool operator<(const TLevelIds& other) const;
    };

    static constexpr size_t RecordSize = sizeof(ui32) + sizeof(ui8);

    typedef TMap<ui8, ui32> TLevels;
    TLevels Levels;

    TFailDomain();
    TFailDomain(const TString &data);
    TString SerializeFailDomain() const;
    TLevelIds MakeIds() const;
    TLevelIds Intersect(const TLevelIds &id) const;
    bool IsColliding(const TFailDomain &other) const;
    bool IsSubdomainOf(const TFailDomain &other) const;
    bool IsEqual(const TFailDomain &other) const;
    bool IsDifferentAt(const TLevelIds &id, const TFailDomain &other) const;
    bool operator<(const TFailDomain &other) const;
    TString ToString() const;
    TFailDomain Slice(ui32 level) const;
};

} // NKikimr

template<>
inline void Out<NKikimr::TBlobStorageGroupInfo::EEncryptionMode>(IOutputStream& o, NKikimr::TBlobStorageGroupInfo::EEncryptionMode e) {
    using E = NKikimr::TBlobStorageGroupInfo::EEncryptionMode;
    switch (e) {
    case E::EEM_NONE:
        o << "NONE";
        break;
    case E::EEM_ENC_V1:
        o << "ENC_V1";
        break;
    }
}

template<>
inline void Out<NKikimr::TBlobStorageGroupInfo::ELifeCyclePhase>(IOutputStream& o, NKikimr::TBlobStorageGroupInfo::ELifeCyclePhase e) {
    using E = NKikimr::TBlobStorageGroupInfo::ELifeCyclePhase;
    switch (e) {
    case E::ELCP_INITIAL:
        o << "INITIAL";
        break;
    case E::ELCP_PROPOSE:
        o << "PROPOSE";
        break;
    case E::ELCP_IN_TRANSITION:
        o << "IN_TRANSITION";
        break;
    case E::ELCP_IN_USE:
        o << "IN_USE";
        break;
    case E::ELCP_KEY_CRC_ERROR:
        o << "KEY_CRC_ERROR";
        break;
    case E::ELCP_KEY_VERSION_ERROR:
        o << "KEY_VERSION_ERROR";
        break;
    case E::ELCP_KEY_ID_ERROR:
        o << "KEY_ID_ERROR";
        break;
    case E::ELCP_KEY_NOT_LOADED:
        o << "KEY_NOT_LOADED";
        break;
    }
}