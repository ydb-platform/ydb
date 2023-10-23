#pragma once

#include "defs.h"
#include "blobstorage_syncer_defs.h"
#include "blobstorage_syncquorum.h"
#include <ydb/core/blobstorage/base/blobstorage_syncstate.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_syncneighbors.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/blobstorage/vdisk/ingress/blobstorage_ingress.h>
#include <ydb/core/driver_lib/version/version.h>

#include <ydb/core/base/appdata.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TEvSyncGuidRecoveryDone -- sync guid recovery done message
    ////////////////////////////////////////////////////////////////////////////
    struct TEvSyncGuidRecoveryDone
        : public TEventLocal<TEvSyncGuidRecoveryDone,
                             TEvBlobStorage::EvSyncGuidRecoveryDone>
    {
        const NKikimrProto::EReplyStatus Status;
        const ui64 DbBirthLsn;

        TEvSyncGuidRecoveryDone(NKikimrProto::EReplyStatus status, ui64 dbBirthLsn)
            : Status(status)
            , DbBirthLsn(dbBirthLsn)
        {}
    };

    namespace NSyncer {

        class TSyncerJobTask;

        ////////////////////////////////////////////////////////////////////////
        // TPeerSyncState
        // This structure stores status of communication with other neighbor
        // (VDisk) from the BlobStorage group called peer.
        // All communication is initiated by Syncer itself (Syncer Scheduler)
        ////////////////////////////////////////////////////////////////////////
        struct TPeerSyncState {
            // from protobuf
            using ESyncStatus = NKikimrVDiskData::TSyncerVDiskEntry::ESyncStatus;
            using TSyncStatusVal = NKikimrVDiskData::TSyncerVDiskEntry;

            ESyncStatus LastSyncStatus = TSyncStatusVal::Unknown;
            TSyncState SyncState = {};
            TInstant SchTime = {};
            TInstant LastTry = {};
            TInstant LastGood = {};

            TPeerSyncState();
            void Serialize(IOutputStream &s) const;
            void ParseFromArcadiaStream(IInputStream &str);
            void Serialize(NKikimrVDiskData::TSyncerVDiskEntry &pb) const;
            void Parse(const NKikimrVDiskData::TSyncerVDiskEntry &pb);
            TString ToString() const;
            void OutputHtml(IOutputStream &str) const;

            static bool Good(ESyncStatus status) {
                return status == TSyncStatusVal::SyncDone ||
                    status == TSyncStatusVal::DisksSynced;
            }
        };

        ////////////////////////////////////////////////////////////////////////
        // TPeerGuidInfo
        // This structure is used for storing NKikimrBlobStorage::TSyncGuidInfo
        // by other VDisk from the group (peer)
        ////////////////////////////////////////////////////////////////////////
        struct TPeerGuidInfo {
            NKikimrBlobStorage::TSyncGuidInfo Info;

            void Serialize(NKikimrVDiskData::TSyncerVDiskEntry &pb) const;
            void Parse(const NKikimrVDiskData::TSyncerVDiskEntry &pb);
            TString ToString() const;
            void OutputHtml(IOutputStream &str) const;
        };

        ////////////////////////////////////////////////////////////////////////////
        // TPeer
        // We manage this structure for every disk in the BlobStorage group;
        // some fields like SyncState we manage ourselves, some fields (like guid info)
        // are used by our peers to save their info
        ////////////////////////////////////////////////////////////////////////////
        struct TPeer {
            TPeerSyncState PeerSyncState;
            TPeerGuidInfo PeerGuidInfo;

            void Serialize(IOutputStream &s) const;
            void ParseFromArcadiaStream(IInputStream &str);
            void Serialize(NKikimrVDiskData::TSyncerVDiskEntry &pb) const;
            void Parse(const NKikimrVDiskData::TSyncerVDiskEntry &pb);
            TString ToString() const;
            void OutputHtml(IOutputStream &str) const;
        };

    } // NSyncer


    ////////////////////////////////////////////////////////////////////////////
    // TSyncNeighbors
    ////////////////////////////////////////////////////////////////////////////
    static const ui32 OldSyncerDataSignature = 0x4567FEDC;
    static const ui32 SyncerDataSignature = 0x4567FEDD;

    class TSyncNeighbors : public TThrRefBase
    {
    public:
        using TNeighbors = NSync::TVDiskNeighborsSerializable<NSyncer::TPeer>;
        using TConstIterator = TNeighbors::TConstIterator;
        using TIterator = TNeighbors::TIterator;
        using TValue = TNeighbors::TValue;

        ui64 DbBirthLsn = 0; // FIXME: remove after switching to the new sync

    private:
        const TString LogPrefix;
        const TActorId NotifyId;
        TNeighbors Neighbors;
        NSync::TQuorumTracker QuorumTracker;

    public:
        TSyncNeighbors(const TString &logPrefix,
                       const TActorId &notifyId,
                       const TVDiskIdShort &self,
                       std::shared_ptr<TBlobStorageGroupInfo::TTopology> top);

        TConstIterator begin() const { return Neighbors.Begin(); }
        TConstIterator end() const { return Neighbors.End(); }
        TIterator begin() { return Neighbors.Begin(); }
        TIterator end() { return Neighbors.End(); }
        TValue &operator [](const TVDiskIdShort &vdisk) { return Neighbors[vdisk]; }
        const TValue &operator [](const TVDiskIdShort &vdisk) const { return Neighbors[vdisk]; }

        template <class TPrinter>
        void OutputHtml(IOutputStream &str,
                        TPrinter &printer,
                        const TString &name,
                        const TString &divClass) const {
            Neighbors.OutputHtml<TPrinter>(str, printer, name, divClass);
        }

        template <class TPrinter>
        void OutputHtmlTable(IOutputStream &str, TPrinter &printer) const {
            Neighbors.OutputHtmlTable<TPrinter>(str, printer);
        }

        class TOldSer;
        class TOldDes;
        class TSer;
        class TDes;
        void OldSerialize(IOutputStream &str, const TBlobStorageGroupInfo *info) const;
        void OldParse(IInputStream &str);
        void OldParse(const TString &data);
        void Serialize(IOutputStream &str, const TBlobStorageGroupInfo *info) const;
        void Serialize(NKikimrVDiskData::TSyncerEntryPoint *pb, const TBlobStorageGroupInfo *info) const;
        void Parse(IInputStream &str);
        void Parse(const TString &data);
        void Parse(const NKikimrVDiskData::TSyncerEntryPoint &pb);
        void ApplyChanges(const TActorContext &ctx,
                          const NSyncer::TSyncerJobTask *task,
                          TDuration syncTimeInterval);
        void RecoverLocally(const TVDiskIdShort &vdisk, const TSyncState &syncState);
    };

    using TSyncNeighborsPtr = TIntrusivePtr<TSyncNeighbors>;


    ////////////////////////////////////////////////////////////////////////////
    // TSyncerDataSerializer
    ////////////////////////////////////////////////////////////////////////////
    class TSyncerDataSerializer {
    private:
        ui32 Signature = 0;
        NKikimrVDiskData::TSyncerEntryPoint Proto;

    public:
        friend struct TSyncerData;
        friend class TProtoState;
        TString Serialize() const;
    };


    ////////////////////////////////////////////////////////////////////////////
    // TSyncerData
    ////////////////////////////////////////////////////////////////////////////
    struct TSyncerData : public TThrRefBase {
        TSyncNeighborsPtr Neighbors;
        NSyncer::TLocalSyncerState LocalSyncerState;
        const TActorId NotifyId;
        std::optional<NKikimrConfig::TStoredCompatibilityInfo> StoredCompatibilityInfo;
        NKikimrConfig::TStoredCompatibilityInfo CurrentCompatibilityInfo;

        TSyncerData(const TString &logPrefix,
                    const TActorId &notifyId,
                    const TVDiskIdShort &selfVDisk,
                    std::shared_ptr<TBlobStorageGroupInfo::TTopology> top,
                    const TString &entryPoint = TString());

        TSyncerData(const TString &logPrefix,
                    const TActorId &notifyId,
                    const TVDiskIdShort &selfVDisk,
                    std::shared_ptr<TBlobStorageGroupInfo::TTopology> top,
                    TContiguousSpan entryPoint);


        // we call this func during local recovery to apply last changes from recovery log
        // FIXME: looks like we MUST NOT call this function!!!!!!!!!!!!!!!!!!!!!!!!
        void PutFromRecoveryLog(const TVDiskIdShort &vdisk, const TSyncState &syncState);
        TString Serialize(const TBlobStorageGroupInfo *info) const;
        void Serialize(TSyncerDataSerializer &s, const TBlobStorageGroupInfo *info) const;
        bool CheckCompatibility(TString& errorReason);

        // check and cut signature
        static TContiguousSpan WithoutSignature(TContiguousSpan entryPoint);
        static TString WithoutSignature(const TString &entryPoint);
        static bool CheckEntryPoint(const TString &logPrefix,
                                    const TActorId &notifyId,
                                    const TVDiskIdShort &selfVDisk,
                                    std::shared_ptr<TBlobStorageGroupInfo::TTopology> top,
                                    const TString &entryPoint,
                                    TString& errorReason,
                                    bool suppressCompatibilityCheck);
        static bool CheckEntryPoint(const TString &logPrefix,
                                    const TActorId &notifyId,
                                    const TVDiskIdShort &selfVDisk,
                                    std::shared_ptr<TBlobStorageGroupInfo::TTopology> top,
                                    const TContiguousSpan &entryPoint,
                                    TString& errorReason,
                                    bool suppressCompatibilityCheck);

        // Convert from old entry point format to protobuf format
        // TODO: we can remove this function after migrating to the protobuf format
        static TString Convert(const TVDiskIdShort &selfVDisk,
                              std::shared_ptr<TBlobStorageGroupInfo::TTopology> top,
                              const TString &entryPoint);

        static TString Convert(const TVDiskIdShort &selfVDisk,
                              std::shared_ptr<TBlobStorageGroupInfo::TTopology> top,
                              const TContiguousSpan &entryPoint);

    private:
        void ParseWOSignature(const TString &serProto);
    };

} // NKikimr
