#include "blobstorage_syncer_data.h"
#include "guid_recovery.h"
#include "syncer_job_task.h"
#include "blobstorage_syncer_dataserdes.h"
#include <ydb/core/blobstorage/base/utility.h>

using namespace NKikimrServices;
using namespace NKikimr::NSyncer;

namespace NKikimr {

    namespace NSyncer {

        ////////////////////////////////////////////////////////////////////////
        // TPeerSyncState
        ////////////////////////////////////////////////////////////////////////
        TPeerSyncState::TPeerSyncState()
            : SchTime(TAppData::TimeProvider->Now())
        {}

        void TPeerSyncState::Serialize(IOutputStream &s) const {
            i32 status = (i32)LastSyncStatus;
            s.Write(&status, sizeof(status));
            SyncState.Serialize(s);
            s.Write(&SchTime, sizeof(SchTime));
            s.Write(&LastTry, sizeof(LastTry));
            s.Write(&LastGood, sizeof(LastGood));
        }

        void TPeerSyncState::ParseFromArcadiaStream(IInputStream &s) {
            i32 status = 0;
            if (s.Load(&status, sizeof(status)) != sizeof(status))
                ythrow yexception() << "invalid status size";
            LastSyncStatus = (ESyncStatus)status;
            if (!SyncState.Deserialize(s))
                ythrow yexception() << "invalid syncstate";
            if (s.Load(&SchTime, sizeof(SchTime)) != sizeof(SchTime))
                ythrow yexception() << "invalid SchTime size";
            if (s.Load(&LastTry, sizeof(LastTry)) != sizeof(LastTry))
                ythrow yexception() << "invalid LastTry size";
            if (s.Load(&LastGood, sizeof(LastGood)) != sizeof(LastGood))
                ythrow yexception() << "invalid LastGood size";
        }

        void TPeerSyncState::Serialize(NKikimrVDiskData::TSyncerVDiskEntry &pb) const {
            pb.SetLastSyncStatus(LastSyncStatus);
            SyncStateFromSyncState(SyncState, pb.MutableSyncState());
            pb.SetSchTime(SchTime.GetValue());
            pb.SetLastTry(LastTry.GetValue());
            pb.SetLastGood(LastGood.GetValue());
        }

        void TPeerSyncState::Parse(const NKikimrVDiskData::TSyncerVDiskEntry &pb) {
            LastSyncStatus = pb.GetLastSyncStatus();
            SyncState = SyncStateFromSyncState(pb.GetSyncState());
            SchTime = TInstant::MicroSeconds(pb.GetSchTime());
            LastTry = TInstant::MicroSeconds(pb.GetLastTry());
            LastGood = TInstant::MicroSeconds(pb.GetLastGood());
        }

        TString TPeerSyncState::ToString() const {
            TStringStream str;
            str << "{" << "LastSyncStatus# " << LastSyncStatus
                << " SyncState# " << SyncState.ToString()
                << " LastTry# " << LastTry << " LastGood# " << LastGood << "}";
            return str.Str();
        }

        void TPeerSyncState::OutputHtml(IOutputStream &str) const {
            HTML(str) {
                ESyncStatus state = LastSyncStatus;
                if (NSyncer::TPeerSyncState::Good(state)) {
                    PARA_CLASS("text-success") {str << state;}
                } else {
                    PARA_CLASS("text-warning") {str << state;}
                }
                SMALL() {
                    SMALL() {
                        str << "SchTime: " << ToStringLocalTimeUpToSeconds(SchTime);
                        str << "<br>";
                        str << "LastTry: " << ToStringLocalTimeUpToSeconds(LastTry);
                        str << "<br>";
                        str << "LastGood: " << ToStringLocalTimeUpToSeconds(LastGood);
                        str << "<br>";
                        str << "[Guid, SyncedLsn]: " << SyncState.ToString();
                        str << "<br>";
                    }
                }
            }
        }


        ////////////////////////////////////////////////////////////////////////
        // TPeerGuidInfo
        ////////////////////////////////////////////////////////////////////////
        void TPeerGuidInfo::Serialize(NKikimrVDiskData::TSyncerVDiskEntry &pb) const {
            *pb.MutableSyncGuidInfo() = Info;
        }

        void TPeerGuidInfo::Parse(const NKikimrVDiskData::TSyncerVDiskEntry &pb) {
            Info = pb.GetSyncGuidInfo();
        }

        TString TPeerGuidInfo::ToString() const {
            TStringStream str;
            str << "{Guid#" << Info.GetGuid() << " State# " << Info.GetState() << "}";
            return str.Str();
        }

        void TPeerGuidInfo::OutputHtml(IOutputStream &str) const {
            HTML(str) {
                SMALL() {
                    SMALL() {
                        str << "Guid: " << Info.GetGuid();
                        str << "<br>";
                        str << "State: " << Info.GetState();
                    }
                }
            }
        }


        ////////////////////////////////////////////////////////////////////////////
        // TPeer
        // We manage this structure for every disk in the group
        ////////////////////////////////////////////////////////////////////////////
        void TPeer::Serialize(IOutputStream &s) const {
            PeerSyncState.Serialize(s);
            // we don't serialize PeerGuidInfo because we can't do it in old format
        }

        void TPeer::ParseFromArcadiaStream(IInputStream &s) {
            PeerSyncState.ParseFromArcadiaStream(s);
            // we don't parse PeerGuidInfo because we can't do it in old format
        }

        void TPeer::Serialize(NKikimrVDiskData::TSyncerVDiskEntry &pb) const {
            PeerSyncState.Serialize(pb);
            PeerGuidInfo.Serialize(pb);
        }

        void TPeer::Parse(const NKikimrVDiskData::TSyncerVDiskEntry &pb) {
            PeerSyncState.Parse(pb);
            PeerGuidInfo.Parse(pb);
        }

        TString TPeer::ToString() const {
            TStringStream str;
            str << "{SyncState# " << PeerSyncState.ToString()
                << " GuidInfo# " << PeerGuidInfo.ToString() << "}";
            return str.Str();
        }

        void TPeer::OutputHtml(IOutputStream &str) const {
            PeerSyncState.OutputHtml(str);
            PeerGuidInfo.OutputHtml(str);
        }

    } // NSyncer

    ////////////////////////////////////////////////////////////////////////////
    // TSyncNeighbors
    ////////////////////////////////////////////////////////////////////////////
    TSyncNeighbors::TSyncNeighbors(const TString &logPrefix,
                                   const TActorId &notifyId,
                                   const TVDiskIdShort &self,
                                   std::shared_ptr<TBlobStorageGroupInfo::TTopology> top)
        : LogPrefix(logPrefix)
        , NotifyId(notifyId)
        , Neighbors(self, top)
        , QuorumTracker(self, top, false)
    {}

    void TSyncNeighbors::OldSerialize(IOutputStream &str, const TBlobStorageGroupInfo *info) const {
        TOldSer ser(str, info);
        Neighbors.GenericSerialize(ser);
    }

    void TSyncNeighbors::Serialize(IOutputStream &str, const TBlobStorageGroupInfo *info) const {
        TSer ser(str, info);
        Neighbors.GenericSerialize(ser);
    }

    void TSyncNeighbors::Serialize(NKikimrVDiskData::TSyncerEntryPoint *pb, const TBlobStorageGroupInfo *info) const {
        TSer ser(pb, info);
        Neighbors.GenericSerialize(ser);
    }

    void TSyncNeighbors::OldParse(IInputStream &str) {
        TOldDes des(str);
        Neighbors.GenericParse(des);
    }

    void TSyncNeighbors::OldParse(const TString &data) {
        Y_ABORT_UNLESS(!data.empty());
        TStringInput str(data);
        OldParse(str);
    }

    void TSyncNeighbors::Parse(IInputStream &str) {
        TDes des(str);
        Neighbors.GenericParse(des);
    }

    void TSyncNeighbors::Parse(const TString &data) {
        Y_ABORT_UNLESS(!data.empty());
        TStringInput str(data);
        Parse(str);
    }

    void TSyncNeighbors::Parse(const NKikimrVDiskData::TSyncerEntryPoint &pb) {
        TDes des(&pb);
        Neighbors.GenericParse(des);
    }


    // this function is called from TSyncerScheduler,
    // so all changes are made from one thread
    void TSyncNeighbors::ApplyChanges(const TActorContext &ctx,
                                      const NSyncer::TSyncerJobTask *task,
                                      TDuration syncTimeInterval) {
        LOG_INFO(ctx, BS_SYNCER, VDISKP(LogPrefix, "JOB_DONE: %s", task->ToString().data()));

        TDuration timeout = task->IsFullRecoveryTask() ? TDuration::Seconds(0) : syncTimeInterval;
        TInstant schTime = TAppData::TimeProvider->Now() + timeout;

        NSyncer::TPeerSyncState &ref = Neighbors[task->VDiskId].Get().PeerSyncState;
        ref = task->GetCurrent();
        ref.SchTime = schTime;
    }

    void TSyncNeighbors::RecoverLocally(const TVDiskIdShort &vdisk, const TSyncState &syncState) {
        Neighbors[vdisk].Get().PeerSyncState.SyncState = syncState;
    }

    ////////////////////////////////////////////////////////////////////////////
    // TSyncerDataSerializer
    ////////////////////////////////////////////////////////////////////////////
    TString TSyncerDataSerializer::Serialize() const {
        // Data Format:
        // data ::= [Signature=4b] TSyncerEntryPoint_Serialized

        // write to stream
        TStringStream str;
        str.Write(&Signature, sizeof(Signature));
        Proto.SerializeToArcadiaStream(&str);
        return str.Str();
    }

    ////////////////////////////////////////////////////////////////////////////
    // TSyncerData
    ////////////////////////////////////////////////////////////////////////////
    void TSyncerData::Serialize(TSyncerDataSerializer &s, const TBlobStorageGroupInfo *info) const {
        s.Signature = SyncerDataSignature;
        Neighbors->Serialize(&s.Proto, info);
        LocalSyncerState.Serialize(s.Proto.MutableLocalGuidInfo());
        s.Proto.MutableCompatibilityInfo()->CopyFrom(CurrentCompatibilityInfo);
    }

    TString TSyncerData::Serialize(const TBlobStorageGroupInfo *info) const {
        TSyncerDataSerializer s;
        Serialize(s, info);
        return s.Serialize();
    }

    void TSyncerData::ParseWOSignature(const TString &serProto) {
        if (!serProto.empty()) {
            NKikimrVDiskData::TSyncerEntryPoint proto;
            auto status = proto.ParseFromString(serProto);
            Y_ABORT_UNLESS(status);
            LocalSyncerState.Parse(proto.GetLocalGuidInfo());
            Neighbors->Parse(proto);

            if (proto.HasCompatibilityInfo()) {
                StoredCompatibilityInfo.emplace();
                StoredCompatibilityInfo->CopyFrom(proto.GetCompatibilityInfo());
            }
        }
    }

    // check and cut signature
    TString TSyncerData::WithoutSignature(const TString &entryPoint) {
        if (entryPoint.empty()) {
            return entryPoint;
        } else {
            TStringInput str(entryPoint);
            ui32 sign = 0;
            if (str.Load(&sign, sizeof(sign)) != sizeof(sign)
                || sign != SyncerDataSignature) {
                ythrow yexception() << "incorrect signature";
            }
            return entryPoint.substr(sizeof(sign));
        }
    }

    TContiguousSpan TSyncerData::WithoutSignature(TContiguousSpan entryPoint) {
        if (entryPoint.size() == 0) {
            return entryPoint;
        } else {
            TMemoryInput str(entryPoint.GetData(), entryPoint.GetSize());
            ui32 sign = 0;
            if (str.Load(&sign, sizeof(sign)) != sizeof(sign)
                || sign != SyncerDataSignature) {
                ythrow yexception() << "incorrect signature";
            }
            return entryPoint.SubSpan(sizeof(sign), entryPoint.size() - sizeof(sign));
        }
    }

    bool TSyncerData::CheckCompatibility(TString& errorReason) {
        if (StoredCompatibilityInfo) {
            return CompatibilityInfo.CheckCompatibility(&*StoredCompatibilityInfo,
                    NKikimrConfig::TCompatibilityRule::VDisk, errorReason);
        } else {
            return CompatibilityInfo.CheckCompatibility(nullptr,
                    NKikimrConfig::TCompatibilityRule::VDisk, errorReason);
        }
    }

    TSyncerData::TSyncerData(const TString &logPrefix,
                             const TActorId &notifyId,
                             const TVDiskIdShort &selfVDisk,
                             std::shared_ptr<TBlobStorageGroupInfo::TTopology> top,
                             const TString &entryPoint)
        : Neighbors(MakeIntrusive<TSyncNeighbors>(logPrefix,
                                                  notifyId,
                                                  selfVDisk,
                                                  top))
        , LocalSyncerState()
        , NotifyId(notifyId)
        , CurrentCompatibilityInfo(CompatibilityInfo.MakeStored(NKikimrConfig::TCompatibilityRule::VDisk))
    {
        TString serProto = WithoutSignature(Convert(selfVDisk, top, entryPoint));
        ParseWOSignature(serProto);
    }

    TSyncerData::TSyncerData(const TString &logPrefix,
                             const TActorId &notifyId,
                             const TVDiskIdShort &selfVDisk,
                             std::shared_ptr<TBlobStorageGroupInfo::TTopology> top,
                             TContiguousSpan entryPoint)
        : Neighbors(MakeIntrusive<TSyncNeighbors>(logPrefix,
                                                  notifyId,
                                                  selfVDisk,
                                                  top))
        , LocalSyncerState()
        , NotifyId(notifyId)
        , CurrentCompatibilityInfo(CompatibilityInfo.MakeStored(NKikimrConfig::TCompatibilityRule::VDisk))
    {
        TString serProto = WithoutSignature(Convert(selfVDisk, top, entryPoint));
        ParseWOSignature(serProto);
    }

    bool TSyncerData::CheckEntryPoint(const TString &logPrefix,
                                      const TActorId &notifyId,
                                      const TVDiskIdShort &selfVDisk,
                                      std::shared_ptr<TBlobStorageGroupInfo::TTopology> top,
                                      const TString &entryPoint,
                                      TString& errorReason,
                                      bool suppressCompatibilityCheck) {
        try {
            TSyncerData n(logPrefix, notifyId, selfVDisk, top);
            TString serProto = WithoutSignature(Convert(selfVDisk, top, entryPoint));
            n.ParseWOSignature(serProto);
            return suppressCompatibilityCheck || n.CheckCompatibility(errorReason);
        } catch (yexception e) {
            errorReason = e.what();
            return false;
        }
    }

    bool TSyncerData::CheckEntryPoint(const TString &logPrefix,
                                      const TActorId &notifyId,
                                      const TVDiskIdShort &selfVDisk,
                                      std::shared_ptr<TBlobStorageGroupInfo::TTopology> top,
                                      const TContiguousSpan &entryPoint,
                                      TString& errorReason,
                                      bool suppressCompatibilityCheck) {
        try {
            TSyncerData n(logPrefix, notifyId, selfVDisk, top);
            TString serProto = WithoutSignature(Convert(selfVDisk, top, entryPoint)); //FIXME(innokentii) unnecessary copy
            n.ParseWOSignature(serProto);
            return suppressCompatibilityCheck || n.CheckCompatibility(errorReason);
        } catch (yexception e) {
            errorReason = e.what();
            return false;
        }
    }

    // Convert from old entry point format to protobuf format
    // TODO: we can remove this function after migrating to the protobuf format
    TString TSyncerData::Convert(const TVDiskIdShort &selfVDisk,
                                std::shared_ptr<TBlobStorageGroupInfo::TTopology> top,
                                const TString &entryPoint) {
        if (entryPoint.empty()) {
            return entryPoint;
        } else {
            TStringInput str(entryPoint);
            ui32 sign = 0;
            if (str.Load(&sign, sizeof(sign)) != sizeof(sign)) {
                ythrow yexception() << "incorrect signature";
            }

            if (sign == SyncerDataSignature) {
                return entryPoint;
            } else if (sign == OldSyncerDataSignature) {
                using TNeighbors = NSync::TVDiskNeighborsSerializable<NSyncer::TPeer>;

                // create empty neighbors
                TNeighbors n(selfVDisk, top);
                // parse from old format data
                TSyncNeighbors::TOldDes des(str);
                n.GenericParse(des);
                // recover groupId and groupGen for further conversion
                using TGroupId = TIdWrapper<ui32, TGroupIdTag>;
                TGroupId groupId = des.GetGroupId();
                ui32 groupGen = des.GetGroupGeneration();
                // serialize into current format
                TStringStream output;
                // current syncer data signature
                output.Write(&SyncerDataSignature, sizeof(SyncerDataSignature));
                TSyncNeighbors::TSer ser(output, groupId, groupGen);
                n.GenericSerialize(ser);
                return output.Str();
            } else {
                ythrow yexception() << "incorrect signature";
            }
        }
    }

    TString TSyncerData::Convert(const TVDiskIdShort &selfVDisk,
                                std::shared_ptr<TBlobStorageGroupInfo::TTopology> top,
                                const TContiguousSpan &entryPoint) {
        if (entryPoint.size() == 0) {
            return TString();
        } else {
            TMemoryInput str(entryPoint.GetData(), entryPoint.GetSize());
            ui32 sign = 0;
            if (str.Load(&sign, sizeof(sign)) != sizeof(sign)) {
                ythrow yexception() << "incorrect signature";
            }

            if (sign == SyncerDataSignature) {
                return TString(entryPoint.GetData(), entryPoint.GetSize());
            } else if (sign == OldSyncerDataSignature) {
                using TNeighbors = NSync::TVDiskNeighborsSerializable<NSyncer::TPeer>;

                // create empty neighbors
                TNeighbors n(selfVDisk, top);
                // parse from old format data
                TSyncNeighbors::TOldDes des(str);
                n.GenericParse(des);
                // recover groupId and groupGen for further conversion
                using TGroupId = TIdWrapper<ui32, TGroupIdTag>;
                TGroupId groupId = des.GetGroupId();
                ui32 groupGen = des.GetGroupGeneration();
                // serialize into current format
                TStringStream output;
                // current syncer data signature
                output.Write(&SyncerDataSignature, sizeof(SyncerDataSignature));
                TSyncNeighbors::TSer ser(output, groupId, groupGen);
                n.GenericSerialize(ser);
                return output.Str();
            } else {
                ythrow yexception() << "incorrect signature";
            }
        }
    }

    // we call this func during local recovery to apply last changes from recovery log
    void TSyncerData::PutFromRecoveryLog(const TVDiskIdShort &vdisk, const TSyncState &syncState) {
        Neighbors->RecoverLocally(vdisk, syncState);
    }

} // NKikimr
