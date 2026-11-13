#include "vdisk_recoverylogwriter.h"
#include "vdisk_events.h"
#include "vdisk_mongroups.h"
#include <ydb/core/blobstorage/lwtrace_probes/blobstorage_probes.h>
#include <util/generic/queue.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>

namespace NKikimr {

LWTRACE_USING(BLOBSTORAGE_PROVIDER);

    ////////////////////////////////////////////////////////////////////////////////
    // TRecoveryLogWriter -- it makes all records to recovery log go sequentially
    // according to lsns
    ////////////////////////////////////////////////////////////////////////////////
    class TRecoveryLogWriter : public TActorBootstrapped<TRecoveryLogWriter> {

        ////////////////////////////////////////////////////////////////////////////
        // TCounters
        ////////////////////////////////////////////////////////////////////////////
        struct TCounters {
            NMonitoring::TDynamicCounters::TCounterPtr LogRecords;

            struct TItem {
                ::NMonitoring::TDynamicCounters::TCounterPtr Msgs;
                ::NMonitoring::TDynamicCounters::TCounterPtr Bytes;
            };

            TIntrusivePtr<::NMonitoring::TDynamicCounters> Group;
            TVector<TItem> Counters;

            TCounters(TIntrusivePtr<::NMonitoring::TDynamicCounters> mon)
                : Group(mon->GetSubgroup("subsystem", "logrecs"))
                , Counters(static_cast<size_t>(TLogSignature::Max))
            {
                LogRecords = Group->GetCounter("LogRecords", true);
            }

            void Update(TLogSignature signature, i64 size) {
                ++*LogRecords;
                auto& item = Counters[static_cast<int>(signature)];
                // Registers on the first record, so signatures a VDisk never logs are not exported.
                if (!item.Msgs) {
                    const TString name = "Log" + signature.ToString();
                    item.Msgs = Group->GetCounter(name + "Msgs", true, NMonGroup::GetVDiskCounterVisibility());
                    item.Bytes = Group->GetCounter(name + "Bytes", true, NMonGroup::GetVDiskCounterVisibility());
                }
                ++*item.Msgs;
                *item.Bytes += size;
            }
        };

        ////////////////////////////////////////////////////////////////////////////
        // TQueueItem
        ////////////////////////////////////////////////////////////////////////////
        struct TQueueItem {
            std::unique_ptr<IEventHandle> Ev;
            ui64 LsnSegmentStart;
            ui64 Lsn;

            TQueueItem(std::unique_ptr<IEventHandle> ev, ui64 lsnSegmentStart, ui64 lsn)
                : Ev(std::move(ev))
                , LsnSegmentStart(lsnSegmentStart)
                , Lsn(lsn)
            {
                Y_DEBUG_ABORT_UNLESS(lsn >= lsnSegmentStart);
            }

            struct TGreater {
                bool operator() (const TQueueItem &x, const TQueueItem &y) const {
                    return x.LsnSegmentStart > y.Lsn;
                }
            };
        };

        typedef TPriorityQueue<TQueueItem, TVector<TQueueItem>, TQueueItem::TGreater> TQueueType;

        const TActorId YardID;
        const TActorId SkeletonID;
        const NPDisk::TOwner Owner;
        const NPDisk::TOwnerRound OwnerRound;
        ui64 CurSentLsn = 0;
        TQueueType Queue;
        ::NMonitoring::TDynamicCounters::TCounterPtr LsmLogBytesWritten;
        TCounters Counters;

        friend class TActorBootstrapped<TRecoveryLogWriter>;

        void Bootstrap(const TActorContext &ctx) {
            Y_UNUSED(ctx);
            Become(&TThis::StateFunc);
        }

        void ProcessQueue(const TActorContext &ctx) {
            const TQueueItem *item = nullptr;
            while (!Queue.empty() && ((item = &Queue.top())->LsnSegmentStart == CurSentLsn + 1)) {
                CurSentLsn = item->Lsn;
                std::unique_ptr<IEventHandle> ev = std::move(const_cast<TQueueItem*>(item)->Ev);
                ui32 type = ev->Type;
                switch (type) {
                    case TEvBlobStorage::EvLog:
                        LWTRACK(VDiskRecoveryLogWriterVPutIsSent, ev->Get<NPDisk::TEvLog>()->Orbit, Owner, item->Lsn);
                        break;
                    case TEvBlobStorage::EvMultiLog:
                        {
                            NPDisk::TEvMultiLog *evLogs = ev->Get<NPDisk::TEvMultiLog>();
                            for (auto &[log, _] : evLogs->Logs) {
                                LWTRACK(VDiskRecoveryLogWriterVPutIsSent, log->Orbit, Owner, log->Lsn);
                            }
                            break;
                        }
                }
                Queue.pop();
                ctx.Send(ev.release());
            }
        }

        void Handle(NPDisk::TEvLog::TPtr &ev, const TActorContext &ctx) {
            ui64 lsnSegmentStart = ev->Get()->LsnSegmentStart;
            ui64 lsn = ev->Get()->Lsn;
            LWTRACK(VDiskRecoveryLogWriterVPutIsRecieved, ev->Get()->Orbit, Owner, lsn);
            TLogSignature signature = ev->Get()->Signature.GetUnmasked();
            Y_ABORT_UNLESS(TLogSignature::First < signature && signature < TLogSignature::Max);
            i64 msgSize = ev->Get()->ApproximateSize();
            // count written bytes
            *LsmLogBytesWritten += msgSize;
            // update generic counters
            Counters.Update(signature, msgSize);
            std::unique_ptr<IEventHandle> converted(ev->Forward(YardID).Release());

            if (lsnSegmentStart == CurSentLsn + 1) {
                // rewrite and send message;
                LWTRACK(VDiskRecoveryLogWriterVPutIsSent, converted->Get<NPDisk::TEvLog>()->Orbit, Owner, lsn);
                ctx.Send(converted.release());
                CurSentLsn = lsn;
                // proceed with elements waiting in the queue
                ProcessQueue(ctx);
            } else {
                Queue.push(TQueueItem(std::move(converted), lsnSegmentStart, lsn));
            }
        }

        void Handle(NPDisk::TEvMultiLog::TPtr &ev, const TActorContext &ctx) {
            NPDisk::TEvMultiLog *logs = ev->Get();
            ui64 lsnSegmentStart = logs->LsnSeg.First;
            ui64 lsn = logs->LsnSeg.Last;
            Y_VERIFY_DEBUG_S(lsnSegmentStart == logs->Logs.front().Event->LsnSegmentStart && lsn == logs->Logs.back().Event->Lsn,
                    "LsnSeg not match with inner logs"
                    << "LsnSeg# " << logs->LsnSeg.ToString()
                    << "Logs.front().LsnSegmentStart# " << logs->Logs.front().Event->LsnSegmentStart
                    << "Logs.back().Lsn# " << logs->Logs.back().Event->Lsn);
            for (auto &[log, _] : logs->Logs) {
                LWTRACK(VDiskRecoveryLogWriterVPutIsRecieved, log->Orbit, Owner, log->Lsn);
                TLogSignature signature = log->Signature.GetUnmasked();
                Y_ABORT_UNLESS(TLogSignature::First < signature && signature < TLogSignature::Max);
                i64 msgSize = log->ApproximateSize();
                // count written bytes
                *LsmLogBytesWritten += msgSize;
                // update generic counters
                Counters.Update(signature, msgSize);
                LWTRACK(VDiskRecoveryLogWriterVPutIsSent, log->Orbit, Owner, lsn);
            }
            std::unique_ptr<IEventHandle> converted(ev->Forward(YardID).Release());

            if (lsnSegmentStart == CurSentLsn + 1) {
                // rewrite and send message;
                ctx.Send(converted.release());
                CurSentLsn = lsn;
                // proceed with elements waiting in the queue
                ProcessQueue(ctx);
            } else {
                Queue.push(TQueueItem(std::move(converted), lsnSegmentStart, lsn));
            }
        }

        void Handle(TEvBlobStorage::TEvVCompact::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            ui64 lsn = CurSentLsn + 1;
            ctx.Send(SkeletonID, new NPDisk::TEvCutLog(Owner, OwnerRound, lsn, 0, 0, 0, 0));
        }

        void HandlePoison(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            Die(ctx);
        }

        STRICT_STFUNC(StateFunc,
            HFunc(NPDisk::TEvLog, Handle)
            HFunc(NPDisk::TEvMultiLog, Handle)
            HFunc(TEvBlobStorage::TEvVCompact, Handle)
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
        )

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_RECOVERY_LOG_WRITER;
        }

        TRecoveryLogWriter(const TActorId &yardID, const TActorId &skeletonID,
                           NPDisk::TOwner owner, NPDisk::TOwnerRound ownerRound, ui64 startLsn,
                           TIntrusivePtr<::NMonitoring::TDynamicCounters> counters)
            : TActorBootstrapped<TRecoveryLogWriter>()
            , YardID(yardID)
            , SkeletonID(skeletonID)
            , Owner(owner)
            , OwnerRound(ownerRound)
            , CurSentLsn(startLsn)
            , Queue()
            , LsmLogBytesWritten(counters->GetSubgroup("subsystem", "lsmhull")->GetCounter("LsmLogBytesWritten", true))
            , Counters(counters)
        {}
    };

    IActor* CreateRecoveryLogWriter(const TActorId &yardID, const TActorId &skeletonID, NPDisk::TOwner owner,
                                    NPDisk::TOwnerRound ownerRound,
                                    ui64 startLsn, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters) {
        return new TRecoveryLogWriter(yardID, skeletonID, owner, ownerRound, startLsn, counters);
    }

} // NKikimr
