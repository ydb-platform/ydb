#pragma once

#include "defs.h"
#include "guid_firstrun.h"
#include "blobstorage_syncer_defs.h"
#include <ydb/core/blobstorage/base/blobstorage_syncstate.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_syncneighbors.h>
#include <ydb/core/blobstorage/vdisk/ingress/blobstorage_ingress.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>

namespace NKikimr {

    namespace NSyncer {

        ////////////////////////////////////////////////////////////////////////////
        // EDecision
        // Enumeration for decision we make after communication with other VDisks
        ////////////////////////////////////////////////////////////////////////////
        enum class EDecision {
            // First run for VDisk, assigning guids
            FirstRun,
            // Local state of VDisk is good
            Good,
            // VDisk lost its data
            LostData,
            // Can't happen for supported number of failures;
            // bugs in code or unsupported failures have taken place
            Inconsistency
        };

        const char *EDecisionToStr(EDecision d);
        inline bool IsBad(EDecision d) {
            return d == EDecision::LostData || d == EDecision::Inconsistency;
        }


        ////////////////////////////////////////////////////////////////////////////
        // TDecision
        // Decision we got after guid recovery algorithm work
        ////////////////////////////////////////////////////////////////////////////
        class TDecision {
            // final decision
            EDecision Decision;
            // if final decision is FirstRun, we have a substep to start with
            EFirstRunStep FirstRunStep;
            // recovered guid
            TVDiskEternalGuid Guid;
            // human readable explanation for inconsistency
            TString Explanation;
            // for FirstRun:
            //     we can detect data loss: AddInfo means LostData
            // for LostData:
            //     we can detect if we recovered from previous run
            //     or not: AddInfo means SubsequentFailure
            bool AddInfo;

            TDecision(EDecision d,
                      EFirstRunStep f,
                      TVDiskEternalGuid guid,
                      const TString &e,
                      bool a);

        public:
            TDecision() = delete;
            TDecision(const TDecision &) = default;
            TDecision &operator=(const TDecision &) = default;
            TDecision(TDecision &&) = default;
            TDecision &operator=(TDecision &&) = default;
            ~TDecision() = default;

            static TDecision FirstRun(EFirstRunStep step, TVDiskEternalGuid guid);
            static TDecision Inconsistency(const TString &expl);
            static TDecision LostData(TVDiskEternalGuid guid, bool subsequentFailure);
            static TDecision LostData(EFirstRunStep step,
                                      TVDiskEternalGuid guid,
                                      bool subsequentFailure);
            static TDecision Good(TVDiskEternalGuid guid);
            static TDecision Good(EFirstRunStep step, TVDiskEternalGuid guid);
            void Output(IOutputStream &str) const;
            TString ToString() const;
            EDecision GetDecision() const { return Decision; }
            TVDiskEternalGuid GetGuid() const { return Guid; }
            EFirstRunStep GetFirstRunStep() const;
            TString GetExplanation() const { return Explanation; }
            bool BadDecision() const { return IsBad(Decision); }
        };


        ////////////////////////////////////////////////////////////////////////////
        // TOutcome
        // Outcome of the final decision that we pass outside
        ////////////////////////////////////////////////////////////////////////////
        struct TOutcome {
            const EDecision Decision;
            const TVDiskEternalGuid Guid;
            const TString Explanation;

            TOutcome(const TDecision &d)
                : Decision(d.GetDecision())
                , Guid(d.GetGuid())
                , Explanation(d.GetExplanation())
            {}

            TOutcome(EDecision decision, ui64 guid)
                : Decision(decision)
                , Guid(guid)
                , Explanation()
            {}

            TOutcome(const TOutcome &) = default;
            TOutcome &operator=(const TOutcome &) = default;
            TOutcome(TOutcome &&) = default;
            TOutcome &operator=(TOutcome &&) = default;
            ~TOutcome() = default;
            bool BadDecision() const { return IsBad(Decision); }
            void Output(IOutputStream &str) const;
            TString ToString() const;
        };

    } // NSyncer


    ////////////////////////////////////////////////////////////////////////////
    // TEvVDiskGuidRecovered
    ////////////////////////////////////////////////////////////////////////////
    struct TEvVDiskGuidRecovered : public
        TEventLocal<TEvVDiskGuidRecovered, TEvBlobStorage::EvVDiskGuidRecovered> {

        NSyncer::TOutcome Outcome;

        TEvVDiskGuidRecovered(const NSyncer::TOutcome &outcome)
            : Outcome(outcome)
        {}
    };


    ////////////////////////////////////////////////////////////////////////////
    // VDISK GUID RECOVERY ACTOR
    // Determine our VDisk Guid by communicating with other VDisks in the
    // blobstorage group.
    ////////////////////////////////////////////////////////////////////////////
    class TVDiskContext;
    IActor *CreateVDiskGuidRecoveryActor(TIntrusivePtr<TVDiskContext> vctx,
                                         TIntrusivePtr<TBlobStorageGroupInfo> info,
                                         const TActorId &committerId,
                                         const TActorId &notifyId,
                                         const NSyncer::TLocalSyncerState &localState,
                                         bool readOnly);

} // NKikimr
