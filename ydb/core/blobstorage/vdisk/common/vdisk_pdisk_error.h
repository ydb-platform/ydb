#pragma once
#include "defs.h"
#include <ydb/core/base/blobstorage.h>
#include "vdisk_config.h"

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TPDiskErrorState
    // We store state about PDisk in this class
    ////////////////////////////////////////////////////////////////////////////
    class TPDiskErrorState {
    public:
        enum EState {
            Unspecified = 0,
            // good state as expected
            Good = 1,
            // we run out of space, we can write only log to support the system up
            WriteOnlyLog = 2,
            // device is out of order, don't do any writes
            NoWrites = 3
        };

        static inline const char *StateToString(EState state) {
            switch (state) {
                case Unspecified:   return "Unspecified";
                case Good:          return "Good";
                case WriteOnlyLog:  return "WriteOnlyLog";
                case NoWrites:      return "NoWrites";
            }
        }

        TPDiskErrorState() {
            SetPrivate(Good);
        }

        EState GetState() const {
            return static_cast<EState>(AtomicGet(State));
        }

        // We call this function when PDisk returned ERROR and we pass pdiskFlags to set the correct state
        EState Set(NKikimrProto::EReplyStatus status, ui32 pdiskFlags) {
            switch (status) {
                case NKikimrProto::CORRUPTED:
                    return SetPrivate(NoWrites);
                case NKikimrProto::OUT_OF_SPACE:
                    // check flags additionally
                    Y_ABORT_UNLESS(pdiskFlags & NKikimrBlobStorage::StatusNotEnoughDiskSpaceForOperation);
                    return SetPrivate(WriteOnlyLog);
                default:
                    Y_ABORT("Unexpected state# %s", NKikimrProto::EReplyStatus_Name(status).data());
            }
        }

    private:
        TAtomic State = 0;

        EState SetPrivate(EState state) {
            // make sure bad state increments (not decrements), use CAS for that
            while (true) {
                EState curState = GetState();
                if (state > curState) {
                    // if state is worse than curState:
                    TAtomicBase newState = static_cast<TAtomicBase>(state);
                    bool done = AtomicCas(&State, newState, curState);
                    if (done)
                        return state;
                } else
                    return curState;
            }
        }
    };

    ////////////////////////////////////////////////////////////////////////////
    // TEvPDiskErrorStateChange
    // Notification message about PDisk error. When PDisk error happens somewhere
    // inside PDisk we abort this operation and send TEvPDiskErrorStateChange to
    // the VDisk to notify other components and switch to appropriate state
    ////////////////////////////////////////////////////////////////////////////
    struct TEvPDiskErrorStateChange :
        public TEventLocal<TEvPDiskErrorStateChange, TEvBlobStorage::EvPDiskErrorStateChange>
    {
        const TPDiskErrorState::EState State;

        TEvPDiskErrorStateChange(TPDiskErrorState::EState state)
            : State(state)
        {}
    };

} // NKikimr

