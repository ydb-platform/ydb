#pragma once

#include "defs.h"
#include "vdisk_config.h"

#include <ydb/core/base/blobstorage.h>

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
            SetPrivate(Good, "");
        }

        EState GetState() const {
            return State;
        }

        const TString& GetErrorReason() const {
            return ErrorReason;
        }

        // We call this function when PDisk returned ERROR and we pass pdiskFlags to set the correct state
        EState Set(NKikimrProto::EReplyStatus status, ui32 pdiskFlags, const TString& errorReason) {
            switch (status) {
                case NKikimrProto::CORRUPTED:
                    return SetPrivate(NoWrites, errorReason);
                case NKikimrProto::OUT_OF_SPACE:
                    // check flags additionally
                    Y_ABORT_UNLESS(pdiskFlags & NKikimrBlobStorage::StatusNotEnoughDiskSpaceForOperation);
                    return SetPrivate(WriteOnlyLog, errorReason);
                default:
                    Y_ABORT("Unexpected state# %s", NKikimrProto::EReplyStatus_Name(status).data());
            }
        }

        TString ToString() const {
            TStringStream ss;
            ss << "State# " << StateToString(State);
            if (!ErrorReason.empty()) {
                ss << ", PDiskError# " << ErrorReason;
            }
            return ss.Str();
        }

    private:
        EState State = EState::Unspecified;

        TString ErrorReason;

        EState SetPrivate(EState state, const TString& errorReason) {
            if (state > State) {
                State = state;
                ErrorReason = errorReason;
                return state;
            } else
                return State;
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
        const NKikimrProto::EReplyStatus Status;
        const ui32 PDiskFlags;
        const TString ErrorReason;

        TEvPDiskErrorStateChange(NKikimrProto::EReplyStatus status, ui32 pdiskFlags, const TString &errorReason)
            : Status(status)
            , PDiskFlags(pdiskFlags)
            , ErrorReason(errorReason)
        {}
    };

} // NKikimr

