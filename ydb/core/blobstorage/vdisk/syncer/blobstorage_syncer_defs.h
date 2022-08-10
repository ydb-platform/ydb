#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/vdisk/common/blobstorage_vdisk_guids.h>
#include <ydb/core/protos/blobstorage.pb.h>

namespace NKikimr {

    namespace NSyncer {

        ////////////////////////////////////////////////////////////////////////
        // TLocalSyncerState
        // After local recovery we got this understanding of the surrounding
        // world
        ////////////////////////////////////////////////////////////////////////
        struct TLocalSyncerState : public TThrRefBase {
            using ELocalState = NKikimrBlobStorage::TLocalGuidInfo::EState;
            using TLocalVal = NKikimrBlobStorage::TLocalGuidInfo;

            // locally saved State
            ELocalState State = TLocalVal::Empty;
            // locally saved Guid
            TVDiskEternalGuid Guid;
            // db birth lsn (see comment in protobuf)
            ui64 DbBirthLsn = 0;

            TLocalSyncerState() = default;
            TLocalSyncerState(const TLocalSyncerState &) = default;
            TLocalSyncerState(ELocalState state, TVDiskEternalGuid guid, ui64 dbBirthLsn)
                : State(state)
                , Guid(guid)
                , DbBirthLsn(dbBirthLsn)
            {}
            TLocalSyncerState &operator=(const TLocalSyncerState &other) = default;

            void Output(IOutputStream &str) const {
                str << "[State# " << State
                << " Guid# " << Guid
                << " DbBirthLsn# " << DbBirthLsn << "]";
            }

            void Serialize(NKikimrBlobStorage::TLocalGuidInfo *localGuidInfo) const {
                localGuidInfo->SetState(State);
                localGuidInfo->SetGuid(Guid);
                localGuidInfo->SetDbBirthLsn(DbBirthLsn);
            }

            void Parse(const NKikimrBlobStorage::TLocalGuidInfo &localGuidInfo) {
                State = localGuidInfo.GetState();
                Guid = localGuidInfo.GetGuid();
                DbBirthLsn = localGuidInfo.GetDbBirthLsn();
            }
        };

    } // NSyncer

} // NKikimr
