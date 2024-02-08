#pragma once

#include "defs.h"

#include "incrhuge_keeper_alloc.h"
#include "incrhuge_keeper_defrag.h"
#include "incrhuge_keeper_delete.h"
#include "incrhuge_keeper_log.h"
#include "incrhuge_keeper_read.h"
#include "incrhuge_keeper_recovery.h"
#include "incrhuge_keeper_write.h"

#include <ydb/library/actors/core/actor.h>

namespace NKikimr {
    namespace NIncrHuge {

        class TKeeper
            : public NActors::TActorBootstrapped<TKeeper>
        {
            TKeeperCommonState State;

            friend class TKeeperComponentBase;

            friend class TAllocator;
            friend class TDefragmenter;
            friend class TDeleter;
            friend class TLogger;
            friend class TReader;
            friend class TRecovery;
            friend class TWriter;

            TAllocator Allocator;
            TDefragmenter Defragmenter;
            TDeleter Deleter;
            TLogger Logger;
            TReader Reader;
            TRecovery Recovery;
            TWriter Writer;

            THashMap<void *, std::unique_ptr<IEventCallback>> CallbackMap;
            ui64 NextCallbackId = 1;

        public:
            static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
                return NKikimrServices::TActivity::BS_INCR_HUGE_KEEPER;
            }

            TKeeper(const TKeeperSettings& settings);

            void Bootstrap(const TActorContext& ctx);

            //////////////////////
            // Callback helpers //
            //////////////////////

            void *RegisterYardCallback(std::unique_ptr<IEventCallback>&& callback);

        private:
            STFUNC(StateFunc);

            /////////////////////////
            // Yard event handlers //
            /////////////////////////

            void Handle(NPDisk::TEvYardInitResult::TPtr& ev, const TActorContext& ctx);
            void Handle(NPDisk::TEvChunkReserveResult::TPtr& ev, const TActorContext& ctx);
            void Handle(NPDisk::TEvChunkWriteResult::TPtr& ev, const TActorContext& ctx);
            void Handle(NPDisk::TEvChunkReadResult::TPtr& ev, const TActorContext& ctx);
            void Handle(NPDisk::TEvLogResult::TPtr& ev, const TActorContext& ctx);
            void Handle(TEvIncrHugeCallback::TPtr& ev, const TActorContext& ctx);
            void Handle(TEvIncrHugeReadLogResult::TPtr& ev, const TActorContext& ctx);
            void Handle(TEvIncrHugeScanResult::TPtr& ev, const TActorContext& ctx);

            void HandlePoison(const TActorContext& ctx);

            void InvokeCallback(void *cookie, NKikimrProto::EReplyStatus status, IEventBase *msg, const TActorContext& ctx);

            void UpdateStatusFlags(NPDisk::TStatusFlags flags, const TActorContext& ctx);
        };

        NActors::IActor *CreateIncrHugeKeeper(const TKeeperSettings& settings);

    } // NIncrHuge
} // NKikimr
