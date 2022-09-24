#pragma once

#include "defs.h"

namespace NKikimr::NTesting {

    enum class EBlobState {
        NOT_WRITTEN,
        POSSIBLY_WRITTEN, // write was in flight, but no confirmation received
        CERTAINLY_WRITTEN, // write was confirmed
        POSSIBLY_COLLECTED, // blob is beyond unconfirmed GC command
        CERTAINLY_COLLECTED_OR_NEVER_WRITTEN, // blob is gonna be collected soon
    };

    class TGroupOverseer {
        class TImpl;
        std::unique_ptr<TImpl> Impl;

    public:
        TGroupOverseer();
        ~TGroupOverseer();
        void AddGroupToOversee(ui32 groupId);
        void ExamineEnqueue(ui32 nodeId, IEventHandle& ev);
        void ExamineEvent(ui32 nodeId, IEventHandle& ev);
        EBlobState GetBlobState(ui32 groupId, TLogoBlobID id) const;
        void EnumerateBlobs(ui32 groupId, const std::function<void(TLogoBlobID, EBlobState)>& callback) const;
    };

} // NKikimr::NTesting
