#pragma once

#include "task.h"

#include <ydb/core/base/events.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>

namespace NKikimr::NOlap::NResourceBroker::NSubscribe {

// A private event of the NSubscribe actor alone: its own ES_PRIVATE space instead of
// NColumnShard::TEvPrivate, whose header pulls in the whole columnshard.
struct TEvPrivate {
    enum EEv {
        EvStartTask = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_PRIVATE)");
};

class TEvStartTask: public NActors::TEventLocal<TEvStartTask, TEvPrivate::EvStartTask> {
private:
    YDB_READONLY_DEF(std::shared_ptr<ITask>, Task);

public:
    explicit TEvStartTask(std::shared_ptr<ITask> task)
        : Task(task)
    {
    }
};

}   // namespace NKikimr::NOlap::NResourceBroker::NSubscribe
