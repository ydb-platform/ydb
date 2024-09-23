#pragma once

#include "task.h"
#include "events.h"
#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/blob_cache.h>

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NOlap::NBlobOperations::NRead {

class TBlobsForRead {
private:
    THashMap<TString, THashMap<TBlobRange, std::vector<std::shared_ptr<ITask>>>> BlobTasks;
public:
    std::vector<std::shared_ptr<ITask>> ExtractTasksAll() {
        THashMap<ui64, std::shared_ptr<ITask>> tasks;
        for (auto&& i : BlobTasks) {
            for (auto&& r : i.second) {
                for (auto&& t : r.second) {
                    tasks.emplace(t->GetTaskIdentifier(), t);
                }
            }
        }
        std::vector<std::shared_ptr<ITask>> result;
        for (auto&& i : tasks) {
            result.emplace_back(i.second);
        }
        return result;
    }

    std::vector<std::shared_ptr<ITask>> Extract(const TString& storageId, const TBlobRange& bRange) {
        auto it = BlobTasks.find(storageId);
        AFL_VERIFY(it != BlobTasks.end());
        auto itBlobRange = it->second.find(bRange);
        auto result = std::move(itBlobRange->second);
        it->second.erase(itBlobRange);
        if (it->second.empty()) {
            BlobTasks.erase(it);
        }
        return result;
    }

    void AddTask(const std::shared_ptr<ITask>& task) {
        for (auto&& i : task->GetAgents()) {
            auto& storage = BlobTasks[i.second->GetStorageId()];
            for (auto&& bRid : i.second->GetGroups()) {
                storage[bRid.first].emplace_back(task);
            }
        }
    }
};

class TReadCoordinatorActor: public NActors::TActorBootstrapped<TReadCoordinatorActor> {
private:
    ui64 TabletId;
    NActors::TActorId Parent;
    TBlobsForRead BlobTasks;
public:
    TReadCoordinatorActor(ui64 tabletId, const TActorId& parent);

    void Handle(TEvStartReadTask::TPtr& ev);
    void Handle(NBlobCache::TEvBlobCache::TEvReadBlobRangeResult::TPtr& ev);

    void Bootstrap() {
        Become(&TThis::StateWait);
    }

    STFUNC(StateWait) {
        TLogContextGuard gLogging(NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tablet_id", TabletId)("parent", Parent));
        switch (ev->GetTypeRewrite()) {
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
            hFunc(TEvStartReadTask, Handle);
            hFunc(NBlobCache::TEvBlobCache::TEvReadBlobRangeResult, Handle);
            default:
                break;
        }
    }

    ~TReadCoordinatorActor();
};

}
