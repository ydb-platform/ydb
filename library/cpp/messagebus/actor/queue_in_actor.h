#pragma once

#include "actor.h"
#include "queue_for_actor.h"

#include <functional>

namespace NActor {
    template <typename TItem>
    class IQueueInActor {
    public:
        virtual void EnqueueAndScheduleV(const TItem& item) = 0;
        virtual void DequeueAllV() = 0;
        virtual void DequeueAllLikelyEmptyV() = 0;

        virtual ~IQueueInActor() {
        }
    };

    template <typename TThis, typename TItem, typename TActorTag = TDefaultTag, typename TQueueTag = TDefaultTag>
    class TQueueInActor: public IQueueInActor<TItem> {
        typedef TQueueInActor<TThis, TItem, TActorTag, TQueueTag> TSelf;

    public:
        // TODO: make protected
        TQueueForActor<TItem> QueueInActor;

    private:
        TActor<TThis, TActorTag>* GetActor() {
            return GetThis();
        }

        TThis* GetThis() {
            return static_cast<TThis*>(this);
        }

        void ProcessItem(const TItem& item) {
            GetThis()->ProcessItem(TActorTag(), TQueueTag(), item);
        }

    public:
        void EnqueueAndNoSchedule(const TItem& item) {
            QueueInActor.Enqueue(item);
        }

        void EnqueueAndSchedule(const TItem& item) {
            EnqueueAndNoSchedule(item);
            GetActor()->Schedule();
        }

        void EnqueueAndScheduleV(const TItem& item) override {
            EnqueueAndSchedule(item);
        }

        void Clear() {
            QueueInActor.Clear();
        }

        void DequeueAll() {
            QueueInActor.DequeueAll(std::bind(&TSelf::ProcessItem, this, std::placeholders::_1));
        }

        void DequeueAllV() override {
            return DequeueAll();
        }

        void DequeueAllLikelyEmpty() {
            QueueInActor.DequeueAllLikelyEmpty(std::bind(&TSelf::ProcessItem, this, std::placeholders::_1));
        }

        void DequeueAllLikelyEmptyV() override {
            return DequeueAllLikelyEmpty();
        }

        bool IsEmpty() {
            return QueueInActor.IsEmpty();
        }
    };

}
