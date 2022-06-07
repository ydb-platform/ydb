#pragma once

#include <library/cpp/threading/future/legacy_future.h>

#include <util/datetime/base.h>
#include <util/generic/object_counter.h>
#include <util/generic/ptr.h>
#include <util/generic/vector.h>
#include <library/cpp/deprecated/atomic/atomic.h>
#include <util/system/condvar.h>
#include <util/system/mutex.h>
#include <util/system/thread.h>

namespace NBus {
    namespace NPrivate {
        class IScheduleItem {
        public:
            inline IScheduleItem(TInstant scheduleTime) noexcept;
            virtual ~IScheduleItem() {
            }

            virtual void Do() = 0;
            inline TInstant GetScheduleTime() const noexcept;

        private:
            TInstant ScheduleTime;
        };

        using IScheduleItemAutoPtr = TAutoPtr<IScheduleItem>;

        class TScheduler {
        public:
            TScheduler();
            ~TScheduler();
            void Stop();
            void Schedule(TAutoPtr<IScheduleItem> i);

            size_t Size() const;

        private:
            void SchedulerThread();

            void FillNextItem();

        private:
            TVector<IScheduleItemAutoPtr> Items;
            IScheduleItemAutoPtr NextItem;
            typedef TMutex TLock;
            TLock Lock;
            TCondVar CondVar;

            TObjectCounter<TScheduler> ObjectCounter;

            bool StopThread;
            NThreading::TLegacyFuture<> Thread;
        };

        inline IScheduleItem::IScheduleItem(TInstant scheduleTime) noexcept
            : ScheduleTime(scheduleTime)
        {
        }

        inline TInstant IScheduleItem::GetScheduleTime() const noexcept {
            return ScheduleTime;
        }

    }
}
