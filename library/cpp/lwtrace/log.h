#pragma once

#include "probe.h"

#include <util/datetime/base.h>
#include <util/generic/algorithm.h>
#include <util/generic/deque.h>
#include <util/generic/noncopyable.h>
#include <util/generic/vector.h>
#include <util/string/printf.h>
#include <library/cpp/deprecated/atomic/atomic.h>
#include <util/system/hp_timer.h>
#include <util/system/mutex.h>
#include <util/system/spinlock.h>
#include <util/system/thread.h>
#include <util/system/tls.h>

namespace NLWTrace {
    // Cyclic buffer that pushes items to its back and pop item from front on overflow
    template <class TItem>
    class TCyclicBuffer: public TNonCopyable {
    private:
        TVector<TItem> Data;
        TItem* Front; // Points to the first item (valid iff Size > 0)
        TItem* Back;  // Points to the last item (valid iff Size > 0)
        size_t Size;  // Number of items in the buffer

        TItem* First() {
            return &*Data.begin();
        }

        TItem* Last() {
            return &*Data.end();
        }

        const TItem* First() const {
            return &*Data.begin();
        }

        const TItem* Last() const {
            return &*Data.end();
        }

    public:
        explicit TCyclicBuffer(size_t capacity)
            : Data(capacity)
            , Size(0)
        {
        }

        TItem* Add() {
            if (Size != 0) {
                Inc(Back);
                if (Back == Front) {
                    Inc(Front); // Forget (pop_front) old items
                } else {
                    Size++;
                }
            } else {
                Front = Back = First();
                Size = 1;
            }
            Back->Clear();
            return Back;
        }

        TItem* GetFront() {
            return Front;
        }

        TItem* GetBack() {
            return Back;
        }

        const TItem* GetFront() const {
            return Front;
        }

        const TItem* GetBack() const {
            return Back;
        }

        size_t GetSize() const {
            return Size;
        }

        bool IsFull() const {
            return Size == Data.size();
        }

        void Inc(TItem*& it) {
            it++;
            if (it == Last()) {
                it = First();
            }
        }

        void Inc(const TItem*& it) const {
            it++;
            if (it == Last()) {
                it = First();
            }
        }

        void Destroy() {
            Data.clear();
            Size = 0;
        }

        void Clear() {
            Size = 0;
        }

        void Swap(TCyclicBuffer& other) {
            Data.swap(other.Data);
            std::swap(Front, other.Front);
            std::swap(Back, other.Back);
            std::swap(Size, other.Size);
        }
    };

    // Buffer that pushes items to its back and pop item from front on expire
    template <class TItem>
    class TDurationBuffer: public TNonCopyable {
    protected:
        TDeque<TItem> Data;
        ui64 StoreDuration;
        ui8 CleanupCounter = 0;

    public:
        explicit TDurationBuffer(TDuration duration)
            : StoreDuration(DurationToCycles(duration))
        {
        }

        TItem* Add() {
            if (!CleanupCounter) {
                Cleanup();
                CleanupCounter = 128; // Make cleanup after every 128 additions
            }
            CleanupCounter--;
            Data.emplace_back();
            return &Data.back();
        }

        TItem* GetFront() {
            return &Data.front();
        }

        TItem* GetBack() {
            return &Data.back();
        }

        const TItem* GetFront() const {
            return &Data.front();
        }

        const TItem* GetBack() const {
            return &Data.back();
        }

        size_t GetSize() const {
            return Data.size();
        }

        bool Empty() const {
            return Data.empty();
        }

        void Destroy() {
            Data.clear();
        }

        void Swap(TDurationBuffer& other) {
            Data.swap(other.Data);
            std::swap(StoreDuration, other.StoreDuration);
        }

    private:
        void Cleanup() {
            ui64 cutoff = GetCycleCount();
            if (cutoff > StoreDuration) {
                cutoff -= StoreDuration;
                while (!Data.empty() && Data.front().GetTimestampCycles() < cutoff) {
                    Data.pop_front();
                }
            }
        }
    };

    struct TLogItem {
        TProbe* Probe = nullptr;
        TParams Params;
        size_t SavedParamsCount;
        TInstant Timestamp;
        ui64 TimestampCycles;

        TLogItem() {
        }

        TLogItem(const TLogItem& other)
            : Probe(other.Probe)
            , SavedParamsCount(other.SavedParamsCount)
            , Timestamp(other.Timestamp)
            , TimestampCycles(other.TimestampCycles)
        {
            Clone(other);
        }

        ~TLogItem() {
            Destroy();
        }

        TLogItem& operator=(const TLogItem& other) {
            Destroy();
            Probe = other.Probe;
            SavedParamsCount = other.SavedParamsCount;
            Timestamp = other.Timestamp;
            TimestampCycles = other.TimestampCycles;
            Clone(other);
            return *this;
        }

        void Clear() {
            Destroy();
            Probe = nullptr;
        }

        void ToProtobuf(TLogItemPb& pb) const {
            pb.SetName(Probe->Event.Name);
            pb.SetProvider(Probe->Event.GetProvider());
            if (SavedParamsCount > 0) {
                TString paramValues[LWTRACE_MAX_PARAMS];
                Probe->Event.Signature.SerializeParams(Params, paramValues);
                for (size_t pi = 0; pi < SavedParamsCount; pi++) {
                    pb.AddParams(paramValues[pi]);
                }
            }
            pb.SetTimestamp(Timestamp.GetValue());
            pb.SetTimestampCycles(TimestampCycles);
        }

        TTypedParam GetParam(const TString& param) const {
            if (SavedParamsCount == 0) {
                return TTypedParam();
            } else {
                size_t idx = Probe->Event.Signature.FindParamIndex(param);
                if (idx >= SavedParamsCount) { // Also covers idx=-1 case (not found)
                    return TTypedParam();
                } else {
                    EParamTypePb type = ParamTypeToProtobuf(Probe->Event.Signature.ParamTypes[idx]);
                    return TTypedParam(type, Params.Param[idx]);
                }
            }
        }

        ui64 GetTimestampCycles() const {
            return TimestampCycles;
        }

    private:
        void Clone(const TLogItem& other) {
            if (Probe && SavedParamsCount > 0) {
                Probe->Event.Signature.CloneParams(Params, other.Params);
            }
        }

        void Destroy() {
            if (Probe && SavedParamsCount > 0) {
                Probe->Event.Signature.DestroyParams(Params);
            }
        }
    };

    struct TTrackLog {
        struct TItem : TLogItem {
            TThread::TId ThreadId;

            TItem() = default;

            TItem(TThread::TId tid, const TLogItem& item)
                : TLogItem(item)
                , ThreadId(tid)
            {
            }
        };

        using TItems = TVector<TItem>;
        TItems Items;
        bool Truncated = false;
        ui64 Id = 0;

        void Clear() {
            Items.clear();
            Truncated = false;
        }

        ui64 GetTimestampCycles() const {
            return Items.empty() ? 0 : Items.front().GetTimestampCycles();
        }
    };

    // Log that uses per-thread cyclic buffers to store items
    template <class T>
    class TCyclicLogImpl: public TNonCopyable {
    public:
        using TLog = TCyclicLogImpl;
        using TItem = T;

    private:
        using TBuffer = TCyclicBuffer<TItem>;

        class TStorage {
        private:
            // Data that can be accessed in lock-free way from reader/writer
            TAtomic Writers = 0;
            mutable TBuffer* volatile CurBuffer = nullptr;

            // Data that can be accessed only from reader
            // NOTE: multiple readers are serialized by TCyclicLogImpl::Lock
            mutable TBuffer* OldBuffer = nullptr;
            mutable TBuffer* NewBuffer = nullptr;

            TLog* volatile Log = nullptr;

            TThread::TId ThreadId = 0;
            TAtomic EventsCount = 0;

        public:
            TStorage() {
            }

            explicit TStorage(TLog* log)
                : CurBuffer(new TBuffer(log->GetCapacity()))
                , OldBuffer(new TBuffer(log->GetCapacity()))
                , NewBuffer(new TBuffer(log->GetCapacity()))
                , Log(log)
                , ThreadId(TThread::CurrentThreadId())
            {
                Log->RegisterThread(this);
            }

            ~TStorage() {
                if (TLog* log = AtomicSwap(&Log, nullptr)) {
                    AtomicBarrier(); // Serialize `Log' and TCyclicLogImpl::Lock memory order
                    // NOTE: the following function swaps `this' with `new TStorage()'
                    log->UnregisterThreadAndMakeOrphan(this);
                } else {
                    // NOTE: `Log' can be nullptr if either it is orphan storage or TryDismiss() succeeded
                    // NOTE: in both cases it is ok to call these deletes
                    delete CurBuffer;
                    delete OldBuffer;
                    delete NewBuffer;
                }
            }

            bool TryDismiss() {
                // TCyclicLogImpl::Lock implied (no readers)
                if (TLog* log = AtomicSwap(&Log, nullptr)) {
                    TBuffer* curBuffer = AtomicSwap(&CurBuffer, nullptr);
                    WaitForWriters();
                    // At this point we guarantee that there is no and wont be active writer
                    delete curBuffer;
                    delete OldBuffer;
                    delete NewBuffer;
                    OldBuffer = nullptr;
                    NewBuffer = nullptr;
                    return true;
                } else {
                    // ~TStorage() is in progress
                    return false;
                }
            }

            void WaitForWriters() const {
                while (AtomicGet(Writers) > 0) {
                    SpinLockPause();
                }
            }

            TThread::TId GetThreadId() const {
                // TCyclicLogImpl::Lock implied (no readers)
                return ThreadId;
            }

            size_t GetEventsCount() const {
                // TCyclicLogImpl::Lock implied (no readers)
                return AtomicGet(EventsCount);
            }

            void Swap(TStorage& other) {
                // TCyclicLogImpl::Lock implied (no readers)
                std::swap(CurBuffer, other.CurBuffer);
                std::swap(OldBuffer, other.OldBuffer);
                std::swap(NewBuffer, other.NewBuffer);
                std::swap(Log, other.Log);
                std::swap(ThreadId, other.ThreadId);
                std::swap(EventsCount, other.EventsCount);
            }

            TBuffer* StartWriter() {
                AtomicIncrement(Writers);
                return const_cast<TBuffer*>(AtomicGet(CurBuffer));
            }

            void StopWriter() {
                AtomicDecrement(Writers);
            }

            void IncEventsCount() {
                AtomicIncrement(EventsCount);
            }

            template <class TReader>
            void ReadItems(TReader& r) const {
                // TCyclicLogImpl::Lock implied
                NewBuffer = AtomicSwap(&CurBuffer, NewBuffer);
                WaitForWriters();

                // Merge new buffer into old buffer
                if (NewBuffer->IsFull()) {
                    std::swap(NewBuffer, OldBuffer);
                } else {
                    if (NewBuffer->GetSize() > 0) {
                        for (const TItem *i = NewBuffer->GetFront(), *e = NewBuffer->GetBack();; NewBuffer->Inc(i)) {
                            TItem* oldSlot = OldBuffer->Add();
                            *oldSlot = *i;
                            if (i == e) {
                                break;
                            }
                        }
                    }
                }

                NewBuffer->Clear();

                // Iterate over old buffer
                if (OldBuffer->GetSize() > 0) {
                    for (const TItem *i = OldBuffer->GetFront(), *e = OldBuffer->GetBack();; OldBuffer->Inc(i)) {
                        r.Push(ThreadId, *i);
                        if (i == e) {
                            break;
                        }
                    }
                }
            }

            template <class TReader>
            void ExtractItems(TReader& r) {
                ReadItems(r);
                for (TItem *i = OldBuffer->GetFront(), *e = OldBuffer->GetBack();; OldBuffer->Inc(i)) {
                    i->Clear();
                    if (i == e) {
                        break;
                    }
                }
                OldBuffer->Clear();
            }
        };

        size_t Capacity;
        Y_THREAD(TStorage)
        PerThreadStorage;
        TSpinLock Lock;
        // If thread exits its storage is destroyed, so we move it into OrphanStorages before destruction
        TVector<TAtomicSharedPtr<TStorage>> OrphanStorages;
        typedef TVector<TStorage*> TStoragesVec;
        TStoragesVec StoragesVec;
        TAtomic ThreadsCount;

    public:
        explicit TCyclicLogImpl(size_t capacity)
            : Capacity(capacity)
            , PerThreadStorage(this)
            , ThreadsCount(0)
        {
        }

        ~TCyclicLogImpl() {
            for (bool again = true; again;) {
                TGuard<TSpinLock> g(Lock);
                AtomicBarrier(); // Serialize `storage->Log' and Lock memory order
                again = false;
                while (!StoragesVec.empty()) {
                    TStorage* storage = StoragesVec.back();
                    // TStorage destructor can be called when TCyclicLogImpl is already destructed
                    // So we ensure this does not lead to problems
                    // NOTE: Y_THREAD(TStorage) destructs TStorage object for a specific thread only on that thread exit
                    // NOTE: this issue can lead to memleaks if threads never exit and many TCyclicLogImpl are created
                    if (storage->TryDismiss()) {
                        StoragesVec.pop_back();
                    } else {
                        // Rare case when another thread is running ~TStorage() -- let it finish
                        again = true;
                        SpinLockPause();
                        break;
                    }
                }
            }
        }

        size_t GetCapacity() const {
            return Capacity;
        }

        size_t GetEventsCount() const {
            size_t events = 0;
            TGuard<TSpinLock> g(Lock);
            for (auto i : StoragesVec) {
                events += i->GetEventsCount();
            }
            for (const auto& orphanStorage : OrphanStorages) {
                events += orphanStorage->GetEventsCount();
            }
            return events;
        }

        size_t GetThreadsCount() const {
            return AtomicGet(ThreadsCount);
        }

        void RegisterThread(TStorage* storage) {
            TGuard<TSpinLock> g(Lock);
            StoragesVec.push_back(storage);
            AtomicIncrement(ThreadsCount);
        }

        void UnregisterThreadAndMakeOrphan(TStorage* storage) {
            TGuard<TSpinLock> g(Lock);
            // `storage' writers are not possible at this scope because
            // UnregisterThreadAndMakeOrphan is only called from exiting threads.
            // `storage' readers are not possible at this scope due to Lock guard.

            Erase(StoragesVec, storage);
            TAtomicSharedPtr<TStorage> orphan(new TStorage());
            orphan->Swap(*storage); // Swap is required because we cannot take ownership from Y_THREAD(TStorage) object
            OrphanStorages.push_back(orphan);
        }

        template <class TReader>
        void ReadThreads(TReader& r) const {
            TGuard<TSpinLock> g(Lock);
            for (auto i : StoragesVec) {
                r.PushThread(i->GetThreadId());
            }
            for (const auto& orphanStorage : OrphanStorages) {
                r.PushThread(orphanStorage->GetThreadId());
            }
        }

        template <class TReader>
        void ReadItems(TReader& r) const {
            TGuard<TSpinLock> g(Lock);
            for (auto i : StoragesVec) {
                i->ReadItems(r);
            }
            for (const auto& orphanStorage : OrphanStorages) {
                orphanStorage->ReadItems(r);
            }
        }

        template <class TReader>
        void ExtractItems(TReader& r) const {
            TGuard<TSpinLock> g(Lock);
            for (auto i: StoragesVec) {
                i->ExtractItems(r);
            }
            for (const auto& orphanStorage: OrphanStorages) {
                orphanStorage->ExtractItems(r);
            }
        }

        class TAccessor {
        private:
            TStorage& Storage;
            TBuffer* Buffer;

        public:
            explicit TAccessor(TLog& log)
                : Storage(log.PerThreadStorage.Get())
                , Buffer(Storage.StartWriter())
            {
            }

            ~TAccessor() {
                Storage.StopWriter();
            }

            TItem* Add() {
                if (Buffer) {
                    Storage.IncEventsCount();
                    return Buffer->Add();
                } else {
                    // TStorage detached from trace due to trace destruction
                    // so we should not try log anything
                    return nullptr;
                }
            }
        };

        friend class TAccessor;
    };

    using TCyclicLog = TCyclicLogImpl<TLogItem>;
    using TCyclicDepot = TCyclicLogImpl<TTrackLog>;

    // Log that uses per-thread buffers to store items up to given duration
    template <class T>
    class TDurationLogImpl: public TNonCopyable {
    public:
        using TLog = TDurationLogImpl;
        using TItem = T;

        class TAccessor;
        friend class TAccessor;
        class TAccessor: public TGuard<TSpinLock> {
        private:
            TLog& Log;

        public:
            explicit TAccessor(TLog& log)
                : TGuard<TSpinLock>(log.PerThreadStorage.Get().Lock)
                , Log(log)
            {
            }

            TItem* Add() {
                return Log.PerThreadStorage.Get().Add();
            }
        };

    private:
        class TStorage: public TDurationBuffer<TItem> {
        private:
            TLog* Log;
            TThread::TId ThreadId;
            ui64 EventsCount;

        public:
            TSpinLock Lock;

            TStorage()
                : TDurationBuffer<TItem>(TDuration::Zero())
                , Log(nullptr)
                , ThreadId(0)
                , EventsCount(0)
            {
            }

            explicit TStorage(TLog* log)
                : TDurationBuffer<TItem>(log->GetDuration())
                , Log(log)
                , ThreadId(TThread::CurrentThreadId())
                , EventsCount(0)
            {
                Log->RegisterThread(this);
            }

            ~TStorage() {
                if (Log) {
                    Log->UnregisterThread(this);
                }
            }

            void DetachFromTraceLog() {
                Log = nullptr;
            }

            TItem* Add() {
                EventsCount++;
                return TDurationBuffer<TItem>::Add();
            }

            bool Expired(ui64 now) const {
                return this->Empty() ? true : this->GetBack()->GetTimestampCycles() + this->StoreDuration < now;
            }

            TThread::TId GetThreadId() const {
                return ThreadId;
            }

            size_t GetEventsCount() const {
                return EventsCount;
            }

            void Swap(TStorage& other) {
                TDurationBuffer<TItem>::Swap(other);
                std::swap(Log, other.Log);
                std::swap(ThreadId, other.ThreadId);
                std::swap(EventsCount, other.EventsCount);
            }

            template <class TReader>
            void ReadItems(ui64 now, ui64 duration, TReader& r) const {
                TGuard<TSpinLock> g(Lock);
                if (now > duration) {
                    ui64 cutoff = now - duration;
                    for (const TItem& item : this->Data) {
                        if (item.GetTimestampCycles() >= cutoff) {
                            r.Push(ThreadId, item);
                        }
                    }
                } else {
                    for (const TItem& item : this->Data) {
                        r.Push(ThreadId, item);
                    }
                }
            }
        };

        TDuration Duration;
        Y_THREAD(TStorage)
        PerThreadStorage;
        TSpinLock Lock;
        typedef TVector<TAtomicSharedPtr<TStorage>> TOrphanStorages;
        TOrphanStorages OrphanStorages; // if thread exits its storage is destroyed, so we move it into OrphanStorages before destruction
        TAtomic OrphanStoragesEventsCount = 0;
        typedef TVector<TStorage*> TStoragesVec;
        TStoragesVec StoragesVec;
        TAtomic ThreadsCount;

    public:
        explicit TDurationLogImpl(TDuration duration)
            : Duration(duration)
            , PerThreadStorage(this)
            , ThreadsCount(0)
        {
        }

        ~TDurationLogImpl() {
            for (auto storage : StoragesVec) {
                // NOTE: Y_THREAD(TStorage) destructs TStorage object for a specific thread only on that thread exit
                // NOTE: this issue can lead to memleaks if threads never exit and many TTraceLogs are created
                storage->Destroy();

                // TraceLogStorage destructor can be called when TTraceLog is already destructed
                // So we ensure this does not lead to problems
                storage->DetachFromTraceLog();
            }
        }

        TDuration GetDuration() const {
            return Duration;
        }

        size_t GetEventsCount() const {
            size_t events = AtomicGet(OrphanStoragesEventsCount);
            TGuard<TSpinLock> g(Lock);
            for (auto i : StoragesVec) {
                events += i->GetEventsCount();
            }
            return events;
        }

        size_t GetThreadsCount() const {
            return AtomicGet(ThreadsCount);
        }

        void RegisterThread(TStorage* storage) {
            TGuard<TSpinLock> g(Lock);
            StoragesVec.push_back(storage);
            AtomicIncrement(ThreadsCount);
        }

        void UnregisterThread(TStorage* storage) {
            TGuard<TSpinLock> g(Lock);
            for (auto i = StoragesVec.begin(), e = StoragesVec.end(); i != e; ++i) {
                if (*i == storage) {
                    StoragesVec.erase(i);
                    break;
                }
            }
            TAtomicSharedPtr<TStorage> orphan(new TStorage());
            orphan->Swap(*storage);
            orphan->DetachFromTraceLog();
            AtomicAdd(OrphanStoragesEventsCount, orphan->GetEventsCount());
            OrphanStorages.push_back(orphan);
            CleanOrphanStorages(GetCycleCount());
        }

        void CleanOrphanStorages(ui64 now) {
            EraseIf(OrphanStorages, [=](const TAtomicSharedPtr<TStorage>& ptr) {
                const TStorage& storage = *ptr;
                return storage.Expired(now);
            });
        }

        template <class TReader>
        void ReadThreads(TReader& r) const {
            TGuard<TSpinLock> g(Lock);
            for (TStorage* i : StoragesVec) {
                r.PushThread(i->GetThreadId());
            }
            for (const auto& orphanStorage : OrphanStorages) {
                r.PushThread(orphanStorage->GetThreadId());
            }
        }

        template <class TReader>
        void ReadItems(ui64 now, ui64 duration, TReader& r) const {
            TGuard<TSpinLock> g(Lock);
            for (TStorage* storage : StoragesVec) {
                storage->ReadItems(now, duration, r);
            }
            for (const auto& orphanStorage : OrphanStorages) {
                orphanStorage->ReadItems(now, duration, r);
            }
        }
    };

    using TDurationLog = TDurationLogImpl<TLogItem>;
    using TDurationDepot = TDurationLogImpl<TTrackLog>;

    // Log that uses one cyclic buffer to store items
    // Each item is a result of execution of some event
    class TInMemoryLog: public TNonCopyable {
    public:
        struct TItem {
            const TEvent* Event;
            TParams Params;
            TInstant Timestamp;

            TItem()
                : Event(nullptr)
            {
            }

            TItem(const TItem& other)
                : Event(other.Event)
                , Timestamp(other.Timestamp)
            {
                Clone(other);
            }

            ~TItem() {
                Destroy();
            }

            TItem& operator=(const TItem& other) {
                Destroy();
                Event = other.Event;
                Timestamp = other.Timestamp;
                Clone(other);
                return *this;
            }

            void Clear() {
                Destroy();
                Event = nullptr;
            }

        private:
            void Clone(const TItem& other) {
                if (Event && Event->Signature.ParamCount > 0) {
                    Event->Signature.CloneParams(Params, other.Params);
                }
            }

            void Destroy() {
                if (Event && Event->Signature.ParamCount > 0) {
                    Event->Signature.DestroyParams(Params);
                }
            }
        };

        class TAccessor;
        friend class TAccessor;
        class TAccessor: public TGuard<TMutex> {
        private:
            TInMemoryLog& Log;

        public:
            explicit TAccessor(TInMemoryLog& log)
                : TGuard<TMutex>(log.Lock)
                , Log(log)
            {
            }

            TItem* Add() {
                return Log.Storage.Add();
            }
        };

    private:
        TMutex Lock;
        TCyclicBuffer<TItem> Storage;

    public:
        explicit TInMemoryLog(size_t capacity)
            : Storage(capacity)
        {
        }

        template <class TReader>
        void ReadItems(TReader& r) const {
            TGuard<TMutex> g(Lock);
            if (Storage.GetSize() > 0) {
                for (const TItem *i = Storage.GetFront(), *e = Storage.GetBack();; Storage.Inc(i)) {
                    r.Push(*i);
                    if (i == e) {
                        break;
                    }
                }
            }
        }
    };

#ifndef LWTRACE_DISABLE

    // Class representing a specific event
    template <LWTRACE_TEMPLATE_PARAMS>
    struct TUserEvent {
        TEvent Event;

        inline void operator()(TInMemoryLog& log, bool logTimestamp, LWTRACE_FUNCTION_PARAMS) const {
            TInMemoryLog::TAccessor la(log);
            if (TInMemoryLog::TItem* item = la.Add()) {
                item->Event = &Event;
                LWTRACE_PREPARE_PARAMS(item->Params);
                if (logTimestamp) {
                    item->Timestamp = TInstant::Now();
                }
            }
        }
    };

#endif

}
