#pragma once

#include <util/network/sock.h>
#include <util/network/pair.h>
#include <util/generic/intrlist.h>
#include <util/system/mutex.h>
#include <util/memory/smallobj.h>

class TConnectedStreamSocket: public TStreamSocket {
public:
    TConnectedStreamSocket(SOCKET fd)
        : TStreamSocket(fd)
    {
    }

    ssize_t Send(const void* msg, size_t len, int flags = 0) {
        ssize_t ret = 0;
        do {
            ret = TStreamSocket::Send(msg, len, flags);
        } while (ret == -EINTR);
        return ret;
    }

    ssize_t Recv(void* buf, size_t len, int flags = 0) {
        ssize_t ret = 0;
        do {
            ret = TStreamSocket::Recv(buf, len, flags);
        } while (ret == -EINTR);
        return ret;
    }
};

// Limited size, syncronized queue based on intrusive list.
// Reader and writer both wait on file descriptors instead
// of traditional cond vars. That allows the queue to be
// employed both in (POSIX) threads and coroutines.
template <class T>
class TBasicPipeQueue {
public:
    // derive your T from TBasicPipeQueue<T>::Item
    typedef TIntrusiveListItem<T> TItem;
    static const size_t MaxQueueSizeDef = (1 << 6); // fits default socket buffer sizes on our Linux and FreeBSD servers

    TBasicPipeQueue(bool isPushBlocking = true,
                    bool isPopBlocking = true,
                    size_t maxQueueSize = MaxQueueSizeDef)
        : CurSize(0)
        , MaxQueueSize(maxQueueSize)
        , PushSock(INVALID_SOCKET)
        , PopSock(INVALID_SOCKET)
        , ShouldContinue(true)
    {
        SOCKET socks[2];
        socks[0] = INVALID_SOCKET;
        socks[1] = INVALID_SOCKET;
        if (SocketPair(socks))
            ythrow yexception() << "TBasicPipeQueue: can't create socket pair: " << LastSystemError();
        TSocketHolder pushSock(socks[0]);
        TSocketHolder popSock(socks[1]);
        PushSock.Swap(pushSock);
        PopSock.Swap(popSock);
        if (!isPushBlocking)
            SetNonBlock(PushFd(), true);
        if (!isPopBlocking)
            SetNonBlock(PopFd(), true);

        char b = '\0';
        for (size_t i = 0; i < MaxQueueSize; i++) {
            if (PopSock.Send(&b, 1) != 1) {
                Y_VERIFY(false, "TBasicPipeQueue: queue size too big (%" PRISZT "): make it less then %" PRISZT " or increase limits", MaxQueueSize, i);
            }
        }
    }

    ~TBasicPipeQueue() {
    }

    void Stop() {
        ShouldContinue = false;
    }

    bool Push(T* t) {
        char b = '\0';
        ssize_t r = 0;
        if (!ShouldContinue)
            return false;

        // make sure the queue has enough space to write to (wait for reading on PushSock or block on Recv)
        r = PushSock.Recv(&b, 1);
        if (r == 0) {
            return false;
        } else if (r < 0) {
            if (errno == EAGAIN)
                return false;
            else
                ythrow yexception() << "TBasicPipeQueue: error in Push(Recv): " << LastSystemError();
        }

        Y_VERIFY(!IsFull(), "TBasicPipeQueue: no space left");

        if (!ShouldContinue)
            return false;

        {
            TGuard<TMutex> guard(Mutex);
            List.PushBack(t);
            CurSize++;
        }

        // signal that the queue has item
        r = PushSock.Send(&b, 1);
        if (r <= 0)
            ythrow yexception() << "TBasicPipeQueue: error in Push(Send): " << LastSystemError();

        return true;
    }

    T* Pop() {
        char b = '\0';
        // wait for an item to appear in the queue
        ssize_t r = PopSock.Recv(&b, 1);
        if (r == 0) {
            return nullptr;
        } else if (r < 0) {
            if (errno == EAGAIN)
                return nullptr;
            else
                ythrow yexception() << "TBasicPipeQueue: error in Pop(Recv): " << LastSystemError();
        }

        T* t = nullptr;
        {
            TGuard<TMutex> guard(Mutex);
            t = List.PopFront();
            CurSize--;
        }

        // signal that the queue has a free slot
        r = PopSock.Send(&b, 1);
        if (r <= 0)
            ythrow yexception() << "TBasicPipeQueue: error in Pop(Send): " << LastSystemError();

        return t;
    }

    SOCKET PushFd() {
        return (SOCKET)PushSock;
    }

    SOCKET PopFd() {
        return (SOCKET)PopSock;
    }

    size_t Size() const {
        TGuard<TMutex> guard(Mutex);
        size_t curSize = CurSize;
        return curSize;
    }

    bool Empty() const {
        TGuard<TMutex> guard(Mutex);
        bool empty = CurSize == 0;
        return empty;
    }

    bool IsFull() const {
        TGuard g{Mutex};
        return CurSize >= MaxQueueSize;
    }

protected:
    typedef TIntrusiveList<T> TListType;
    TListType List;
    size_t CurSize;
    size_t MaxQueueSize;
    TConnectedStreamSocket PushSock;
    TConnectedStreamSocket PopSock;
    TMutex Mutex;
    bool ShouldContinue;
    /**
     *    --{push(write)}--> PushSock ---> PopSock --{pop(read)}-->
     */
};

template <class T>
class TSyncSmallObjAllocator {
public:
    TSyncSmallObjAllocator(IAllocator* alloc = TDefaultAllocator::Instance())
        : Allocator(alloc)
    {
    }

    void Release(T* t) {
        t->~T();
        {
            TGuard<TMutex> guard(Mutex);
            Allocator.Release(t);
        }
    }

    T* Alloc() {
        void* t;
        {
            TGuard<TMutex> guard(Mutex);
            t = Allocator.Allocate();
        }
        return new (t) T();
    }

protected:
    TSmallObjAllocator<T> Allocator;
    TMutex Mutex;
};

template <class T>
struct TPipeQueueItem: public TIntrusiveListItem<TPipeQueueItem<T>> {
    T Val;
};

template <class T>
class TPipeQueue
   : protected  TSyncSmallObjAllocator<TPipeQueueItem<T>>,
      protected TBasicPipeQueue<TPipeQueueItem<T>> {
    typedef TPipeQueueItem<T> TItem;
    typedef TSyncSmallObjAllocator<TItem> TAlloc;
    typedef TBasicPipeQueue<TItem> TBaseQueue;

public:
    TPipeQueue(bool isPushBlocking = true,
               bool isPopBlocking = true,
               size_t maxQueueSize = TBaseQueue::MaxQueueSizeDef,
               IAllocator* alloc = TDefaultAllocator::Instance())
        : TAlloc(alloc)
        , TBaseQueue(isPushBlocking, isPopBlocking, maxQueueSize)
    {
    }

    bool Push(const T& t) {
        TItem* item = TAlloc::Alloc();
        item->Val = t;
        bool res = TBaseQueue::Push(item);
        if (!res)
            TAlloc::Release(item);
        return res;
    }

    bool Pop(T* t) {
        TItem* item = TBaseQueue::Pop();
        if (item == nullptr)
            return false;

        *t = item->Val;
        TAlloc::Release(item);
        return true;
    }

    using TBaseQueue::Empty;
    using TBaseQueue::PopFd;
    using TBaseQueue::PushFd;
    using TBaseQueue::Size;
    using TBaseQueue::Stop;
};
