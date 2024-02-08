#pragma once

#include <util/system/thread.h>
#include <ydb/library/actors/util/funnel_queue.h>

#include "interconnect_stream.h"

#include <memory>
#include <functional>
#include <unordered_map>

namespace NInterconnect {
    using NActors::TFDDelegate;
    using NActors::TSharedDescriptor;

    class TPollerUnit {
    public:
        typedef std::unique_ptr<TPollerUnit> TPtr;

        static TPtr Make(bool useSelect);

        void Start();
        void Stop();

        virtual void StartReadOperation(
            const TIntrusivePtr<TSharedDescriptor>& stream,
            TFDDelegate&& operation);

        virtual void StartWriteOperation(
            const TIntrusivePtr<TSharedDescriptor>& stream,
            TFDDelegate&& operation);

        virtual ~TPollerUnit();

    private:
        virtual void ProcessRead() = 0;
        virtual void ProcessWrite() = 0;

        template <bool IsWrite>
        static void* IdleThread(void* param);

        template <bool IsWrite>
        void RunLoop();

        volatile bool StopFlag;
        TThread ReadLoop, WriteLoop;

    protected:
        TPollerUnit();

        struct TSide {
            using TOperations =
                std::unordered_map<SOCKET,
                                   std::pair<TIntrusivePtr<TSharedDescriptor>, TFDDelegate>>;

            TOperations Operations;
            using TItem = TOperations::mapped_type;
            TFunnelQueue<TItem> InputQueue;

            void ProcessInput();
        } Read, Write;

        template <bool IsWrite>
        TSide& GetSide();
    };

}
