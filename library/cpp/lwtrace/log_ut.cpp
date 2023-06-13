#include "log.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NLWTrace;

namespace
{
    struct TData
    {
        ui32 Val = 0;

        void Clear()
        {
        }
    };

    struct TReader
    {
        ui32 NumSeen = 0;

        void Reset()
        {
            NumSeen = 0;
        }

        void Push(TThread::TId tid, const TData& val)
        {
            Y_UNUSED(tid);
            Y_UNUSED(val);

            ++NumSeen;
        }
    };
}

Y_UNIT_TEST_SUITE(LWTraceLog) {
    Y_UNIT_TEST(ShouldAccumulateTracesViaReadItems) {
        TCyclicLogImpl<TData> log(100);

        {
            TCyclicLogImpl<TData>::TAccessor acc1(log);
            TCyclicLogImpl<TData>::TAccessor acc2(log);
            TCyclicLogImpl<TData>::TAccessor acc3(log);

            acc1.Add()->Val = 1;
            acc2.Add()->Val = 2;
            acc3.Add()->Val = 3;
        }

        TReader reader;

        log.ReadItems(reader);
        UNIT_ASSERT_VALUES_EQUAL(3, reader.NumSeen);

        {
            TCyclicLogImpl<TData>::TAccessor acc1(log);
            TCyclicLogImpl<TData>::TAccessor acc2(log);
            TCyclicLogImpl<TData>::TAccessor acc3(log);

            acc1.Add()->Val = 4;
            acc2.Add()->Val = 5;
            acc3.Add()->Val = 6;
        }

        reader.Reset();
        log.ReadItems(reader);

        UNIT_ASSERT_VALUES_EQUAL(6, reader.NumSeen);
   }

    Y_UNIT_TEST(ShouldNotReturnProcessedItemsViaMoveItems) {
        struct TData
        {
            ui32 Val = 0;

            void Clear()
            {
            }
        };

        struct TReader
        {
            ui32 NumSeen = 0;

            void Reset()
            {
                NumSeen = 0;
            }

            void Push(TThread::TId tid, const TData& val)
            {
                Y_UNUSED(tid);
                Y_UNUSED(val);

                ++NumSeen;
            }
        };

        TCyclicLogImpl<TData> log(100);

        {
            TCyclicLogImpl<TData>::TAccessor acc1(log);
            TCyclicLogImpl<TData>::TAccessor acc2(log);
            TCyclicLogImpl<TData>::TAccessor acc3(log);

            acc1.Add()->Val = 1;
            acc2.Add()->Val = 2;
            acc3.Add()->Val = 3;
        }

        TReader reader;

        log.ExtractItems(reader);
        UNIT_ASSERT_VALUES_EQUAL(3, reader.NumSeen);

        {
            TCyclicLogImpl<TData>::TAccessor acc1(log);
            TCyclicLogImpl<TData>::TAccessor acc2(log);
            TCyclicLogImpl<TData>::TAccessor acc3(log);

            acc1.Add()->Val = 4;
            acc2.Add()->Val = 5;
            acc3.Add()->Val = 6;
        }

        reader.Reset();
        log.ExtractItems(reader);

        UNIT_ASSERT_VALUES_EQUAL(3, reader.NumSeen);
   }
}
