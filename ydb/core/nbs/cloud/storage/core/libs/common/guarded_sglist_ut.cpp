#include "guarded_sglist.h"

#include "sglist_test.h"

#include <library/cpp/deprecated/atomic/atomic.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/thread/factory.h>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TGuardedSgListTest)
{
    Y_UNIT_TEST(Empty)
    {
        TGuardedSgList empty;
        {
            auto guard = empty.Acquire();
            UNIT_ASSERT(guard);
            UNIT_ASSERT_VALUES_EQUAL(TSgList{}, guard.Get());
        }

        empty.Close();
        UNIT_ASSERT(!empty.Acquire());
    }

    Y_UNIT_TEST(Acquire)
    {
        auto data = TString(4096, 'a');
        TSgList sglist = {{data.data(), 4096}};
        TGuardedSgList guardedSgList(sglist);

        {
            auto guard = guardedSgList.Acquire();
            UNIT_ASSERT(guard);
            UNIT_ASSERT_VALUES_EQUAL(sglist, guard.Get());
        }
        UNIT_ASSERT(guardedSgList.Acquire());
    }

    Y_UNIT_TEST(AcquireCopy)
    {
        auto data = TString(4096, 'a');
        TSgList sglist = {{data.data(), 4096}};
        TGuardedSgList guardedSgList(sglist);
        auto copy = guardedSgList;

        {
            auto guard = guardedSgList.Acquire();
            UNIT_ASSERT(guard);
            UNIT_ASSERT_VALUES_EQUAL(sglist, guard.Get());
        }
        {
            auto guard = copy.Acquire();
            UNIT_ASSERT(guard);
            UNIT_ASSERT_VALUES_EQUAL(sglist, guard.Get());
        }
    }

    Y_UNIT_TEST(Close)
    {
        TGuardedSgList guardedSgList;

        guardedSgList.Close();
        UNIT_ASSERT(!guardedSgList.Acquire());

        guardedSgList.Close();
        UNIT_ASSERT(!guardedSgList.Acquire());
    }

    Y_UNIT_TEST(CloseCopy)
    {
        TGuardedSgList guardedSgList;
        auto copy = guardedSgList;

        copy.Close();
        UNIT_ASSERT(!copy.Acquire());
        UNIT_ASSERT(!guardedSgList.Acquire());
    }

    Y_UNIT_TEST(ConstructFromAnother)
    {
        TGuardedSgList src;

        auto data = TString(4096, 'a');
        TSgList sglist = {{data.data(), data.size()}};
        auto guardedSgList = src.Create(sglist);

        {
            auto guard = guardedSgList.Acquire();
            UNIT_ASSERT(guard);
            UNIT_ASSERT_VALUES_EQUAL(sglist, guard.Get());
        }

        src.Close();
        UNIT_ASSERT(!guardedSgList.Acquire());
    }

    Y_UNIT_TEST(SetSgList)
    {
        TGuardedSgList src;
        auto guardedSgList = src;

        auto data = TString(4096, 'a');
        TSgList sglist = {{data.data(), data.size()}};
        guardedSgList.SetSgList(sglist);

        {
            auto guard = guardedSgList.Acquire();
            UNIT_ASSERT(guard);
            UNIT_ASSERT_VALUES_EQUAL(sglist, guard.Get());
        }

        src.Close();
        UNIT_ASSERT(!guardedSgList.Acquire());
    }

    Y_UNIT_TEST(CreateDepender)
    {
        auto data = TString(4096, 'a');
        TSgList sglist = {{data.data(), 4096}};

        TGuardedSgList src(sglist);
        auto depender1 = src.CreateDepender();
        auto depender2 = src.CreateDepender();
        auto depender3 = src.CreateDepender();

        {
            auto guard1 = depender1.Acquire();
            auto guard2 = depender2.Acquire();
            auto guard3 = depender3.Acquire();
            UNIT_ASSERT(guard1 && guard2 && guard3);
            UNIT_ASSERT_VALUES_EQUAL(sglist, guard1.Get());
            UNIT_ASSERT_VALUES_EQUAL(sglist, guard2.Get());
            UNIT_ASSERT_VALUES_EQUAL(sglist, guard3.Get());
        }

        depender2.Close();
        UNIT_ASSERT(depender1.Acquire());
        UNIT_ASSERT(!depender2.Acquire());
        UNIT_ASSERT(depender3.Acquire());

        src.Close();
        UNIT_ASSERT(!depender1.Acquire());
        UNIT_ASSERT(!depender2.Acquire());
        UNIT_ASSERT(!depender3.Acquire());
    }

    Y_UNIT_TEST(CreateUnion)
    {
        TVector<TString> blocks[3];

        auto sgList1 = TGuardedSgList(
            ResizeBlocks(blocks[0], 2, TString(DefaultBlockSize, 'a')));
        auto sgList2 = TGuardedSgList(
            ResizeBlocks(blocks[1], 1, TString(DefaultBlockSize, 'b')));
        auto sgList3 = TGuardedSgList();
        auto sgList4 = TGuardedSgList(
            ResizeBlocks(blocks[2], 3, TString(DefaultBlockSize, 'c')));

        auto unionSgList =
            TGuardedSgList::CreateUnion({sgList1, sgList2, sgList3, sgList4});

        {
            auto guard = unionSgList.Acquire();
            UNIT_ASSERT(guard);
            const auto& sgList = guard.Get();
            UNIT_ASSERT(sgList.size() == 6);
            UNIT_ASSERT_EQUAL(sgList[0].AsStringBuf(),
                              TString(DefaultBlockSize, 'a'));
            UNIT_ASSERT_EQUAL(sgList[1].AsStringBuf(),
                              TString(DefaultBlockSize, 'a'));
            UNIT_ASSERT_EQUAL(sgList[2].AsStringBuf(),
                              TString(DefaultBlockSize, 'b'));
            UNIT_ASSERT_EQUAL(sgList[3].AsStringBuf(),
                              TString(DefaultBlockSize, 'c'));
            UNIT_ASSERT_EQUAL(sgList[4].AsStringBuf(),
                              TString(DefaultBlockSize, 'c'));
            UNIT_ASSERT_EQUAL(sgList[5].AsStringBuf(),
                              TString(DefaultBlockSize, 'c'));
        }

        sgList2.Close();
        UNIT_ASSERT(!unionSgList.Acquire());
    }

    Y_UNIT_TEST(CloseUnion)
    {
        TString buf(DefaultBlockSize, 'a');
        TGuardedSgList guardedSgList({{buf.data(), buf.size()}});

        TVector<TGuardedSgList> sgLists(4, std::move(guardedSgList));

        auto unionSgList = TGuardedSgList::CreateUnion(sgLists);

        UNIT_ASSERT(unionSgList.Acquire());

        unionSgList.Close();
        UNIT_ASSERT(!unionSgList.Acquire());

        for (const auto& sgList: sgLists) {
            UNIT_ASSERT(sgList.Acquire());
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TGuardedBufferTest)
{
    Y_UNIT_TEST(ShouldDestoryDataAfterExtracting)
    {
        TGuardedBuffer<TString> owner;

        TString data(4096, 'a');
        TSgList sglist = {{data.data(), data.size()}};

        owner = TGuardedBuffer(std::move(data));

        const TString& ref = owner.Get();
        TSgList refSglist = {{ref.data(), ref.size()}};
        UNIT_ASSERT_VALUES_EQUAL(sglist, refSglist);

        {
            ui32 offset = 1024;
            TSgList subSglist = {
                {sglist[0].Data() + offset, sglist[0].Size() - offset}};

            auto guardedSgList = owner.CreateGuardedSgList(subSglist);
            auto guard = guardedSgList.Acquire();
            UNIT_ASSERT(guard);
            UNIT_ASSERT_VALUES_EQUAL(subSglist, guard.Get());
        }

        TString extracted = owner.Extract();
        TSgList extractedSglist = {{extracted.data(), extracted.size()}};
        UNIT_ASSERT_VALUES_EQUAL(sglist, extractedSglist);

        auto guardedSgList = owner.CreateGuardedSgList(sglist);
        UNIT_ASSERT(!guardedSgList.Acquire());
    }

    Y_UNIT_TEST(ShouldDestroyDataIfGuardedSgListExists)
    {
        TString data(4096, 'a');
        TSgList sglist = {{data.data(), data.size()}};

        TGuardedBuffer<TString> owner(std::move(data));
        auto guardedSgList = owner.CreateGuardedSgList(sglist);

        owner = TGuardedBuffer<TString>();

        auto guard = guardedSgList.Acquire();
        UNIT_ASSERT(!guard);
    }

    Y_UNIT_TEST(ShouldGetGuardedSgListForString)
    {
        TString data(4096, 'a');
        TSgList sglist = {{data.data(), data.size()}};
        TGuardedBuffer buffer(std::move(data));

        auto guardedSgList = buffer.GetGuardedSgList();
        auto guard = guardedSgList.Acquire();
        UNIT_ASSERT(guard);
        UNIT_ASSERT_VALUES_EQUAL(sglist, guard.Get());
    }

    Y_UNIT_TEST(ShouldGetGuardedSgListForBuffer)
    {
        TBuffer data("aaaa", 4);
        TSgList sglist = {{data.Data(), data.Size()}};
        TGuardedBuffer buffer(std::move(data));

        auto guardedSgList = buffer.GetGuardedSgList();
        auto guard = guardedSgList.Acquire();
        UNIT_ASSERT(guard);
        UNIT_ASSERT_VALUES_EQUAL(sglist, guard.Get());
    }
}

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TGuardedSgListWithThreadsTest)
{
    class TScopedTasks
    {
    private:
        using TThread = THolder<IThreadFactory::IThread>;

        TVector<TThread> Workers;
        TAtomic ShouldStart = 0;

    public:
        ~TScopedTasks()
        {
            for (auto& w: Workers) {
                w->Join();
            }
        }

        template <typename F>
        void Add(F f)
        {
            Workers.push_back(
                SystemThreadFactory()->Run(
                    [this, f = std::move(f)]()
                    {
                        while (AtomicGet(ShouldStart) != 1) {
                        }
                        f();
                    }));
        }

        void Start()
        {
            AtomicSet(ShouldStart, 1);
        }
    };

    Y_UNIT_TEST(Acquire)
    {
        TGuardedSgList guardedSgList;
        TScopedTasks tasks;

        const auto task = [&guardedSgList]()
        {
            for (int i = 0; i < 5; ++i) {
                UNIT_ASSERT(guardedSgList.Acquire());
            }
        };
        for (int i = 0; i < 5; ++i) {
            tasks.Add(task);
        }
        tasks.Start();
    }

    Y_UNIT_TEST(Destroy)
    {
        TGuardedSgList guardedSgList;

        {
            TScopedTasks tasks;

            const auto acquireTask = [&guardedSgList]()
            {
                for (int i = 0; i < 5; ++i) {
                    auto guard = guardedSgList.Acquire();
                }
            };
            for (int i = 0; i < 5; ++i) {
                tasks.Add(acquireTask);
            }

            const auto closeTask = [&guardedSgList]()
            {
                guardedSgList.Close();
            };
            for (int i = 0; i < 2; ++i) {
                tasks.Add(closeTask);
            }

            tasks.Start();
        }
        UNIT_ASSERT(!guardedSgList.Acquire());
    }

    Y_UNIT_TEST(CreateDepender)
    {
        TGuardedSgList guardedSgList;

        {
            TScopedTasks tasks;

            const auto dependerTask = [&guardedSgList]()
            {
                for (int i = 0; i < 5; ++i) {
                    auto sglist = guardedSgList.CreateDepender();
                }
            };
            for (int i = 0; i < 5; ++i) {
                tasks.Add(dependerTask);
            }

            const auto closeTask = [&guardedSgList]()
            {
                guardedSgList.Close();
            };
            for (int i = 0; i < 2; ++i) {
                tasks.Add(closeTask);
            }

            tasks.Start();
        }
        UNIT_ASSERT(!guardedSgList.Acquire());
    }

    Y_UNIT_TEST(Depender)
    {
        TGuardedSgList guardedSgList;
        auto depender = guardedSgList.CreateDepender();

        {
            TScopedTasks tasks;

            const auto dependerTask = [&depender]()
            {
                for (int i = 0; i < 5; ++i) {
                    auto guard = depender.Acquire();
                }
            };
            for (int i = 0; i < 5; ++i) {
                tasks.Add(dependerTask);
            }

            const auto acquireTask = [&guardedSgList]()
            {
                auto guard = guardedSgList.Acquire();
            };
            for (int i = 0; i < 5; ++i) {
                tasks.Add(acquireTask);
            }

            const auto closeTask = [&guardedSgList]()
            {
                guardedSgList.Close();
            };
            for (int i = 0; i < 2; ++i) {
                tasks.Add(closeTask);
            }

            tasks.Start();
        }
        UNIT_ASSERT(!guardedSgList.Acquire());
    }
}

}   // namespace NYdb::NBS
