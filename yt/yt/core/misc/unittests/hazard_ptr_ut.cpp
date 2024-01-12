#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/hazard_ptr.h>
#include <yt/yt/core/misc/atomic_ptr.h>
#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/concurrency/delayed_executor.h>

#include <library/cpp/yt/threading/event_count.h>

#include <library/cpp/yt/memory/new.h>

#include <util/system/thread.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TTestAllocator
{
public:
    explicit TTestAllocator(IOutputStream* output)
        : Output_(output)
    { }

    void* Allocate(size_t size)
    {
        *Output_ << 'A';
        ++AllocatedCount_;

        size += sizeof(void*);
        auto* ptr = ::malloc(size);
        auto* header = static_cast<TTestAllocator**>(ptr);
        *header = this;
        return header + 1;
    }

    static void Free(void* ptr)
    {
        auto* header = static_cast<TTestAllocator**>(ptr) - 1;
        auto* allocator = *header;

        *allocator->Output_ << 'F';
        ++allocator->DeallocatedCount_;

        ::free(header);
    }

    ~TTestAllocator()
    {
        YT_VERIFY(AllocatedCount_ == DeallocatedCount_);
    }

private:
    IOutputStream* const Output_;
    int AllocatedCount_ = 0;
    int DeallocatedCount_ = 0;
};

class TSampleObject final
{
public:
    using TAllocator = TTestAllocator;
    static constexpr bool EnableHazard = true;

    explicit TSampleObject(IOutputStream* output)
        : Output_(output)
    {
        *Output_ << 'C';
    }

    ~TSampleObject()
    {
        *Output_ << 'D';
    }

    void DoSomething()
    {
        *Output_ << '!';
    }

private:
    IOutputStream* const Output_;
};

class THazardPtrTest
    : public ::testing::Test
{
protected:
    void SetUp() override
    {
        // Ensure that delete list is empty.
        ReclaimHazardPointers();
    }
};

TEST_F(THazardPtrTest, RefCountedPtrBehavior)
{
    TStringStream output;
    TTestAllocator allocator(&output);

    {
        auto ptr = New<TSampleObject>(&allocator, &output);
        {
            auto anotherPtr = ptr;
            anotherPtr->DoSomething();
        }
        {
            auto anotherPtr = ptr;
            anotherPtr->DoSomething();
        }
        ptr->DoSomething();
    }

    EXPECT_STREQ("AC!!!D", output.Str().c_str());

    ReclaimHazardPointers(/*flush*/ false);

    EXPECT_STREQ("AC!!!DF", output.Str().c_str());
}

TEST_F(THazardPtrTest, DelayedDeallocation)
{
    TStringStream output;
    TTestAllocator allocator(&output);

    auto ptr = New<TSampleObject>(&allocator, &output);
    ptr->DoSomething();

    auto hazardPtr = THazardPtr<TSampleObject>::Acquire([&] {
        return ptr.Get();
    });

    ptr = nullptr;

    EXPECT_STREQ("AC!D", output.Str().c_str());

    EXPECT_TRUE(hazardPtr);
    EXPECT_FALSE(NDetail::TryMakeStrongFromHazard(hazardPtr));

    ReclaimHazardPointers(/*flush*/ false);

    EXPECT_STREQ("AC!D", output.Str().c_str());

    hazardPtr.Reset();
    ReclaimHazardPointers(/*flush*/ false);

    EXPECT_STREQ("AC!DF", output.Str().c_str());
}

TEST_F(THazardPtrTest, DelayedDeallocationWithMultipleHPs)
{
    TStringStream output;
    TTestAllocator allocator(&output);

    auto ptr = New<TSampleObject>(&allocator, &output);
    ptr->DoSomething();

    auto hazardPtr1 = THazardPtr<TSampleObject>::Acquire([&] {
        return ptr.Get();
    });

    auto hazardPtr2 = THazardPtr<TSampleObject>::Acquire([&] {
        return ptr.Get();
    });

    ptr = nullptr;

    EXPECT_STREQ("AC!D", output.Str().c_str());

    EXPECT_TRUE(hazardPtr1);
    EXPECT_FALSE(NDetail::TryMakeStrongFromHazard(hazardPtr1));

    ReclaimHazardPointers(/*flush*/ false);

    EXPECT_STREQ("AC!D", output.Str().c_str());

    hazardPtr1.Reset();
    ReclaimHazardPointers(/*flush*/ false);

    EXPECT_STREQ("AC!D", output.Str().c_str());
    hazardPtr2.Reset();
    ReclaimHazardPointers(/*flush*/ false);

    EXPECT_STREQ("AC!DF", output.Str().c_str());
}

TEST_F(THazardPtrTest, CombinedLogic)
{
    TStringStream output;
    TTestAllocator allocator(&output);

    auto ptr = New<TSampleObject>(&allocator, &output);
    ptr->DoSomething();

    auto ptrCopy = ptr;
    auto rawPtr = ptrCopy.Release();

    auto hazardPtr = THazardPtr<TSampleObject>::Acquire([&] {
        return ptr.Get();
    });

    ptr = nullptr;

    EXPECT_STREQ("AC!", output.Str().c_str());

    RetireHazardPointer(rawPtr, [] (auto* ptr) {
        Unref(ptr);
    });

    ReclaimHazardPointers(/*flush*/ false);

    EXPECT_STREQ("AC!", output.Str().c_str());

    {
        hazardPtr.Reset();
        ReclaimHazardPointers(/*flush*/ false);

        EXPECT_STREQ("AC!D", output.Str().c_str());
    }

    {
        auto hazardPtr = THazardPtr<TSampleObject>::Acquire([&] {
            return rawPtr;
        });

        ReclaimHazardPointers(/*flush*/ false);
        EXPECT_STREQ("AC!D", output.Str().c_str());
    }

    {
        ReclaimHazardPointers(/*flush*/ false);
        EXPECT_STREQ("AC!DF", output.Str().c_str());
    }
}

TEST_W(THazardPtrTest, ThreadMaintenance)
{
    TStringStream output;
    TTestAllocator allocator(&output);

    {
        auto ptr = New<TSampleObject>(&allocator, &output);
        ptr->DoSomething();
    }

    EXPECT_STREQ("AC!D", output.Str().c_str());

    NConcurrency::TDelayedExecutor::WaitForDuration(TDuration::Seconds(3));

    EXPECT_STREQ("AC!DF", output.Str().c_str());
}

////////////////////////////////////////////////////////////////////////////////

class TSamplePolymorphicObject
    : public TRefCounted
{
public:
    using TAllocator = TTestAllocator;
    static constexpr bool EnableHazard = true;

    explicit TSamplePolymorphicObject(IOutputStream* output)
        : Output_(output)
    {
        *Output_ << 'C';
    }

    ~TSamplePolymorphicObject()
    {
        *Output_ << 'D';
    }

    void DoSomething()
    {
        *Output_ << '!';
    }

private:
    IOutputStream* const Output_;
};

TEST_F(THazardPtrTest, DelayedDeallocationPolymorphic)
{
    TStringStream output;
    TTestAllocator allocator(&output);

    auto ptr = New<TSamplePolymorphicObject>(&allocator, &output);
    ptr->DoSomething();

    auto hazardPtr = THazardPtr<TSamplePolymorphicObject>::Acquire([&] {
        return ptr.Get();
    });

    ptr = nullptr;

    EXPECT_STREQ("AC!D", output.Str().c_str());

    ReclaimHazardPointers(/*flush*/ false);

    EXPECT_STREQ("AC!D", output.Str().c_str());

    hazardPtr.Reset();
    ReclaimHazardPointers(/*flush*/ false);

    EXPECT_STREQ("AC!DF", output.Str().c_str());
}

NThreading::TEvent Started;
NThreading::TEvent Finish;

#ifndef _win_
TEST_F(THazardPtrTest, SupportFork)
{
    TStringStream output;
    TTestAllocator allocator(&output);

    auto ptr = New<TSamplePolymorphicObject>(&allocator, &output);
    ptr->DoSomething();

    auto hazardPtr = THazardPtr<TSamplePolymorphicObject>::Acquire([&] {
        return ptr.Get();
    });

    TThread thread1([] (void* opaque) -> void* {
        auto ptrRef = static_cast<TIntrusivePtr<TSamplePolymorphicObject>*>(opaque);
        auto hazardPtr = THazardPtr<TSamplePolymorphicObject>::Acquire([&] {
            return ptrRef->Get();
        });

        EXPECT_TRUE(hazardPtr);
        hazardPtr.Reset();

        Started.NotifyOne();
        Finish.Wait();

        return nullptr;
    }, &ptr);

    thread1.Start();
    Started.Wait();

    ptr = nullptr;

    EXPECT_STREQ("AC!D", output.Str().c_str());

    ReclaimHazardPointers(/*flush*/ false);

    EXPECT_STREQ("AC!D", output.Str().c_str());

    auto childPid = fork();
    if (childPid < 0) {
        THROW_ERROR_EXCEPTION("fork failed")
            << TError::FromSystem();
    }

    if (childPid == 0) {
        thread1.Detach();

        EXPECT_TRUE(hazardPtr);

        ReclaimHazardPointers(/*flush*/ false);
        EXPECT_STREQ("AC!D", output.Str().c_str());

        hazardPtr.Reset();
        ReclaimHazardPointers(/*flush*/ false);

        EXPECT_STREQ("AC!DF", output.Str().c_str());

        // Do not test hazard pointer manager shutdown
        // because of broken (after fork) NYT::Shutdown.
        ::_exit(0);
    } else {
        Sleep(TDuration::Seconds(1));
        hazardPtr.Reset();
        ReclaimHazardPointers(/*flush*/ false);

        EXPECT_STREQ("AC!DF", output.Str().c_str());

        Finish.NotifyOne();
        thread1.Join();
    }
}
#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
