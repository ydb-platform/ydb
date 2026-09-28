#include <library/cpp/scheme/scimpl.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/system/event.h>
#include <util/system/sanitizers.h>
#include <util/system/sys_alloc.h>
#include <util/system/thread.h>

namespace {
    class TReusingAllocator final : public IAllocator {
    public:
        ~TReusingAllocator() override {
            y_deallocate(Block_.Data);
        }

        TBlock Allocate(size_t len) override {
            Y_ABORT_UNLESS(!Block_.Data);
            Block_ = {y_allocate(len), len};
            return Block_;
        }

        void Release(const TBlock& block) override {
            Y_ABORT_UNLESS(block.Data == Block_.Data);
            Y_ABORT_UNLESS(block.Len == Block_.Len);

            Released_.Signal();
            Reused_.WaitI();
        }

        void ReuseAndCheckAfter(TManualEvent& destructionFinished) {
            Released_.WaitI();

            // Model malloc handing this block to another thread. A real malloc
            // allocation is unpoisoned by the MSan interceptor at this point.
            NSan::Unpoison(Block_.Data, Block_.Len);
            Reused_.Signal();

            destructionFinished.WaitI();
            NSan::CheckMemIsInitialized(Block_.Data, Block_.Len);
        }

    private:
        TBlock Block_ = {nullptr, 0};
        TManualEvent Released_;
        TManualEvent Reused_;
    };
}

Y_UNIT_TEST_SUITE(TSchemeLifetimeTest) {
    Y_UNIT_TEST(TScCorePoolOutlivesDestructor) {
        TReusingAllocator allocator;
        auto pool = NSc::TValue::TPoolPtr(new NSc::NDefinitions::TPool(
            NSc::NDefinitions::POOL_BLOCK_SIZE,
            TMemoryPool::TExpGrow::Instance(),
            &allocator));

        auto* rawCore = new (pool->Pool.Allocate<NSc::TValue::TScCore>()) NSc::TValue::TScCore(pool);
        TIntrusivePtr<NSc::TValue::TScCore> core(rawCore);
        pool.Reset();

        TManualEvent destructionFinished;
        TThread reuser([&] {
            allocator.ReuseAndCheckAfter(destructionFinished);
        });
        reuser.Start();

        core.Reset();
        destructionFinished.Signal();
        reuser.Join();
    }
}
