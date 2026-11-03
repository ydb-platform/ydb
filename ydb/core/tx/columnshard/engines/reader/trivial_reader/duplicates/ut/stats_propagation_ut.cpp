#include <ydb/core/formats/arrow/reader/stats/stats.h>
#include <ydb/core/tx/general_cache/usage/events.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NOlap::NReader::NDuplicateStatsTests {
namespace {

struct TTestPolicy {
    using TAddress = ui64;
    using TObject = ui64;
    using TSourceId = ui64;
    enum class EConsumer { Undefined, Test };
};

class TCallback final : public ::NKikimr::NGeneralCache::NPublic::ICallback<TTestPolicy> {
    void DoOnResultReady(THashMap<ui64, ui64>&&, THashSet<ui64>&&,
        ::NKikimr::NGeneralCache::NPublic::TErrorAddresses<TTestPolicy>&&) override {
    }

    bool DoIsAborted() const override {
        return false;
    }
};

using TRequest = ::NKikimr::NGeneralCache::NPublic::TEvents<TTestPolicy>::TEvAskData;

std::unique_ptr<TRequest> MakeRequest() {
    return std::make_unique<TRequest>(TTestPolicy::EConsumer::Test,
        THashSet<ui64>{11}, std::make_shared<TCallback>());
}

} // namespace

Y_UNIT_TEST_SUITE(TDuplicateStatsPropagation) {
    Y_UNIT_TEST(CacheRequestRetainsOwnerAndReleasesItWithRequest) {
        auto collector = std::make_shared<NScanStats::TCollector>(44);
        collector->BindScanId("dedup-request");
        std::weak_ptr<NScanStats::TCollector> weakCollector = collector;
        auto owner = collector->Start(NScanStats::TStep::Merge, "OWNER", "Build duplicate inputs", {});
        owner->Update([](auto& record) {
            record.SetMetaStep("PKFilter");
            record.SetCanonicalPath("Filter");
        });
        std::unique_ptr<TRequest> request;
        {
            NScanStats::TScope scope(owner, collector);
            request = MakeRequest();
        }
        owner->Finish();
        UNIT_ASSERT_VALUES_EQUAL(request->GetStatsCapture().Operation.get(), owner.get());
        UNIT_ASSERT_VALUES_EQUAL(request->GetStatsCapture().Collector.get(), collector.get());
        {
            NScanStats::TScope metadata(request->GetStatsCapture());
            auto child = NScanStats::CurrentCollector()->Start(NScanStats::TStep::Assemble,
                "CHILD", "Assemble cache-miss inputs", NScanStats::Current());
            NScanStats::TScope work(child, collector);
            child->Finish();
        }
        auto report = collector->Drain(true);
        UNIT_ASSERT_VALUES_EQUAL(report->StepsSize(), 2);
        bool found = false;
        for (const auto& step : report->GetSteps()) {
            if (step.GetStepKind() == NScanStats::TStep::Assemble) {
                UNIT_ASSERT_VALUES_EQUAL(step.GetMetaStep(), "PKFilter");
                UNIT_ASSERT_VALUES_EQUAL(step.GetCanonicalPath(), "Filter/Merge");
                UNIT_ASSERT(!step.HasSourceId());
                found = true;
            }
        }
        UNIT_ASSERT(found);
        owner.reset();
        collector.reset();
        UNIT_ASSERT(!weakCollector.expired());
        request.reset();
        UNIT_ASSERT(weakCollector.expired());
    }

    Y_UNIT_TEST(DisabledRequestDoesNotBorrowAnotherOwner) {
        auto collector = std::make_shared<NScanStats::TCollector>(45);
        auto unrelated = collector->Start(NScanStats::TStep::Reader, "OTHER", "Other read", {});
        std::unique_ptr<TRequest> request;
        {
            NScanStats::TScope active(unrelated, collector);
            {
                NScanStats::TScope disabled(nullptr, nullptr);
                request = MakeRequest();
            }
            UNIT_ASSERT_VALUES_EQUAL(NScanStats::Current().get(), unrelated.get());
            unrelated->Finish();
        }
        UNIT_ASSERT(!request->GetStatsCapture().Operation);
        UNIT_ASSERT(!request->GetStatsCapture().Collector);
    }
}

} // namespace NKikimr::NOlap::NReader::NDuplicateStatsTests
