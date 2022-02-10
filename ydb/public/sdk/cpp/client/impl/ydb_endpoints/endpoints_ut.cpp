#include <ydb/public/sdk/cpp/client/impl/ydb_endpoints/endpoints.h>

#include <library/cpp/testing/unittest/registar.h> 
#include <library/cpp/testing/unittest/tests_data.h> 
#include <library/cpp/threading/future/core/future.h>

#include <util/system/thread.h>
#include <util/random/random.h>

#include <unordered_set>

using namespace NYdb;

class TTestObj : public NYdb::TEndpointObj {
public:
    TTestObj(int& counter)
        : Counter_(&counter)
    { }

    TTestObj()
        : Counter_(nullptr)
    { }

    ~TTestObj() {
        if (ObjectRegistred() && Counter_) {
            (*Counter_)++;
        }
        Unlink();
    }

    virtual void OnEndpointRemoved() {
        HostRemoved_ = true;
    }

    bool HostRemoved() const {
        return HostRemoved_;
    }

    bool HostRemoved_ = false;

    int* Counter_;
};

class TDiscoveryEmulator : public TThread {
public:
    TDiscoveryEmulator(TEndpointElectorSafe& elector, ui64 maxEvents)
        : TThread(&ThreadProc, this)
        , Elector_(elector)
        , MaxEvents_(maxEvents)
    {
        Finished_.store(false);
    }

    static void* ThreadProc(void* _this) {
        SetCurrentThreadName("TDiscoveryEmulator");
        static_cast<TDiscoveryEmulator*>(_this)->Exec();
        return nullptr;
    }

    void Exec() {
        for (ui64 i = 0; i < MaxEvents_; i++) {
            ui8 mask = RandomNumber<ui8>(16);
            TVector<TEndpointRecord> endpoints;

            for (size_t i = 0; i < Pool_.size(); i++) {
                if (mask & (1 << i))
                    endpoints.emplace_back(Pool_[i]);
            }

            Elector_.SetNewState(std::move(endpoints));

            if (i % 256 == 0) {
                Sleep(TDuration::MilliSeconds(10));
            }
        }
        Finished_.store(true);
    }

    const TVector<TEndpointRecord>& GetPool() const {
        return Pool_;
    }

    bool Finished() const {
        return Finished_.load();
    }

private:
    TEndpointElectorSafe& Elector_;
    ui64 MaxEvents_;
    std::atomic_bool Finished_;

    static const TVector<TEndpointRecord> Pool_;
};

const TVector<TEndpointRecord> TDiscoveryEmulator::Pool_ = TVector<TEndpointRecord>{{"One", 1}, {"Two", 2}, {"Three", 3}, {"Four", 4}};

Y_UNIT_TEST_SUITE(CheckUtils) {

    Y_UNIT_TEST(NewPromiseInitialized) {
        NThreading::TPromise<void> promise = NThreading::NewPromise<void>();
        UNIT_ASSERT(promise.Initialized());
    }

    Y_UNIT_TEST(PromiseDefaultCtorNotInitialized) {
        NThreading::TPromise<void> promise;
        UNIT_ASSERT(!promise.Initialized());
    }
}

Y_UNIT_TEST_SUITE(EndpointElector) {

    Y_UNIT_TEST(Empty) {
        TEndpointElectorSafe elector;
        UNIT_ASSERT_VALUES_EQUAL(elector.GetEndpoint("").Endpoint, "");
    }

    Y_UNIT_TEST(DiffOnRemove) {
        TEndpointElectorSafe elector;
        auto removed = elector.SetNewState(TVector<TEndpointRecord>{{"Two", 2}, {"One", 1}});
        UNIT_ASSERT_VALUES_EQUAL(removed.size(), 0);
        removed = elector.SetNewState(TVector<TEndpointRecord>{{"One", 1}});
        UNIT_ASSERT_VALUES_EQUAL(removed.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(removed[0], TString("Two"));
    }

    Y_UNIT_TEST(Pessimization) {
        TEndpointElectorSafe elector;
        elector.SetNewState(TVector<TEndpointRecord>{{"Two", 2}, {"One", 1}});
        UNIT_ASSERT_VALUES_EQUAL(elector.GetPessimizationRatio(), 0);
        elector.PessimizeEndpoint("One");
        UNIT_ASSERT_VALUES_EQUAL(elector.GetPessimizationRatio(), 50);
        elector.SetNewState(TVector<TEndpointRecord>{{"Two", 2}});
        UNIT_ASSERT_VALUES_EQUAL(elector.GetPessimizationRatio(), 0);
    }

    Y_UNIT_TEST(Election) {
        TEndpointElectorSafe elector;
        elector.SetNewState(TVector<TEndpointRecord>{{"Two", 2}, {"One_A", 1}, {"Three", 3}, {"One_B", 1}});
        std::unordered_set<TString> endpoints;
        // Just to make sure no possible to get more than expected
        size_t extra_attempts = 1000;
        while (endpoints.size() != 2 || --extra_attempts) {
            endpoints.insert(elector.GetEndpoint("").Endpoint);
            UNIT_ASSERT_VALUES_EQUAL(elector.GetEndpoint("Three").Endpoint, "Three");
        }
        UNIT_ASSERT_VALUES_EQUAL(endpoints.size(), 2);
        UNIT_ASSERT(endpoints.find("One_A") != endpoints.end());
        UNIT_ASSERT(endpoints.find("One_B") != endpoints.end());

        elector.SetNewState(TVector<TEndpointRecord>{{"One", 1}});
        // no prefered endpoint, expect avaliable
        UNIT_ASSERT_VALUES_EQUAL(elector.GetEndpoint("Three").Endpoint, "One");
        UNIT_ASSERT_VALUES_EQUAL(elector.GetEndpoint("").Endpoint, "One");
    }

    Y_UNIT_TEST(EndpointAssociationTwoThreadsNoRace) {
        TEndpointElectorSafe elector;

        TDiscoveryEmulator emulator(elector, 10000);

        emulator.Start();

        int counter1 = 0;
        int counter2 = 0;

        TVector<std::unique_ptr<TTestObj>> storage;
        while (!emulator.Finished()) {
            auto obj = std::make_unique<TTestObj>(counter2);
            if (elector.LinkObjToEndpoint("Two", obj.get(), nullptr)) {
                counter1++;
            }
            storage.emplace_back(std::move(obj));
            // collect some objects
            if (counter1 % 10 == 0)
                storage.clear();
        }

        emulator.Join();

        storage.clear();
        UNIT_ASSERT_VALUES_EQUAL(counter1, counter2);
    }

    Y_UNIT_TEST(EndpointAssiciationSingleThread) {
        TEndpointElectorSafe elector;
        elector.SetNewState(TVector<TEndpointRecord>{{"Two", 2}, {"One_A", 1}, {"Three", 3}, {"One_B", 1}});

        auto obj1 = std::make_unique<TTestObj>();
        auto obj2 = std::make_unique<TTestObj>();
        auto obj3 = std::make_unique<TTestObj>();
        auto obj4 = std::make_unique<TTestObj>();
        auto obj5 = std::make_unique<TTestObj>();

        UNIT_ASSERT_VALUES_EQUAL(obj1->ObjectRegistred(), false);

        UNIT_ASSERT(elector.LinkObjToEndpoint("Two", obj1.get(), nullptr));
        // Registred of same object twice is not allowed
        UNIT_ASSERT_VALUES_EQUAL(elector.LinkObjToEndpoint("Two", obj1.get(), nullptr), false);

        UNIT_ASSERT(elector.LinkObjToEndpoint("Three", obj2.get(), nullptr));
        UNIT_ASSERT(elector.LinkObjToEndpoint("Three", obj3.get(), nullptr));
        UNIT_ASSERT(elector.LinkObjToEndpoint("One_B", obj4.get(), nullptr));
        UNIT_ASSERT(elector.LinkObjToEndpoint("One_B", obj5.get(), (void*)1));

        UNIT_ASSERT_VALUES_EQUAL(obj1->ObjectRegistred(), true);

        UNIT_ASSERT_VALUES_EQUAL(obj1->ObjectCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(obj2->ObjectCount(), 2);
        UNIT_ASSERT_VALUES_EQUAL(obj3->ObjectCount(), 2);
        UNIT_ASSERT_VALUES_EQUAL(obj4->ObjectCount(), 1);

        {
            TMap<TString, size_t> sizes;
            size_t i = 0;
            elector.ForEachEndpoint([&sizes, &i](const TString& endpoint, const NYdb::IObjRegistryHandle& handle) {
                sizes[endpoint] = handle.Size();
                i++;
            }, 0, Max<i32>(), nullptr);
            UNIT_ASSERT_VALUES_EQUAL(sizes.size(), 4);
            UNIT_ASSERT_VALUES_EQUAL(i, 4);
            UNIT_ASSERT_VALUES_EQUAL(sizes.at("Two"), 1);
            UNIT_ASSERT_VALUES_EQUAL(sizes.at("One_A"), 0);
            UNIT_ASSERT_VALUES_EQUAL(sizes.at("One_B"), 1);
            UNIT_ASSERT_VALUES_EQUAL(sizes.at("Three"), 2);
        }

        {
            TMap<TString, size_t> sizes;
            size_t i = 0;
            elector.ForEachEndpoint([&sizes, &i](const TString& endpoint, const NYdb::IObjRegistryHandle& handle) {
                sizes[endpoint] = handle.Size();
                i++;
            }, 0, Max<i32>(), (void*)1);
            UNIT_ASSERT_VALUES_EQUAL(sizes.size(), 4);
            UNIT_ASSERT_VALUES_EQUAL(i, 4);
            UNIT_ASSERT_VALUES_EQUAL(sizes.at("Two"), 0);
            UNIT_ASSERT_VALUES_EQUAL(sizes.at("One_A"), 0);
            UNIT_ASSERT_VALUES_EQUAL(sizes.at("One_B"), 1);
            UNIT_ASSERT_VALUES_EQUAL(sizes.at("Three"), 0);
        }

        UNIT_ASSERT_VALUES_EQUAL(obj1->HostRemoved(), false);
        UNIT_ASSERT_VALUES_EQUAL(obj2->HostRemoved(), false);
        UNIT_ASSERT_VALUES_EQUAL(obj3->HostRemoved(), false);
        UNIT_ASSERT_VALUES_EQUAL(obj4->HostRemoved(), false);
        UNIT_ASSERT_VALUES_EQUAL(obj5->HostRemoved(), false);

        obj1.reset();

        UNIT_ASSERT_VALUES_EQUAL(obj2->HostRemoved(), false);
        UNIT_ASSERT_VALUES_EQUAL(obj3->HostRemoved(), false);
        UNIT_ASSERT_VALUES_EQUAL(obj4->HostRemoved(), false);

        {
            TMap<TString, size_t> sizes;
            size_t i = 0;
            elector.ForEachEndpoint([&sizes, &i](const TString& endpoint, const NYdb::IObjRegistryHandle& handle) {
                sizes[endpoint] = handle.Size();
                i++;
            }, 0, Max<i32>(), nullptr);
            UNIT_ASSERT_VALUES_EQUAL(sizes.size(), 4);
            UNIT_ASSERT_VALUES_EQUAL(i, 4);
            UNIT_ASSERT_VALUES_EQUAL(sizes.at("Two"), 0);
            UNIT_ASSERT_VALUES_EQUAL(sizes.at("One_A"), 0);
            UNIT_ASSERT_VALUES_EQUAL(sizes.at("One_B"), 1);
            UNIT_ASSERT_VALUES_EQUAL(sizes.at("Three"), 2);
        }

        obj1.reset(new TTestObj());
        elector.LinkObjToEndpoint("Two", obj1.get(), nullptr);

        elector.SetNewState(TVector<TEndpointRecord>{{"Two", 2}, {"One_A", 1}, {"One_C", 1}});

        UNIT_ASSERT_VALUES_EQUAL(obj1->HostRemoved(), false);
        UNIT_ASSERT_VALUES_EQUAL(obj2->HostRemoved(), true);
        UNIT_ASSERT_VALUES_EQUAL(obj3->HostRemoved(), true);
        UNIT_ASSERT_VALUES_EQUAL(obj4->HostRemoved(), true);
        UNIT_ASSERT_VALUES_EQUAL(obj5->HostRemoved(), true);

        {
            TMap<TString, size_t> sizes;
            size_t i = 0;
            elector.ForEachEndpoint([&sizes, &i](const TString& endpoint, const NYdb::IObjRegistryHandle& handle) {
                sizes[endpoint] = handle.Size();
                i++;
            }, 0, Max<i32>(), nullptr);
            UNIT_ASSERT_VALUES_EQUAL(sizes.size(), 3);
            UNIT_ASSERT_VALUES_EQUAL(i, 3);
            UNIT_ASSERT_VALUES_EQUAL(sizes.at("Two"), 1);
            UNIT_ASSERT_VALUES_EQUAL(sizes.at("One_C"), 0);
            UNIT_ASSERT_VALUES_EQUAL(sizes.at("One_A"), 0);
        }

        UNIT_ASSERT_VALUES_EQUAL(obj1->ObjectCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(obj2->ObjectCount(), 2);
        UNIT_ASSERT_VALUES_EQUAL(obj3->ObjectCount(), 2);
        UNIT_ASSERT_VALUES_EQUAL(obj4->ObjectCount(), 1);

        obj2.reset();

        UNIT_ASSERT_VALUES_EQUAL(obj3->ObjectCount(), 1);
    }
}
