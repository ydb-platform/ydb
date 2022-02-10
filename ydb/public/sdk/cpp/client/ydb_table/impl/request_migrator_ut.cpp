#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
 
#include <util/system/thread.h> 
#include <util/thread/pool.h> 
 
#include <ydb/public/api/protos/ydb_value.pb.h>
 
#define YDB_IMPL_TABLE_CLIENT_SESSION_UT 1 
 
#include <ydb/public/sdk/cpp/client/ydb_table/impl/client_session.h>
#include <ydb/public/sdk/cpp/client/ydb_table/impl/request_migrator.h>
 
namespace NYdb { 
 
using namespace NTable; 
 
Y_UNIT_TEST_SUITE(MathTest) { 
    using namespace NMath; 
    Y_UNIT_TEST(CVCalc) { 
        UNIT_ASSERT_VALUES_EQUAL(CalcCV(TVector<size_t>()).Cv, 0); 
        UNIT_ASSERT_VALUES_EQUAL(CalcCV(TVector<size_t>{1}).Cv, 0); 
        UNIT_ASSERT_VALUES_EQUAL(CalcCV(TVector<size_t>{10, 20}).Cv, 47); 
        UNIT_ASSERT_VALUES_EQUAL(CalcCV(TVector<size_t>{11, 11, 11, 11}).Cv, 0); 
        UNIT_ASSERT_VALUES_EQUAL(CalcCV(TVector<size_t>{11, 11, 10, 12}).Cv, 7); 
        UNIT_ASSERT_VALUES_EQUAL(CalcCV(TVector<size_t>{0, 0, 0, 0}).Cv, 0); 
        UNIT_ASSERT_VALUES_EQUAL(CalcCV(TVector<size_t>{1, 4, 0, 0, 1, 0, 1, 1, 0}).Cv, 143); 
    } 
} 
 
class TSessionLiveEmulator : public TThread { 
public: 
    TSessionLiveEmulator(const TString& hostname, std::function<bool(TSession::TImpl*)> cb) 
        : TThread(&ThreadProc, this) 
        , Hostname_(hostname) 
        , Cb_(cb) 
    { 
        Cont_.store(true);
    } 
 
    static void* ThreadProc(void* _this) { 
        SetCurrentThreadName("TSessionLiveEmulator"); 
        static_cast<TSessionLiveEmulator*>(_this)->Exec(); 
        return nullptr; 
    } 
 
    void Exec() { 
       while (Cont_.load()) {
           auto rowSession = new TSession::TImpl("someSessionId", Hostname_, true, 100);
           if (Cb_(rowSession)) { 
               Processed_++; 
           } 
           delete rowSession; 
       } 
    } 
 
    void Stop() { 
        Cont_.store(false);
    } 
 
    ui64 GetProcessed() const { 
        return Processed_; 
    } 
 
    const TString& GetHostname() const { 
        return Hostname_; 
    } 
 
private: 
    std::atomic_bool Cont_;
    const TString Hostname_; 
    std::function<bool(TSession::TImpl*)> Cb_; 
    ui64 Processed_ = 0; 
}; 
 
class TMigratorClient : public IMigratorClient { 
public: 
    TMigratorClient() 
        : Pool_(new TThreadPool(TThreadPool::TParams().SetBlocking(true).SetCatching(false)))
    { 
        Scheduled_.store(0);
    } 
 
    void Stop() { 
        Pool_->Stop(); 
    } 
 
    void Start() { 
        Pool_->Start(2, 1000); 
    } 
 
    virtual TAsyncPrepareQueryResult PrepareDataQuery(const TSession& session, const TString& query, 
        const TPrepareDataQuerySettings& settings) override { 
        Y_UNUSED(session); 
        Y_UNUSED(query); 
        Y_UNUSED(settings); 
        Y_VERIFY(false); 
        return {}; 
    } 
 
    void ScheduleTaskUnsafe(std::function<void()>&& fn, TDuration timeout) override { 
        Y_VERIFY(!timeout); 
        ++Scheduled_;
        Y_VERIFY(Pool_->AddFunc(std::move(fn))); 
    } 
 
    size_t GetScheduledCount() const { 
        return Scheduled_.load();
    } 
private: 
    std::unique_ptr<IThreadPool> Pool_; 
    std::atomic_int Scheduled_;
}; 
 
 
Y_UNIT_TEST_SUITE(RequestMigratorTest) { 
    Y_UNIT_TEST(ThreadingNoMatch) { 
        TRequestMigrator migrator; 
 
        std::shared_ptr<TMigratorClient> client = std::make_shared<TMigratorClient>(); 
 
        auto cb = [&migrator, client](TSession::TImpl* session) { 
            return migrator.DoCheckAndMigrate(session, client); 
        }; 
 
        TSessionLiveEmulator liveEmulator[2] = {{"host0", cb}, {"host1", cb}}; 
        for (int i = 0; i < 2; i++) { 
            liveEmulator[i].Start(); 
        } 
 
        Sleep(TDuration::MilliSeconds(100)); 
        for (int i = 0; i < 2; i++) { 
            liveEmulator[i].Stop(); 
            liveEmulator[i].Join(); 
            UNIT_ASSERT_VALUES_EQUAL(liveEmulator[i].GetProcessed(), 0); 
        } 
    } 
 
    Y_UNIT_TEST(ShutDown) { 
 
        std::shared_ptr<TMigratorClient> client = std::make_shared<TMigratorClient>(); 
 
        client->Start(); 
 
        const int attempts = 100; 
        for (int i = 0; i < attempts; i++) { 
            TRequestMigrator migrator; 
 
            migrator.SetHost("host1"); 
 
            auto session = new TSession::TImpl("someSessionId", "host1", true, 100);
            UNIT_ASSERT(migrator.DoCheckAndMigrate(session, client)); 
            delete session; 
 
            migrator.Wait(); 
        } 
 
        UNIT_ASSERT_VALUES_EQUAL(client->GetScheduledCount(), attempts); 
 
        client->Stop(); 
    } 
 
    void DoThreadingMatchTest(bool withReset) { 
        TRequestMigrator migrator; 
 
        std::shared_ptr<TMigratorClient> client = std::make_shared<TMigratorClient>(); 
 
        auto cb = [&migrator, client](TSession::TImpl* session) { 
            return migrator.DoCheckAndMigrate(session, client); 
        }; 
 
        TSessionLiveEmulator liveEmulator[2] = {{"host0", cb}, {"host1", cb}}; 
 
        client->Start(); 
 
        // Start emulation of session live: create it and destroy in multiple threads. 
        for (int i = 0; i < 2; i++) { 
            liveEmulator[i].Start(); 
        } 
 
        ui64 proposed = 0; 
        { 
            // Emulate balancing scan. 
            NThreading::TFuture<ui64> readyToMigrate; 
            for (int i = 0; i < 1000; i++) { 
                if (!withReset) 
                    Sleep(TDuration::MilliSeconds(1)); 
                if (readyToMigrate.Initialized() && !readyToMigrate.HasValue()) { 
                    if (withReset) { 
                        if (migrator.Reset()) { 
                            proposed--; 
                        } 
                    } 
                    continue; 
                } 
                if (readyToMigrate.Initialized()) 
                    UNIT_ASSERT_VALUES_EQUAL(readyToMigrate.GetValue(), 0); 
                proposed++; 
                readyToMigrate = migrator.SetHost("host1"); 
            } 
            if (readyToMigrate.Initialized()) { 
                // whait the last proposed host to be processed 
                while(!readyToMigrate.HasValue()) { 
                    Sleep(TDuration::MilliSeconds(1)); 
                } 
            } 
        } 
 
        for (int i = 0; i < 2; i++) { 
            liveEmulator[i].Stop(); 
            liveEmulator[i].Join(); 
        } 
 
        client->Stop(); 
 
        UNIT_ASSERT_VALUES_EQUAL(liveEmulator[0].GetProcessed(), 0); 
        UNIT_ASSERT_VALUES_EQUAL(liveEmulator[1].GetProcessed(), proposed); 
    } 
 
    Y_UNIT_TEST(ThreadingMatch) { 
        DoThreadingMatchTest(false); 
    } 
 
    Y_UNIT_TEST(ThreadingMatchWithReset) { 
        DoThreadingMatchTest(true); 
    } 
 
 
} 
 
} 
