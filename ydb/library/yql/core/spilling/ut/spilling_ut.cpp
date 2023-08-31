#include <ydb/library/yql/minikql/mkql_runtime_version.h>
#include <library/cpp/testing/unittest/registar.h>

#include <chrono>
#include <iostream>
#include <cstring>
#include <vector>
#include <cassert>
#include <thread>

#include <util/system/fs.h>
#include <util/system/compiler.h>
#include <util/stream/null.h>
#include <util/system/mem_info.h>

#include <cstdint>

#include <ydb/library/yql/core/spilling/interface/spilling.h>
#include <ydb/library/yql/core/spilling/storage/storage.h>

namespace NYql {
namespace NSpilling {

using namespace NSpilling;
using namespace std::chrono_literals;

constexpr bool IsVerbose = true;
#define CTEST (IsVerbose ? Cerr : Cnull)


Y_UNIT_TEST_SUITE(TYDBLibrarySpillingTest) {

    Y_UNIT_TEST(TestCreateProxy) {
        TFileStorageConfig config;
        config.Path = NFs::CurrentWorkingDirectory();
        TTempStorageExecutionPolicy policy;
        std::pair < THolder<ITempStorageProxy>, TOperationResults> tproxy = CreateFileStorageProxy(config, policy );
        CTEST << "Status: " << ui32( tproxy.second.Status ) << Endl;
        UNIT_ASSERT(tproxy.second.Status == EOperationStatus::Success);
    }

    Y_UNIT_TEST(TestCreateSession) {
        TFileStorageConfig config;
        config.Path = NFs::CurrentWorkingDirectory();
        TTempStorageExecutionPolicy policy;
        std::pair < THolder<ITempStorageProxy>, TOperationResults> tproxy = CreateFileStorageProxy(config, policy );
        THolder<ISession> session = tproxy.first->CreateSession();
        UNIT_ASSERT(session != nullptr);
    }

    Y_UNIT_TEST(TestSave) {
        TFileStorageConfig config;
        config.Path = NFs::CurrentWorkingDirectory();
        TTempStorageExecutionPolicy policy;
        std::pair < THolder<ITempStorageProxy>, TOperationResults> tproxy = CreateFileStorageProxy(config, policy );
        THolder<ISession> session = tproxy.first->CreateSession();
        NThreading::TFuture<TOperationResults> ftr;
        const ui32 bufSize = 1024 * sizeof(int);
        const ui32 iters = 1000;
        std::vector<TBuffer> buffers, buffers1;

        NMemInfo::TMemInfo mi = NMemInfo::GetMemInfo();
        CTEST << "Mem usage before buffs prepare (MB): " << mi.RSS / (1024 * 1024) << Endl;
        std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
        for (ui32 i = 0; i < iters; i++) {
            TBuffer buf(bufSize);
            buf.Resize(bufSize);
            memset(buf.Data(), i+1, bufSize);
            buffers1.emplace_back(std::move(buf));
        }
        mi = NMemInfo::GetMemInfo();
        CTEST << "Mem usage after buffs prepare (MB): " << mi.RSS / (1024 * 1024) << Endl;
        std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
        ui64 execTime = std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();
        CTEST << "Execution time for " << iters << " prepare buf calls (microseconds): " << execTime << Endl;

        begin = std::chrono::steady_clock::now();
        for (ui32 i = 0; i < iters; i++) {
            buffers.emplace_back(std::move(buffers1[i]));
        }
        end = std::chrono::steady_clock::now();
        execTime = std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();
        CTEST << "Execution time for  " << iters << " moving buffers: (microseconds): " << execTime << Endl;


        begin = std::chrono::steady_clock::now();
        for (ui32 i = 0; i < iters; i++) {
            ftr = session->Save(TString("Test"), TString("test" + std::to_string(i)), std::move(buffers[i]));
        }
        end = std::chrono::steady_clock::now();
        execTime = std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();
        CTEST << "Execution time for " << iters << " save calls (microseconds): " << execTime << Endl;
        CTEST << "Per one call (microseconds): " << execTime/iters << Endl;

        mi = NMemInfo::GetMemInfo();
        CTEST << "Mem usage after buffs save (MB): " << mi.RSS / (1024 * 1024) << Endl;

        TSessionDataStat dstat = session->GetSessionDataStat();
        CTEST << "Session total data: " << dstat.Provided << Endl;
        CTEST << "Session spilled data: " << dstat.Spilled << Endl;
        CTEST << "Session in memory data: " << dstat.InMemory << Endl;

        std::this_thread::sleep_for(5000ms);

        dstat = session->GetSessionDataStat();
        CTEST << "Session spilled data after sleep: " << dstat.Spilled << Endl;
        CTEST << "Session loaded from memory data: " << dstat.LoadedFromMemory << Endl;
        CTEST << "Session in memory data: " << dstat.InMemory << Endl;

        mi = NMemInfo::GetMemInfo();
        CTEST << "Mem usage after buffs writing (MB): " << mi.RSS / (1024 * 1024) << Endl;

        UNIT_ASSERT(ftr.Initialized());
    }

    Y_UNIT_TEST(TestLoad) {
        TFileStorageConfig config;
        config.Path = NFs::CurrentWorkingDirectory();
        TTempStorageExecutionPolicy policy;
        std::pair < THolder<ITempStorageProxy>, TOperationResults> tproxy = CreateFileStorageProxy(config, policy );
        THolder<ISession> session = tproxy.first->CreateSession();

        NThreading::TFuture<TOperationResults> ftr;
        std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
        const ui32 bufSize = 1024*1024;
        const ui32 iters = 1000;
        for (ui32 i = 0; i < iters; i++) {
            TBuffer buf(bufSize);
            buf.Resize(bufSize);
            memset(buf.Data(), i+1, bufSize );
            ftr = session->Save(TString("Test"), TString("test" + std::to_string(i)), std::move(buf));
        }
        std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
        ui64 execTime = std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();
        CTEST << "Execution time for " << iters << " save calls (microseconds): " << execTime << Endl;
        CTEST << "Per one call (microseconds): " << execTime/iters << Endl;

//        std::this_thread::sleep_for(3000ms);

        begin = std::chrono::steady_clock::now();
        NThreading::TFuture<TLoadOperationResults> ftrl;
        for (ui32 i = 0; i < iters; i++) {
            ftrl = session->Load(TString("Test"), TString("test" + std::to_string(i)));
        }
        end = std::chrono::steady_clock::now();
        execTime = std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();
        CTEST << "Execution time for " << iters << " load calls (microseconds): " << execTime << Endl;
        CTEST << "Per one call (microseconds): " << execTime/iters << Endl;

        TSessionDataStat dstat = session->GetSessionDataStat();
        CTEST << "Session total data: " << dstat.Provided << Endl;
        CTEST << "Session spilled data: " << dstat.Spilled << Endl;
        CTEST << "Session loaded from memory data: " << dstat.LoadedFromMemory << Endl;
        CTEST << "Session in memory data: " << dstat.InMemory << Endl;

        UNIT_ASSERT(ftrl.Initialized());
    }

    Y_UNIT_TEST(TestSetOp) {
        TString name{"Test"}; 
        TSpillMetaRecord mr{EOperationType::Add, name, 0, 0, 1, 2};
        auto res = mr.GetOpType();
        CTEST << "OpType: " << (ui32) res << Endl;
        CTEST << mr.AsString() << Endl;
        UNIT_ASSERT(res == EOperationType::Add);

    }

    Y_UNIT_TEST(TestSpillMetaPack) {
        TString name{"Test1"}; 
        TSpillMetaRecord mr{EOperationType::Add, name, 1, 2, 3, 4};
        auto res = mr.GetOpType();        
        CTEST << mr.AsString() << Endl;
        TBuffer buf;
        mr.Pack(buf);
        CTEST << "Buf size: " << buf.Size() << Endl;

        TString name1{"Test22222"}; 
        TSpillMetaRecord mr1{EOperationType::Add, name1, 2, 3, 5, 6};
        CTEST << mr1.AsString() << Endl;

        mr1.Unpack(buf);
        CTEST << mr1.AsString() << Endl;

        UNIT_ASSERT(res == EOperationType::Add);

    }

    Y_UNIT_TEST(TestSpillSaveLoad) {
        TFileStorageConfig config;
        config.Path = NFs::CurrentWorkingDirectory();
        TTempStorageExecutionPolicy policy;
        std::pair < THolder<ITempStorageProxy>, TOperationResults> tproxy = CreateFileStorageProxy(config, policy );
        THolder<ISession> session = tproxy.first->CreateSession();

        NThreading::TFuture<TOperationResults> ftr;
        std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
        const ui32 bufSize = 1024*1024;
        const ui32 iters = 1000;
        for (ui32 i = 0; i < iters; i++) {
            TBuffer buf(bufSize);
            buf.Resize(bufSize);
            memset(buf.Data(), i+1, bufSize );
            ftr = session->Save(TString("Test"), TString("test" + std::to_string(i)), std::move(buf));
        }
        std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
        ui64 execTime = std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();
        CTEST << "Execution time for " << iters << " save calls (microseconds): " << execTime << Endl;
        CTEST << "Per one call (microseconds): " << execTime/iters << Endl;

        std::this_thread::sleep_for(3000ms);

        begin = std::chrono::steady_clock::now();
        NThreading::TFuture<TLoadOperationResults> ftrl;
        for (ui32 i = 0; i < iters; i++) {
            ftrl = session->Load(TString("Test"), TString("test" + std::to_string(i)));
        }
        end = std::chrono::steady_clock::now();
        execTime = std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();
        CTEST << "Execution time for " << iters << " load calls (microseconds): " << execTime << Endl;
        CTEST << "Per one call (microseconds): " << execTime/iters << Endl;

        std::this_thread::sleep_for(6000ms);

        TSessionDataStat dstat = session->GetSessionDataStat();
        CTEST << "Session total data: " << dstat.Provided << Endl;
        CTEST << "Session spilled data: " << dstat.Spilled << Endl;
        CTEST << "Session loaded from memory data: " << dstat.LoadedFromMemory << Endl;
        CTEST << "Session loaded from storage data: " << dstat.LoadedFromStorage << Endl;
        CTEST << "Session in memory data: " << dstat.InMemory << Endl;

        UNIT_ASSERT(ftrl.Initialized());

    }

    Y_UNIT_TEST(TestGarbageCollection) {
        TFileStorageConfig config;
        config.Path = NFs::CurrentWorkingDirectory();
        TTempStorageExecutionPolicy policy;
        std::pair < THolder<ITempStorageProxy>, TOperationResults> tproxy = CreateFileStorageProxy(config, policy );
        for (ui32 sessNum = 0; sessNum < 10; sessNum++) {
            THolder<ISession> session = tproxy.first->CreateSession();

            NThreading::TFuture<TOperationResults> ftr;
            std::chrono::steady_clock::time_point begin =
                std::chrono::steady_clock::now();
            const ui32 bufSize = 1024 * 1024;
            const ui32 iters = 1000;
            for (ui32 i = 0; i < iters; i++) {
              TBuffer buf(bufSize);
              buf.Resize(bufSize);
              memset(buf.Data(), i + 1, bufSize );
              ftr = session->Save(TString("Test"),
                                  TString("test" + std::to_string(i)),
                                  std::move(buf));
            }
            std::chrono::steady_clock::time_point end =
                std::chrono::steady_clock::now();
            ui64 execTime =
                std::chrono::duration_cast<std::chrono::microseconds>(end -
                                                                      begin)
                    .count();
            CTEST << "Execution time for " << iters
                  << " save calls (microseconds): " << execTime << Endl;
            CTEST << "Per one call (microseconds): " << execTime / iters
                  << Endl;

            std::this_thread::sleep_for(3000ms);

            TSessionDataStat dstat = session->GetSessionDataStat();
            CTEST << "Session total data after save: " << dstat.Provided << Endl;
            CTEST << "Session spilled data after save: " << dstat.Spilled << Endl;
            CTEST << "Session loaded from memory data after save : "
                  << dstat.LoadedFromMemory << Endl;
            CTEST << "Session loaded from storage data after save: "
                  << dstat.LoadedFromStorage << Endl;
            CTEST << "Session in memory data after save: " << dstat.InMemory << Endl;


            begin = std::chrono::steady_clock::now();
            NThreading::TFuture<TLoadOperationResults> ftrl;
            for (ui32 i = 0; i < iters; i++) {
              ftrl = session->Load(TString("Test"),
                                   TString("test" + std::to_string(i)));
            }
            end = std::chrono::steady_clock::now();
            execTime = std::chrono::duration_cast<std::chrono::microseconds>(
                           end - begin)
                           .count();
            CTEST << "Execution time for " << iters
                  << " load calls (microseconds): " << execTime << Endl;
            CTEST << "Per one call (microseconds): " << execTime / iters
                  << Endl;

            std::this_thread::sleep_for(6000ms);

            dstat = session->GetSessionDataStat();
            CTEST << "Session total data: " << dstat.Provided << Endl;
            CTEST << "Session spilled data: " << dstat.Spilled << Endl;
            CTEST << "Session loaded from memory data: "
                  << dstat.LoadedFromMemory << Endl;
            CTEST << "Session loaded from storage data: "
                  << dstat.LoadedFromStorage << Endl;
            CTEST << "Session in memory data: " << dstat.InMemory << Endl;

            UNIT_ASSERT(ftrl.Initialized());
        }
    }


    Y_UNIT_TEST(TestSpillStreamSaveLoad) {
        TFileStorageConfig config;
        config.Path = NFs::CurrentWorkingDirectory();
        TTempStorageExecutionPolicy policy;
        std::pair < THolder<ITempStorageProxy>, TOperationResults> tproxy = CreateFileStorageProxy(config, policy );
        THolder<ISession> session = tproxy.first->CreateSession();

        NThreading::TFuture<TOperationResults> ftr;
        std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
        const ui32 bufSize = 1024*1024;
        const ui32 iters = 1000;
        auto res = session->OpenStream(TString("Test"), TString("Stream1"));
        auto st1 = std::move(res.first);
        CTEST << "Stream size before save: " << st1->Size() << Endl;
        for (ui32 i = 0; i < iters; i++) {
            TBuffer buf(bufSize);
            buf.Resize(bufSize);
            memset(buf.Data(), i+1, bufSize );
            ftr = st1->Save(std::move(buf));
        }
        CTEST << "Stream size after save: " << st1->Size() << Endl;        
        std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
        ui64 execTime = std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();
        CTEST << "Execution time for " << iters << " save calls (microseconds): " << execTime << Endl;
        CTEST << "Per one call (microseconds): " << execTime/iters << Endl;

        std::this_thread::sleep_for(3000ms);

        begin = std::chrono::steady_clock::now();
        NThreading::TFuture<TLoadOperationResults> ftrl;
        for (ui32 i = 0; i < iters; i++) {
            ftrl = st1->Load(i);
        }
        end = std::chrono::steady_clock::now();
        execTime = std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();
        CTEST << "Execution time for " << iters << " load calls (microseconds): " << execTime << Endl;
        CTEST << "Per one call (microseconds): " << execTime/iters << Endl;

        std::this_thread::sleep_for(6000ms);

        TSessionDataStat dstat = session->GetSessionDataStat();
        CTEST << "Session total data: " << dstat.Provided << Endl;
        CTEST << "Session spilled data: " << dstat.Spilled << Endl;
        CTEST << "Session loaded from memory data: " << dstat.LoadedFromMemory << Endl;
        CTEST << "Session loaded from storage data: " << dstat.LoadedFromStorage << Endl;
        CTEST << "Session in memory data: " << dstat.InMemory << Endl;

        UNIT_ASSERT(ftrl.Initialized());

    }



}

}

}
