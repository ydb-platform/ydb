#include <ydb/library/yql/minikql/mkql_runtime_version.h>
#include <library/cpp/testing/unittest/registar.h>

#include <chrono>
#include <iostream>
#include <cstring>
#include <vector>
#include <cassert>

#include <util/system/compiler.h>
#include <util/stream/null.h>
#include <util/system/fs.h>

#include <cstdint>

#include <ydb/library/yql/core/spilling/interface/spilling.h>
#include <ydb/library/yql/core/spilling/storage/storage.h>

namespace NYql {
namespace NSpilling {

using namespace NSpilling;

constexpr bool IsVerbose = true;
#define CTEST (IsVerbose ? Cerr : Cnull)


Y_UNIT_TEST_SUITE(TSpillingTest) {

Y_UNIT_TEST(TestCreateStorage) {
    TFileStorageConfig config;
    config.Path = NFs::CurrentWorkingDirectory();
    std::pair< THolder<ISpillStorage>, TOperationResults > sp = OpenFileStorageForSpilling(config);
    CTEST << "Path: " << config.Path << Endl;
    UNIT_ASSERT(sp.second.Status == EOperationStatus::Success);
}


Y_UNIT_TEST(TestCreateNoStorage) {
    TFileStorageConfig config;
    config.Path = "Temp123456";
    std::pair< THolder<ISpillStorage>, TOperationResults > sp = OpenFileStorageForSpilling(config);
    CTEST << "Path: " << config.Path << Endl;
    CTEST << "Error string: " << sp.second.ErrorString << Endl;
    UNIT_ASSERT(sp.second.Status == EOperationStatus::CannotOpenStorageProxy);
}

Y_UNIT_TEST(TestNamespaces) {
    TFileStorageConfig config;
    config.Path = NFs::CurrentWorkingDirectory();
    std::pair< THolder<ISpillStorage>, TOperationResults > sp = OpenFileStorageForSpilling(config);
    TVector<TString> ns = sp.first->GetNamespaces();
    for ( auto n : ns) {
        CTEST << "Namespace: " << n << Endl;
        TVector<TString> nsf = sp.first->GetNamespaceFiles(n);
        TOperationResults opres = sp.first->LastOperationResults();
        for ( auto n : nsf) {
            CTEST << "File: " << n << Endl;
        }
        UNIT_ASSERT(opres.Status == EOperationStatus::Success);
    }
    UNIT_ASSERT(sp.second.Status == EOperationStatus::Success);
    UNIT_ASSERT(ns.size() != 0);
}


Y_UNIT_TEST(TestRecordSizes) {
    size_t s1 = sizeof(TSpillMetaRecord);
    size_t s2 = sizeof(TTimeMarkRecord);
    CTEST << "SpillMetaRecord bytes: " << s1 << Endl;
    CTEST << "TimeMarkRecord bytes: " << s2 << Endl;
    UNIT_ASSERT(s1+8 == s2);
}

Y_UNIT_TEST(TestCreateNamespaces) {

    TFileStorageConfig config;
    config.Path = NFs::CurrentWorkingDirectory();
    std::pair< THolder<ISpillStorage>, TOperationResults > sp = OpenFileStorageForSpilling(config);
    TVector<TString> ns = sp.first->GetNamespaces();
    for ( auto n : ns) {
        CTEST << "Namespace: " << n << Endl;
        TVector<TString> nsf = sp.first->GetNamespaceFiles(n);
        TOperationResults opres = sp.first->LastOperationResults();
        for ( auto n : nsf) {
            CTEST << "File: " << n << Endl;
        }
        UNIT_ASSERT(opres.Status == EOperationStatus::Success);
    }
    THolder<ISpillFile> spf = sp.first->CreateSpillFile(TString("temp1"), TString("ydbspl.1.met"), 4*1024*1024);
    TOperationResults opres = sp.first->LastOperationResults();
    UNIT_ASSERT(opres.Status == EOperationStatus::Success);
    UNIT_ASSERT(spf->IsLocked());

}



}

}
}


