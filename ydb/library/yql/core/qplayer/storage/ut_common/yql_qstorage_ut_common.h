#pragma once
#include <ydb/library/yql/core/qplayer/storage/interface/yql_qstorage.h>

#include <library/cpp/testing/unittest/registar.h>

void QStorageTestEmpty_Impl(const NYql::IQStoragePtr& storage);
void QStorageTestNoCommit_Impl(const NYql::IQStoragePtr& storage);
void QStorageTestOne_Impl(const NYql::IQStoragePtr& storage);
void QStorageTestManyKeys_Impl(const NYql::IQStoragePtr& storage);
void QStorageTestInterleaveReadWrite_Impl(const NYql::IQStoragePtr& storage, bool commit);
void QStorageTestLimitWriterItems_Impl(const NYql::IQStoragePtr& storage);
void QStorageTestLimitWriterBytes_Impl(const NYql::IQStoragePtr& storage);

#define GENERATE_ONE_TEST(NAME, FACTORY) \
    Y_UNIT_TEST(NAME) { \
        auto storage = FACTORY(); \
        if (storage) { \
            QStorageTest##NAME##_Impl(storage); \
        } \
    }

#define GENERATE_ONE_TEST_OPT(NAME, FACTORY, OPT) \
    Y_UNIT_TEST(NAME) { \
        auto storage = FACTORY(); \
        if (storage) { \
            QStorageTest##NAME##_Impl(storage, OPT); \
        } \
    }    

#define GENERATE_TESTS(FACTORY, commit)\
    GENERATE_ONE_TEST(Empty, FACTORY) \
    GENERATE_ONE_TEST(NoCommit, FACTORY) \
    GENERATE_ONE_TEST(One, FACTORY) \
    GENERATE_ONE_TEST(ManyKeys, FACTORY) \
    GENERATE_ONE_TEST_OPT(InterleaveReadWrite, FACTORY, commit) \
    GENERATE_ONE_TEST(LimitWriterItems, FACTORY) \
    GENERATE_ONE_TEST(LimitWriterBytes, FACTORY)
