#pragma once
#include <yql/essentials/core/qplayer/storage/interface/yql_qstorage.h>

#include <library/cpp/testing/unittest/registar.h>

void QStorageTestEmptyImpl(const NYql::IQStoragePtr& storage);
void QStorageTestNoCommitImpl(const NYql::IQStoragePtr& storage);
void QStorageTestOneImpl(const NYql::IQStoragePtr& storage);
void QStorageTestManyKeysImpl(const NYql::IQStoragePtr& storage);
void QStorageTestInterleaveReadWriteImpl(const NYql::IQStoragePtr& storage, bool commit);
void QStorageTestLimitWriterItemsImpl(const NYql::IQStoragePtr& storage);
void QStorageTestLimitWriterBytesImpl(const NYql::IQStoragePtr& storage);

#define GENERATE_ONE_TEST(NAME, FACTORY) \
    Y_UNIT_TEST(NAME) { \
        auto storage = FACTORY(); \
        if (storage) { \
            QStorageTest##NAME##Impl(storage); \
        } \
    }

#define GENERATE_ONE_TEST_OPT(NAME, FACTORY, OPT) \
    Y_UNIT_TEST(NAME) { \
        auto storage = FACTORY(); \
        if (storage) { \
            QStorageTest##NAME##Impl(storage, OPT); \
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
