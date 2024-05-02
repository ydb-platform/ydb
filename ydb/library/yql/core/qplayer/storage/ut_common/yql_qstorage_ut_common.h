#pragma once
#include <ydb/library/yql/core/qplayer/storage/interface/yql_qstorage.h>

#include <library/cpp/testing/unittest/registar.h>

void QStorageTestEmpty_Impl(const NYql::IQStoragePtr& storage);
void QStorageTestOne_Impl(const NYql::IQStoragePtr& storage);
void QStorageTestIterateWithoutValue_Impl(const NYql::IQStoragePtr& storage);
void QStorageTestManyKeys_Impl(const NYql::IQStoragePtr& storage);
void QStorageTestInterleaveReadWrite_Impl(const NYql::IQStoragePtr& storage);

#define GENERATE_ONE_TEST(NAME, FACTORY) \
    Y_UNIT_TEST(NAME) { \
        auto storage = FACTORY(); \
        if (storage) { \
            QStorageTest##NAME##_Impl(storage); \
        } \
    }

#define GENERATE_TESTS(FACTORY)\
    GENERATE_ONE_TEST(Empty, FACTORY) \
    GENERATE_ONE_TEST(One, FACTORY) \
    GENERATE_ONE_TEST(IterateWithoutValue, FACTORY) \
    GENERATE_ONE_TEST(ManyKeys, FACTORY) \
    GENERATE_ONE_TEST(InterleaveReadWrite, FACTORY)
