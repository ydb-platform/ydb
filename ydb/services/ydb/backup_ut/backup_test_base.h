#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>

#include <util/generic/maybe.h>

#define YDB_SDK_CLIENT(type, funcName)                               \
    protected:                                                       \
    TMaybe<type> Y_CAT(funcName, Instance);                          \
    public:                                                          \
    type& funcName() {                                               \
        if (!Y_CAT(funcName, Instance)) {                            \
            Y_CAT(funcName, Instance).ConstructInPlace(YdbDriver()); \
        }                                                            \
        return *Y_CAT(funcName, Instance);                           \
    }                                                                \
    /**/

