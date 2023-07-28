#pragma once

#include <yt/yt/core/yson/consumer.h>

#include <library/cpp/testing/gtest/gtest.h>

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

class TMockYsonConsumer
    : public TYsonConsumerBase
{
public:
    MOCK_METHOD(void, OnStringScalar, (TStringBuf value), (override));
    MOCK_METHOD(void, OnInt64Scalar, (i64 value), (override));
    MOCK_METHOD(void, OnUint64Scalar, (ui64 value), (override));
    MOCK_METHOD(void, OnDoubleScalar, (double value), (override));
    MOCK_METHOD(void, OnBooleanScalar, (bool value), (override));
    MOCK_METHOD(void, OnEntity, (), (override));

    MOCK_METHOD(void, OnBeginList, (), (override));
    MOCK_METHOD(void, OnListItem, (), (override));
    MOCK_METHOD(void, OnEndList, (), (override));

    MOCK_METHOD(void, OnBeginMap, (), (override));
    MOCK_METHOD(void, OnKeyedItem, (TStringBuf name), (override));
    MOCK_METHOD(void, OnEndMap, (), (override));

    MOCK_METHOD(void, OnBeginAttributes, (), (override));
    MOCK_METHOD(void, OnEndAttributes, (), (override));
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
