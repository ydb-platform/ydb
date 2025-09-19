#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/json/config.h>
#include <yt/yt/core/json/json_writer.h>

#include <yt/yt/core/yson/consumer.h>

#include <yt/yt/core/ytree/fluent.h>

#include <util/string/strip.h>

namespace NYT::NJson {
namespace {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TEST(TWebJsonConsumerTest, OneString)
{
    TStringStream outputStream;
    auto consumer = CreateWebJsonConsumer(&outputStream);

    consumer->OnStringScalar("hello world");
    consumer->Flush();

    EXPECT_EQ("\"hello world\"", outputStream.Str());
}

TEST(TWebJsonConsumerTest, List)
{
    TStringStream outputStream;
    auto consumer = CreateWebJsonConsumer(&outputStream);

    consumer->OnBeginList();
    consumer->OnListItem();
    consumer->OnInt64Scalar(100);
    consumer->OnListItem();
    consumer->OnStringScalar("foo");
    consumer->OnEndList();
    consumer->Flush();

    TString output = "[100,\"foo\"]";
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TWebJsonConsumerTest, UnsafeSignedIntegers)
{
    TStringStream outputStream;
    auto consumer = CreateWebJsonConsumer(&outputStream);

    const i64 MaxSafeInteger = ((i64)1 << 53) - 1;

    consumer->OnBeginList();
    consumer->OnListItem();
    consumer->OnInt64Scalar(MaxSafeInteger);
    consumer->OnListItem();
    consumer->OnInt64Scalar(MaxSafeInteger + 1);
    consumer->OnListItem();
    consumer->OnInt64Scalar(-MaxSafeInteger);
    consumer->OnListItem();
    consumer->OnInt64Scalar(-MaxSafeInteger - 1);
    consumer->OnEndList();
    consumer->Flush();

    TString output = "[9007199254740991,"
                     "{\"$type\":\"int64\",\"$value\":\"9007199254740992\"},"
                     "-9007199254740991,"
                     "{\"$type\":\"int64\",\"$value\":\"-9007199254740992\"}]";
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TWebJsonConsumerTest, UnsafeUnsignedIntegers)
{
    TStringStream outputStream;
    auto consumer = CreateWebJsonConsumer(&outputStream);

    const ui64 MaxSafeInteger = ((ui64)1 << 53) - 1;

    consumer->OnBeginList();
    consumer->OnListItem();
    consumer->OnUint64Scalar(MaxSafeInteger);
    consumer->OnListItem();
    consumer->OnUint64Scalar(MaxSafeInteger + 1);
    consumer->OnEndList();
    consumer->Flush();

    TString output = "[9007199254740991,"
                     "{\"$type\":\"uint64\",\"$value\":\"9007199254740992\"}]";
    EXPECT_EQ(output, outputStream.Str());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NJson
