#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/client/unittests/mock/table_value_consumer.h>

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/table_consumer.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/yson/parser.h>

namespace NYT::NTableClient {
namespace {

using ::testing::InSequence;
using ::testing::StrictMock;
using ::testing::NiceMock;

using namespace NYTree;
using namespace NYson;
using namespace NTableClient;
using namespace NFormats;
using namespace NComplexTypes;

////////////////////////////////////////////////////////////////////////////////

struct TEmptyValueConsumer
    : public IValueConsumer
{
    const TNameTablePtr& GetNameTable() const override
    {
        return NameTable;
    }

    const TTableSchemaPtr& GetSchema() const override
    {
        return Schema_;
    }

    bool GetAllowUnknownColumns() const override
    {
        return true;
    }

    void OnBeginRow() override
    { }

    void OnValue(const TUnversionedValue& /*value*/) override
    { }

    void OnEndRow() override
    { }

private:
    const TTableSchemaPtr Schema_ = New<TTableSchema>();
    const TNameTablePtr NameTable = New<TNameTable>();
};

////////////////////////////////////////////////////////////////////////////////

TEST(TTableConsumer, EntityAsNull)
{
    StrictMock<TMockValueConsumer> mock(New<TNameTable>(), true);
    EXPECT_CALL(mock, OnBeginRow());
    EXPECT_CALL(mock, OnMyValue(MakeUnversionedSentinelValue(EValueType::Null, 0)));
    EXPECT_CALL(mock, OnEndRow());

    TYsonConverterConfig config{
        .ComplexTypeMode = EComplexTypeMode::Positional,
    };
    std::unique_ptr<IYsonConsumer> consumer(new TTableConsumer(config, &mock));
    consumer->OnBeginMap();
        consumer->OnKeyedItem("a");
        consumer->OnEntity();
    consumer->OnEndMap();
}

TEST(TTableConsumer, TopLevelAttributes)
{
    StrictMock<TMockValueConsumer> mock(New<TNameTable>(), true);
    EXPECT_CALL(mock, OnBeginRow());

    TYsonConverterConfig config{
        .ComplexTypeMode = EComplexTypeMode::Positional,
    };
    std::unique_ptr<IYsonConsumer> consumer(new TTableConsumer(config, &mock));
    consumer->OnBeginMap();
        consumer->OnKeyedItem("a");
        EXPECT_THROW(consumer->OnBeginAttributes(), std::exception);
}

TEST(TTableConsumer, RowAttributes)
{
    StrictMock<TMockValueConsumer> mock(New<TNameTable>(), true);

    TYsonConverterConfig config{
        .ComplexTypeMode = EComplexTypeMode::Positional,
    };
    std::unique_ptr<IYsonConsumer> consumer(new TTableConsumer(config, &mock));
    consumer->OnBeginAttributes();
    consumer->OnKeyedItem("table_index");
    consumer->OnInt64Scalar(0);
    consumer->OnEndAttributes();
    EXPECT_THROW(consumer->OnBeginMap(), std::exception);
}

TEST(TYsonParserTest, ContextInExceptions_TableConsumer)
{
    try {
        TEmptyValueConsumer emptyValueConsumer;

        TYsonConverterConfig config{
            .ComplexTypeMode = EComplexTypeMode::Positional,
        };
        TTableConsumer consumer(config, &emptyValueConsumer);
        TYsonParser parser(&consumer, EYsonType::ListFragment);
        parser.Read("{foo=bar};");
        parser.Read("{bar=baz};YT_LOG_IN");
        parser.Read("FO something happened");
        parser.Finish();
        GTEST_FAIL() << "Expected exception to be thrown";
    } catch (const std::exception& ex) {
        EXPECT_THAT(ex.what(), testing::HasSubstr("YT_LOG_INFO something happened"));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NVersionedTableClient

