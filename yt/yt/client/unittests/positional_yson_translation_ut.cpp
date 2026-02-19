#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/client/complex_types/positional_yson_translation.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/library/logical_type_shortcuts/logical_type_shortcuts.h>

#include <yt/yt/core/yson/writer.h>

namespace NYT::NComplexTypes {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NTableClient::NLogicalTypeShortcuts;
using namespace NTableClient;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TComplexTypeFieldDescriptor CreateFieldDescriptor(TLogicalTypePtr type)
{
    return TComplexTypeFieldDescriptor("<test>", std::move(type));
}

TString ParseUnversionedValue(TUnversionedValue value)
{
    TString result;

    TStringOutput out(result);
    TYsonWriter writer(&out, EYsonFormat::Text, EYsonType::Node);
    UnversionedValueToYson(value, &writer);
    writer.Flush();

    return result;
}

////////////////////////////////////////////////////////////////////////////////

TEST(TStructTranslationTest, Basic)
{
    auto sourceDescriptor = CreateFieldDescriptor(StructLogicalType(
        {
            {"name", "name", String()},
            {"age", "age", Int8()},
            {"login", "login", String()},
            {"password", "password", String()},
        },
        /*removedFieldStableNames*/ {}));

    auto targetDescriptor = CreateFieldDescriptor(StructLogicalType(
        {
            {"staff_login", "login", String()},
            {"name_ru", "name", String()},
            {"is_dismissed", "is_dismissed", Optional(Bool())},
            {"age", "age", Optional(Int16())},
            {"password_hash", "password_hash", Optional(String())},
        },
        /*removedFieldStableNames*/ {"password"}));

    auto translator = CreatePositionalYsonTranslator(sourceDescriptor, targetDescriptor);

    auto source = MakeUnversionedCompositeValue(R"(["sergey";23;"s-berdnikov";"qwerty";])");
    auto translated = translator(source);

    EXPECT_EQ(translated.Type, EValueType::Composite);
    EXPECT_EQ(ParseUnversionedValue(translated), R"(["s-berdnikov";"sergey";#;23;#;])");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NComplexTypes
