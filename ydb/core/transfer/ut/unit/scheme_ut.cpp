#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/transfer/scheme.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NKikimr::NReplication::NTransfer;
using namespace NKikimr::NSchemeCache;

namespace {

TSysTables::TTableColumnInfo MakeColumn(
    const TString& name,
    ui32 id,
    NScheme::TTypeInfo type,
    i32 keyOrder = -1)
{
    return TSysTables::TTableColumnInfo(name, id, type, {}, keyOrder);
}

TAutoPtr<TSchemeCacheNavigate> MakeNavigate(
    std::initializer_list<TSysTables::TTableColumnInfo> columns,
    const THashSet<TString>& notNull = {})
{
    TAutoPtr<TSchemeCacheNavigate> nav(new TSchemeCacheNavigate());
    auto& entry = nav->ResultSet.emplace_back();
    entry.Path = {"Root", "Table"};
    entry.NotNullColumns = notNull;
    for (const auto& column : columns) {
        entry.Columns[column.Id] = column;
    }
    return nav;
}

} // namespace

Y_UNIT_TEST_SUITE(TransferScheme) {

Y_UNIT_TEST(BuildSchemeOrdersKeyColumnsAndSystemTarget) {
    auto nav = MakeNavigate({
        MakeColumn("Value", 2, NScheme::TTypeInfo(NScheme::NTypeIds::Utf8)),
        MakeColumn("Key", 1, NScheme::TTypeInfo(NScheme::NTypeIds::Uint64), 0),
        MakeColumn("Extra", 3, NScheme::TTypeInfo(NScheme::NTypeIds::Int32)),
    }, {"Key"});

    auto scheme = BuildScheme(nav);

    UNIT_ASSERT_VALUES_EQUAL(scheme->TableColumns.size(), 3u);
    UNIT_ASSERT_VALUES_EQUAL(scheme->TableColumns[0].Name, "Key");
    UNIT_ASSERT(scheme->TableColumns[0].KeyColumn);
    UNIT_ASSERT(!scheme->TableColumns[0].Nullable);
    UNIT_ASSERT_VALUES_EQUAL(scheme->TableColumns[1].Name, "Value");
    UNIT_ASSERT(!scheme->TableColumns[1].KeyColumn);
    UNIT_ASSERT(scheme->TableColumns[1].Nullable);

    UNIT_ASSERT_VALUES_EQUAL(scheme->StructMetadata.size(), 4u);
    UNIT_ASSERT_VALUES_EQUAL(scheme->ColumnsMetadata.size(), 3u);
    UNIT_ASSERT_VALUES_EQUAL(scheme->WriteIndex.size(), 3u);
    UNIT_ASSERT_VALUES_EQUAL(scheme->ReadIndex.size(), 3u);
    UNIT_ASSERT_VALUES_EQUAL(scheme->Types->size(), 3u);

    UNIT_ASSERT_VALUES_EQUAL(scheme->StructMetadata[scheme->TargetTableIndex].GetName(), SystemColumns::TargetTable);
    UNIT_ASSERT(!scheme->StructMetadata[scheme->TargetTableIndex].GetNotNull());
}

Y_UNIT_TEST(BuildSchemeCompositeKeyKeepsKeyOrder) {
    auto nav = MakeNavigate({
        MakeColumn("K2", 2, NScheme::TTypeInfo(NScheme::NTypeIds::Utf8), 1),
        MakeColumn("Val", 3, NScheme::TTypeInfo(NScheme::NTypeIds::Int32)),
        MakeColumn("K1", 1, NScheme::TTypeInfo(NScheme::NTypeIds::Uint64), 0),
    }, {"K1", "K2"});

    auto scheme = BuildScheme(nav);
    UNIT_ASSERT_VALUES_EQUAL(scheme->TableColumns.size(), 3u);
    UNIT_ASSERT_VALUES_EQUAL(scheme->TableColumns[0].Name, "K1");
    UNIT_ASSERT_VALUES_EQUAL(scheme->TableColumns[1].Name, "K2");
    UNIT_ASSERT(scheme->TableColumns[0].KeyColumn);
    UNIT_ASSERT(scheme->TableColumns[1].KeyColumn);
    UNIT_ASSERT(!scheme->TableColumns[2].KeyColumn);
}

Y_UNIT_TEST(BuildSchemeDecimalColumn) {
    auto nav = MakeNavigate({
        MakeColumn("Key", 1, NScheme::TTypeInfo(NScheme::NTypeIds::Uint64), 0),
        MakeColumn("Amount", 2, NScheme::TTypeInfo(NScheme::TDecimalType(22, 9))),
    }, {"Key", "Amount"});

    auto scheme = BuildScheme(nav);

    UNIT_ASSERT_VALUES_EQUAL(scheme->TableColumns.size(), 2u);
    UNIT_ASSERT_VALUES_EQUAL(scheme->TableColumns[1].Name, "Amount");
    UNIT_ASSERT_VALUES_EQUAL(scheme->TableColumns[1].PType.GetDecimalType().GetPrecision(), 22u);
    UNIT_ASSERT_VALUES_EQUAL(scheme->TableColumns[1].PType.GetDecimalType().GetScale(), 9u);

    UNIT_ASSERT_VALUES_EQUAL(scheme->Types->size(), 2u);
    const auto& amountType = (*scheme->Types)[0].first == "Amount" ? (*scheme->Types)[0].second : (*scheme->Types)[1].second;
    UNIT_ASSERT(amountType.has_decimal_type());
    UNIT_ASSERT_VALUES_EQUAL(amountType.decimal_type().precision(), 22);
    UNIT_ASSERT_VALUES_EQUAL(amountType.decimal_type().scale(), 9);

    bool foundAmountMeta = false;
    for (const auto& meta : scheme->StructMetadata) {
        if (meta.GetName() == "Amount") {
            foundAmountMeta = true;
            UNIT_ASSERT(meta.HasTypeInfo());
            UNIT_ASSERT(meta.GetNotNull());
        }
    }
    UNIT_ASSERT(foundAmountMeta);
}

Y_UNIT_TEST(MakeOutputSchemaWrapsNullableAndNotNullFields) {
    TVector<TSchemeColumn> columns = {
        {.Name = "Key", .Id = 1, .PType = NScheme::TTypeInfo(NScheme::NTypeIds::Uint64), .KeyColumn = true, .Nullable = false},
        {.Name = "Message", .Id = 2, .PType = NScheme::TTypeInfo(NScheme::NTypeIds::Utf8), .KeyColumn = false, .Nullable = true},
        {.Name = "Amount", .Id = 3, .PType = NScheme::TTypeInfo(NScheme::TDecimalType(35, 10)), .KeyColumn = false, .Nullable = true},
    };

    const auto node = MakeOutputSchema(columns);
    UNIT_ASSERT_VALUES_EQUAL(node.AsList()[0].AsString(), "StructType");

    const auto& rootMembers = node.AsList()[1].AsList();
    UNIT_ASSERT_VALUES_EQUAL(rootMembers.size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(rootMembers[0].AsList()[0].AsString(), SystemColumns::Root);

    const auto& structMembers = rootMembers[0].AsList()[1].AsList()[1].AsList();
    UNIT_ASSERT_VALUES_EQUAL(structMembers.size(), 4u);
    UNIT_ASSERT_VALUES_EQUAL(structMembers[0].AsList()[0].AsString(), SystemColumns::TargetTable);
    UNIT_ASSERT_VALUES_EQUAL(structMembers[0].AsList()[1].AsList()[0].AsString(), "OptionalType");
    UNIT_ASSERT_VALUES_EQUAL(structMembers[1].AsList()[0].AsString(), "Key");
    UNIT_ASSERT_VALUES_EQUAL(structMembers[1].AsList()[1].AsList()[0].AsString(), "DataType");
    UNIT_ASSERT_VALUES_EQUAL(structMembers[2].AsList()[0].AsString(), "Message");
    UNIT_ASSERT_VALUES_EQUAL(structMembers[2].AsList()[1].AsList()[0].AsString(), "OptionalType");
    UNIT_ASSERT_VALUES_EQUAL(structMembers[3].AsList()[0].AsString(), "Amount");
    UNIT_ASSERT_VALUES_EQUAL(structMembers[3].AsList()[1].AsList()[1].AsList()[1].AsString(), "Decimal");
    UNIT_ASSERT_VALUES_EQUAL(structMembers[3].AsList()[1].AsList()[1].AsList()[2].AsString(), "35");
    UNIT_ASSERT_VALUES_EQUAL(structMembers[3].AsList()[1].AsList()[1].AsList()[3].AsString(), "10");
}

} // Y_UNIT_TEST_SUITE(TransferScheme)
