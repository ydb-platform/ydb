#include <ydb/core/sys_view/show_create/formatters/create_table_formatter.h>

#include <ydb/core/scheme/scheme_type_id.h>
#include <ydb/library/testlib/helpers.h>
#include <ydb/public/lib/ydb_cli/dump/util/query_utils.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/builder.h>

namespace NKikimr::NSysView {
namespace {

using namespace NKikimrSchemeOp;

TTableDescription MakeTableDescription() {
    TTableDescription table;
    auto* key = table.AddColumns();
    key->SetId(1);
    key->SetName("key");
    key->SetType("Uint64");
    key->SetTypeId(NScheme::NTypeIds::Uint64);
    auto* value = table.AddColumns();
    value->SetId(2);
    value->SetName("value");
    value->SetType("String");
    value->SetTypeId(NScheme::NTypeIds::String);
    auto* cover = table.AddColumns();
    cover->SetId(3);
    cover->SetName("cover");
    cover->SetType("Uint32");
    cover->SetTypeId(NScheme::NTypeIds::Uint32);
    table.AddKeyColumnIds(1);
    table.AddKeyColumnNames("key");
    return table;
}

TString ExtractQuery(TFormatResult result) {
    UNIT_ASSERT_C(result.IsSuccess(), result.GetError());
    return result.ExtractOut();
}

void AssertQueryEqual(TFormatResult result, const TString& expected) {
    const auto query = ExtractQuery(std::move(result));
    TString formattedExpected;
    NYql::TIssues issues;
    UNIT_ASSERT_C(NYdb::NDump::Format(expected, formattedExpected, issues), issues.ToString());
    UNIT_ASSERT_STRINGS_EQUAL(query, formattedExpected);
}

} // namespace

Y_UNIT_TEST_SUITE(CreateTableFormatter) {
    Y_UNIT_TEST_TWIN(Float8VectorIndex, E5M2) {
        auto table = MakeTableDescription();
        auto& index = *table.AddTableIndexes();
        index.SetName("idx");
        index.SetType(EIndexTypeGlobalVectorKmeansTree);
        index.AddKeyColumnNames("value");
        index.AddIndexImplTableDescriptions();
        index.AddIndexImplTableDescriptions();
        auto* treeSettings = index.MutableVectorIndexKmeansTreeDescription()->MutableSettings();
        treeSettings->mutable_settings()->set_metric(Ydb::Table::VectorIndexSettings::DISTANCE_COSINE);
        treeSettings->mutable_settings()->set_vector_dimension(8);
        treeSettings->set_clusters(4);
        treeSettings->set_levels(2);
        auto* settings = index.MutableVectorIndexKmeansTreeDescription()->MutableSettings()->mutable_settings();
        settings->set_vector_type(E5M2
            ? Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT8_E5M2
            : Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT8_E4M3);
        const TString type = E5M2 ? "float8_e5m2" : "float8_e4m3";
        const TString indexQuery = TStringBuilder()
            << "INDEX `idx` GLOBAL USING vector_kmeans_tree ON (`value`) "
            << "WITH (distance = cosine, vector_type = '" << type << "', vector_dimension = 8, clusters = 4, levels = 2)";
        const TString columns = "`key` Uint64, `value` String, `cover` Uint32";
        TCreateTableFormatter formatter;
        AssertQueryEqual(formatter.Format("table", "/Root/table", table, false, {}, {}),
            TStringBuilder() << "CREATE TABLE `table` (" << columns << ", " << indexQuery << ", PRIMARY KEY (`key`));");
    }
}

} // namespace NKikimr::NSysView
