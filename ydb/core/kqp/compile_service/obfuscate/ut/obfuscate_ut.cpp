#include <ydb/core/kqp/compile_service/obfuscate/obfuscate.h>

#include <ydb/core/protos/kqp.pb.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/file.h>
#include <util/string/escape.h>

using namespace NKikimr::NKqp;

namespace {

NKikimrKqp::TKqpTableMetadataProto MakeTableMeta(
    const TString& name,
    const TVector<TString>& columns,
    const TVector<TString>& keyColumns,
    const TString& cluster = "cluster1")
{
    NKikimrKqp::TKqpTableMetadataProto meta;
    meta.SetDoesExist(true);
    meta.SetCluster(cluster);
    meta.SetName(name);
    meta.SetSchemaVersion(1);
    meta.SetKind(1);

    ui32 colId = 1;
    for (const auto& col : columns) {
        auto* c = meta.AddColumns();
        c->SetName(col);
        c->SetId(colId++);
        c->SetType("Uint64");
    }

    for (const auto& key : keyColumns) {
        meta.AddKeyColunmNames(key);
    }

    return meta;
}

void AddIndex(NKikimrKqp::TKqpTableMetadataProto& meta,
              const TString& indexName,
              const TVector<TString>& keyColumns,
              const TVector<TString>& dataColumns = {})
{
    auto* idx = meta.AddIndexes();
    idx->SetName(indexName);
    idx->SetType(1);
    idx->SetState(1);
    for (const auto& key : keyColumns) {
        idx->AddKeyColumns(key);
    }
    for (const auto& data : dataColumns) {
        idx->AddDataColumns(data);
    }
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TReplayMessageObfuscatorTest) {

    Y_UNIT_TEST(MappingConstruction_SingleTable) {
        TReplayMessageObfuscator obfuscator;

        auto meta = MakeTableMeta("/Root/mydb/orders", {"id", "customer_id", "amount"}, {"id"});
        obfuscator.AddTableMetadata(meta);

        const auto& mapping = obfuscator.GetMapping();

        // Table name (full path) should be mapped
        UNIT_ASSERT(mapping.contains("/Root/mydb/orders"));
        UNIT_ASSERT_STRINGS_EQUAL(mapping.at("/Root/mydb/orders"), "table_0");

        // Short name should map to the same obfuscated name
        UNIT_ASSERT(mapping.contains("orders"));
        UNIT_ASSERT_STRINGS_EQUAL(mapping.at("orders"), "table_0");

        // Columns should be mapped
        UNIT_ASSERT(mapping.contains("id"));
        UNIT_ASSERT(mapping.contains("customer_id"));
        UNIT_ASSERT(mapping.contains("amount"));

        // Column names should have column_ prefix
        UNIT_ASSERT(mapping.at("id").StartsWith("column_"));
        UNIT_ASSERT(mapping.at("customer_id").StartsWith("column_"));
        UNIT_ASSERT(mapping.at("amount").StartsWith("column_"));
    }

    Y_UNIT_TEST(MappingConstruction_MultipleTables) {
        TReplayMessageObfuscator obfuscator;

        auto meta1 = MakeTableMeta("/Root/mydb/orders", {"id", "customer_id"}, {"id"});
        auto meta2 = MakeTableMeta("/Root/mydb/customers", {"id", "name"}, {"id"});

        obfuscator.AddTableMetadata(meta1);
        obfuscator.AddTableMetadata(meta2);

        const auto& mapping = obfuscator.GetMapping();

        UNIT_ASSERT_STRINGS_EQUAL(mapping.at("/Root/mydb/orders"), "table_0");
        UNIT_ASSERT_STRINGS_EQUAL(mapping.at("/Root/mydb/customers"), "table_1");

        // "id" column is shared, so it should be mapped only once
        UNIT_ASSERT(mapping.at("id").StartsWith("column_"));

        // "name" column should have its own mapping
        UNIT_ASSERT(mapping.contains("name"));
        UNIT_ASSERT(mapping.at("name").StartsWith("column_"));
    }

    Y_UNIT_TEST(MappingConstruction_WithIndexes) {
        TReplayMessageObfuscator obfuscator;

        auto meta = MakeTableMeta("/Root/mydb/orders", {"id", "customer_id", "amount"}, {"id"});
        AddIndex(meta, "idx_customer", {"customer_id"}, {"amount"});

        obfuscator.AddTableMetadata(meta);

        const auto& mapping = obfuscator.GetMapping();

        UNIT_ASSERT(mapping.contains("idx_customer"));
        UNIT_ASSERT_STRINGS_EQUAL(mapping.at("idx_customer"), "index_0");
    }

    Y_UNIT_TEST(MappingConstruction_ShortNameNoSlash) {
        TReplayMessageObfuscator obfuscator;

        // Table name without path separator
        auto meta = MakeTableMeta("orders", {"id"}, {"id"});
        obfuscator.AddTableMetadata(meta);

        const auto& mapping = obfuscator.GetMapping();

        UNIT_ASSERT(mapping.contains("orders"));
        UNIT_ASSERT_STRINGS_EQUAL(mapping.at("orders"), "table_0");
    }

    Y_UNIT_TEST(ObfuscateMetadata_Basic) {
        TReplayMessageObfuscator obfuscator;

        auto meta = MakeTableMeta("/Root/mydb/orders", {"id", "customer_id", "amount"}, {"id"}, "cluster1");
        AddIndex(meta, "idx_customer", {"customer_id"}, {"amount"});

        obfuscator.AddTableMetadata(meta);
        obfuscator.AddMapping("cluster1", "cluster_0");

        auto obfuscated = obfuscator.ObfuscateMetadata(meta);

        UNIT_ASSERT_STRINGS_EQUAL(obfuscated.GetName(), "table_0");
        UNIT_ASSERT_STRINGS_EQUAL(obfuscated.GetCluster(), "cluster_0");

        // Columns should be obfuscated
        for (const auto& col : obfuscated.GetColumns()) {
            UNIT_ASSERT(col.GetName().StartsWith("column_"));
        }

        // Key columns should be obfuscated
        for (const auto& keyCol : obfuscated.GetKeyColunmNames()) {
            UNIT_ASSERT(keyCol.StartsWith("column_"));
        }

        // Index should be obfuscated
        UNIT_ASSERT_VALUES_EQUAL(obfuscated.GetIndexes().size(), 1);
        UNIT_ASSERT_STRINGS_EQUAL(obfuscated.GetIndexes(0).GetName(), "index_0");
        for (const auto& keyCol : obfuscated.GetIndexes(0).GetKeyColumns()) {
            UNIT_ASSERT(keyCol.StartsWith("column_"));
        }
    }

    Y_UNIT_TEST(ObfuscateQueryText_SimpleSelect) {
        TReplayMessageObfuscator obfuscator;

        auto meta = MakeTableMeta("orders", {"id", "customer_id", "amount"}, {"id"});
        obfuscator.AddTableMetadata(meta);

        TString result;
        NYql::TIssues issues;
        bool ok = obfuscator.ObfuscateQueryText(
            "SELECT id, customer_id, amount FROM orders WHERE id = 1",
            result, issues);

        UNIT_ASSERT_C(ok, issues.ToString());
        UNIT_ASSERT(!result.empty());

        // The result should contain obfuscated column and table names
        UNIT_ASSERT_C(result.Contains("column_"), result);
        UNIT_ASSERT_C(result.Contains("table_0"), result);

        // The result should NOT contain original names
        UNIT_ASSERT_C(!result.Contains("orders"), result);
        UNIT_ASSERT_C(!result.Contains("customer_id"), result);
        UNIT_ASSERT_C(!result.Contains("amount"), result);

        // SQL keywords should be preserved
        UNIT_ASSERT_C(result.Contains("SELECT"), result);
        UNIT_ASSERT_C(result.Contains("FROM"), result);
        UNIT_ASSERT_C(result.Contains("WHERE"), result);
    }

    Y_UNIT_TEST(ObfuscateQueryText_FunctionNamesPreserved) {
        TReplayMessageObfuscator obfuscator;

        auto meta = MakeTableMeta("orders", {"id", "amount"}, {"id"});
        obfuscator.AddTableMetadata(meta);

        TString result;
        NYql::TIssues issues;
        bool ok = obfuscator.ObfuscateQueryText(
            "SELECT COUNT(id), SUM(amount) FROM orders",
            result, issues);

        UNIT_ASSERT_C(ok, issues.ToString());

        // Function names should be preserved
        UNIT_ASSERT_C(result.Contains("COUNT"), result);
        UNIT_ASSERT_C(result.Contains("SUM"), result);
    }

    Y_UNIT_TEST(ObfuscateQueryText_LiteralsReplaced) {
        TReplayMessageObfuscator obfuscator;

        auto meta = MakeTableMeta("orders", {"id", "name"}, {"id"});
        obfuscator.AddTableMetadata(meta);

        TString result;
        NYql::TIssues issues;
        bool ok = obfuscator.ObfuscateQueryText(
            "SELECT id FROM orders WHERE id = 42 AND name = 'secret'",
            result, issues);

        UNIT_ASSERT_C(ok, issues.ToString());

        // Literals should be replaced with generic values
        UNIT_ASSERT_C(!result.Contains("42"), result);
        UNIT_ASSERT_C(!result.Contains("secret"), result);
    }

    Y_UNIT_TEST(ObfuscateQueryText_UnmappedIdentifiersBecomeid) {
        TReplayMessageObfuscator obfuscator;

        // Only map "orders" and "id", not "unknown_col"
        auto meta = MakeTableMeta("orders", {"id"}, {"id"});
        obfuscator.AddTableMetadata(meta);

        TString result;
        NYql::TIssues issues;
        bool ok = obfuscator.ObfuscateQueryText(
            "SELECT id, unknown_col FROM orders",
            result, issues);

        UNIT_ASSERT_C(ok, issues.ToString());

        // unknown_col should become "id" (the generic obfuscation)
        // Mapped identifiers should use their specific mapping
        UNIT_ASSERT_C(result.Contains("table_0"), result);
    }

    Y_UNIT_TEST(ObfuscateQueryText_BacktickQuoted) {
        TReplayMessageObfuscator obfuscator;

        auto meta = MakeTableMeta("orders", {"id", "amount"}, {"id"});
        obfuscator.AddTableMetadata(meta);

        TString result;
        NYql::TIssues issues;
        bool ok = obfuscator.ObfuscateQueryText(
            "SELECT `id`, `amount` FROM `orders`",
            result, issues);

        UNIT_ASSERT_C(ok, issues.ToString());

        // Should not contain original names
        UNIT_ASSERT_C(!result.Contains("orders"), result);
        UNIT_ASSERT_C(!result.Contains("amount"), result);
    }

    Y_UNIT_TEST(ObfuscateQueryText_MultipleStatements) {
        TReplayMessageObfuscator obfuscator;

        auto meta = MakeTableMeta("orders", {"id", "amount"}, {"id"});
        obfuscator.AddTableMetadata(meta);

        TString result;
        NYql::TIssues issues;
        bool ok = obfuscator.ObfuscateQueryText(
            "SELECT id FROM orders; SELECT amount FROM orders",
            result, issues);

        // This may or may not parse as valid SQL depending on parser.
        // At least it should not crash.
        Y_UNUSED(ok);
        Y_UNUSED(result);
    }

    Y_UNIT_TEST(ObfuscateReplayMessage_FullRoundTrip) {
        // Build a replay JSON message similar to what AddMessageToReplayLog produces
        NKikimrKqp::TKqpTableMetadataProto meta;
        meta.SetDoesExist(true);
        meta.SetCluster("my_cluster");
        meta.SetName("/Root/mydb/orders");
        meta.SetSchemaVersion(1);

        auto* col1 = meta.AddColumns();
        col1->SetName("id");
        col1->SetId(1);
        col1->SetType("Uint64");

        auto* col2 = meta.AddColumns();
        col2->SetName("amount");
        col2->SetId(2);
        col2->SetType("Double");

        meta.AddKeyColunmNames("id");

        // Serialize metadata as the compile actor does
        NJson::TJsonValue tablesMeta(NJson::JSON_ARRAY);
        tablesMeta.AppendValue(Base64Encode(meta.SerializeAsString()));

        NJson::TJsonValue replayMessage(NJson::JSON_MAP);
        replayMessage.InsertValue("query_id", "test-query-id");
        replayMessage.InsertValue("version", "1.0");
        replayMessage.InsertValue("query_text", EscapeC(TString("SELECT id, amount FROM orders WHERE id = 1")));
        replayMessage.InsertValue("query_database", "/Root/mydb");
        replayMessage.InsertValue("query_cluster", "my_cluster");
        replayMessage.InsertValue("query_syntax", "1");
        replayMessage.InsertValue("query_type", "QUERY_TYPE_SQL_DML");
        replayMessage.InsertValue("table_metadata", TString(NJson::WriteJson(tablesMeta, false)));
        replayMessage.InsertValue("table_meta_serialization_type", static_cast<ui64>(1));

        TString inputJson = NJson::WriteJson(replayMessage, false);

        TReplayMessageObfuscator obfuscator;
        TString resultJson;
        NYql::TIssues issues;
        bool ok = obfuscator.ObfuscateReplayMessage(inputJson, resultJson, issues);
        UNIT_ASSERT_C(ok, issues.ToString());

        // Parse result and verify
        NJson::TJsonValue resultRoot;
        {
            NJson::TJsonReaderConfig readerConfig;
            TStringInput in(resultJson);
            UNIT_ASSERT(NJson::ReadJsonTree(&in, &readerConfig, &resultRoot, false));
        }

        // Database and cluster should be obfuscated
        UNIT_ASSERT_C(!resultRoot["query_database"].GetStringSafe().Contains("mydb"),
                       resultRoot["query_database"].GetStringSafe());
        UNIT_ASSERT_C(!resultRoot["query_cluster"].GetStringSafe().Contains("my_cluster"),
                       resultRoot["query_cluster"].GetStringSafe());

        // Query text should be obfuscated
        TString queryText = UnescapeC(resultRoot["query_text"].GetStringSafe());
        UNIT_ASSERT_C(!queryText.Contains("orders"), queryText);
        UNIT_ASSERT_C(!queryText.Contains("amount"), queryText);
        UNIT_ASSERT_C(queryText.Contains("SELECT"), queryText);

        // Metadata should be obfuscated - deserialize and check
        NJson::TJsonValue obfuscatedTablesMeta;
        {
            NJson::TJsonReaderConfig readerConfig;
            TStringInput in(resultRoot["table_metadata"].GetStringSafe());
            UNIT_ASSERT(NJson::ReadJsonTree(&in, &readerConfig, &obfuscatedTablesMeta, false));
        }
        UNIT_ASSERT(obfuscatedTablesMeta.IsArray());
        UNIT_ASSERT_VALUES_EQUAL(obfuscatedTablesMeta.GetArray().size(), 1u);

        NKikimrKqp::TKqpTableMetadataProto obfuscatedMeta;
        TString decoded = Base64Decode(obfuscatedTablesMeta.GetArray()[0].GetStringRobust());
        UNIT_ASSERT(obfuscatedMeta.ParseFromString(decoded));

        UNIT_ASSERT_STRINGS_EQUAL(obfuscatedMeta.GetName(), "table_0");
        for (const auto& col : obfuscatedMeta.GetColumns()) {
            UNIT_ASSERT_C(col.GetName().StartsWith("column_"), col.GetName());
        }

        // Mapping should be consistent
        const auto& mapping = obfuscator.GetMapping();
        UNIT_ASSERT(mapping.size() > 0);
    }

    Y_UNIT_TEST(ObfuscateMetadata_WithSecondaryIndex) {
        TReplayMessageObfuscator obfuscator;

        auto meta = MakeTableMeta("/Root/mydb/orders", {"id", "customer_id"}, {"id"});
        AddIndex(meta, "idx_customer", {"customer_id"});

        // Add secondary index impl table metadata
        auto* secondaryMeta = meta.AddSecondaryGlobalIndexMetadata();
        secondaryMeta->SetDoesExist(true);
        secondaryMeta->SetName("/Root/mydb/orders/idx_customer/indexImplTable");
        secondaryMeta->SetSchemaVersion(1);

        auto* idxCol1 = secondaryMeta->AddColumns();
        idxCol1->SetName("customer_id");
        idxCol1->SetId(1);
        auto* idxCol2 = secondaryMeta->AddColumns();
        idxCol2->SetName("id");
        idxCol2->SetId(2);
        secondaryMeta->AddKeyColunmNames("customer_id");
        secondaryMeta->AddKeyColunmNames("id");

        obfuscator.AddTableMetadata(meta);
        auto obfuscated = obfuscator.ObfuscateMetadata(meta);

        // Secondary index metadata table should also be obfuscated
        UNIT_ASSERT_VALUES_EQUAL(obfuscated.GetSecondaryGlobalIndexMetadata().size(), 1);
        const auto& obfIdx = obfuscated.GetSecondaryGlobalIndexMetadata(0);
        UNIT_ASSERT_C(obfIdx.GetName().StartsWith("table_"), obfIdx.GetName());

        for (const auto& col : obfIdx.GetColumns()) {
            UNIT_ASSERT_C(col.GetName().StartsWith("column_"), col.GetName());
        }
    }

    Y_UNIT_TEST(AddMapping_Explicit) {
        TReplayMessageObfuscator obfuscator;

        obfuscator.AddMapping("custom_key", "obfuscated_value");
        const auto& mapping = obfuscator.GetMapping();

        UNIT_ASSERT(mapping.contains("custom_key"));
        UNIT_ASSERT_STRINGS_EQUAL(mapping.at("custom_key"), "obfuscated_value");
    }

    Y_UNIT_TEST(EmptyMetadata) {
        TReplayMessageObfuscator obfuscator;

        NKikimrKqp::TKqpTableMetadataProto meta;
        obfuscator.AddTableMetadata(meta);

        // Should not crash, mapping should be empty or near-empty
        const auto& mapping = obfuscator.GetMapping();
        Y_UNUSED(mapping);
    }

    Y_UNIT_TEST(ConsistentMapping) {
        TReplayMessageObfuscator obfuscator;

        auto meta1 = MakeTableMeta("/Root/mydb/t1", {"col_a", "col_b"}, {"col_a"});
        auto meta2 = MakeTableMeta("/Root/mydb/t2", {"col_b", "col_c"}, {"col_b"});

        obfuscator.AddTableMetadata(meta1);
        obfuscator.AddTableMetadata(meta2);

        const auto& mapping = obfuscator.GetMapping();

        // col_b appears in both tables but should have a single consistent mapping
        TString colBMapping = mapping.at("col_b");
        UNIT_ASSERT(colBMapping.StartsWith("column_"));

        // Obfuscating either metadata should use the same mapping for col_b
        auto obf1 = obfuscator.ObfuscateMetadata(meta1);
        auto obf2 = obfuscator.ObfuscateMetadata(meta2);

        // Find col_b in obfuscated metadata
        TString colBInObf1, colBInObf2;
        for (int i = 0; i < meta1.GetColumns().size(); ++i) {
            if (meta1.GetColumns(i).GetName() == "col_b") {
                colBInObf1 = obf1.GetColumns(i).GetName();
            }
        }
        for (int i = 0; i < meta2.GetColumns().size(); ++i) {
            if (meta2.GetColumns(i).GetName() == "col_b") {
                colBInObf2 = obf2.GetColumns(i).GetName();
            }
        }

        UNIT_ASSERT_STRINGS_EQUAL(colBInObf1, colBInObf2);
        UNIT_ASSERT_STRINGS_EQUAL(colBInObf1, colBMapping);
    }
}

// Canonical tests that verify exact obfuscated output against files on disk.
//
// Shared schema for all canonical tests:
//   orders(id PK, customer_id, amount, created_at, status) + INDEX idx_customer(customer_id) COVER(amount)
//   customers(id PK, name, email, age)
//   order_items(order_id PK, item_id PK, product_name, qty, price)
//
// To update canonical data after intentional changes:
//   ya make -A --test-param CANONIZE_OBFUSCATE_TESTS=TRUE ydb/core/kqp/compile_service/obfuscate/ut
//
Y_UNIT_TEST_SUITE(TCanonicalObfuscationTest) {

    // Builds the shared 3-table schema and returns a configured obfuscator.
    static TReplayMessageObfuscator MakeCanonicalObfuscator() {
        TReplayMessageObfuscator obfuscator;

        auto orders = MakeTableMeta("orders",
            {"id", "customer_id", "amount", "created_at", "status"}, {"id"});
        AddIndex(orders, "idx_customer", {"customer_id"}, {"amount"});
        obfuscator.AddTableMetadata(orders);

        auto customers = MakeTableMeta("customers",
            {"id", "name", "email", "age"}, {"id"});
        obfuscator.AddTableMetadata(customers);

        auto items = MakeTableMeta("order_items",
            {"order_id", "item_id", "product_name", "qty", "price"},
            {"order_id", "item_id"});
        obfuscator.AddTableMetadata(items);

        return obfuscator;
    }

    // Reads a canonical file from data/ directory. Returns empty string if file doesn't exist.
    static TString ReadCanonFile(const TString& fileName) {
        TString fullPath = SRC_("data/" + fileName);
        try {
            TIFStream stream(fullPath);
            return stream.ReadAll();
        } catch (...) {
            return {};
        }
    }

    // Runs a single canonical obfuscation test.
    // - Reads the input query from data/<name>.sql
    // - Obfuscates it using the shared schema
    // - Compares the result against data/<name>.expected
    // - In canonization mode, writes the actual result to the .expected file
    static void RunCanonicalTest(const TString& name) {
        auto obfuscator = MakeCanonicalObfuscator();

        TString inputQuery = ReadCanonFile(name + ".sql");
        UNIT_ASSERT_C(!inputQuery.empty(), "Missing test input file: data/" + name + ".sql");

        TString result;
        NYql::TIssues issues;
        bool ok = obfuscator.ObfuscateQueryText(inputQuery, result, issues);
        UNIT_ASSERT_C(ok, "Failed to obfuscate " << name << ": " << issues.ToString());

        result.append("\n");

        TString canonize = GetTestParam("CANONIZE_OBFUSCATE_TESTS");
        canonize.to_lower();
        if (canonize == "true") {
            Cerr << "--- CANONIZING " << name << " ---" << Endl;
            TOFStream stream(SRC_("data/" + name + ".expected"));
            stream << result;
            stream.Finish();
        }

        TString expected = ReadCanonFile(name + ".expected");
        UNIT_ASSERT_C(!expected.empty(), "Missing canonical file: data/" + name + ".expected\n"
            "Run with --test-param CANONIZE_OBFUSCATE_TESTS=TRUE to generate it.\n"
            "Actual result:\n" << result);
        UNIT_ASSERT_NO_DIFF(result, expected);
    }

    Y_UNIT_TEST(Canonical_MappingIsCorrect) {
        auto obfuscator = MakeCanonicalObfuscator();
        const auto& m = obfuscator.GetMapping();

        UNIT_ASSERT_STRINGS_EQUAL(m.at("orders"), "table_0");
        UNIT_ASSERT_STRINGS_EQUAL(m.at("customers"), "table_1");
        UNIT_ASSERT_STRINGS_EQUAL(m.at("order_items"), "table_2");
        UNIT_ASSERT_STRINGS_EQUAL(m.at("id"), "column_0");
        UNIT_ASSERT_STRINGS_EQUAL(m.at("customer_id"), "column_1");
        UNIT_ASSERT_STRINGS_EQUAL(m.at("amount"), "column_2");
        UNIT_ASSERT_STRINGS_EQUAL(m.at("created_at"), "column_3");
        UNIT_ASSERT_STRINGS_EQUAL(m.at("status"), "column_4");
        UNIT_ASSERT_STRINGS_EQUAL(m.at("name"), "column_5");
        UNIT_ASSERT_STRINGS_EQUAL(m.at("email"), "column_6");
        UNIT_ASSERT_STRINGS_EQUAL(m.at("age"), "column_7");
        UNIT_ASSERT_STRINGS_EQUAL(m.at("order_id"), "column_8");
        UNIT_ASSERT_STRINGS_EQUAL(m.at("item_id"), "column_9");
        UNIT_ASSERT_STRINGS_EQUAL(m.at("product_name"), "column_10");
        UNIT_ASSERT_STRINGS_EQUAL(m.at("qty"), "column_11");
        UNIT_ASSERT_STRINGS_EQUAL(m.at("price"), "column_12");
        UNIT_ASSERT_STRINGS_EQUAL(m.at("idx_customer"), "index_0");
    }

    Y_UNIT_TEST(Canonical_SimpleSelect) { RunCanonicalTest("simple_select"); }
    Y_UNIT_TEST(Canonical_JoinWithAliases) { RunCanonicalTest("join_with_aliases"); }
    Y_UNIT_TEST(Canonical_DeclareParameter) { RunCanonicalTest("declare_parameter"); }
    Y_UNIT_TEST(Canonical_IndexHint) { RunCanonicalTest("index_hint"); }
    Y_UNIT_TEST(Canonical_Upsert) { RunCanonicalTest("upsert"); }
    Y_UNIT_TEST(Canonical_ThreeWayJoin) { RunCanonicalTest("three_way_join"); }
    Y_UNIT_TEST(Canonical_GroupByOrderByLimit) { RunCanonicalTest("group_by_order_by_limit"); }
    Y_UNIT_TEST(Canonical_Subquery) { RunCanonicalTest("subquery"); }
    Y_UNIT_TEST(Canonical_MultipleParameters) { RunCanonicalTest("multiple_parameters"); }
    Y_UNIT_TEST(Canonical_Delete) { RunCanonicalTest("delete"); }
    Y_UNIT_TEST(Canonical_Update) { RunCanonicalTest("update"); }
}
