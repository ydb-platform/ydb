#pragma once

#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/string.h>
#include <util/string/builder.h>

namespace NSchemeShardUT_Private::NLocalIndexes {

inline TString OlapTableWithBloomAndNgramIndexes(const TString& tableName) {
    return TStringBuilder() << R"(
            Name: ")" << tableName << R"("
            ColumnShardCount: 1
            Schema {
                Columns { Name: "timestamp" Type: "Timestamp" NotNull: true }
                Columns { Name: "resource_id" Type: "Utf8" }
                Columns { Name: "uid" Type: "Utf8" NotNull: true }
                KeyColumnNames: "timestamp"
                KeyColumnNames: "uid"
                Indexes {
                    Id: 1
                    Name: "idx_bloom"
                    ClassName: "BLOOM_FILTER"
                    BloomFilter {
                        FalsePositiveProbability: 0.01
                        ColumnIds: 2
                    }
                }
                Indexes {
                    Id: 2
                    Name: "idx_ngram"
                    ClassName: "BLOOM_NGRAMM_FILTER"
                    BloomNGrammFilter {
                        NGrammSize: 3
                        FalsePositiveProbability: 0.01
                        CaseSensitive: true
                        ColumnId: 2
                    }
                }
            }
        )";
}

// Asserts the index path exists, has the expected local-index type and key
// columns, and is ready. Combines four otherwise-repeated NLs:: checks into
// a single named call.
inline void CheckLocalIndexReady(NActors::TTestActorRuntime& runtime,
        const TString& tablePath, const TString& indexName,
        NKikimrSchemeOp::EIndexType expectedType,
        std::initializer_list<TString> expectedKeys) {
    const auto descr = DescribePrivatePath(runtime, tablePath + "/" + indexName, true, true);
    TestDescribeResult(descr, {
        NLs::PathExist,
        NLs::IndexType(expectedType),
        NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
        NLs::IndexKeys(expectedKeys),
    });
}

// Asserts the table has the canonical {idx_bloom, idx_ngram} pair (as produced by
// OlapTableWithBloomAndNgramIndexes above) as ready scheme-object children.
inline void CheckOlapTableWithBloomAndNgramIndexesReady(NActors::TTestActorRuntime& runtime,
        const TString& tablePath) {
    TestDescribeResult(DescribePath(runtime, tablePath),
        {NLs::PathExist, NLs::ChildrenCount(2)});
    CheckLocalIndexReady(runtime, tablePath, "idx_bloom",
        NKikimrSchemeOp::EIndexTypeLocalBloomFilter, {"resource_id"});
    CheckLocalIndexReady(runtime, tablePath, "idx_ngram",
        NKikimrSchemeOp::EIndexTypeLocalBloomNgramFilter, {"resource_id"});
}

// Asserts the column-table local index's two versions agree, and the index
// is present in the parent column table's schema.
inline void CheckIndexVersionsConsistent(NActors::TTestActorRuntime& runtime,
        const TString& tablePath, const TString& indexName) {
    const auto indexPath = tablePath + "/" + indexName;
    const auto indexDescr = DescribePrivatePath(runtime, indexPath, true, true);
    const auto& self = indexDescr.GetPathDescription().GetSelf();
    const auto& tableIndex = indexDescr.GetPathDescription().GetTableIndex();
    UNIT_ASSERT_VALUES_EQUAL_C(tableIndex.GetSchemaVersion(), self.GetVersion().GetTableIndexVersion(),
        TStringBuilder() << "Version mismatch on " << indexPath
            << ": TableIndex.SchemaVersion=" << tableIndex.GetSchemaVersion()
            << " vs Self.Version.TableIndexVersion=" << self.GetVersion().GetTableIndexVersion());

    const auto tableDescr = DescribePrivatePath(runtime, tablePath, true, true);
    const auto& schema = tableDescr.GetPathDescription().GetColumnTableDescription().GetSchema();
    bool found = false;
    for (const auto& idx : schema.GetIndexes()) {
        if (idx.GetName() == indexName) {
            found = true;
            break;
        }
    }
    UNIT_ASSERT_C(found, TStringBuilder() << "Index '" << indexName
        << "' present in scheme tree but missing from column table '" << tablePath << "' schema");
}

// Helper structure to define expected bloom filter index properties
struct TBloomIndexExpectation {
    TString Name;
    std::vector<TString> KeyColumns;
};

// Asserts that a row table has the expected bloom filter prefixes
inline void CheckRowTableBloomSchemeObjects(NActors::TTestActorRuntime& runtime,
        const TString& tablePath,
        const std::vector<ui32>& expectedPrefixLengths,
        const std::vector<TBloomIndexExpectation>& expectedIndexes) {
    auto tableDescr = DescribePath(runtime, tablePath, true);
    const auto& table = tableDescr.GetPathDescription().GetTable();

    // Check engine prefixes
    const auto& partitionConfig = table.GetPartitionConfig();
    UNIT_ASSERT_VALUES_EQUAL_C(partitionConfig.ByKeyFilterPrefixesSize(), expectedPrefixLengths.size(),
        TStringBuilder() << "Expected " << expectedPrefixLengths.size() << " prefixes, got "
        << partitionConfig.ByKeyFilterPrefixesSize());

    for (size_t i = 0; i < expectedPrefixLengths.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL_C(partitionConfig.GetByKeyFilterPrefixes(i).GetPrefixLength(),
            expectedPrefixLengths[i],
            TStringBuilder() << "Prefix " << i << " length mismatch");
    }

    // Check scheme objects exist
    UNIT_ASSERT_VALUES_EQUAL_C(table.TableIndexesSize(), expectedIndexes.size(),
        TStringBuilder() << "Expected " << expectedIndexes.size() << " indexes, got "
        << table.TableIndexesSize());

    std::vector<bool> foundIndexes(expectedIndexes.size(), false);
    for (const auto& idx : table.GetTableIndexes()) {
        UNIT_ASSERT_VALUES_EQUAL(idx.GetType(), NKikimrSchemeOp::EIndexTypeLocalBloomFilter);
        UNIT_ASSERT_VALUES_EQUAL(idx.GetState(), NKikimrSchemeOp::EIndexStateReady);

        // Find matching expectation
        for (size_t i = 0; i < expectedIndexes.size(); ++i) {
            if (idx.GetName() == expectedIndexes[i].Name) {
                foundIndexes[i] = true;
                UNIT_ASSERT_VALUES_EQUAL_C(idx.KeyColumnNamesSize(), expectedIndexes[i].KeyColumns.size(),
                    TStringBuilder() << "Index '" << expectedIndexes[i].Name << "' key column count mismatch");

                for (size_t j = 0; j < expectedIndexes[i].KeyColumns.size(); ++j) {
                    UNIT_ASSERT_VALUES_EQUAL_C(idx.GetKeyColumnNames(j), expectedIndexes[i].KeyColumns[j],
                        TStringBuilder() << "Index '" << expectedIndexes[i].Name << "' key column " << j << " mismatch");
                }
                break;
            }
        }
    }

    // Verify all expected indexes were found
    for (size_t i = 0; i < expectedIndexes.size(); ++i) {
        UNIT_ASSERT_C(foundIndexes[i], TStringBuilder() << "Expected index '" << expectedIndexes[i].Name << "' not found");
    }
}

}   // namespace NSchemeShardUT_Private::NLocalIndexes
