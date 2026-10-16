#include <ydb/core/sys_view/show_create/formatters/create_table_formatter.h>

#include <ydb/core/scheme/scheme_type_id.h>
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

TIndexDescription& AddIndex(TTableDescription& table, EIndexType type, const TString& name = "idx") {
    auto& index = *table.AddTableIndexes();
    index.SetName(name);
    index.SetType(type);
    index.AddKeyColumnNames("value");

    switch (type) {
        case EIndexTypeLocalBloomFilter:
            index.MutableBloomFilterDescription()->SetFalsePositiveProbability(0.01);
            break;
        case EIndexTypeLocalBloomNgramFilter:
            index.MutableBloomNGrammFilterDescription()->SetNGrammSize(3);
            index.MutableBloomNGrammFilterDescription()->SetFalsePositiveProbability(0.02);
            index.MutableBloomNGrammFilterDescription()->SetCaseSensitive(false);
            break;
        case EIndexTypeLocalMinMax:
            break;
        default:
            // Enough implementation tables for every supported global index layout.
            for (int i = 0; i < 4; ++i) {
                index.AddIndexImplTableDescriptions();
            }
            break;
    }

    if (type == EIndexTypeGlobalVectorKmeansTree) {
        auto* settings = index.MutableVectorIndexKmeansTreeDescription()->MutableSettings();
        settings->mutable_settings()->set_metric(Ydb::Table::VectorIndexSettings::DISTANCE_COSINE);
        settings->mutable_settings()->set_vector_type(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT);
        settings->mutable_settings()->set_vector_dimension(8);
        settings->set_clusters(4);
        settings->set_levels(2);
    } else if (type == EIndexTypeGlobalFulltextPlain || type == EIndexTypeGlobalFulltextRelevance
            || type == EIndexTypeGlobalFulltextCompact || type == EIndexTypeGlobalFulltextCompactRelevance) {
        auto* analyzers = index.MutableFulltextIndexDescription()->MutableSettings()
            ->add_columns()->mutable_analyzers();
        analyzers->set_tokenizer(Ydb::Table::FulltextIndexSettings::STANDARD);
        analyzers->set_language("russian,english");
        analyzers->set_use_filter_lowercase(false);
    }
    return index;
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
    Y_UNIT_TEST(GlobalIndexTypes) {
        const std::pair<EIndexType, TString> cases[] = {
            {EIndexTypeGlobal, "GLOBAL SYNC"},
            {EIndexTypeGlobalAsync, "GLOBAL ASYNC"},
            {EIndexTypeGlobalUnique, "GLOBAL UNIQUE SYNC"},
            {EIndexTypeGlobalVectorKmeansTree, "GLOBAL USING vector_kmeans_tree"},
            {EIndexTypeGlobalFulltextPlain, "GLOBAL USING fulltext_plain"},
            {EIndexTypeGlobalFulltextRelevance, "GLOBAL USING fulltext_relevance"},
            {EIndexTypeGlobalFulltextCompact, "GLOBAL USING fulltext_plain"},
            {EIndexTypeGlobalFulltextCompactRelevance, "GLOBAL USING fulltext_relevance"},
            {EIndexTypeGlobalJson, "GLOBAL USING json"},
            {EIndexTypeGlobalJsonCompact, "GLOBAL USING json"},
        };

        TCreateTableFormatter defaultFormatter;
        TCreateTableFormatter disabledFormatter(false);
        TCreateTableFormatter separateFormatter(true);
        for (const auto& [type, declaration] : cases) {
            auto table = MakeTableDescription();
            auto& index = AddIndex(table, type);
            index.AddDataColumnNames("cover");

            TStringBuilder indexQuery;
            indexQuery << "INDEX `idx` " << declaration << " ON (`value`) COVER (`cover`)";
            if (index.HasVectorIndexKmeansTreeDescription()) {
                indexQuery << " WITH (distance = cosine, vector_type = 'float', vector_dimension = 8, clusters = 4, levels = 2)";
            } else if (index.HasFulltextIndexDescription()) {
                indexQuery << " WITH (tokenizer = standard, language = 'russian,english', use_filter_lowercase = false)";
            }

            const TString inlineQuery = TStringBuilder()
                << "CREATE TABLE `table` (`key` Uint64, `value` String, `cover` Uint32, "
                << indexQuery << ", PRIMARY KEY (`key`));";
            const TString separateQuery = TStringBuilder()
                << "CREATE TABLE `table` (`key` Uint64, `value` String, `cover` Uint32, PRIMARY KEY (`key`));"
                << "ALTER TABLE `table` ADD " << indexQuery << ";";

            AssertQueryEqual(defaultFormatter.Format("table", "/Root/table", table, false, {}, {}), inlineQuery);
            AssertQueryEqual(disabledFormatter.Format("table", "/Root/table", table, false, {}, {}), inlineQuery);
            AssertQueryEqual(separateFormatter.Format("table", "/Root/table", table, false, {}, {}), separateQuery);
        }
    }

    Y_UNIT_TEST(MultipleGlobalIndexesInlineByDefault) {
        auto table = MakeTableDescription();
        AddIndex(table, EIndexTypeGlobal, "sync").AddKeyColumnNames("cover");
        AddIndex(table, EIndexTypeGlobalAsync, "async").AddDataColumnNames("cover");
        AddIndex(table, EIndexTypeGlobalUnique, "unique");

        TCreateTableFormatter formatter;
        AssertQueryEqual(formatter.Format("table", "/Root/table", table, false, {}, {}), R"(
            CREATE TABLE `table` (
                `key` Uint64,
                `value` String,
                `cover` Uint32,
                INDEX `sync` GLOBAL SYNC ON (`value`, `cover`),
                INDEX `async` GLOBAL ASYNC ON (`value`) COVER (`cover`),
                INDEX `unique` GLOBAL UNIQUE SYNC ON (`value`),
                PRIMARY KEY (`key`)
            );
        )");
    }

    Y_UNIT_TEST(MixedIndexesInlineWhenDisabled) {
        auto table = MakeTableDescription();
        AddIndex(table, EIndexTypeGlobal, "sync").AddDataColumnNames("cover");
        AddIndex(table, EIndexTypeLocalBloomFilter, "bloom");
        AddIndex(table, EIndexTypeGlobalAsync, "async").AddKeyColumnNames("cover");
        AddIndex(table, EIndexTypeLocalBloomNgramFilter, "ngram");
        AddIndex(table, EIndexTypeGlobalUnique, "unique");
        AddIndex(table, EIndexTypeLocalMinMax, "minmax");

        TCreateTableFormatter formatter(false);
        AssertQueryEqual(formatter.Format("table", "/Root/table", table, false, {}, {}), R"(
            CREATE TABLE `table` (
                `key` Uint64,
                `value` String,
                `cover` Uint32,
                INDEX `sync` GLOBAL SYNC ON (`value`) COVER (`cover`),
                INDEX `bloom` LOCAL USING bloom_filter ON (`value`) WITH (false_positive_probability = 0.01),
                INDEX `async` GLOBAL ASYNC ON (`value`, `cover`),
                INDEX `ngram` LOCAL USING bloom_ngram_filter ON (`value`)
                    WITH (ngram_size = 3, false_positive_probability = 0.02, case_sensitive = FALSE),
                INDEX `unique` GLOBAL UNIQUE SYNC ON (`value`),
                INDEX `minmax` LOCAL USING min_max ON (`value`),
                PRIMARY KEY (`key`)
            );
        )");
    }

    Y_UNIT_TEST(GlobalIndexInlineBeforeAlterIndexSettingsWhenDisabled) {
        auto table = MakeTableDescription();
        AddIndex(table, EIndexTypeGlobal).MutableIndexImplTableDescriptions(0)
            ->MutablePartitionConfig()->MutablePartitioningPolicy()->SetMaxPartitionsCount(10);
        table.MutablePartitionConfig()->AddByKeyFilterPrefixes()->SetPrefixLength(1);

        TCreateTableFormatter formatter(false);
        AssertQueryEqual(formatter.Format("table", "/Root/table", table, false, {}, {}), R"(
            CREATE TABLE `table` (
                `key` Uint64,
                `value` String,
                `cover` Uint32,
                INDEX `idx` GLOBAL SYNC ON (`value`),
                INDEX `idx_bloom_1` LOCAL USING bloom_filter ON (`key`),
                PRIMARY KEY (`key`)
            );
            ALTER TABLE `table` ALTER INDEX `idx` SET (AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 10);
        )");
    }

    Y_UNIT_TEST(MixedGlobalAndLocalIndexes) {
        auto table = MakeTableDescription();
        AddIndex(table, EIndexTypeGlobal, "sync").AddDataColumnNames("cover");
        AddIndex(table, EIndexTypeLocalBloomFilter, "bloom");
        AddIndex(table, EIndexTypeGlobalAsync, "async").AddKeyColumnNames("cover");
        AddIndex(table, EIndexTypeLocalBloomNgramFilter, "ngram");
        AddIndex(table, EIndexTypeGlobalUnique, "unique");
        AddIndex(table, EIndexTypeLocalMinMax, "minmax");

        TCreateTableFormatter formatter(true);
        AssertQueryEqual(formatter.Format("table", "/Root/table", table, false, {}, {}), R"(
            CREATE TABLE `table` (
                `key` Uint64,
                `value` String,
                `cover` Uint32,
                INDEX `bloom` LOCAL USING bloom_filter ON (`value`) WITH (false_positive_probability = 0.01),
                INDEX `ngram` LOCAL USING bloom_ngram_filter ON (`value`)
                    WITH (ngram_size = 3, false_positive_probability = 0.02, case_sensitive = FALSE),
                INDEX `minmax` LOCAL USING min_max ON (`value`),
                PRIMARY KEY (`key`)
            );
            ALTER TABLE `table` ADD INDEX `sync` GLOBAL SYNC ON (`value`) COVER (`cover`);
            ALTER TABLE `table` ADD INDEX `async` GLOBAL ASYNC ON (`value`, `cover`);
            ALTER TABLE `table` ADD INDEX `unique` GLOBAL UNIQUE SYNC ON (`value`);
        )");
    }

    Y_UNIT_TEST(SynthesizedLocalBloomFilter) {
        auto table = MakeTableDescription();
        AddIndex(table, EIndexTypeGlobal);
        table.MutablePartitionConfig()->AddByKeyFilterPrefixes()->SetPrefixLength(1);

        TCreateTableFormatter formatter(true);
        AssertQueryEqual(formatter.Format("table", "/Root/table", table, false, {}, {}), R"(
            CREATE TABLE `table` (
                `key` Uint64,
                `value` String,
                `cover` Uint32,
                INDEX `idx_bloom_1` LOCAL USING bloom_filter ON (`key`),
                PRIMARY KEY (`key`)
            );
            ALTER TABLE `table` ADD INDEX `idx` GLOBAL SYNC ON (`value`);
        )");
    }

    Y_UNIT_TEST(AdditionalDdlOrdering) {
        auto table = MakeTableDescription();
        AddIndex(table, EIndexTypeGlobal).MutableIndexImplTableDescriptions(0)
            ->MutablePartitionConfig()->MutablePartitioningPolicy()->SetMaxPartitionsCount(10);

        auto* feed = table.AddCdcStreams();
        feed->SetName("feed");
        feed->SetMode(ECdcStreamModeUpdate);
        feed->SetFormat(ECdcStreamFormatJson);
        THashMap<TString, THolder<TPersQueueGroupDescription>> persQueues;
        persQueues["table/feed/streamImpl"] = MakeHolder<TPersQueueGroupDescription>();

        auto* sequence = table.AddSequences();
        sequence->SetName("_serial_column_key");
        sequence->MutablePathId()->SetOwnerId(1);
        sequence->MutablePathId()->SetLocalId(2);
        table.MutableColumns(0)->SetType("Int64");
        table.MutableColumns(0)->SetTypeId(NScheme::NTypeIds::Int64);
        table.MutableColumns(0)->SetDefaultFromSequence("_serial_column_key");
        const TPathId sequencePathId(1, 2);
        THashMap<TPathId, THolder<NSequenceProxy::TEvSequenceProxy::TEvGetSequenceResult>> sequences;
        auto sequenceResult = MakeHolder<NSequenceProxy::TEvSequenceProxy::TEvGetSequenceResult>(sequencePathId);
        sequenceResult->StartValue = 1;
        sequenceResult->NextValue = 7;
        sequenceResult->Increment = 1;
        sequences[sequencePathId] = std::move(sequenceResult);

        TCreateTableFormatter formatter(true);
        AssertQueryEqual(formatter.Format("table", "/Root/table", table, false, persQueues, sequences), R"(
            CREATE TABLE `table` (`key` Serial8, `value` String, `cover` Uint32, PRIMARY KEY (`key`));
            ALTER TABLE `table` ADD INDEX `idx` GLOBAL SYNC ON (`value`);
            ALTER TABLE `table` ADD CHANGEFEED `feed` WITH (MODE = 'UPDATES', FORMAT = 'JSON');
            ALTER SEQUENCE `/Root/table/_serial_column_key` RESTART WITH 7;
            ALTER TABLE `table` ALTER INDEX `idx` SET (AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 10);
        )");
    }

    Y_UNIT_TEST(ColumnTableLocalIndexesUnaffected) {
        TColumnTableDescription table;
        auto* schema = table.MutableSchema();
        auto* key = schema->AddColumns();
        key->SetId(1);
        key->SetName("key");
        key->SetType("Uint64");
        key->SetNotNull(true);
        schema->AddKeyColumnNames("key");
        auto* index = schema->AddIndexes();
        index->SetName("bloom");
        index->MutableBloomFilter()->AddColumnIds(1);

        TCreateTableFormatter defaultFormatter;
        TCreateTableFormatter separateFormatter(true);
        for (const bool enableLocalIndexAsSchemeObject : {false, true}) {
            const auto defaultQuery = ExtractQuery(defaultFormatter.Format("table", "/Root/table", table, false,
                enableLocalIndexAsSchemeObject));
            const auto separateQuery = ExtractQuery(separateFormatter.Format("table", "/Root/table", table, false,
                enableLocalIndexAsSchemeObject));
            UNIT_ASSERT_STRINGS_EQUAL(defaultQuery, separateQuery);
        }
    }

    Y_UNIT_TEST(NoGlobalIndexesAndFormatterReuse) {
        TCreateTableFormatter formatter(true);
        auto table = MakeTableDescription();
        AddIndex(table, EIndexTypeGlobal);
        const auto indexedQuery = ExtractQuery(formatter.Format("table", "/Root/table", table, false, {}, {}));
        UNIT_ASSERT_C(indexedQuery.Contains("ADD INDEX"), indexedQuery);

        table.ClearTableIndexes();
        AssertQueryEqual(formatter.Format("table", "/Root/table", table, false, {}, {}), R"(
            CREATE TABLE `table` (`key` Uint64, `value` String, `cover` Uint32, PRIMARY KEY (`key`));
        )");

        AddIndex(table, EIndexTypeLocalBloomFilter);
        TCreateTableFormatter defaultFormatter;
        UNIT_ASSERT_STRINGS_EQUAL(
            ExtractQuery(formatter.Format("table", "/Root/table", table, false, {}, {})),
            ExtractQuery(defaultFormatter.Format("table", "/Root/table", table, false, {}, {})));
    }

    Y_UNIT_TEST(TemporaryTableAndEscapedNames) {
        auto table = MakeTableDescription();
        AddIndex(table, EIndexTypeGlobalAsync, "index with spaces");

        TCreateTableFormatter formatter(true);
        AssertQueryEqual(formatter.Format("table with spaces", "/Root/table with spaces", table, true, {}, {}), R"(
            CREATE TEMPORARY TABLE `table with spaces` (
                `key` Uint64, `value` String, `cover` Uint32, PRIMARY KEY (`key`)
            );
            ALTER TABLE `table with spaces` ADD INDEX `index with spaces` GLOBAL ASYNC ON (`value`);
        )");
    }
}

} // namespace NKikimr::NSysView
