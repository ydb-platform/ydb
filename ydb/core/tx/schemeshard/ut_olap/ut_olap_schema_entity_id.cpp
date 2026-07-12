#include <ydb/core/scheme_types/scheme_types_defs.h>
#include <ydb/core/tx/schemeshard/olap/common/common.h>
#include <ydb/core/tx/schemeshard/olap/schema/schema.h>
#include <library/cpp/testing/unittest/registar.h>

#include <google/protobuf/text_format.h>

namespace NKikimr::NSchemeShard {

namespace {

NKikimrSchemeOp::TColumnTableSchema MakeLegacyOverlappingSchemaProto() {
    NKikimrSchemeOp::TColumnTableSchema schemaProto;
    const char* text = R"(
        NextColumnId: 3
        Version: 1
        Columns { Name: "timestamp" Type: "Timestamp" NotNull: true Id: 1 }
        Columns { Name: "uid" Type: "Utf8" NotNull: true Id: 2 }
        KeyColumnNames: "timestamp"
        KeyColumnNames: "uid"
        Indexes {
            Id: 1
            Name: "idx_bloom"
            ClassName: "BLOOM_FILTER"
            BloomFilter { FalsePositiveProbability: 0.01 ColumnIds: 2 }
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
        Indexes {
            Id: 3
            Name: "idx_minmax"
            ClassName: "MIN_MAX"
            MinMaxIndex { ColumnId: 2 }
        }
    )";
    Y_ABORT_UNLESS(google::protobuf::TextFormat::ParseFromString(text, &schemaProto));
    schemaProto.MutableColumns(0)->SetTypeId(NScheme::NTypeIds::Timestamp);
    schemaProto.MutableColumns(1)->SetTypeId(NScheme::NTypeIds::Utf8);
    return schemaProto;
}

} // namespace

Y_UNIT_TEST_SUITE(OlapSchemaEntityId) {
    Y_UNIT_TEST(ParseFromLocalDbAdvancesNextColumnIdPastIndexes) {
        const auto schemaProto = MakeLegacyOverlappingSchemaProto();

        TOlapSchema schema;
        schema.ParseFromLocalDB(schemaProto);

        UNIT_ASSERT_VALUES_EQUAL(schema.GetNextColumnId(), 4u);
    }
}

} // namespace NKikimr::NSchemeShard
