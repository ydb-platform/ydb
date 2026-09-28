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

Y_UNIT_TEST_SUITE(OlapSchemaGeneratedVirtual) {
    Y_UNIT_TEST(PhysicalSerializationKeepsLogicalIdsAndMetadata) {
        NKikimrSchemeOp::TColumnTableSchema schemaProto;
        schemaProto.SetNextColumnId(4);
        schemaProto.SetVersion(7);
        schemaProto.AddKeyColumnNames("key");

        auto* key = schemaProto.AddColumns();
        key->SetId(1);
        key->SetName("key");
        key->SetType("Uint64");
        key->SetTypeId(NScheme::NTypeIds::Uint64);
        key->SetNotNull(true);

        auto* source = schemaProto.AddColumns();
        source->SetId(3);
        source->SetName("source");
        source->SetType("Int64");
        source->SetTypeId(NScheme::NTypeIds::Int64);

        auto* derived = schemaProto.AddColumns();
        derived->SetId(2);
        derived->SetName("derived");
        derived->SetType("Int64");
        derived->SetTypeId(NScheme::NTypeIds::Int64);
        auto* generated = derived->MutableDefaultFromExpression();
        generated->SetExprText("COALESCE(source, 0) * 2");
        generated->AddDependencyColumnNames("source");
        generated->SetStored(false);

        TOlapSchema schema;
        schema.ParseFromLocalDB(schemaProto);

        NKikimrSchemeOp::TColumnTableSchema logical;
        schema.Serialize(logical);
        UNIT_ASSERT_VALUES_EQUAL(logical.GetVersion(), 7u);
        UNIT_ASSERT_VALUES_EQUAL(logical.GetNextColumnId(), 4u);
        UNIT_ASSERT_VALUES_EQUAL(logical.ColumnsSize(), 3);
        const NKikimrSchemeOp::TOlapColumnDescription* logicalDerivedPtr = nullptr;
        for (const auto& column : logical.GetColumns()) {
            if (column.GetName() == "derived") {
                logicalDerivedPtr = &column;
                break;
            }
        }
        UNIT_ASSERT(logicalDerivedPtr);
        const auto& logicalDerived = *logicalDerivedPtr;
        UNIT_ASSERT_VALUES_EQUAL(logicalDerived.GetId(), 2u);
        UNIT_ASSERT(logicalDerived.HasDefaultFromExpression());
        UNIT_ASSERT_VALUES_EQUAL(logicalDerived.GetDefaultFromExpression().GetExprText(),
            "COALESCE(source, 0) * 2");
        UNIT_ASSERT_VALUES_EQUAL(logicalDerived.GetDefaultFromExpression().DependencyColumnNamesSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(logicalDerived.GetDefaultFromExpression().GetDependencyColumnNames(0), "source");
        UNIT_ASSERT_VALUES_EQUAL(logicalDerived.GetDefaultFromExpression().GetStored(), false);

        NKikimrSchemeOp::TColumnTableSchema physical;
        schema.SerializeForColumnShard(physical);
        UNIT_ASSERT_VALUES_EQUAL(physical.GetVersion(), 7u);
        UNIT_ASSERT_VALUES_EQUAL(physical.GetNextColumnId(), 4u);
        UNIT_ASSERT_VALUES_EQUAL(physical.ColumnsSize(), 2);
        THashSet<ui32> physicalIds;
        THashSet<TString> physicalNames;
        for (const auto& column : physical.GetColumns()) {
            physicalIds.insert(column.GetId());
            physicalNames.insert(column.GetName());
        }
        UNIT_ASSERT_VALUES_EQUAL(physicalIds, THashSet<ui32>({1, 3}));
        UNIT_ASSERT_VALUES_EQUAL(physicalNames, THashSet<TString>({"key", "source"}));
    }
}

} // namespace NKikimr::NSchemeShard
