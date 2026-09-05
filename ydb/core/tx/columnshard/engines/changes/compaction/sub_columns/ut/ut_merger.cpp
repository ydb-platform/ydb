#include <ydb/core/formats/arrow/accessor/composite/accessor.h>
#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/accessor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/constructor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/settings.h>
#include <ydb/core/tx/columnshard/counters/indexation.h>
#include <ydb/core/tx/columnshard/engines/changes/compaction/abstract/merger.h>
#include <ydb/core/tx/columnshard/engines/changes/compaction/common/context.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/snapshot_scheme.h>
#include <ydb/core/tx/columnshard/test_helper/helper.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_binary.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <library/cpp/testing/unittest/registar.h>
#include <yql/essentials/types/binary_json/read.h>
#include <yql/essentials/types/binary_json/write.h>

// Reproduces the compaction crash where the sub-columns merger deduces a native scalar value type from
// only the first chunk of a multi-chunk source. When a key's scalar type differs between chunks of the
// same source, the merged column is stamped with the first chunk's type (e.g. String) yet the builder
// stores re-encoded BinaryJson bytes for the diverging chunk.
Y_UNIT_TEST_SUITE(SubColumnsCompaction) {
    using namespace NKikimr;
    using namespace NKikimr::NOlap;
    using namespace NKikimr::NOlap::NCompaction;
    using namespace NKikimr::NArrow::NAccessor;
    using namespace NKikimr::NArrow::NAccessor::NSubColumns;

    TSettings MakeSettings() {
        TSettings s(20, 1024, 50u * 1024 * 1024, 0.0, TDataAdapterContainer::GetDefault(), 0.0);
        s.SetEnableNativeColumns(true);
        return s;
    }

    std::shared_ptr<TSubColumnsArray> BuildChunk(const std::vector<TString>& jsons, const TSettings& settings) {
        TTrivialArray::TPlainBuilder<arrow::BinaryType> b;
        for (ui32 i = 0; i < jsons.size(); ++i) {
            auto v = NBinaryJson::SerializeToBinaryJson(jsons[i]);
            auto* bj = std::get_if<NBinaryJson::TBinaryJson>(&v);
            UNIT_ASSERT_C(bj, "cannot serialize " << jsons[i]);
            b.AddRecord(i, std::string_view(bj->data(), bj->size()));
        }
        auto arr = b.Finish(jsons.size());
        return TSubColumnsArray::Make(arr, settings, arr->GetDataType()).DetachResult();
    }

    ISnapshotSchema::TPtr MakeSchema(const TSettings& settings) {
        auto storages = TTestStoragesManager::GetInstance();
        auto cache = std::make_shared<TSchemaObjectsCache>();

        NKikimrSchemeOp::TColumnTableSchema proto;
        *proto.MutableColumns()->Add() = NArrow::NTest::TTestColumn("pk", NScheme::TTypeInfo(NScheme::NTypeIds::Uint64)).CreateColumn(1);

        auto dataCol = NArrow::NTest::TTestColumn("data", NScheme::TTypeInfo(NScheme::NTypeIds::JsonDocument)).CreateColumn(2);
        auto* dac = dataCol.MutableDataAccessorConstructor();
        dac->SetClassName(TConstructor::GetClassNameStatic());
        *dac->MutableSubColumns()->MutableSettings() = settings.SerializeToProto();
        *proto.MutableColumns()->Add() = dataCol;

        proto.AddKeyColumnNames("pk");
        proto.SetVersion(1);

        auto info = TIndexInfo::BuildFromProto(1, proto, storages, cache);
        UNIT_ASSERT(info);
        return std::make_shared<TSnapshotSchema>(cache->UpsertIndexInfo(std::move(*info)), TSnapshot(1, 1));
    }

    // Render each stored BinaryJson document as text. For the corrupt portion this will trigger rendering
    // of incorrectly labeled binary data.
    TString RenderDocs(const std::shared_ptr<IChunkedArray>& arr) {
        auto ca = arr->GetChunkedArray();
        TStringBuilder out;
        for (int c = 0; c < ca->num_chunks(); ++c) {
            const auto& bin = static_cast<const arrow::BinaryArray&>(*ca->chunk(c));
            for (i64 i = 0; i < bin.length(); ++i) {
                if (bin.IsNull(i)) {
                    out << "null;";
                    continue;
                }
                const auto view = bin.GetView(i);
                out << NBinaryJson::SerializeToJson(TStringBuf(view.data(), view.size())) << ";";
            }
        }
        return out;
    }

    std::shared_ptr<TSubColumnsArray> MergeChunks(const std::vector<std::shared_ptr<TSubColumnsArray>>& chunks, const TSettings& settings) {
        NArrow::NAccessor::TCompositeChunkedArray::TBuilder cb(chunks.front()->GetDataType());
        ui32 total = 0;
        for (const auto& chunk : chunks) {
            cb.AddChunk(chunk);
            total += chunk->GetRecordsCount();
        }
        std::vector<std::shared_ptr<IChunkedArray>> inputs = { cb.Finish() };

        const ui32 columnId = 2;
        auto schema = MakeSchema(settings);
        TColumnMergeContext mergeCtx(columnId, schema, 8u * 1024 * 1024, std::nullopt);
        THolder<IColumnMerger> merger = IColumnMerger::TFactory::MakeHolder(TConstructor::GetClassNameStatic(), mergeCtx);
        UNIT_ASSERT(merger);

        TMergingContext mergingCtx({});
        merger->Start(inputs, mergingCtx);

        // Output row i is mapped to row i of the single (0-th) input.
        arrow::UInt16Builder idxB;
        arrow::UInt32Builder recB;
        for (ui32 i = 0; i < total; ++i) {
            UNIT_ASSERT(idxB.Append(0).ok());
            UNIT_ASSERT(recB.Append(i).ok());
        }
        auto pkSchema = arrow::schema({ arrow::field(IColumnMerger::PortionIdFieldName, arrow::uint16()),
            arrow::field(IColumnMerger::PortionRecordIndexFieldName, arrow::uint32()) });
        auto pkBatch = arrow::RecordBatch::Make(pkSchema, total, { idxB.Finish().ValueOrDie(), recB.Finish().ValueOrDie() });

        TMergingChunkContext chunkCtxOwner(pkBatch);
        NColumnShard::TIndexationCounters counters("Compaction");
        TChunkMergeContext chunkCtx(counters, chunkCtxOwner.Slice(0, total));
        auto result = merger->Execute(chunkCtx, mergingCtx);
        UNIT_ASSERT_VALUES_EQUAL(result.GetChunks().size(), 1);

        auto loader = schema->GetIndexInfo().GetColumnLoaderVerified(columnId);
        return std::static_pointer_cast<TSubColumnsArray>(loader->ApplyVerified(result.GetChunks()[0]->GetData(), total));
    }

    Y_UNIT_TEST(MultiChunkDivergentScalarType) {
        const auto settings = MakeSettings();

        const std::vector<TString> docs0 = { R"({"a":"xxxx"})", R"({"a":"yyyy"})", R"({"a":"zzzz"})", R"({"a":"wwww"})" };
        const std::vector<TString> docs1 = { R"({"a":1})", R"({"a":2})", R"({"a":3})", R"({"a":4})" };

        auto chunk0 = BuildChunk(docs0, settings);
        auto chunk1 = BuildChunk(docs1, settings);
        UNIT_ASSERT_VALUES_EQUAL(chunk0->GetColumnsData().GetStats().GetColumnsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL_C((ui32)chunk0->GetColumnsData().GetStats().GetValueType(0), (ui32)EValueType::String,
            "chunk0 key 'a' must be a native String column");
        UNIT_ASSERT_VALUES_EQUAL_C((ui32)chunk1->GetColumnsData().GetStats().GetValueType(0), (ui32)EValueType::Double,
            "chunk1 key 'a' must be a native Double column");

        auto merged = MergeChunks({ chunk0, chunk1 }, settings);

        // The merged portion must round-trip to the input documents.
        const TString expected =
            RenderDocs(BuildChunk({ docs0[0], docs0[1], docs0[2], docs0[3], docs1[0], docs1[1], docs1[2], docs1[3] }, settings));
        UNIT_ASSERT_VALUES_EQUAL(RenderDocs(merged), expected);
    }

    Y_UNIT_TEST(DictionaryNativeString) {
        auto settings = MakeSettings();
        settings.SetDictionaryUniqueFraction(1.0);
        const std::vector<TString> docs = { R"({"a":"xxxx"})", R"({"a":"xxxx"})", R"({"a":"yyyy"})", R"({"a":"yyyy"})" };
        auto chunk = BuildChunk(docs, settings);

        auto merged = MergeChunks({ chunk }, settings);
        const auto& stats = merged->GetColumnsData().GetStats();
        UNIT_ASSERT_VALUES_EQUAL(stats.GetAccessorType(0), IChunkedArray::EType::Dictionary);
        UNIT_ASSERT_VALUES_EQUAL(RenderDocs(merged), RenderDocs(BuildChunk(docs, settings)));
    }

    Y_UNIT_TEST(DictionaryNativeStringWithNulls) {
        auto settings = MakeSettings();
        settings.SetDictionaryUniqueFraction(1.0);
        const std::vector<TString> docs = { R"({"a":"xxxx"})", "{}", R"({"a":"yyyy"})", "{}" };
        auto merged = MergeChunks({ BuildChunk(docs, settings) }, settings);
        const auto& stats = merged->GetColumnsData().GetStats();
        UNIT_ASSERT_VALUES_EQUAL(stats.GetAccessorType(0), IChunkedArray::EType::Dictionary);

        auto path = merged->GetPathAccessor("$.a", docs.size()).DetachResult();
        TStringBuilder values;
        path->VisitValues([&](const std::optional<TStringBuf>& value) {
            values << (value ? *value : TStringBuf("<null>")) << ';';
        });
        UNIT_ASSERT_VALUES_EQUAL(values, "xxxx;<null>;yyyy;<null>;");
        UNIT_ASSERT_VALUES_EQUAL(RenderDocs(merged), RenderDocs(BuildChunk(docs, settings)));
    }

    // Bug-triggering scenario:
    // Source portions expose an ancestor and its child as separated columns.
    // The unselected path must move to Others in merged portion; it must never be remapped to the selected path.
    Y_UNIT_TEST(ChildAndAncestorPathMix) {
        auto settings = MakeSettings();
        settings.SetColumnsLimit(1);
        const std::vector<TString> ancestorDocs = {
            R"({"a":["xxxxxxxxxxxxxxxx"]})",
            R"({"a":["yyyyyyyyyyyyyyyy"]})",
        };
        const std::vector<TString> childDocs = {
            R"({"a":{"b":1}})",
            R"({"a":{"b":2}})",
        };

        {
            auto merged = MergeChunks({ BuildChunk(ancestorDocs, settings), BuildChunk(childDocs, settings) }, settings);
            const auto& stats = merged->GetColumnsData().GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetColumnsCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetColumnNameString(0), R"("a")");
            UNIT_ASSERT_VALUES_EQUAL(
                RenderDocs(merged), R"({"a":["xxxxxxxxxxxxxxxx"]};{"a":["yyyyyyyyyyyyyyyy"]};{"a":{"b":1}};{"a":{"b":2}};)");
        }

        const std::vector<TString> scalarAncestorDocs = {
            R"({"a":1})",
            R"({"a":2})",
        };
        const std::vector<TString> largeChildDocs = {
            R"({"a":{"b":"xxxxxxxxxxxxxxxx"}})",
            R"({"a":{"b":"yyyyyyyyyyyyyyyy"}})",
        };

        {
            auto merged = MergeChunks({ BuildChunk(scalarAncestorDocs, settings), BuildChunk(largeChildDocs, settings) }, settings);
            const auto& stats = merged->GetColumnsData().GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetColumnsCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetColumnNameString(0), R"("a"."b")");
            UNIT_ASSERT_VALUES_EQUAL(RenderDocs(merged), R"({"a":1};{"a":2};{"a":{"b":"xxxxxxxxxxxxxxxx"}};{"a":{"b":"yyyyyyyyyyyyyyyy"}};)");
        }
    }
}
