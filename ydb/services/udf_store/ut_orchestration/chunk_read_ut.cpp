#include <ydb/services/udf_store/table_query.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/cast.h>

namespace NKikimr::NUdfStore::NTableQuery {
namespace {

Ydb::Table::ExecuteDataQueryResponse MakePage(const TVector<ui64>& indices, bool truncated = false) {
    Ydb::Table::ExecuteQueryResult result;
    auto& rows = *result.add_result_sets();
    auto& indexColumn = *rows.add_columns();
    indexColumn.set_name("chunk_idx");
    indexColumn.mutable_type()->set_type_id(Ydb::Type::UINT64);
    auto& dataColumn = *rows.add_columns();
    dataColumn.set_name("data");
    dataColumn.mutable_type()->set_type_id(Ydb::Type::STRING);
    rows.set_truncated(truncated);
    for (ui64 index : indices) {
        auto& row = *rows.add_rows();
        row.add_items()->set_uint64_value(index);
        row.add_items()->set_bytes_value(ToString(index));
    }
    Ydb::Table::ExecuteDataQueryResponse response;
    response.mutable_operation()->set_status(Ydb::StatusIds::SUCCESS);
    response.mutable_operation()->mutable_result()->PackFrom(result);
    return response;
}

} // namespace

Y_UNIT_TEST_SUITE(TWasmChunkReads) {
    Y_UNIT_TEST(AppendsMultiplePages) {
        for (auto append : {AppendSourceChunksResponse, AppendArtifactChunksResponse}) {
            TVector<TString> chunks;
            UNIT_ASSERT(append(MakePage({0, 1, 2, 3}), chunks));
            UNIT_ASSERT(append(MakePage({4, 5, 6, 7}), chunks));
            UNIT_ASSERT(append(MakePage({8, 9}), chunks));
            UNIT_ASSERT_VALUES_EQUAL(chunks.size(), 10);
            for (ui64 index = 0; index < chunks.size(); ++index) {
                UNIT_ASSERT_VALUES_EQUAL(chunks[index], ToString(index));
            }
        }
    }

    Y_UNIT_TEST(EmptyPageAfterFullPagePreservesData) {
        TVector<TString> chunks;
        UNIT_ASSERT(AppendSourceChunksResponse(MakePage({0, 1, 2, 3}), chunks));
        const auto expected = chunks;
        UNIT_ASSERT(AppendSourceChunksResponse(MakePage({}), chunks));
        UNIT_ASSERT(chunks == expected);
    }

    Y_UNIT_TEST(RejectsGapsDuplicatesAndReplayedPages) {
        for (auto append : {AppendSourceChunksResponse, AppendArtifactChunksResponse}) {
            for (const TVector<ui64>& indices : {TVector<ui64>{4, 6}, {4, 4}, {0, 1}, {5, 4}}) {
                TVector<TString> chunks;
                UNIT_ASSERT(append(MakePage({0, 1, 2, 3}), chunks));
                const auto expected = chunks;
                UNIT_ASSERT(!append(MakePage(indices), chunks));
                UNIT_ASSERT(chunks == expected);
            }
        }
    }

    Y_UNIT_TEST(RejectsTruncatedAndOversizedPages) {
        for (auto append : {AppendSourceChunksResponse, AppendArtifactChunksResponse}) {
            TVector<TString> chunks;
            UNIT_ASSERT(!append(MakePage({0, 1}, true), chunks));
            UNIT_ASSERT(!append(MakePage({0, 1, 2, 3, 4}), chunks));
            UNIT_ASSERT(chunks.empty());
        }
    }

    Y_UNIT_TEST(RejectsFailedRequest) {
        auto response = MakePage({0});
        response.mutable_operation()->set_status(Ydb::StatusIds::PRECONDITION_FAILED);
        TVector<TString> chunks;
        UNIT_ASSERT(!AppendSourceChunksResponse(response, chunks));
        UNIT_ASSERT(!AppendArtifactChunksResponse(response, chunks));
        UNIT_ASSERT(chunks.empty());
    }
}

} // namespace NKikimr::NUdfStore::NTableQuery
