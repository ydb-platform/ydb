#include <ydb/core/grpc_services/rpc_load_rows.h>

#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme/scheme_type_info.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;

Y_UNIT_TEST_SUITE(RowsToBatchTests) {

    // Helper: build a minimal schema and valid rows for testing
    static void MakeSchemaAndRows(
        TVector<std::pair<TString, NScheme::TTypeInfo>>& ydbSchema,
        TVector<std::pair<TSerializedCellVec, TString>>& rows,
        bool corruptValues = false)
    {
        // Schema: one key column (Uint64) and one value column (Utf8)
        ydbSchema.clear();
        ydbSchema.emplace_back("key", NScheme::TTypeInfo(NScheme::NTypeIds::Uint64));
        ydbSchema.emplace_back("value", NScheme::TTypeInfo(NScheme::NTypeIds::Utf8));

        rows.clear();

        // Create a valid row
        ui64 keyVal = 42;
        TCell keyCell(reinterpret_cast<const char*>(&keyVal), sizeof(keyVal));
        TSerializedCellVec serializedKey(TConstArrayRef<TCell>(&keyCell, 1));

        TString valueStr = "hello";
        TCell valueCell(valueStr.data(), valueStr.size());
        TString serializedValue = TSerializedCellVec::Serialize(TConstArrayRef<TCell>(&valueCell, 1));

        if (corruptValues) {
            // Corrupt the serialized value data
            serializedValue = "\x01\x02\x03";
        }

        rows.emplace_back(std::move(serializedKey), std::move(serializedValue));
    }

    Y_UNIT_TEST(ValidDataProducesBatch) {
        TVector<std::pair<TString, NScheme::TTypeInfo>> ydbSchema;
        TVector<std::pair<TSerializedCellVec, TString>> rows;
        MakeSchemaAndRows(ydbSchema, rows, /* corruptValues = */ false);

        std::set<std::string> notNullColumns;
        TString errorMessage;

        auto batch = NGRpcService::RowsToBatch(rows, ydbSchema, notNullColumns, true, errorMessage);

        UNIT_ASSERT_C(batch, "RowsToBatch should succeed for valid data, error: " + errorMessage);
        UNIT_ASSERT_VALUES_EQUAL(batch->num_rows(), 1);
        UNIT_ASSERT_VALUES_EQUAL(batch->num_columns(), 2);
        UNIT_ASSERT(errorMessage.empty());
    }

    Y_UNIT_TEST(CorruptedValueReturnsError) {
        TVector<std::pair<TString, NScheme::TTypeInfo>> ydbSchema;
        TVector<std::pair<TSerializedCellVec, TString>> rows;
        MakeSchemaAndRows(ydbSchema, rows, /* corruptValues = */ true);

        std::set<std::string> notNullColumns;
        TString errorMessage;

        auto batch = NGRpcService::RowsToBatch(rows, ydbSchema, notNullColumns, true, errorMessage);

        UNIT_ASSERT(!batch);
        UNIT_ASSERT_STRING_CONTAINS(errorMessage, "Cannot parse serialized cell vec");
    }

    Y_UNIT_TEST(EmptyRowsDoesNotCrash) {
        TVector<std::pair<TString, NScheme::TTypeInfo>> ydbSchema;
        ydbSchema.emplace_back("key", NScheme::TTypeInfo(NScheme::NTypeIds::Uint64));
        ydbSchema.emplace_back("value", NScheme::TTypeInfo(NScheme::NTypeIds::Utf8));

        TVector<std::pair<TSerializedCellVec, TString>> rows;
        std::set<std::string> notNullColumns;
        TString errorMessage;

        // Should not crash; result may be nullptr for zero rows
        auto batch = NGRpcService::RowsToBatch(rows, ydbSchema, notNullColumns, true, errorMessage);
        Y_UNUSED(batch);
    }
}
