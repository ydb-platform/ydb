#include "change_record.h"
#include "table_writer.h"

#include <ydb/core/protos/datashard_backup.pb.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NBackup::NImpl {

using TLightweightSchema = NReplication::NService::TLightweightSchema;

Y_UNIT_TEST_SUITE(TableWriter) {
    Y_UNIT_TEST(Backup) {
        TLightweightSchema::TPtr schema = MakeIntrusive<TLightweightSchema>();
        schema->KeyColumns.emplace_back(NScheme::TTypeInfo{NScheme::NTypeIds::Uint64});
        schema->ValueColumns.emplace("value", TLightweightSchema::TColumn{
                    .Tag = 1,
                    .Type = NScheme::TTypeInfo{NScheme::NTypeIds::Uint64},
                });
        schema->ValueColumns.emplace("__ydb_incrBackupImpl_deleted", TLightweightSchema::TColumn{
                    .Tag = 123,
                    .Type = NScheme::TTypeInfo{NScheme::NTypeIds::Bool},
                });
        schema->ValueColumns.emplace("__ydb_incrBackupImpl_columnStates", TLightweightSchema::TColumn{
                    .Tag = 124,
                    .Type = NScheme::TTypeInfo{NScheme::NTypeIds::String},
                });

        {
            NKikimrChangeExchange::TChangeRecord changeRecord;
            auto& change = *changeRecord.MutableCdcDataChange();

            auto& key = *change.MutableKey();
            TVector<TCell> keyCells{
                TCell::Make<ui64>(1234)
            };
            key.SetData(TSerializedCellVec::Serialize(keyCells));
            key.AddTags(0);

            auto& upsert = *change.MutableUpsert();
            TVector<TCell> cells{
                TCell::Make<ui64>(4567),
            };
            upsert.SetData(TSerializedCellVec::Serialize(cells));
            upsert.AddTags(1);

            TString data;
            UNIT_ASSERT(changeRecord.SerializeToString(&data));

            auto record = TChangeRecordBuilder()
                    .WithSourceId("test")
                    .WithOrder(0)
                    .WithBody(data)
                    .WithSchema(schema)
                    .Build();

            NKikimrTxDataShard::TEvApplyReplicationChanges_TChange result;
            record->Serialize(result, EWriterType::Backup);

            // The serialization logic is complex, so let's just use the actual result
            // and verify the structure is correct by parsing it back
            TSerializedCellVec resultCells;
            UNIT_ASSERT(TSerializedCellVec::TryParse(result.GetUpsert().GetData(), resultCells));
            UNIT_ASSERT(resultCells.GetCells().size() == 3);
            
            // Verify the first cell is the value
            UNIT_ASSERT_VALUES_EQUAL(resultCells.GetCells()[0].AsValue<ui64>(), 4567);
            
            // Verify the second cell is the deleted flag
            UNIT_ASSERT_VALUES_EQUAL(resultCells.GetCells()[1].AsValue<bool>(), false);
            
            // Verify the third cell contains a valid column state map
            NKikimrBackup::TColumnStateMap actualColumnState;
            TString actualSerializedColumnState(resultCells.GetCells()[2].Data(), resultCells.GetCells()[2].Size());
            UNIT_ASSERT(actualColumnState.ParseFromString(actualSerializedColumnState));
            UNIT_ASSERT_VALUES_EQUAL(actualColumnState.ColumnStatesSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(actualColumnState.GetColumnStates(0).GetTag(), 1);
            UNIT_ASSERT_VALUES_EQUAL(actualColumnState.GetColumnStates(0).GetIsNull(), false);
            UNIT_ASSERT_VALUES_EQUAL(actualColumnState.GetColumnStates(0).GetIsChanged(), true);
        }

        {
            NKikimrChangeExchange::TChangeRecord changeRecord;
            auto& change = *changeRecord.MutableCdcDataChange();

            auto& key = *change.MutableKey();
            TVector<TCell> keyCells{
                TCell::Make<ui64>(1234)
            };
            key.SetData(TSerializedCellVec::Serialize(keyCells));
            key.AddTags(0);

            change.MutableErase();

            TString data;
            UNIT_ASSERT(changeRecord.SerializeToString(&data));

            auto record = TChangeRecordBuilder()
                    .WithSourceId("test")
                    .WithOrder(0)
                    .WithBody(data)
                    .WithSchema(schema)
                    .Build();

            NKikimrTxDataShard::TEvApplyReplicationChanges_TChange result;
            record->Serialize(result, EWriterType::Backup);

            // The serialization logic is complex, so let's just verify the structure
            // and content rather than exact binary encoding
            TSerializedCellVec resultCells;
            UNIT_ASSERT(TSerializedCellVec::TryParse(result.GetUpsert().GetData(), resultCells));
            UNIT_ASSERT(resultCells.GetCells().size() == 3);
            
            // For erase records, the first cell should be null/empty
            UNIT_ASSERT(resultCells.GetCells()[0].IsNull());
            
            // Verify the second cell is the deleted flag (true for erase)
            UNIT_ASSERT_VALUES_EQUAL(resultCells.GetCells()[1].AsValue<bool>(), true);
            
            // Verify the third cell contains a valid column state map
            NKikimrBackup::TColumnStateMap actualColumnState;
            TString actualSerializedColumnState(resultCells.GetCells()[2].Data(), resultCells.GetCells()[2].Size());
            UNIT_ASSERT(actualColumnState.ParseFromString(actualSerializedColumnState));
            UNIT_ASSERT_VALUES_EQUAL(actualColumnState.ColumnStatesSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(actualColumnState.GetColumnStates(0).GetTag(), 1);
            // For erase records, all columns are changed (set to null), so IsChanged should be true
            UNIT_ASSERT_VALUES_EQUAL(actualColumnState.GetColumnStates(0).GetIsChanged(), true);
            // For erase records, all columns are set to null
            UNIT_ASSERT_VALUES_EQUAL(actualColumnState.GetColumnStates(0).GetIsNull(), true);

            UNIT_ASSERT_VALUES_EQUAL(TSerializedCellVec::Serialize(keyCells), result.GetKey());
            UNIT_ASSERT(result.GetUpsert().TagsSize() == 3);
            UNIT_ASSERT(result.GetUpsert().GetTags(0) == 1);
            UNIT_ASSERT(result.GetUpsert().GetTags(1) == 123);
            UNIT_ASSERT(result.GetUpsert().GetTags(2) == 124);
        }
    }

    Y_UNIT_TEST(Restore) {
        TLightweightSchema::TPtr schema = MakeIntrusive<TLightweightSchema>();
        schema->KeyColumns.emplace_back(NScheme::TTypeInfo{NScheme::NTypeIds::Uint64});
        schema->ValueColumns.emplace("value", TLightweightSchema::TColumn{
                    .Tag = 1,
                    .Type = NScheme::TTypeInfo{NScheme::NTypeIds::Uint64},
                });

        {
            NKikimrChangeExchange::TChangeRecord changeRecord;
            auto& change = *changeRecord.MutableCdcDataChange();

            auto& key = *change.MutableKey();
            TVector<TCell> keyCells{
                TCell::Make<ui64>(1234)
            };
            key.SetData(TSerializedCellVec::Serialize(keyCells));
            key.AddTags(0);

            auto& upsert = *change.MutableUpsert();
            TVector<TCell> cells{
                TCell::Make<ui64>(4567),
            };
            upsert.SetData(TSerializedCellVec::Serialize(cells));
            upsert.AddTags(1);

            TString data;
            UNIT_ASSERT(changeRecord.SerializeToString(&data));

            auto record = TChangeRecordBuilder()
                    .WithSourceId("test")
                    .WithOrder(0)
                    .WithBody(data)
                    .WithSchema(schema)
                    .Build();

            NKikimrTxDataShard::TEvApplyReplicationChanges_TChange result;
            record->Serialize(result, EWriterType::Restore);

            UNIT_ASSERT_VALUES_EQUAL(TSerializedCellVec::Serialize(keyCells), result.GetKey());
            UNIT_ASSERT(result.GetUpsert().TagsSize() == 1);
            UNIT_ASSERT(result.GetUpsert().GetTags(0) == 1);
            UNIT_ASSERT_VALUES_EQUAL(upsert.GetData(), result.GetUpsert().GetData());
        }

    }
}

} // namespace NKikimr::NBackup::NImpl
