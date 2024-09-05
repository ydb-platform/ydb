#include "table_writer_impl.h"

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
            TChangeRecordBuilderContextTrait<NBackup::NImpl::TChangeRecord> ctx(EWriterType::Backup);
            record->Serialize(result, ctx);

            TVector<TCell> outCells{
                TCell::Make<ui64>(4567),
                TCell::Make<bool>(false),
            };

            TString out = TSerializedCellVec::Serialize(outCells);

            UNIT_ASSERT_VALUES_EQUAL(TSerializedCellVec::Serialize(keyCells), result.GetKey());
            UNIT_ASSERT(result.GetUpsert().TagsSize() == 2);
            UNIT_ASSERT(result.GetUpsert().GetTags(0) == 1);
            UNIT_ASSERT(result.GetUpsert().GetTags(1) == 123);
            UNIT_ASSERT_VALUES_EQUAL(out, result.GetUpsert().GetData());
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
            TChangeRecordBuilderContextTrait<NBackup::NImpl::TChangeRecord> ctx(EWriterType::Backup);
            record->Serialize(result, ctx);

            TVector<TCell> outCells{
                TCell(),
                TCell::Make<bool>(true),
            };

            TString out = TSerializedCellVec::Serialize(outCells);

            UNIT_ASSERT_VALUES_EQUAL(TSerializedCellVec::Serialize(keyCells), result.GetKey());
            UNIT_ASSERT(result.GetUpsert().TagsSize() == 2);
            UNIT_ASSERT(result.GetUpsert().GetTags(1) == 123);
            UNIT_ASSERT(result.GetUpsert().GetTags(0) == 1);
            UNIT_ASSERT_VALUES_EQUAL(out, result.GetUpsert().GetData());
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
            TChangeRecordBuilderContextTrait<NBackup::NImpl::TChangeRecord> ctx(EWriterType::Restore);
            record->Serialize(result, ctx);

            UNIT_ASSERT_VALUES_EQUAL(TSerializedCellVec::Serialize(keyCells), result.GetKey());
            UNIT_ASSERT(result.GetUpsert().TagsSize() == 1);
            UNIT_ASSERT(result.GetUpsert().GetTags(0) == 1);
            UNIT_ASSERT_VALUES_EQUAL(upsert.GetData(), result.GetUpsert().GetData());
        }

    }
}

} // namespace NKikimr::NBackup::NImpl
