#pragma once

#include <ydb/core/tablet_flat/flat_dbase_apply.h>
#include <ydb/core/tablet_flat/flat_database.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/tablet/tablet_exception.h>
#include <ydb/library/ydb_issue/proto/issue_id.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>
#include <ydb/core/formats/factory.h>
#include <ydb/core/base/appdata.h>

namespace NKikimr {
namespace NMiniKQL {

class TFlatLocalReadColumns : public NTabletFlatExecutor::ITransaction {
public:
    TFlatLocalReadColumns(TActorId sender, TEvTablet::TEvLocalReadColumns::TPtr &ev)
        : Sender(sender)
        , Ev(ev)
    {}

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override {
        Y_UNUSED(ctx);

        Response.Reset(new TEvTablet::TEvLocalReadColumnsResponse);

        if (Ev->Get()->Record.GetMaxRows()) {
            RowsLimit = Ev->Get()->Record.GetMaxRows();
        }

        if (Ev->Get()->Record.GetMaxBytes()) {
            BytesLimit = Ev->Get()->Record.GetMaxBytes();
        }

        TString tableName = Ev->Get()->Record.GetTableName();
        auto nameIt = txc.DB.GetScheme().TableNames.find(tableName);

        if (nameIt == txc.DB.GetScheme().TableNames.end()) {
            SetError(Ydb::StatusIds::SCHEME_ERROR, Sprintf("Unknown table %s", tableName.c_str()));
            return true;
        }

        const ui64 tableId = nameIt->second;
        const auto* tableInfo = txc.DB.GetScheme().GetTableInfo(tableId);

        TString format = "clickhouse_native";
        if (Ev->Get()->Record.HasFormat()) {
            format = Ev->Get()->Record.GetFormat();
        }
        std::unique_ptr<IBlockBuilder> blockBuilder = AppData()->FormatFactory->CreateBlockBuilder(format);
        if (!blockBuilder) {
            SetError(Ydb::StatusIds::BAD_REQUEST,
                     Sprintf("Unsupported block format \"%s\"", format.data()));
            return true;
        }

        TVector<NScheme::TTypeInfo> keyColumnTypes;
        TSmallVec<TRawTypeValue> keyFrom;
        TSmallVec<TRawTypeValue> keyTo;
        bool inclusiveFrom = Ev->Get()->Record.GetFromKeyInclusive();
        bool inclusiveTo = Ev->Get()->Record.GetToKeyInclusive();

        // TODO: check schemas
        for (ui32 keyColId : tableInfo->KeyColumns) {
            keyColumnTypes.push_back(tableInfo->Columns.FindPtr(keyColId)->PType);
        }

        TSerializedCellVec fromKeyCells(Ev->Get()->Record.GetFromKey());
        keyFrom.clear();
        for (ui32 i = 0; i < fromKeyCells.GetCells().size(); ++i) {
            keyFrom.push_back(TRawTypeValue(fromKeyCells.GetCells()[i].AsRef(), keyColumnTypes[i].GetTypeId()));
        }
        keyFrom.resize(tableInfo->KeyColumns.size());

        TSerializedCellVec toKeyCells(Ev->Get()->Record.GetToKey());
        keyTo.clear();
        for (ui32 i = 0; i < toKeyCells.GetCells().size(); ++i) {
            keyTo.push_back(TRawTypeValue(toKeyCells.GetCells()[i].AsRef(), keyColumnTypes[i].GetTypeId()));
        }

        TVector<NTable::TTag> valueColumns;
        TVector<std::pair<TString, NScheme::TTypeInfo>> columns;
        for (const auto& col : Ev->Get()->Record.GetColumns()) {
            const auto* colNameInfo = tableInfo->ColumnNames.FindPtr(col);
            if (!colNameInfo) {
                SetError(Ydb::StatusIds::SCHEME_ERROR, Sprintf("Unknown column %s", col.c_str()));
                return true;
            }

            NTable::TTag colId = *colNameInfo;
            valueColumns.push_back(colId);
            columns.push_back({col, tableInfo->Columns.FindPtr(colId)->PType});
        }

        // TODO: make sure KeyFrom and KeyTo properly reference non-inline cells data

        if (!Precharge(txc.DB, tableId, keyFrom, keyTo, valueColumns))
            return false;

        size_t rows = 0;
        size_t bytes = 0;

        ui64 rowsPerBlock = Ev->Get()->Record.GetMaxRows() ? Ev->Get()->Record.GetMaxRows() : 64000;
        ui64 bytesPerBlock = 64000;

        bool shardFinished = false;

        {
            TString err;
            if (!blockBuilder->Start(columns, rowsPerBlock, bytesPerBlock, err)) {
                SetError(Ydb::StatusIds::BAD_REQUEST,
                         Sprintf("Failed to create block builder \"%s\"", err.data()));
                return true;
            }

            NTable::TKeyRange iterRange;
            iterRange.MinKey = keyFrom;
            iterRange.MinInclusive = inclusiveFrom;
            iterRange.MaxKey = keyTo;
            iterRange.MaxInclusive = inclusiveTo;

            auto iter = txc.DB.IterateRange(tableId, iterRange, valueColumns);

            TString lastKeySerialized;
            bool lastKeyInclusive = true;
            while (iter->Next(NTable::ENext::All) == NTable::EReady::Data) {
                TDbTupleRef rowKey = iter->GetKey();
                lastKeySerialized = TSerializedCellVec::Serialize(rowKey.Cells());

                // Skip erased row
                if (iter->Row().GetRowState() == NTable::ERowOp::Erase) {
                    continue;
                }

                TDbTupleRef rowValues = iter->GetValues();

                blockBuilder->AddRow(rowKey, rowValues);

                rows++;
                bytes = blockBuilder->Bytes();

                if (rows >= RowsLimit || bytes >= BytesLimit)
                    break;
            }

            // We don't want to do many restarts if pages weren't precharged
            // So we just return whatever we read so far and the client can request more rows
            if (iter->Last() == NTable::EReady::Page && rows < 1000 && bytes < 100000 && Restarts < 1) {
                ++Restarts;
                return false;
            }

            if (iter->Last() == NTable::EReady::Gone) {
                shardFinished = true;
                lastKeySerialized.clear();
                lastKeyInclusive = false;
            }

            TString buffer = blockBuilder->Finish();
            buffer.resize(blockBuilder->Bytes());

            Response->Record.SetStatus(Ydb::StatusIds::SUCCESS);
            Response->Record.SetBlocks(buffer);
            Response->Record.SetLastKey(lastKeySerialized);
            Response->Record.SetLastKeyInclusive(lastKeyInclusive);
            Response->Record.SetEndOfShard(shardFinished);
        }

        return true;
    }

    void Complete(const TActorContext &ctx) override {
        MakeResponse(ctx);
    }

private:
    bool Precharge(NTable::TDatabase& db, ui32 localTid,
                   const TSmallVec<TRawTypeValue>& keyFrom,
                   const TSmallVec<TRawTypeValue>& keyTo,
                   const TVector<NTable::TTag>& valueColumns) {
        bool ready =  db.Precharge(localTid,
                                   keyFrom,
                                   keyTo,
                                   valueColumns,
                                   0,
                                   RowsLimit, BytesLimit);
        return ready;
    }

    void SetError(ui32 status, TString descr) {
        Response->Record.SetStatus(status);
        Response->Record.SetErrorDescription(descr);
    }

    void MakeResponse(const TActorContext &ctx) {
        ctx.Send(Sender, Response.Release());
    }

private:
    const TActorId Sender;
    TEvTablet::TEvLocalReadColumns::TPtr Ev;
    TAutoPtr<TEvTablet::TEvLocalReadColumnsResponse> Response;

    ui64 RowsLimit = 10000000;
    ui64 BytesLimit = 30*1024*1024;
    ui64 Restarts = 0;
};

}}
