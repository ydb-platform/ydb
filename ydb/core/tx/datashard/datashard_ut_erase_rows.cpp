#include "defs.h"
#include "datashard_distributed_erase.h"
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include "datashard_ut_common_kqp.h"

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/tx.h>

#include <util/generic/bitmap.h>
#include <util/string/printf.h>
#include <util/string/strip.h>

namespace NKikimr {

using namespace NDataShard::NKqpHelpers;
using namespace Tests;

struct TProto {
    using TEvEraseRequest = NKikimrTxDataShard::TEvEraseRowsRequest;
    using TEvEraseResponse = NKikimrTxDataShard::TEvEraseRowsResponse;

    using TEvCondEraseRequest = NKikimrTxDataShard::TEvConditionalEraseRowsRequest;
    using TEvCondEraseResponse = NKikimrTxDataShard::TEvConditionalEraseRowsResponse;

    using TLimits = TEvCondEraseRequest::TLimits;

    using TEvProposeTx = NKikimrTxDataShard::TEvProposeTransaction;
    using TEvProposeTxResult = NKikimrTxDataShard::TEvProposeTransactionResult;

    using TDistributedEraseTx = NKikimrTxDataShard::TDistributedEraseTransaction;
};

using EUnit = NKikimrSchemeOp::TTTLSettings::EUnit;
struct TUnit {
    static constexpr EUnit AUTO = NKikimrSchemeOp::TTTLSettings::UNIT_AUTO;
    static constexpr EUnit SECONDS = NKikimrSchemeOp::TTTLSettings::UNIT_SECONDS;
    static constexpr EUnit MILLISECONDS = NKikimrSchemeOp::TTTLSettings::UNIT_MILLISECONDS;
    static constexpr EUnit MICROSECONDS = NKikimrSchemeOp::TTTLSettings::UNIT_MICROSECONDS;
    static constexpr EUnit NANOSECONDS = NKikimrSchemeOp::TTTLSettings::UNIT_NANOSECONDS;
};

namespace {

static ui64 currentTime = 1704067200000000ull; //TInstant::ParseIso8601("2024-01-01").GetValue();

void CreateTable(TServer::TPtr server, const TActorId& sender, const TString& root,
        const TString& name, const TString& ttlColType = "Timestamp") {
    auto opts = TShardedTableOptions()
        .EnableOutOfOrder(false)
        .Columns({
            {"key", "Uint32", true, false},
            {"value", ttlColType, false, false}
        });
    CreateShardedTable(server, sender, root, name, opts);
}

void CreateIndexedTable(TServer::TPtr server, const TActorId& sender, const TString& root,
        const TString& name, const TString& ttlColType = "Timestamp") {
    auto opts = TShardedTableOptions()
        .EnableOutOfOrder(false)
        .Columns({
            {"key", "Uint32", true, false},
            {"skey", "Uint32", false, false},
            {"tkey", "Uint32", false, false},
            {"value", ttlColType, false, false}
        })
        .Indexes({
            {"by_skey", {"skey"}},
            {"by_tkey", {"tkey"}}
        });
    CreateShardedTable(server, sender, root, name, opts);
}

using TKeyCellsMaker = std::function<TVector<TCell>(ui32)>;

TVector<TCell> MakeKeyCells(ui32 key) {
    return {TCell::Make(key)};
}

TVector<TCell> KeyCellsRepeater(ui32 key) {
    return {TCell::Make(key), TCell::Make(key)};
};

TVector<TString> SerializeKeys(const TVector<ui32>& keys, TKeyCellsMaker makeKeyCells = &MakeKeyCells) {
    TVector<TString> serializedKeys;

    for (const ui32 key : keys) {
        serializedKeys.emplace_back(TSerializedCellVec::Serialize(makeKeyCells(key)));
    }

    return serializedKeys;
}

TProto::TEvEraseRequest MakeEraseRowsRequest(
        const TTableId& tableId,
        const TVector<ui32>& keyTags,
        const TVector<TString>& keys)
{
    TProto::TEvEraseRequest request;

    request.SetTableId(tableId.PathId.LocalPathId);
    request.SetSchemaVersion(tableId.SchemaVersion);

    for (const ui32 tag : keyTags) {
        request.AddKeyColumnIds(tag);
    }

    for (const auto& key : keys) {
        request.AddKeyColumns(key);
    }

    return request;
}

template <typename TEvResponse, typename TDerived>
class TRequestRunner: public TActorBootstrapped<TDerived> {
    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->Status == NKikimrProto::OK) {
            return;
        }

        Bootstrap();
    }

protected:
    void Reply(typename TEvResponse::TPtr& ev) {
        TlsActivationContext->AsActorContext().Send(ev->Forward(ReplyTo));
    }

    virtual void Handle(typename TEvResponse::TPtr& ev) {
        Reply(ev);
        PassAway();
    }

    void PassAway() override {
        if (Pipe) {
            NTabletPipe::CloseAndForgetClient(this->SelfId(), Pipe);
        }

        IActor::PassAway();
    }

    virtual IEventBase* MakeRequest() const = 0;

public:
    explicit TRequestRunner(const TActorId& replyTo, ui64 tabletID)
        : ReplyTo(replyTo)
        , TabletID(tabletID)
    {
    }

    void Bootstrap() {
        if (Pipe) {
            NTabletPipe::CloseAndForgetClient(this->SelfId(), Pipe);
        }

        Pipe = this->Register(NTabletPipe::CreateClient(this->SelfId(), TabletID));
        NTabletPipe::SendData(this->SelfId(), Pipe, this->MakeRequest());

        this->Become(&TDerived::StateWork);
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            cFunc(TEvTabletPipe::TEvClientDestroyed::EventType, Bootstrap);
            hFunc(TEvResponse, Handle);
        }
    }

    using TBase = TRequestRunner<TEvResponse, TDerived>;

private:
    const TActorId ReplyTo;
    const ui64 TabletID;

    TActorId Pipe;
};

void EraseRows(
        TServer::TPtr server, const TActorId& sender, const TString& path,
        const TTableId& tableId, TVector<ui32> keyTags, TVector<TString> keys,
        ui32 status = TProto::TEvEraseResponse::OK, const TString& error = "")
{
    using TEvRequest = TEvDataShard::TEvEraseRowsRequest;
    using TEvResponse = TEvDataShard::TEvEraseRowsResponse;

    class TEraser: public TRequestRunner<TEvResponse, TEraser> {
    public:
        explicit TEraser(
                const TActorId& replyTo, ui64 tabletID,
                const TTableId& tableId, TVector<ui32> keyTags, TVector<TString> keys)
            : TBase(replyTo, tabletID)
            , TableId(tableId)
            , KeyTags(std::move(keyTags))
            , Keys(std::move(keys))
        {
        }

        IEventBase* MakeRequest() const override {
            auto request = MakeHolder<TEvRequest>();
            request->Record = MakeEraseRowsRequest(TableId, KeyTags, Keys);
            return request.Release();
        }

    private:
        const TTableId TableId;
        const TVector<ui32> KeyTags;
        const TVector<TString> Keys;
    };

    auto tabletIDs = GetTableShards(server, sender, path);
    UNIT_ASSERT_VALUES_EQUAL(tabletIDs.size(), 1);
    server->GetRuntime()->Register(new TEraser(sender, tabletIDs[0], tableId, std::move(keyTags), std::move(keys)));

    auto ev = server->GetRuntime()->GrabEdgeEventRethrow<TEvResponse>(sender);
    UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(ev->Get()->Record.GetStatus()), status);
    if (error) {
        UNIT_ASSERT_STRING_CONTAINS(ev->Get()->Record.GetErrorDescription(), error);
    }
}

void ConditionalEraseRows(
        TServer::TPtr server, const TActorId& sender, const TString& path,
        const TTableId& tableId, ui32 columnId, ui64 threshold, EUnit unit = TUnit::AUTO,
        const NDataShard::TIndexes& indexes = {}, const TProto::TLimits& limits = {},
        ui32 status = TProto::TEvCondEraseResponse::ACCEPTED, const TString& error = "") {
    using TEvRequest = TEvDataShard::TEvConditionalEraseRowsRequest;
    using TEvResponse = TEvDataShard::TEvConditionalEraseRowsResponse;

    class TEraser: public TRequestRunner<TEvResponse, TEraser> {
    protected:
        void Handle(TEvResponse::TPtr& ev) override {
            bool continue_ = false;

            switch (ev->Get()->Record.GetStatus()) {
            case TEvResponse::ProtoRecordType::ACCEPTED:
            case TEvResponse::ProtoRecordType::PARTIAL:
                continue_ = true;
                break;

            default:
                break;
            }

            Reply(ev);

            if (!continue_) {
                PassAway();
            }
        }

    public:
        explicit TEraser(
                const TActorId& replyTo, ui64 tabletID,
                const TTableId& tableId, ui32 columnId, ui64 threshold, EUnit unit,
                const TProto::TLimits& limits,
                const NDataShard::TIndexes& indexes)
            : TBase(replyTo, tabletID)
            , TableId(tableId)
            , ColumnId(columnId)
            , Threshold(threshold)
            , ColumnUnit(unit)
            , Limits(limits)
            , Indexes(indexes)
        {
        }

        IEventBase* MakeRequest() const override {
            auto request = MakeHolder<TEvRequest>();
            request->Record.SetTableId(TableId.PathId.LocalPathId);
            request->Record.SetSchemaVersion(TableId.SchemaVersion);
            request->Record.MutableExpiration()->SetColumnId(ColumnId);
            request->Record.MutableExpiration()->SetWallClockTimestamp(Threshold);
            request->Record.MutableExpiration()->SetColumnUnit(ColumnUnit);
            request->Record.MutableLimits()->CopyFrom(Limits);

            for (const auto& [indexId, columnIds] : Indexes) {
                auto& index = *request->Record.MutableIndexes()->Add();

                index.SetOwnerId(indexId.PathId.OwnerId);
                index.SetPathId(indexId.PathId.LocalPathId);
                index.SetSchemaVersion(indexId.SchemaVersion);

                for (const auto& [indexColumnId, mainColumnId] : columnIds) {
                    auto& keyMap = *index.MutableKeyMap()->Add();
                    keyMap.SetIndexColumnId(indexColumnId);
                    keyMap.SetMainColumnId(mainColumnId);
                }
            }

            return request.Release();
        }

    private:
        const TTableId TableId;
        const ui32 ColumnId;
        const ui64 Threshold;
        const EUnit ColumnUnit;
        const TProto::TLimits Limits;
        const NDataShard::TIndexes Indexes;
    };

    auto tabletIDs = GetTableShards(server, sender, path);
    UNIT_ASSERT_VALUES_EQUAL(tabletIDs.size(), 1);
    server->GetRuntime()->Register(new TEraser(sender, tabletIDs[0], tableId, columnId, threshold, unit, limits, indexes));

    auto ev = server->GetRuntime()->GrabEdgeEventRethrow<TEvResponse>(sender);
    UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(ev->Get()->Record.GetStatus()), status);
    if (error) {
        UNIT_ASSERT_STRING_CONTAINS(ev->Get()->Record.GetErrorDescription(), error);
    }
}

void DistributedEraseTx(
        TServer::TPtr server, const TActorId& sender, const TString& path,
        ui64 txId, const TProto::TDistributedEraseTx& tx,
        ui32 status = TProto::TEvProposeTxResult::PREPARED, const TString& error = "") {
    using TEvRequest = TEvDataShard::TEvProposeTransaction;
    using TEvResponse = TEvDataShard::TEvProposeTransactionResult;

    class TEraser: public TRequestRunner<TEvResponse, TEraser> {
    public:
        explicit TEraser(
                const TActorId& replyTo, ui64 tabletId,
                ui64 txId, const TProto::TDistributedEraseTx& tx)
            : TBase(replyTo, tabletId)
            , TxId(txId)
            , Tx(tx)
        {
        }

        IEventBase* MakeRequest() const override {
            return new TEvRequest(
                NKikimrTxDataShard::TX_KIND_DISTRIBUTED_ERASE, SelfId(), TxId, Tx.SerializeAsString()
            );
        }

    private:
        const ui64 TxId;
        const TProto::TDistributedEraseTx Tx;
    };

    auto tabletIds = GetTableShards(server, sender, path);
    UNIT_ASSERT_VALUES_EQUAL(tabletIds.size(), 1);
    server->GetRuntime()->Register(new TEraser(sender, tabletIds[0], txId, tx));

    auto ev = server->GetRuntime()->GrabEdgeEventRethrow<TEvResponse>(sender);
    UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(ev->Get()->Record.GetStatus()), status);
    if (error) {
        UNIT_ASSERT(ev->Get()->Record.ErrorSize());
        UNIT_ASSERT_STRING_CONTAINS(ev->Get()->Record.GetError(0).GetReason(), error);
    }
}

} // anonymous

Y_UNIT_TEST_SUITE(EraseRowsTests) {
    void EraseRowsShouldSuccess(TMaybe<ui64> injectSchemaVersion) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings
            .SetDomainName("Root")
            .SetUseRealThreads(false);

        TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        const TActorId sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
        InitRoot(server, sender);

        CreateTable(server, sender, "/Root", "table-1");
        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table-1` (key, value) VALUES
            (1, CAST("1970-01-01T00:00:00.000000Z" AS Timestamp)),
            (2, CAST("1990-03-01T00:00:00.000000Z" AS Timestamp)),
            (3, CAST("2020-04-15T00:00:00.000000Z" AS Timestamp));
        )");

        auto tableId = ResolveTableId(server, sender, "/Root/table-1");
        auto testTableId = TTableId(tableId.PathId, injectSchemaVersion.GetOrElse(tableId.SchemaVersion));
        EraseRows(server, sender, "/Root/table-1", testTableId, {1}, SerializeKeys({1, 2}));

        auto content = ReadShardedTable(server, "/Root/table-1");
        UNIT_ASSERT_STRINGS_EQUAL(StripInPlace(content), "key = 3, value = 2020-04-15T00:00:00.000000Z");
    }

    Y_UNIT_TEST(EraseRowsShouldSuccess) {
        EraseRowsShouldSuccess(Nothing());
    }

    Y_UNIT_TEST(EraseRowsShouldFailOnVariousErrors) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings
            .SetDomainName("Root")
            .SetUseRealThreads(false);

        TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        const TActorId sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
        InitRoot(server, sender);

        CreateTable(server, sender, "/Root", "table-1");
        auto tableId = ResolveTableId(server, sender, "/Root/table-1");

        EraseRows(server, sender, "/Root/table-1", TTableId(), {1}, SerializeKeys({1, 2}),
            TProto::TEvEraseResponse::SCHEME_ERROR, "Unknown table id");

        EraseRows(server, sender, "/Root/table-1", TTableId(tableId.PathId, 100), {1}, SerializeKeys({1, 2}),
            TProto::TEvEraseResponse::SCHEME_ERROR, "Schema version mismatch");

        EraseRows(server, sender, "/Root/table-1", tableId, {0, 1}, SerializeKeys({1, 2}),
            TProto::TEvEraseResponse::SCHEME_ERROR, "Key column count mismatch");

        EraseRows(server, sender, "/Root/table-1", tableId, {0}, SerializeKeys({1, 2}),
            TProto::TEvEraseResponse::SCHEME_ERROR, "Key column schema mismatch");

        EraseRows(server, sender, "/Root/table-1", tableId, {1}, {"trash"},
            TProto::TEvEraseResponse::BAD_REQUEST, "Cannot parse key");

        EraseRows(server, sender, "/Root/table-1", tableId, {1}, SerializeKeys({1, 2}, &KeyCellsRepeater),
            TProto::TEvEraseResponse::SCHEME_ERROR, "Cell count doesn't match row scheme");
    }

    void ConditionalEraseShouldSuccess(const TString& ttlColType, EUnit unit, const TString& toUpload, const TString& afterErase, const bool enableDatetime64 = false) {
        using TEvResponse = TEvDataShard::TEvConditionalEraseRowsResponse;

        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableTableDatetime64(enableDatetime64);

        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings
            .SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetFeatureFlags(featureFlags);

        TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        const TActorId sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
        InitRoot(server, sender);

        CreateTable(server, sender, "/Root", "table-1", ttlColType);
        ExecSQL(server, sender, toUpload);

        auto tableId = ResolveTableId(server, sender, "/Root/table-1");
        ConditionalEraseRows(server, sender, "/Root/table-1", tableId, 2, currentTime, unit);

        auto ev = server->GetRuntime()->GrabEdgeEventRethrow<TEvResponse>(sender);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetStatus(), TEvResponse::ProtoRecordType::OK);

        auto content = ReadShardedTable(server, "/Root/table-1");
        UNIT_ASSERT_STRINGS_EQUAL(StripInPlace(content), Strip(afterErase));
    }

    Y_UNIT_TEST(ConditionalEraseRowsShouldErase) {
        ConditionalEraseShouldSuccess("Timestamp", TUnit::AUTO, R"(
UPSERT INTO `/Root/table-1` (key, value) VALUES
(1, CAST("1970-01-01T00:00:00.000000Z" AS Timestamp)),
(2, CAST("1990-03-01T00:00:00.000000Z" AS Timestamp)),
(3, CAST("2030-04-15T00:00:00.000000Z" AS Timestamp)),
(4, NULL);
        )", R"(
key = 3, value = 2030-04-15T00:00:00.000000Z
key = 4, value = (empty maybe)
        )");
    }

    Y_UNIT_TEST(ConditionalEraseRowsShouldNotErase) {
        ConditionalEraseShouldSuccess("Timestamp", TUnit::AUTO, R"(
UPSERT INTO `/Root/table-1` (key, value) VALUES
(1, CAST("2030-04-15T00:00:00.000000Z" AS Timestamp)),
(2, CAST("2030-04-15T00:00:00.000000Z" AS Timestamp)),
(3, CAST("2030-04-15T00:00:00.000000Z" AS Timestamp));
        )", R"(
key = 1, value = 2030-04-15T00:00:00.000000Z
key = 2, value = 2030-04-15T00:00:00.000000Z
key = 3, value = 2030-04-15T00:00:00.000000Z
        )");
    }

    Y_UNIT_TEST(ConditionalEraseRowsShouldEraseOnUint32) {
        ConditionalEraseShouldSuccess("Uint32", TUnit::SECONDS, R"(
UPSERT INTO `/Root/table-1` (key, value) VALUES
(1, 0),
(2, 636249600),
(3, 1902441600),
(4, NULL);
        )", R"(
key = 3, value = 1902441600
key = 4, value = (empty maybe)
        )");
    }

    Y_UNIT_TEST(ConditionalEraseRowsShouldEraseOnUint64Seconds) {
        ConditionalEraseShouldSuccess("Uint64", TUnit::SECONDS, R"(
UPSERT INTO `/Root/table-1` (key, value) VALUES
(1, 0),
(2, 636249600),
(3, 1902441600),
(4, NULL);
        )", R"(
key = 3, value = 1902441600
key = 4, value = (empty maybe)
        )");
    }

    Y_UNIT_TEST(ConditionalEraseRowsShouldEraseOnUint64MilliSeconds) {
        ConditionalEraseShouldSuccess("Uint64", TUnit::MILLISECONDS, R"(
UPSERT INTO `/Root/table-1` (key, value) VALUES
(1, 0),
(2, 636249600000),
(3, 1902441600000),
(4, NULL);
        )", R"(
key = 3, value = 1902441600000
key = 4, value = (empty maybe)
        )");
    }

    Y_UNIT_TEST(ConditionalEraseRowsShouldEraseOnUint64MicroSeconds) {
        ConditionalEraseShouldSuccess("Uint64", TUnit::MICROSECONDS, R"(
UPSERT INTO `/Root/table-1` (key, value) VALUES
(1, 0),
(2, 636249600000000),
(3, 1902441600000000),
(4, NULL);
        )", R"(
key = 3, value = 1902441600000000
key = 4, value = (empty maybe)
        )");
    }

    Y_UNIT_TEST(ConditionalEraseRowsShouldEraseOnUint64NanoSeconds) {
        ConditionalEraseShouldSuccess("Uint64", TUnit::NANOSECONDS, R"(
UPSERT INTO `/Root/table-1` (key, value) VALUES
(1, 0),
(2, 636249600000000000),
(3, 1902441600000000000),
(4, NULL);
        )", R"(
key = 3, value = 1902441600000000000
key = 4, value = (empty maybe)
        )");
    }

    Y_UNIT_TEST(ConditionalEraseRowsShouldEraseOnDyNumberSeconds) {
        ConditionalEraseShouldSuccess("DyNumber", TUnit::SECONDS, R"(
--!syntax_v1
UPSERT INTO `/Root/table-1` (key, value) VALUES
(1, CAST("0" As DyNumber)),
(2, CAST("636249600" As DyNumber)),
(3, CAST("636249600.123" As DyNumber)),
(4, CAST("-1902441600" As DyNumber)),
(5, CAST("1902441600" As DyNumber)),
(6, CAST("636249600000" As DyNumber)),
(7, NULL);
        )", R"(
key = 5, value = .19024416e10
key = 6, value = .6362496e12
key = 7, value = (empty maybe)
        )");
    }

    Y_UNIT_TEST(ConditionalEraseRowsShouldEraseOnDyNumberMilliSeconds) {
        ConditionalEraseShouldSuccess("DyNumber", TUnit::MILLISECONDS, R"(
--!syntax_v1
UPSERT INTO `/Root/table-1` (key, value) VALUES
(1, CAST("0" As DyNumber)),
(2, CAST("636249600000" As DyNumber)),
(3, CAST("1902441600000" As DyNumber)),
(4, CAST("636249600000000" As DyNumber)),
(5, NULL);
        )", R"(
key = 3, value = .19024416e13
key = 4, value = .6362496e15
key = 5, value = (empty maybe)
        )");
    }

    Y_UNIT_TEST(ConditionalEraseRowsShouldEraseOnDyNumberMicroSeconds) {
        ConditionalEraseShouldSuccess("DyNumber", TUnit::MICROSECONDS, R"(
--!syntax_v1
UPSERT INTO `/Root/table-1` (key, value) VALUES
(1, CAST("-9.9999999999999999999999999999999999999E+125" As DyNumber)),
(2, CAST("-1E-130" As DyNumber)),
(3, CAST("0" As DyNumber)),
(4, CAST("1E-130" As DyNumber)),
(5, CAST("636249600000000" As DyNumber)),
(6, CAST("1902441600000000" As DyNumber)),
(7, CAST("9.9999999999999999999999999999999999999E+125" As DyNumber)),
(8, NULL);
        )", R"(
key = 6, value = .19024416e16
key = 7, value = .99999999999999999999999999999999999999e126
key = 8, value = (empty maybe)
        )");
    }

    Y_UNIT_TEST(ConditionalEraseRowsShouldEraseOnDyNumberNanoSeconds) {
        ConditionalEraseShouldSuccess("DyNumber", TUnit::NANOSECONDS, R"(
--!syntax_v1
UPSERT INTO `/Root/table-1` (key, value) VALUES
(1, CAST("0" As DyNumber)),
(2, CAST("636249600000000000" As DyNumber)),
(3, CAST("1902441600000000000" As DyNumber)),
(4, NULL);
        )", R"(
key = 3, value = .19024416e19
key = 4, value = (empty maybe)
        )");
    }

    Y_UNIT_TEST(ConditionalEraseRowsShouldEraseOnDate32) {
        ConditionalEraseShouldSuccess("Date32", TUnit::AUTO, R"(
UPSERT INTO `/Root/table-1` (key, value) VALUES
(1, CAST("1960-01-01" AS Date32)),
(2, CAST("1970-01-01" AS Date32)),
(3, CAST("1990-03-01" AS Date32)),
(4, CAST("2030-04-15" AS Date32)),
(5, NULL);
        )", R"(
key = 4, value = 22019
key = 5, value = (empty maybe)
        )", true);
    } 

    Y_UNIT_TEST(ConditionalEraseRowsShouldEraseOnDatetime64) {
        ConditionalEraseShouldSuccess("Datetime64", TUnit::AUTO, R"(
UPSERT INTO `/Root/table-1` (key, value) VALUES
(1, CAST("1960-01-01T00:00:00Z" AS Datetime64)),
(2, CAST("1970-01-01T00:00:00Z" AS Datetime64)),
(3, CAST("1990-03-01T00:00:00Z" AS Datetime64)),
(4, CAST("2030-04-15T00:00:00Z" AS Datetime64)),
(5, NULL);
        )", R"(
key = 4, value = 1902441600
key = 5, value = (empty maybe)
        )", true);
    } 
    
    Y_UNIT_TEST(ConditionalEraseRowsShouldEraseOnTimestamp64) {
        ConditionalEraseShouldSuccess("Timestamp64", TUnit::AUTO, R"(
UPSERT INTO `/Root/table-1` (key, value) VALUES
(1, CAST("1960-01-01T00:00:00.000000Z" AS Timestamp64)),
(2, CAST("1970-01-01T00:00:00.000000Z" AS Timestamp64)),
(3, CAST("1990-03-01T00:00:00.000000Z" AS Timestamp64)),
(4, CAST("2030-04-15T00:00:00.000000Z" AS Timestamp64)),
(5, NULL);
        )", R"(
key = 4, value = 1902441600000000
key = 5, value = (empty maybe)
        )", true);
    }    



    Y_UNIT_TEST(ConditionalEraseRowsShouldFailOnVariousErrors) {
        using TEvResponse = TEvDataShard::TEvConditionalEraseRowsResponse;

        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings
            .SetDomainName("Root")
            .SetUseRealThreads(false);

        TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        const TActorId sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
        InitRoot(server, sender);

        int tableNum = 1;
        for (TStringBuf ct : {"Date", "Datetime", "Timestamp", "Uint32", "Uint64", "DyNumber", "String"}) {
            const auto tableName = Sprintf("table-%i", tableNum++);
            const auto tablePath = Sprintf("/Root/%s", tableName.data());

            CreateTable(server, sender, "/Root", tableName, TString(ct));
            auto tableId = ResolveTableId(server, sender, tablePath);

            ConditionalEraseRows(server, sender, tablePath, TTableId(), 2, 0, TUnit::AUTO, {}, {},
                TEvResponse::ProtoRecordType::BAD_REQUEST, "Unknown table id");

            ConditionalEraseRows(server, sender, tablePath, tableId, 3, 0, TUnit::AUTO, {}, {},
                TEvResponse::ProtoRecordType::BAD_REQUEST, "Unknown column id");

            if (ct == "Date" || ct == "Datetime" || ct == "Timestamp") {
                for (auto unit : {TUnit::SECONDS, TUnit::MILLISECONDS, TUnit::MICROSECONDS}) {
                    ConditionalEraseRows(server, sender, tablePath, tableId, 2, 0, unit, {}, {},
                        TEvResponse::ProtoRecordType::BAD_REQUEST, "Unit cannot be specified for date type column");
                }
            } else if (ct == "Uint32" || ct == "Uint64" || ct == "DyNumber") {
                ConditionalEraseRows(server, sender, tablePath, tableId, 2, 0, TUnit::AUTO, {}, {},
                    TEvResponse::ProtoRecordType::BAD_REQUEST, "Unit should be specified for integral type column");
            } else {
                ConditionalEraseRows(server, sender, tablePath, tableId, 2, 0, TUnit::AUTO, {}, {},
                    TEvResponse::ProtoRecordType::BAD_REQUEST, "Unsupported type");
            }
        }
    }

    Y_UNIT_TEST_TWIN(ConditionalEraseRowsShouldBreakLocks, StreamLookup) {
        using TEvResponse = TEvDataShard::TEvConditionalEraseRowsResponse;

        TPortManager pm;
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamLookup(StreamLookup);
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings
            .SetAppConfig(appConfig)
            .SetDomainName("Root")
            .SetUseRealThreads(false);

        TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        const TActorId sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
        InitRoot(server, sender);

        CreateTable(server, sender, "/Root", "table-1");
        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table-1` (key, value) VALUES
            (1, CAST("1970-01-01T00:00:00.000000Z" AS Timestamp)),
            (2, CAST("1990-03-01T00:00:00.000000Z" AS Timestamp)),
            (3, CAST("2030-04-15T00:00:00.000000Z" AS Timestamp));
        )");

        TString sessionId = CreateSessionRPC(runtime);

        TString txId;
        {
            auto result = KqpSimpleBegin(runtime, sessionId, txId, R"(
                SELECT value FROM `/Root/table-1` WHERE key = 1
            )");

            UNIT_ASSERT_VALUES_EQUAL(result, "{ items { uint64_value: 0 } }");
        }

        {
            auto tableId = ResolveTableId(server, sender, "/Root/table-1");
            ConditionalEraseRows(server, sender, "/Root/table-1", tableId, 2, currentTime);

            auto ev = server->GetRuntime()->GrabEdgeEventRethrow<TEvResponse>(sender);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetStatus(), TEvResponse::ProtoRecordType::OK);
        }

        {
            auto result = KqpSimpleCommit(runtime, sessionId, txId, R"(
                UPSERT INTO `/Root/table-1` (key, value) VALUES
                (4, CAST("2031-04-15T00:00:00.000000Z" AS Timestamp));
            )");
            UNIT_ASSERT_VALUES_EQUAL(result, "ERROR: ABORTED");
        }
    }

    Y_UNIT_TEST(ConditionalEraseRowsShouldNotEraseModifiedRows) {
        using TEvResponse = TEvDataShard::TEvConditionalEraseRowsResponse;

        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings
            .SetDomainName("Root")
            .SetUseRealThreads(false);

        TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        const TActorId sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
        InitRoot(server, sender);

        CreateTable(server, sender, "/Root", "table-1");
        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table-1` (key, value) VALUES
            (1, CAST("1970-01-01T00:00:00.000000Z" AS Timestamp)),
            (2, CAST("1990-03-01T00:00:00.000000Z" AS Timestamp)),
            (3, CAST("2020-04-15T00:00:00.000000Z" AS Timestamp));
        )");

        THolder<IEventHandle> delayed;
        auto prevObserver = server->GetRuntime()->SetObserverFunc([&delayed](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
            case TEvDataShard::TEvEraseRowsRequest::EventType:
                delayed.Reset(ev.Release());
                return TTestActorRuntime::EEventAction::DROP;
            default:
                return TTestActorRuntime::EEventAction::PROCESS;
            }
        });

        auto tableId = ResolveTableId(server, sender, "/Root/table-1");
        ConditionalEraseRows(server, sender, "/Root/table-1", tableId, 2, currentTime);

        if (!delayed) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&delayed](IEventHandle&) -> bool {
                return !!delayed;
            });
            server->GetRuntime()->DispatchEvents(opts);
        }

        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table-1` (key, value) VALUES
            (3, CAST("2030-04-15T00:00:00.000000Z" AS Timestamp));
        )");

        server->GetRuntime()->SetObserverFunc(prevObserver);
        server->GetRuntime()->Send(delayed.Release(), 0, true);

        auto ev = server->GetRuntime()->GrabEdgeEventRethrow<TEvResponse>(sender);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetStatus(), TEvResponse::ProtoRecordType::OK);

        auto content = ReadShardedTable(server, "/Root/table-1");
        UNIT_ASSERT_STRINGS_EQUAL(StripInPlace(content), "key = 3, value = 2030-04-15T00:00:00.000000Z");
    }

    Y_UNIT_TEST(EraseRowsFromReplicatedTable) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings
            .SetDomainName("Root")
            .SetUseRealThreads(false);

        TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        const TActorId sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);

        InitRoot(server, sender);
        CreateShardedTable(server, sender, "/Root", "table-1", TShardedTableOptions().Replicated(true));

        auto tableId = ResolveTableId(server, sender, "/Root/table-1");
        EraseRows(server, sender, "/Root/table-1", tableId, {1}, SerializeKeys({1, 2}),
            TProto::TEvEraseResponse::EXEC_ERROR, "Can't execute erase at replicated table");
    }
}

Y_UNIT_TEST_SUITE(DistributedEraseTests) {
    using TNavigate = NSchemeCache::TSchemeCacheNavigate;
    using TResolve = NSchemeCache::TSchemeCacheRequest;

    void FillPath(TNavigate::TEntry& entry, const TString& path) {
        entry.Path = SplitPath(path);
        entry.RequestType = TNavigate::TEntry::ERequestType::ByPath;
    }

    void FillPath(TNavigate::TEntry& entry, const TTableId& tableId) {
        entry.TableId = tableId;
        entry.RequestType = TNavigate::TEntry::ERequestType::ByTableId;
    }

    template <typename TPath>
    THolder<TNavigate> Navigate(TServer::TPtr server, const TActorId& sender, const TPath& path, TNavigate::EOp op) {
        using TEvRequest = TEvTxProxySchemeCache::TEvNavigateKeySet;
        using TEvResponse = TEvTxProxySchemeCache::TEvNavigateKeySetResult;

        auto& runtime = *server->GetRuntime();

        auto request = MakeHolder<TNavigate>();
        auto& entry = request->ResultSet.emplace_back();
        FillPath(entry, path);
        entry.Operation = op;
        entry.ShowPrivatePath = true;
        runtime.Send(MakeSchemeCacheID(), sender, new TEvRequest(request.Release()));

        auto ev = runtime.GrabEdgeEventRethrow<TEvResponse>(sender);
        UNIT_ASSERT(ev);
        UNIT_ASSERT(ev->Get());

        auto* response = ev->Get()->Request.Release();
        UNIT_ASSERT(response);
        UNIT_ASSERT(response->ErrorCount == 0);
        UNIT_ASSERT_VALUES_EQUAL(response->ResultSet.size(), 1);

        return THolder(response);
    }

    NDataShard::TIndexes GetIndexes(TServer::TPtr server, const TActorId& sender, const TString& path) {
        auto mainTable = Navigate(server, sender, path, TNavigate::OpTable);
        const auto& mainEntry = mainTable->ResultSet.at(0);

        TVector<ui32> keyOrder;
        THashMap<TString, ui32> columnNameToId;
        for (const auto& [id, column] : mainEntry.Columns) {
            if (column.KeyOrder >= 0) {
                if (keyOrder.size() < static_cast<ui32>(column.KeyOrder + 1)) {
                    keyOrder.resize(column.KeyOrder + 1);
                }
                keyOrder[column.KeyOrder] = id;
            }

            UNIT_ASSERT(columnNameToId.emplace(column.Name, id).second);
        }

        NDataShard::TIndexes indexes;
        for (const auto& index : mainEntry.Indexes) {
            if (index.GetType() == NKikimrSchemeOp::EIndexTypeGlobalAsync) {
                continue;
            }

            THashSet<TString> seen;

            NDataShard::TKeyMap keyMap;
            ui32 i = 0;
            for (; i < index.KeyColumnNamesSize(); ++i) {
                const TString& name = index.GetKeyColumnNames(i);
                seen.insert(name);

                UNIT_ASSERT(columnNameToId.contains(name));
                keyMap.emplace_back(std::make_pair(i + 1, columnNameToId.at(name)));
            }

            for (const ui32 id : keyOrder) {
                UNIT_ASSERT(mainEntry.Columns.contains(id));
                if (seen.contains(mainEntry.Columns.at(id).Name)) {
                    continue;
                }

                keyMap.emplace_back(std::make_pair(++i, id));
            }

            const auto tableId = TTableId(index.GetPathOwnerId(), index.GetLocalPathId(), index.GetSchemaVersion());
            auto indexTable = Navigate(server, sender, tableId, TNavigate::OpList);
            const auto& indexEntry = indexTable->ResultSet.at(0);

            UNIT_ASSERT(indexEntry.ListNodeEntry);
            UNIT_ASSERT_VALUES_EQUAL(indexEntry.ListNodeEntry->Children.size(), 1);
            const auto& child = indexEntry.ListNodeEntry->Children.at(0);

            UNIT_ASSERT(indexes.emplace(TTableId(child.PathId, child.SchemaVersion), std::move(keyMap)).second);
        }

        return indexes;
    }

    TVector<THolder<IEventHandle>> ConditionalEraseRowsDelayedPlan(
            TServer::TPtr server, const TActorId& sender, const TString& path,
            const TTableId& tableId, ui32 columnId, ui64 threshold, const NDataShard::TIndexes& indexes) {

        TVector<THolder<IEventHandle>> delayed;

        auto& runtime = *server->GetRuntime();
        auto prevObserver = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
            case TEvTxProxy::TEvProposeTransaction::EventType:
                delayed.emplace_back(ev.Release());
                return TTestActorRuntime::EEventAction::DROP;
            default:
                return TTestActorRuntime::EEventAction::PROCESS;
            }
        });

        ConditionalEraseRows(server, sender, path, tableId, columnId, threshold, TUnit::AUTO, indexes);

        if (delayed.size() < 1) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&delayed](IEventHandle&) -> bool {
                return delayed.size() >= 1;
            });
            runtime.DispatchEvents(opts);
        }

        runtime.SetObserverFunc(prevObserver);
        return delayed;
    }

    TVector<THolder<IEventHandle>> ConditionalEraseRowsDelayedResolve(
            TServer::TPtr server, const TActorId& sender, const TString& path,
            const TTableId& tableId, ui32 columnId, ui64 threshold, const NDataShard::TIndexes& indexes) {

        TVector<THolder<IEventHandle>> delayed;
        TActorId eraser;

        auto& runtime = *server->GetRuntime();
        auto prevObserver = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
            case TEvDataShard::TEvEraseRowsRequest::EventType:
                delayed.emplace_back(ev.Release());
                return TTestActorRuntime::EEventAction::DROP;
            case TEvTxProxySchemeCache::TEvNavigateKeySetResult::EventType:
                if (ev->Recipient == eraser) {
                    delayed.emplace_back(ev.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
            case TEvTxUserProxy::TEvAllocateTxId::EventType:
                eraser = ev->Sender;
            default:
                return TTestActorRuntime::EEventAction::PROCESS;
            }
        });

        ConditionalEraseRows(server, sender, path, tableId, columnId, threshold, TUnit::AUTO, indexes);

        if (!eraser) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&eraser](IEventHandle&) -> bool {
                return !!eraser;
            });
            runtime.DispatchEvents(opts);
        }

        if (delayed.size() < 2) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&delayed](IEventHandle&) -> bool {
                return delayed.size() >= 2;
            });
            runtime.DispatchEvents(opts);
        }

        runtime.SetObserverFunc(prevObserver);
        return delayed;
    }

    void ConditionalEraseShouldSuccess(
            const TString& ttlColType, EUnit unit,
            const TString& toUpload, const THashMap<TString, TString>& afterErase,
            const THashMap<TString, ui32>& shardsConfig = {}) {
        using TEvResponse = TEvDataShard::TEvConditionalEraseRowsResponse;

        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings
            .SetDomainName("Root")
            .SetUseRealThreads(false);

        TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        const TActorId sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
        InitRoot(server, sender);

        CreateIndexedTable(server, sender, "/Root", "table-1", ttlColType);
        ExecSQL(server, sender, toUpload);

        if (shardsConfig) {
            SimulateSleep(server, TDuration::Seconds(1));
            SetSplitMergePartCountLimit(&runtime, -1);
        }

        for (const auto& [path, splitKey] : shardsConfig) {
            auto tabletIds = GetTableShards(server, sender, path);
            UNIT_ASSERT_VALUES_EQUAL(tabletIds.size(), 1);
            const ui64 txId = AsyncSplitTable(server, sender, path, tabletIds[0], splitKey);
            WaitTxNotification(server, sender, txId);
        }

        auto tableId = ResolveTableId(server, sender, "/Root/table-1");
        auto indexes = GetIndexes(server, sender, "/Root/table-1");
        ConditionalEraseRows(server, sender, "/Root/table-1", tableId, 4, currentTime, unit, indexes);

        auto ev = server->GetRuntime()->GrabEdgeEventRethrow<TEvResponse>(sender);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetStatus(), TEvResponse::ProtoRecordType::OK);

        for (const auto& [path, expectedContent] : afterErase) {
            auto content = ReadShardedTable(server, path);
            UNIT_ASSERT_STRINGS_EQUAL(StripInPlace(content), Strip(expectedContent));
        }
    }

    Y_UNIT_TEST(ConditionalEraseRowsShouldErase) {
        ConditionalEraseShouldSuccess("Timestamp", TUnit::AUTO, R"(
UPSERT INTO `/Root/table-1` (key, skey, tkey, value) VALUES
(1, 10, 400, CAST("1970-01-01T00:00:00.000000Z" AS Timestamp)),
(2, 20, 300, CAST("1990-03-01T00:00:00.000000Z" AS Timestamp)),
(3, 30, 200, CAST("2030-04-15T00:00:00.000000Z" AS Timestamp)),
(4, 40, 100, NULL);
        )", {{
"/Root/table-1", R"(
key = 3, skey = 30, tkey = 200, value = 2030-04-15T00:00:00.000000Z
key = 4, skey = 40, tkey = 100, value = (empty maybe)
        )"}, {
"/Root/table-1/by_skey/indexImplTable", R"(
skey = 30, key = 3
skey = 40, key = 4
        )"}, {
"/Root/table-1/by_tkey/indexImplTable", R"(
tkey = 100, key = 4
tkey = 200, key = 3
        )"}});
    }

    Y_UNIT_TEST(ConditionalEraseRowsShouldEraseOnUint32) {
        ConditionalEraseShouldSuccess("Uint32", TUnit::SECONDS, R"(
UPSERT INTO `/Root/table-1` (key, skey, tkey, value) VALUES
(1, 10, 400, 0),
(2, 20, 300, 636249600),
(3, 30, 200, 1902441600),
(4, 40, 100, NULL);
        )", {{
"/Root/table-1", R"(
key = 3, skey = 30, tkey = 200, value = 1902441600
key = 4, skey = 40, tkey = 100, value = (empty maybe)
        )"}, {
"/Root/table-1/by_skey/indexImplTable", R"(
skey = 30, key = 3
skey = 40, key = 4
        )"}, {
"/Root/table-1/by_tkey/indexImplTable", R"(
tkey = 100, key = 4
tkey = 200, key = 3
        )"}});
    }

    Y_UNIT_TEST(ConditionalEraseRowsShouldNotErase) {
        ConditionalEraseShouldSuccess("Timestamp", TUnit::AUTO, R"(
UPSERT INTO `/Root/table-1` (key, skey, tkey, value) VALUES
(1, 10, 300, CAST("2030-04-15T00:00:00.000000Z" AS Timestamp)),
(2, 20, 200, CAST("2030-04-15T00:00:00.000000Z" AS Timestamp)),
(3, 30, 100, CAST("2030-04-15T00:00:00.000000Z" AS Timestamp));
        )", {{
"/Root/table-1", R"(
key = 1, skey = 10, tkey = 300, value = 2030-04-15T00:00:00.000000Z
key = 2, skey = 20, tkey = 200, value = 2030-04-15T00:00:00.000000Z
key = 3, skey = 30, tkey = 100, value = 2030-04-15T00:00:00.000000Z
        )"}, {
"/Root/table-1/by_skey/indexImplTable", R"(
skey = 10, key = 1
skey = 20, key = 2
skey = 30, key = 3
        )"}, {
"/Root/table-1/by_tkey/indexImplTable", R"(
tkey = 100, key = 3
tkey = 200, key = 2
tkey = 300, key = 1
        )"}});
    }

    Y_UNIT_TEST(ConditionalEraseRowsShouldSuccessOnShardedIndex) {
        ConditionalEraseShouldSuccess("Timestamp", TUnit::AUTO, R"(
UPSERT INTO `/Root/table-1` (key, skey, tkey, value) VALUES
(1, 10, 400, CAST("1970-01-01T00:00:00.000000Z" AS Timestamp)),
(2, 20, 300, CAST("1990-03-01T00:00:00.000000Z" AS Timestamp)),
(3, 30, 200, CAST("2020-04-15T00:00:00.000000Z" AS Timestamp)),
(4, 40, 100, NULL);
        )", {{
"/Root/table-1", R"(
key = 4, skey = 40, tkey = 100, value = (empty maybe)
        )"}, {
"/Root/table-1/by_skey/indexImplTable", R"(
skey = 40, key = 4
        )"}, {
"/Root/table-1/by_tkey/indexImplTable", R"(
tkey = 100, key = 4
        )"}}, {{
"/Root/table-1/by_skey/indexImplTable", 20
        }, {
"/Root/table-1/by_tkey/indexImplTable", 300
        }});
    }

    Y_UNIT_TEST(ConditionalEraseRowsShouldNotEraseModifiedRows) {
        using TEvResponse = TEvDataShard::TEvConditionalEraseRowsResponse;

        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings
            .SetDomainName("Root")
            .SetUseRealThreads(false);

        TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        const TActorId sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
        InitRoot(server, sender);

        CreateIndexedTable(server, sender, "/Root", "table-1");
        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table-1` (key, skey, tkey, value) VALUES
            (1, 10, 300, CAST("1970-01-01T00:00:00.000000Z" AS Timestamp)),
            (2, 20, 200, CAST("1990-03-01T00:00:00.000000Z" AS Timestamp)),
            (3, 30, 100, CAST("2020-04-15T00:00:00.000000Z" AS Timestamp));
        )");

        auto tableId = ResolveTableId(server, sender, "/Root/table-1");
        auto indexes = GetIndexes(server, sender, "/Root/table-1");
        auto delayed = ConditionalEraseRowsDelayedPlan(server, sender, "/Root/table-1",
            tableId, 4, currentTime, indexes);

        // case 1: modify ttl column
        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table-1` (key, skey, tkey, value) VALUES
            (3, 90, 900, CAST("2030-04-15T00:00:00.000000Z" AS Timestamp));
        )");

        for (auto& ev : delayed) {
            runtime.Send(ev.Release(), 0, true);
        }

        {
            auto ev = server->GetRuntime()->GrabEdgeEventRethrow<TEvResponse>(sender);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetStatus(), TEvResponse::ProtoRecordType::OK);
        }
        {
            auto content = ReadShardedTable(server, "/Root/table-1");
            UNIT_ASSERT_STRINGS_EQUAL(StripInPlace(content), "key = 3, skey = 90, tkey = 900, value = 2030-04-15T00:00:00.000000Z");
        }
        {
            auto content = ReadShardedTable(server, "/Root/table-1/by_skey/indexImplTable");
            UNIT_ASSERT_STRINGS_EQUAL(StripInPlace(content), "skey = 90, key = 3");
        }
        {
            auto content = ReadShardedTable(server, "/Root/table-1/by_tkey/indexImplTable");
            UNIT_ASSERT_STRINGS_EQUAL(StripInPlace(content), "tkey = 900, key = 3");
        }

        // restore previous value in ttl column
        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table-1` (key, skey, tkey, value) VALUES
            (3, 90, 900, CAST("2020-04-15T00:00:00.000000Z" AS Timestamp));
        )");

        delayed = ConditionalEraseRowsDelayedPlan(server, sender, "/Root/table-1",
            tableId, 4, currentTime, indexes);

        // case 2: modify index column
        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table-1` (key, skey, tkey, value) VALUES
            (3, 30, 100, CAST("2020-04-15T00:00:00.000000Z" AS Timestamp));
        )");

        for (auto& ev : delayed) {
            runtime.Send(ev.Release(), 0, true);
        }

        {
            auto ev = server->GetRuntime()->GrabEdgeEventRethrow<TEvResponse>(sender);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetStatus(), TEvResponse::ProtoRecordType::OK);
        }
        {
            auto content = ReadShardedTable(server, "/Root/table-1");
            UNIT_ASSERT_STRINGS_EQUAL(StripInPlace(content), "key = 3, skey = 30, tkey = 100, value = 2020-04-15T00:00:00.000000Z");
        }
        {
            auto content = ReadShardedTable(server, "/Root/table-1/by_skey/indexImplTable");
            UNIT_ASSERT_STRINGS_EQUAL(StripInPlace(content), "skey = 30, key = 3");
        }
        {
            auto content = ReadShardedTable(server, "/Root/table-1/by_tkey/indexImplTable");
            UNIT_ASSERT_STRINGS_EQUAL(StripInPlace(content), "tkey = 100, key = 3");
        }

        // after one more run, all records should be deleted
        ConditionalEraseRows(server, sender, "/Root/table-1", tableId, 4, currentTime, TUnit::AUTO, indexes);
        {
            auto ev = server->GetRuntime()->GrabEdgeEventRethrow<TEvResponse>(sender);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetStatus(), TEvResponse::ProtoRecordType::OK);
        }
        {
            auto content = ReadShardedTable(server, "/Root/table-1");
            UNIT_ASSERT_STRINGS_EQUAL(StripInPlace(content), "");
        }
        {
            auto content = ReadShardedTable(server, "/Root/table-1/by_skey/indexImplTable");
            UNIT_ASSERT_STRINGS_EQUAL(StripInPlace(content), "");
        }
        {
            auto content = ReadShardedTable(server, "/Root/table-1/by_tkey/indexImplTable");
            UNIT_ASSERT_STRINGS_EQUAL(StripInPlace(content), "");
        }
    }

    Y_UNIT_TEST(ConditionalEraseRowsShouldNotFailOnMissingRows) {
        using TEvResponse = TEvDataShard::TEvConditionalEraseRowsResponse;

        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings
            .SetDomainName("Root")
            .SetUseRealThreads(false);

        TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        const TActorId sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
        InitRoot(server, sender);

        CreateIndexedTable(server, sender, "/Root", "table-1");
        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table-1` (key, skey, tkey, value) VALUES
            (1, 10, 400, CAST("1970-01-01T00:00:00.000000Z" AS Timestamp)),
            (2, 20, 300, CAST("1990-03-01T00:00:00.000000Z" AS Timestamp)),
            (3, 30, 200, CAST("2020-04-15T00:00:00.000000Z" AS Timestamp)),
            (4, 40, 100, CAST("2030-04-15T00:00:00.000000Z" AS Timestamp));
        )");

        auto tableId = ResolveTableId(server, sender, "/Root/table-1");
        auto indexes = GetIndexes(server, sender, "/Root/table-1");
        auto delayed = ConditionalEraseRowsDelayedPlan(server, sender, "/Root/table-1",
            tableId, 4, currentTime, indexes);

        ExecSQL(server, sender, "DELETE FROM `/Root/table-1` WHERE key < 3;");

        for (auto& ev : delayed) {
            runtime.Send(ev.Release(), 0, true);
        }

        auto ev = server->GetRuntime()->GrabEdgeEventRethrow<TEvResponse>(sender);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetStatus(), TEvResponse::ProtoRecordType::OK);

        {
            auto content = ReadShardedTable(server, "/Root/table-1");
            UNIT_ASSERT_STRINGS_EQUAL(StripInPlace(content), "key = 4, skey = 40, tkey = 100, value = 2030-04-15T00:00:00.000000Z");
        }
        {
            auto content = ReadShardedTable(server, "/Root/table-1/by_skey/indexImplTable");
            UNIT_ASSERT_STRINGS_EQUAL(StripInPlace(content), "skey = 40, key = 4");
        }
        {
            auto content = ReadShardedTable(server, "/Root/table-1/by_tkey/indexImplTable");
            UNIT_ASSERT_STRINGS_EQUAL(StripInPlace(content), "tkey = 100, key = 4");
        }
    }

    Y_UNIT_TEST(ConditionalEraseRowsShouldFailOnVariousErrors) {
        using TEvRequest = TEvDataShard::TEvEraseRowsRequest;
        using TEvResponse = TEvDataShard::TEvEraseRowsResponse;
        using TEvNavigate = TEvTxProxySchemeCache::TEvNavigateKeySetResult;
        using TEvResolve = TEvTxProxySchemeCache::TEvResolveKeySetResult;

        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings
            .SetDomainName("Root")
            .SetUseRealThreads(false);

        TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        const TActorId sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
        InitRoot(server, sender);

        CreateIndexedTable(server, sender, "/Root", "table-1");
        auto tableId = ResolveTableId(server, sender, "/Root/table-1");
        auto indexes = GetIndexes(server, sender, "/Root/table-1");

        using TSimpleEventObserver = std::function<void(TAutoPtr<IEventHandle>&)>;
        auto check = [&](TEvResponse::ProtoRecordType::EStatus status, const TString& error, TSimpleEventObserver observer) {
            auto prevObserver = runtime.SetObserverFunc([observer](TAutoPtr<IEventHandle>& ev) {
                observer(ev);
                return TTestActorRuntime::EEventAction::PROCESS;
            });

            const auto eraser = runtime.Register(NDataShard::CreateDistributedEraser(sender, tableId, indexes));
            auto ev = runtime.GrabEdgeEventRethrow<TEvResponse>(sender);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetStatus(), status);
            UNIT_ASSERT_STRING_CONTAINS(ev->Get()->Record.GetErrorDescription(), error);
            runtime.Send(new IEventHandle(eraser, sender, new TEvents::TEvPoisonPill()));

            runtime.SetObserverFunc(prevObserver);
        };

        /// resolve tables

        check(TEvResponse::ProtoRecordType::SCHEME_ERROR, "Empty result", [](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvNavigate::EventType) {
                ev->Get<TEvNavigate>()->Request.Reset();
            }
        });

        check(TEvResponse::ProtoRecordType::SCHEME_ERROR, "Entries count mismatch", [](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvNavigate::EventType) {
                ev->Get<TEvNavigate>()->Request->ResultSet.clear();
            }
        });

        check(TEvResponse::ProtoRecordType::SCHEME_ERROR, "Failed to resolve table", [](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvNavigate::EventType) {
                ev->Get<TEvNavigate>()->Request->ResultSet.at(0).Status = TNavigate::EStatus::Unknown;
            }
        });

        check(TEvResponse::ProtoRecordType::SCHEME_ERROR, "Main table's path id mismatch", [](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvNavigate::EventType) {
                ev->Get<TEvNavigate>()->Request->ResultSet.at(0).TableId = TTableId();
            }
        });

        check(TEvResponse::ProtoRecordType::SCHEME_ERROR, "Main table's schema version mismatch", [](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvNavigate::EventType) {
                ev->Get<TEvNavigate>()->Request->ResultSet.at(0).TableId.SchemaVersion = 0;
            }
        });

        check(TEvResponse::ProtoRecordType::SCHEME_ERROR, "Indexes count mismatch", [](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvNavigate::EventType) {
                ev->Get<TEvNavigate>()->Request->ResultSet.at(0).Indexes.clear();
            }
        });

        check(TEvResponse::ProtoRecordType::SCHEME_ERROR, "Invalid index state", [](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvNavigate::EventType) {
                ev->Get<TEvNavigate>()->Request->ResultSet.at(0).Indexes.at(0).SetState(NKikimrSchemeOp::EIndexStateInvalid);
            }
        });

        check(TEvResponse::ProtoRecordType::SCHEME_ERROR, "Failed to resolve table", [](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvNavigate::EventType) {
                ev->Get<TEvNavigate>()->Request->ResultSet.at(1).Status = TNavigate::EStatus::Unknown;
            }
        });

        check(TEvResponse::ProtoRecordType::SCHEME_ERROR, "Unknown index", [](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvNavigate::EventType) {
                ev->Get<TEvNavigate>()->Request->ResultSet.at(1).TableId = TTableId();
            }
        });

        check(TEvResponse::ProtoRecordType::SCHEME_ERROR, "Duplicate index", [](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvNavigate::EventType) {
                auto& resultSet = ev->Get<TEvNavigate>()->Request->ResultSet;
                resultSet.at(1).TableId = resultSet.at(2).TableId;
            }
        });

        check(TEvResponse::ProtoRecordType::SCHEME_ERROR, "Empty domain info", [](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvNavigate::EventType) {
                ev->Get<TEvNavigate>()->Request->ResultSet.at(0).DomainInfo.Drop();
            }
        });

        check(TEvResponse::ProtoRecordType::SCHEME_ERROR, "Failed locality check", [](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvNavigate::EventType) {
                ev->Get<TEvNavigate>()->Request->ResultSet.at(0).DomainInfo = new NSchemeCache::TDomainInfo(TPathId(), TPathId());
            }
        });

        /// resolve keys

        check(TEvResponse::ProtoRecordType::SCHEME_ERROR, "Empty result", [](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvResolve::EventType) {
                ev->Get<TEvResolve>()->Request.Reset();
            }
        });

        check(TEvResponse::ProtoRecordType::SCHEME_ERROR, "Entries count mismatch", [](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvResolve::EventType) {
                ev->Get<TEvResolve>()->Request->ResultSet.clear();
            }
        });

        check(TEvResponse::ProtoRecordType::SCHEME_ERROR, "Failed to resolve table", [](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvResolve::EventType) {
                ev->Get<TEvResolve>()->Request->ResultSet.at(0).Status = TResolve::EStatus::Unknown;
            }
        });

        check(TEvResponse::ProtoRecordType::SCHEME_ERROR, "Empty partitions list", [](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvResolve::EventType) {
                ev->Get<TEvResolve>()->Request->ResultSet.at(0).KeyDescription->Partitioning =
                    std::make_shared<TVector<TKeyDesc::TPartitionInfo>>();
            }
        });

        /// process request

        using TRequestMaker = std::function<IEventBase*(void)>;
        auto badRequest = [&](TEvResponse::ProtoRecordType::EStatus status, const TString& error, TRequestMaker maker) {
            const auto eraser = runtime.Register(NDataShard::CreateDistributedEraser(sender, tableId, indexes));

            runtime.Send(new IEventHandle(eraser, sender, maker()), 0, true);
            auto ev = runtime.GrabEdgeEventRethrow<TEvResponse>(sender);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetStatus(), status);
            UNIT_ASSERT_STRING_CONTAINS(ev->Get()->Record.GetErrorDescription(), error);

            runtime.Send(new IEventHandle(eraser, sender, new TEvents::TEvPoisonPill()));
        };

        badRequest(TEvResponse::ProtoRecordType::BAD_REQUEST, "Unknown condition", []() -> IEventBase* {
            return new TEvRequest();
        });

        badRequest(TEvResponse::ProtoRecordType::BAD_REQUEST, "Cannot parse key", []() -> IEventBase* {
            auto request = MakeHolder<TEvRequest>();
            request->Record.MutableExpiration();
            request->Record.AddKeyColumns("trash");
            return request.Release();
        });

        badRequest(TEvResponse::ProtoRecordType::BAD_REQUEST, "Key column is absent", []() -> IEventBase* {
            auto request = MakeHolder<TEvRequest>();
            request->Record.MutableExpiration();
            for (const auto& key : SerializeKeys({1, 2})) {
                request->Record.AddKeyColumns(key);
            }
            return request.Release();
        });
    }

    Y_UNIT_TEST(ConditionalEraseRowsShouldFailOnSplit) {
        using TEvResponse = TEvDataShard::TEvConditionalEraseRowsResponse;

        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings
            .SetDomainName("Root")
            .SetUseRealThreads(false);

        TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        const TActorId sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
        InitRoot(server, sender);

        CreateIndexedTable(server, sender, "/Root", "table-1");
        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table-1` (key, skey, tkey, value) VALUES
            (1, 10, 300, CAST("1970-01-01T00:00:00.000000Z" AS Timestamp)),
            (2, 20, 200, CAST("1990-03-01T00:00:00.000000Z" AS Timestamp)),
            (3, 30, 100, CAST("2020-04-15T00:00:00.000000Z" AS Timestamp));
        )");

        auto tableId = ResolveTableId(server, sender, "/Root/table-1");
        auto indexes = GetIndexes(server, sender, "/Root/table-1");
        auto delayed = ConditionalEraseRowsDelayedResolve(server, sender, "/Root/table-1",
            tableId, 4, currentTime, indexes);

        SimulateSleep(server, TDuration::Seconds(1));
        SetSplitMergePartCountLimit(&runtime, -1);
        auto tabletIds = GetTableShards(server, sender, "/Root/table-1");
        UNIT_ASSERT_VALUES_EQUAL(tabletIds.size(), 1);
        const ui64 txId = AsyncSplitTable(server, sender, "Root/table-1", tabletIds[0], 2);
        WaitTxNotification(server, sender, txId);

        for (auto& ev : delayed) {
            runtime.Send(ev.Release(), 0, true);
        }

        auto ev = server->GetRuntime()->GrabEdgeEventRethrow<TEvResponse>(sender);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetStatus(), TEvResponse::ProtoRecordType::ABORTED);
    }

    Y_UNIT_TEST(ConditionalEraseRowsShouldFailOnSchemeTx) {
        using TEvResponse = TEvDataShard::TEvConditionalEraseRowsResponse;

        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings
            .SetDomainName("Root")
            .SetUseRealThreads(false);

        TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        const TActorId sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
        InitRoot(server, sender);

        CreateIndexedTable(server, sender, "/Root", "table-1");
        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table-1` (key, skey, tkey, value) VALUES
            (1, 10, 300, CAST("1970-01-01T00:00:00.000000Z" AS Timestamp)),
            (2, 20, 200, CAST("1990-03-01T00:00:00.000000Z" AS Timestamp)),
            (3, 30, 100, CAST("2020-04-15T00:00:00.000000Z" AS Timestamp));
        )");

        auto tableId = ResolveTableId(server, sender, "/Root/table-1");
        auto indexes = GetIndexes(server, sender, "/Root/table-1");
        auto delayed = ConditionalEraseRowsDelayedResolve(server, sender, "/Root/table-1",
            tableId, 4, currentTime, indexes);

        const ui64 txId = AsyncAlterAddExtraColumn(server, "/Root", "table-1");
        WaitTxNotification(server, sender, txId);

        for (auto& ev : delayed) {
            runtime.Send(ev.Release(), 0, true);
        }

        auto ev = server->GetRuntime()->GrabEdgeEventRethrow<TEvResponse>(sender);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetStatus(), TEvResponse::ProtoRecordType::ERASE_ERROR);
    }

    Y_UNIT_TEST(ConditionalEraseRowsShouldFailOnDeadShard) {
        using TEvResponse = TEvDataShard::TEvConditionalEraseRowsResponse;

        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings
            .SetDomainName("Root")
            .SetUseRealThreads(false);

        TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        const TActorId sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
        InitRoot(server, sender);

        CreateIndexedTable(server, sender, "/Root", "table-1");
        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table-1` (key, skey, tkey, value) VALUES
            (1, 10, 300, CAST("1970-01-01T00:00:00.000000Z" AS Timestamp)),
            (2, 20, 200, CAST("1990-03-01T00:00:00.000000Z" AS Timestamp)),
            (3, 30, 100, CAST("2020-04-15T00:00:00.000000Z" AS Timestamp));
        )");

        auto tableId = ResolveTableId(server, sender, "/Root/table-1");
        auto indexes = GetIndexes(server, sender, "/Root/table-1");
        auto delayed = ConditionalEraseRowsDelayedPlan(server, sender, "/Root/table-1",
            tableId, 4, currentTime, indexes);

        auto tabletIds = GetTableShards(server, sender, "/Root/table-1/by_skey/indexImplTable");
        UNIT_ASSERT_VALUES_EQUAL(tabletIds.size(), 1);
        RebootTablet(runtime, tabletIds[0], sender);

        for (auto& ev : delayed) {
            runtime.Send(ev.Release(), 0, true);
        }

        auto ev = server->GetRuntime()->GrabEdgeEventRethrow<TEvResponse>(sender);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetStatus(), TEvResponse::ProtoRecordType::ERASE_ERROR);
    }

    Y_UNIT_TEST(DistributedEraseTxShouldFailOnVariousErrors) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings
            .SetDomainName("Root")
            .SetUseRealThreads(false);

        TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        const TActorId sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
        InitRoot(server, sender);

        CreateIndexedTable(server, sender, "/Root", "table-1");
        auto tableId = ResolveTableId(server, sender, "/Root/table-1");

        ui64 txId = 100;
        TProto::TDistributedEraseTx tx;

        *tx.MutableEraseRowsRequest() = MakeEraseRowsRequest(TTableId(), {1}, SerializeKeys({1, 2}));
        DistributedEraseTx(server, sender, "/Root/table-1", txId++, tx,
            TProto::TEvProposeTxResult::BAD_REQUEST, "Unknown table id");

        *tx.MutableEraseRowsRequest() = MakeEraseRowsRequest(TTableId(tableId.PathId, 100), {1}, SerializeKeys({1, 2}));
        DistributedEraseTx(server, sender, "/Root/table-1", txId++, tx,
            TProto::TEvProposeTxResult::BAD_REQUEST, "Schema version mismatch");

        *tx.MutableEraseRowsRequest() = MakeEraseRowsRequest(tableId, {0, 1}, SerializeKeys({1, 2}));
        DistributedEraseTx(server, sender, "/Root/table-1", txId++, tx,
            TProto::TEvProposeTxResult::BAD_REQUEST, "Key column count mismatch");

        *tx.MutableEraseRowsRequest() = MakeEraseRowsRequest(tableId, {0}, SerializeKeys({1, 2}));
        DistributedEraseTx(server, sender, "/Root/table-1", txId++, tx,
            TProto::TEvProposeTxResult::BAD_REQUEST, "Key column schema mismatch");

        *tx.MutableEraseRowsRequest() = MakeEraseRowsRequest(tableId, {1}, {"trash"});
        DistributedEraseTx(server, sender, "/Root/table-1", txId++, tx,
            TProto::TEvProposeTxResult::BAD_REQUEST, "Cannot parse key");

        *tx.MutableEraseRowsRequest() = MakeEraseRowsRequest(tableId, {1}, SerializeKeys({1, 2}, &KeyCellsRepeater));
        DistributedEraseTx(server, sender, "/Root/table-1", txId++, tx,
            TProto::TEvProposeTxResult::BAD_REQUEST, "Cell count doesn't match row scheme");

        *tx.MutableEraseRowsRequest() = MakeEraseRowsRequest(tableId, {1}, SerializeKeys({1, 2}));
        // underlying request is valid now

        tx.MutableDependents()->Add();
        auto& dependency = *tx.MutableDependencies()->Add();
        DistributedEraseTx(server, sender, "/Root/table-1", txId++, tx,
            TProto::TEvProposeTxResult::BAD_REQUEST, "can only have dependents or dependencies");

        tx.MutableDependents()->Clear();
        dependency.SetPresentRows(NDataShard::SerializeBitMap(TDynBitMap().Push(1)));
        DistributedEraseTx(server, sender, "/Root/table-1", txId++, tx,
            TProto::TEvProposeTxResult::BAD_REQUEST, "Present rows count mismatch");
    }

    Y_UNIT_TEST(ConditionalEraseRowsCheckLimits) {
        using TEvResponse = TEvDataShard::TEvConditionalEraseRowsResponse;

        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings
            .SetDomainName("Root")
            .SetUseRealThreads(false);

        TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        const TActorId sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
        InitRoot(server, sender);

        auto check = [&](const char* table, const TProto::TLimits& limits, ui32 expectedRuns) {
            CreateIndexedTable(server, sender, "/Root", table);
            ExecSQL(server, sender, Sprintf(R"(
                UPSERT INTO `/Root/%s` (key, skey, tkey, value) VALUES
                (1, 10, 300, CAST("1970-01-01T00:00:00.000000Z" AS Timestamp)),
                (2, 20, 200, CAST("1990-03-01T00:00:00.000000Z" AS Timestamp)),
                (3, 30, 100, CAST("2020-04-15T00:00:00.000000Z" AS Timestamp));
            )", table));

            auto tableId = ResolveTableId(server, sender, Sprintf("/Root/%s", table));
            auto indexes = GetIndexes(server, sender, Sprintf("/Root/%s", table));
            ConditionalEraseRows(server, sender, Sprintf("/Root/%s", table), tableId, 4, currentTime, TUnit::AUTO, indexes, limits);

            ui32 runs = 0;
            TEvResponse::ProtoRecordType::EStatus status;

            do {
                auto ev = server->GetRuntime()->GrabEdgeEventRethrow<TEvResponse>(sender);
                ++runs;
                status = ev->Get()->Record.GetStatus();
            } while (status != TEvResponse::ProtoRecordType::OK);

            UNIT_ASSERT_VALUES_EQUAL(runs, expectedRuns);
        };

        auto makeLimits = [](ui32 maxBytes, ui32 minKeys, ui32 maxKeys) -> TProto::TLimits {
            TProto::TLimits result;

            result.SetBatchMaxBytes(maxBytes);
            result.SetBatchMinKeys(minKeys);
            result.SetBatchMaxKeys(maxKeys);

            return result;
        };

        check("table-1", makeLimits(100, 1, 1), 4); // 3 PARTIAL (1 per key) + 1 final OK when exhausted
        check("table-2", makeLimits(1, 2, 3), 2); // 1 PARTIAL (first 2 keys) + 1 final OK (last key + exhausted)
        check("table-3", makeLimits(100, 1, 3), 2); // 1 PARTIAL (for 3 keys) + 1 final OK when exhausted
        check("table-4", makeLimits(100, 1, 4), 1); // just final OK when exhausted
    }

    Y_UNIT_TEST(ConditionalEraseRowsAsyncIndex) {
        using TEvResponse = TEvDataShard::TEvConditionalEraseRowsResponse;

        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings
            .SetDomainName("Root")
            .SetUseRealThreads(false);

        TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        const TActorId sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
        InitRoot(server, sender);

        auto check = [&](const char* table, const TShardedTableOptions& opts) {
            CreateShardedTable(server, sender, "/Root", table, opts);
            ExecSQL(server, sender, Sprintf(R"(
                UPSERT INTO `/Root/%s` (key, skey, tkey, value) VALUES
                (1, 10, 300, CAST("1970-01-01T00:00:00.000000Z" AS Timestamp)),
                (2, 20, 200, CAST("1990-03-01T00:00:00.000000Z" AS Timestamp)),
                (3, 30, 100, CAST("2020-04-15T00:00:00.000000Z" AS Timestamp));
            )", table));

            auto tableId = ResolveTableId(server, sender, Sprintf("/Root/%s", table));
            auto indexes = GetIndexes(server, sender, Sprintf("/Root/%s", table));
            ConditionalEraseRows(server, sender, Sprintf("/Root/%s", table), tableId, 4, currentTime, TUnit::AUTO, indexes);

            auto ev = server->GetRuntime()->GrabEdgeEventRethrow<TEvResponse>(sender);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetStatus(), TEvResponse::ProtoRecordType::OK);

            for (const auto& index : opts.Indexes_) {
                do {
                    auto content = ReadShardedTable(server, Sprintf("/Root/%s/%s/indexImplTable", table, index.Name.c_str()));

                    if (index.Type == NKikimrSchemeOp::EIndexTypeGlobal) {
                        UNIT_ASSERT_STRINGS_EQUAL(StripInPlace(content), "");
                        break;
                    } else if (index.Type == NKikimrSchemeOp::EIndexTypeGlobalAsync) {
                        if (StripInPlace(content) == "") {
                            break;
                        }
                    } else {
                        UNIT_ASSERT_C(false, "Unknown index type: " << static_cast<ui32>(index.Type));
                    }

                    SimulateSleep(server, TDuration::Seconds(1));
                } while (true);
            }
        };

        check("table-1", TShardedTableOptions()
            .EnableOutOfOrder(false)
            .Columns({
                {"key", "Uint32", true, false},
                {"skey", "Uint32", false, false},
                {"tkey", "Uint32", false, false},
                {"value", "Timestamp", false, false}
            })
            .Indexes({
                {"by_skey", {"skey"}, {}, NKikimrSchemeOp::EIndexTypeGlobalAsync},
                {"by_tkey", {"tkey"}, {}, NKikimrSchemeOp::EIndexTypeGlobalAsync}
            })
        );

        check("table-2", TShardedTableOptions()
            .EnableOutOfOrder(false)
            .Columns({
                {"key", "Uint32", true, false},
                {"skey", "Uint32", false, false},
                {"tkey", "Uint32", false, false},
                {"value", "Timestamp", false, false}
            })
            .Indexes({
                {"by_skey", {"skey"}, {}, NKikimrSchemeOp::EIndexTypeGlobal},
                {"by_tkey", {"tkey"}, {}, NKikimrSchemeOp::EIndexTypeGlobalAsync}
            })
        );
    }
}

} // namespace NKikimr
