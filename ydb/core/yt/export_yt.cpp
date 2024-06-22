#ifndef KIKIMR_DISABLE_YT

#include "export_yt.h"
#include "yt_wrapper.h"

#include <contrib/ydb/core/protos/flat_scheme_op.pb.h>
#include <contrib/ydb/library/services/services.pb.h>
#include <contrib/ydb/core/tablet_flat/flat_row_state.h>
#include <contrib/ydb/core/tx/datashard/export_common.h>
#include <contrib/ydb/library/binary_json/read.h>
#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/hfunc.h>

#include <yt/yt/client/table_client/config.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/core/misc/shutdown.h>

#include <util/generic/maybe.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>
#include <util/string/join.h>

namespace NKikimr {
namespace NYndx {

using namespace NDataShard::NExportScan;
using namespace NWrappers;

using namespace NYT::NApi;
using namespace NYT::NObjectClient;
using namespace NYT::NTableClient;
using namespace NYT::NYPath;
using namespace NYT::NYTree;
using namespace NYT::NYson;

using TTableColumns = TYtExport::TTableColumns;
using TEvExportScan = NDataShard::TEvExportScan;
using TEvBuffer = TEvExportScan::TEvBuffer<TVector<TUnversionedOwningRow>>;

namespace NCustomErrorCodes {

    constexpr auto InvalidNodeType = NYT::TErrorCode(100000);

} // NCustomErrorCodes

class TYtUploader: public TActorBootstrapped<TYtUploader> {
    static ESimpleLogicalValueType ConvertType(NScheme::TTypeId type) {
        switch (type) {
        case NScheme::NTypeIds::Int32:
            return ESimpleLogicalValueType::Int32;
        case NScheme::NTypeIds::Uint32:
            return ESimpleLogicalValueType::Uint32;
        case NScheme::NTypeIds::Int64:
            return ESimpleLogicalValueType::Int64;
        case NScheme::NTypeIds::Uint64:
            return ESimpleLogicalValueType::Uint64;
        case NScheme::NTypeIds::Uint8:
            return ESimpleLogicalValueType::Uint8;
        case NScheme::NTypeIds::Int8:
            return ESimpleLogicalValueType::Int8;
        case NScheme::NTypeIds::Int16:
            return ESimpleLogicalValueType::Int16;
        case NScheme::NTypeIds::Uint16:
            return ESimpleLogicalValueType::Uint16;
        case NScheme::NTypeIds::Bool:
            return ESimpleLogicalValueType::Boolean;
        case NScheme::NTypeIds::Double:
        case NScheme::NTypeIds::Float:
            return ESimpleLogicalValueType::Double;
        case NScheme::NTypeIds::Date:
            return ESimpleLogicalValueType::Date;
        case NScheme::NTypeIds::Datetime:
            return ESimpleLogicalValueType::Datetime;
        case NScheme::NTypeIds::Timestamp:
            return ESimpleLogicalValueType::Timestamp;
        case NScheme::NTypeIds::Interval:
            return ESimpleLogicalValueType::Interval;
        case NScheme::NTypeIds::Date32:
            return ESimpleLogicalValueType::Date32;
        case NScheme::NTypeIds::Datetime64:
            return ESimpleLogicalValueType::Datetime64;
        case NScheme::NTypeIds::Timestamp64:
            return ESimpleLogicalValueType::Timestamp64;
        case NScheme::NTypeIds::Interval64:
            return ESimpleLogicalValueType::Interval64;
        case NScheme::NTypeIds::Utf8:
            return ESimpleLogicalValueType::Utf8;
        case NScheme::NTypeIds::Yson:
            return ESimpleLogicalValueType::Any;
        case NScheme::NTypeIds::Json:
        case NScheme::NTypeIds::JsonDocument:
            return ESimpleLogicalValueType::Utf8;
        default:
            return ESimpleLogicalValueType::String;
        }
    }

    static TVector<TColumnSchema> GenColumnsSchema(const TTableColumns& columns) {
        TVector<TColumnSchema> schema;

        for (const auto& [_, column] : columns) {
            // TODO: support pg types
            Y_ABORT_UNLESS(column.Type.GetTypeId() != NScheme::NTypeIds::Pg, "pg types are not supported");
            schema.emplace_back(column.Name, ConvertType(column.Type.GetTypeId()));
        }

        return schema;
    }

    static TTableSchema GenTableSchema(const TTableColumns& columns) {
        return TTableSchema(GenColumnsSchema(columns), false /* strict */);
    }

    static bool IsRetryableError(const NYT::TError& error) {
        if (// Table errors
               error.FindMatching(NYT::NTableClient::EErrorCode::SortOrderViolation)
            || error.FindMatching(NYT::NTableClient::EErrorCode::InvalidDoubleValue)
            || error.FindMatching(NYT::NTableClient::EErrorCode::IncomparableTypes)
            || error.FindMatching(NYT::NTableClient::EErrorCode::UnhashableType)
            || error.FindMatching(NYT::NTableClient::EErrorCode::CorruptedNameTable)
            || error.FindMatching(NYT::NTableClient::EErrorCode::UniqueKeyViolation)
            || error.FindMatching(NYT::NTableClient::EErrorCode::SchemaViolation)
            || error.FindMatching(NYT::NTableClient::EErrorCode::RowWeightLimitExceeded)
            || error.FindMatching(NYT::NTableClient::EErrorCode::InvalidColumnFilter)
            || error.FindMatching(NYT::NTableClient::EErrorCode::InvalidColumnRenaming)
            || error.FindMatching(NYT::NTableClient::EErrorCode::IncompatibleKeyColumns)
            || error.FindMatching(NYT::NTableClient::EErrorCode::TimestampOutOfRange)
            || error.FindMatching(NYT::NTableClient::EErrorCode::InvalidSchemaValue)
            || error.FindMatching(NYT::NTableClient::EErrorCode::FormatCannotRepresentRow)
            || error.FindMatching(NYT::NTableClient::EErrorCode::IncompatibleSchemas)
            // Cypress errors
            || error.FindMatching(NYT::NYTree::EErrorCode::MaxChildCountViolation)
            || error.FindMatching(NYT::NYTree::EErrorCode::MaxStringLengthViolation)
            || error.FindMatching(NYT::NYTree::EErrorCode::MaxAttributeSizeViolation)
            || error.FindMatching(NYT::NYTree::EErrorCode::MaxKeyLengthViolation)
            // Security errors
            || error.FindMatching(NYT::NRpc::EErrorCode::AuthenticationError)
            || error.FindMatching(NYT::NSecurityClient::EErrorCode::AuthenticationError)
            || error.FindMatching(NYT::NSecurityClient::EErrorCode::AuthorizationError)
            || error.FindMatching(NYT::NSecurityClient::EErrorCode::AccountLimitExceeded)
            || error.FindMatching(NYT::NSecurityClient::EErrorCode::UserBanned)
            || error.FindMatching(NYT::NSecurityClient::EErrorCode::NoSuchAccount)
            // Custom errors
            || error.FindMatching(NCustomErrorCodes::InvalidNodeType)
        ) {
            return false;
        }

        return true;
    }

    static NYT::TError CheckNodeType(const TString& path, const TYsonString& yson, const TString& expected) {
        try {
            auto node = ConvertToNode(yson);
            const TString actual = node->Attributes().Get<TString>("type");

            if (actual == expected) {
                return {};
            }

            return NYT::TError(NCustomErrorCodes::InvalidNodeType, TStringBuilder() << "Invalid type of " << path
                << ": expected \"" << expected << "\""
                << ", actual \"" << actual << "\"");
        } catch (const yexception& ex) {
            return NYT::TError(TStringBuilder() << "Error while checking type of " << path
                << ": " << ex.what());
        }
    }

    void Handle(TEvYtWrapper::TEvNodeExistsResponse::TPtr& ev) {
        const auto& result = ev->Get()->Result;

        EXPORT_LOG_D("Handle TEvYtWrapper::TEvNodeExistsResponse"
            << ": self# " << SelfId()
            << ", result# " << ToString(result));

        if (!CheckResult(result, TStringBuf("NodeExists"))) {
            return;
        }

        if (!result.Value()) {
            TCreateNodeOptions opts;
            opts.Recursive = true;
            opts.IgnoreExisting = true;

            if (UseTypeV3) {
                auto attrs = CreateEphemeralAttributes();
                attrs->Set("schema", Schema);
                opts.Attributes = std::move(attrs);
            }

            Send(Client, new TEvYtWrapper::TEvCreateNodeRequest(DstPath.GetPath(), EObjectType::Table, opts));
        } else {
            TGetNodeOptions opts;
            opts.Attributes = TVector<TString>{"type"};

            Send(Client, new TEvYtWrapper::TEvGetNodeRequest(DstPath.GetPath(), opts));
        }
    }

    static TTableWriterOptions TableWriterOptions() {
        auto opts = TTableWriterOptions();
        opts.Config = NYT::New<TTableWriterConfig>();
        opts.Config->MaxRowWeight = MaxRowWeightLimit;
        opts.Config->MaxKeyWeight = MaxKeyWeightLimit;
        return opts;
    }

    void Handle(TEvYtWrapper::TEvCreateNodeResponse::TPtr& ev) {
        const auto& result = ev->Get()->Result;

        EXPORT_LOG_D("Handle TEvYtWrapper::TEvCreateNodeResponse"
            << ": self# " << SelfId()
            << ", result# " << ToString(result));

        if (!CheckResult(result, TStringBuf("CreateNode"))) {
            return;
        }

        Send(Client, new TEvYtWrapper::TEvCreateTableWriterRequest(DstPath, TableWriterOptions()));
    }

    void Handle(TEvYtWrapper::TEvGetNodeResponse::TPtr& ev) {
        const auto& result = ev->Get()->Result;

        EXPORT_LOG_D("Handle TEvYtWrapper::TEvGetNodeResponse"
            << ": self# " << SelfId()
            << ", result# " << ToString(result));

        if (!CheckResult(result, TStringBuf("GetNode"))) {
            return;
        }

        if (!CheckResult(CheckNodeType(DstPath.GetPath(), result.Value(), "table"), TStringBuf("CheckNodeType"))) {
            return;
        }

        Send(Client, new TEvYtWrapper::TEvCreateTableWriterRequest(DstPath, TableWriterOptions()));
    }

    void Handle(TEvYtWrapper::TEvCreateTableWriterResponse::TPtr& ev) {
        const auto& result = ev->Get()->Result;

        EXPORT_LOG_D("Handle TEvYtWrapper::TEvCreateTableWriterResponse"
            << ": self# " << SelfId()
            << ", result# " << ToString(result));

        if (!CheckResult(result, TStringBuf("CreateTableWriter"))) {
            return;
        }

        Writer = result.Value();
        Send(Writer, new TEvYtWrapper::TEvGetNameTableRequest());
    }

    void Handle(TEvYtWrapper::TEvGetNameTableResponse::TPtr& ev) {
        const auto& result = ev->Get()->Result;

        EXPORT_LOG_D("Handle TEvYtWrapper::TEvGetNameTableResponse"
            << ": self# " << SelfId()
            << ", result# " << ToString(result));

        if (!CheckResult(result, TStringBuf("GetNameTable"))) {
            return;
        }

        NameTable = result.Value();
        for (const auto& column : Schema.Columns()) {
            NameTable->RegisterName(column.Name());
        }

        if (Scanner) {
            Send(Scanner, new TEvExportScan::TEvFeed());
        }
    }

    void Handle(TEvExportScan::TEvReady::TPtr& ev) {
        EXPORT_LOG_D("Handle TEvExportScan::TEvReady"
            << ": self# " << SelfId()
            << ", sender# " << ev->Sender);

        Scanner = ev->Sender;

        if (Error) {
            return PassAway();
        }

        if (Writer && NameTable) {
            Send(Scanner, new TEvExportScan::TEvFeed());
        }
    }

    void Handle(TEvBuffer::TPtr& ev) {
        EXPORT_LOG_D("Handle TEvExportScan::TEvBuffer"
            << ": self# " << SelfId()
            << ", sender# " << ev->Sender
            << ", msg# " << ev->Get()->ToString());

        if (ev->Sender != Scanner) {
            EXPORT_LOG_W("Received buffer from unknown scanner"
                << ": self# " << SelfId()
                << ", sender# " << ev->Sender
                << ", scanner# " << Scanner);
            return;
        }

        Last = ev->Get()->Last;
        Send(Writer, new TEvYtWrapper::TEvWriteTableRequest(std::move(ev->Get()->Buffer), Last));
    }

    void Handle(TEvYtWrapper::TEvWriteTableResponse::TPtr& ev) {
        const auto& result = ev->Get()->Result;

        EXPORT_LOG_D("Handle TEvYtWrapper::TEvWriteTableResponse"
            << ": self# " << SelfId()
            << ", result# " << ToString(result));

        if (!CheckResult(result, TStringBuf("WriteTable"))) {
            return;
        }

        if (Last) {
            return Finish();
        }

        Send(Scanner, new TEvExportScan::TEvFeed());
    }

    template <typename TResult>
    bool CheckResult(const TResult& result, const TStringBuf marker) {
        if (result.IsOK()) {
            return true;
        }

        EXPORT_LOG_E("Error at '" << marker << "'"
            << ": self# " << SelfId()
            << ", error# " << ToString(result));
        RetryOrFinish(result);

        return false;
    }

    void RetryOrFinish(const NYT::TError& error) {
        if (Attempt++ < Retries && IsRetryableError(error)) {
            Schedule(TDuration::Minutes(1), new TEvents::TEvWakeup());
        } else {
            Finish(false, TStringBuilder() << "YT error: " << ToString(error));
        }
    }

    void Finish(bool success = true, const TString& error = TString()) {
        EXPORT_LOG_I("Finish"
            << ": self# " << SelfId()
            << ", success# " << success
            << ", error# " << error);

        if (!success) {
            Error = error;
        }

        if (!Scanner) {
            return;
        }

        PassAway();
    }

    void PassAway() override {
        if (Scanner) {
            Send(Scanner, new TEvExportScan::TEvFinish(Error.Empty(), Error.GetOrElse(TString())));
        }

        Send(Writer, new TEvents::TEvPoisonPill());
        Send(Client, new TEvents::TEvPoisonPill());

        TActor::PassAway();
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::EXPORT_YT_UPLOADER_ACTOR;
    }

    static constexpr TStringBuf LogPrefix() {
        return "yt"sv;
    }

    explicit TYtUploader(
            const TTableColumns& columns,
            const TString& serverName,
            const TString& token,
            const TString& dstPath,
            bool useTypeV3,
            ui32 retries)
        : ServerName(serverName)
        , Token(token)
        , DstPath(TRichYPath::Parse(dstPath))
        , UseTypeV3(useTypeV3)
        , Schema(GenTableSchema(columns))
        , Retries(retries)
        , Attempt(0)
    {
    }

    void Bootstrap() {
        EXPORT_LOG_D("Bootstrap"
            << ": self# " << SelfId()
            << ", attempt# " << Attempt);

        NameTable.Reset();
        Last = false;

        if (Attempt) {
            Send(std::exchange(Scanner, TActorId()), new TEvExportScan::TEvReset());
            Send(std::exchange(Writer, TActorId()), new TEvents::TEvPoisonPill());
            Send(std::exchange(Client, TActorId()), new TEvents::TEvPoisonPill());
        }

        Client = RegisterWithSameMailbox(CreateYtWrapper(ServerName, Token));
        Send(Client, new TEvYtWrapper::TEvNodeExistsRequest(DstPath.GetPath(), TNodeExistsOptions()));

        Become(&TThis::StateWork);
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvYtWrapper::TEvNodeExistsResponse, Handle);
            hFunc(TEvYtWrapper::TEvCreateNodeResponse, Handle);
            hFunc(TEvYtWrapper::TEvGetNodeResponse, Handle);
            hFunc(TEvYtWrapper::TEvCreateTableWriterResponse, Handle);
            hFunc(TEvYtWrapper::TEvGetNameTableResponse, Handle);
            hFunc(TEvYtWrapper::TEvWriteTableResponse, Handle);

            hFunc(TEvExportScan::TEvReady, Handle);
            hFunc(TEvBuffer, Handle);

            cFunc(TEvents::TEvWakeup::EventType, Bootstrap);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }

private:
    const TString ServerName;
    const TString Token;
    const TRichYPath DstPath;
    const bool UseTypeV3;
    const TTableSchema Schema;
    const ui32 Retries;

    ui32 Attempt;
    TActorId Client;
    TActorId Writer;
    TActorId Scanner;

    TNameTablePtr NameTable;
    bool Last;
    TMaybe<TString> Error;

}; // TYtUploader

class TYtBuffer: public IBuffer {
    struct TColumn {
        NScheme::TTypeId Type;
        int Id; // in name table
    };

    using TColumns = TMap<ui32, TColumn>;
    using TOrderedColumns = TVector<TColumns::const_iterator>;

    static TColumns MakeColumns(const TTableColumns& columns) {
        TColumns result;

        int i = 0;
        for (const auto& [tag, column] : columns) {
            // TODO: support pg types
            Y_ABORT_UNLESS(column.Type.GetTypeId() != NScheme::NTypeIds::Pg, "pg types are not supported");
            result[tag] = {column.Type.GetTypeId(), i++};
        }

        return result;
    }

    static TUnversionedValue ConvertValue(NScheme::TTypeId type, const TCell& cell, int id, bool useTypeV3, TString& buffer) {
        if (cell.IsNull()) {
            return MakeUnversionedNullValue(id);
        }

        switch (type) {
        case NScheme::NTypeIds::Int32:
            return MakeUnversionedInt64Value(cell.AsValue<i32>(), id);
        case NScheme::NTypeIds::Uint32:
            return MakeUnversionedUint64Value(cell.AsValue<ui32>(), id);
        case NScheme::NTypeIds::Int64:
            return MakeUnversionedInt64Value(cell.AsValue<i64>(), id);
        case NScheme::NTypeIds::Uint64:
            return MakeUnversionedUint64Value(cell.AsValue<ui64>(), id);
        case NScheme::NTypeIds::Uint8:
        //case NScheme::NTypeIds::Byte:
            return MakeUnversionedUint64Value(cell.AsValue<ui8>(), id);
        case NScheme::NTypeIds::Int8:
            return MakeUnversionedInt64Value(cell.AsValue<i8>(), id);
        case NScheme::NTypeIds::Int16:
            return MakeUnversionedInt64Value(cell.AsValue<i16>(), id);
        case NScheme::NTypeIds::Uint16:
            return MakeUnversionedUint64Value(cell.AsValue<ui16>(), id);
        case NScheme::NTypeIds::Bool:
            return MakeUnversionedBooleanValue(cell.AsValue<bool>(), id);
        case NScheme::NTypeIds::Double:
            return MakeUnversionedDoubleValue(cell.AsValue<double>(), id);
        case NScheme::NTypeIds::Float:
            return MakeUnversionedDoubleValue(cell.AsValue<float>(), id);
        default:
            if (useTypeV3) {
                switch (type) {
                case NScheme::NTypeIds::Date:
                    return MakeUnversionedUint64Value(cell.AsValue<ui16>(), id);
                case NScheme::NTypeIds::Datetime:
                    return MakeUnversionedUint64Value(cell.AsValue<ui32>(), id);
                case NScheme::NTypeIds::Timestamp:
                    return MakeUnversionedUint64Value(cell.AsValue<ui64>(), id);
                case NScheme::NTypeIds::Interval:
                    return MakeUnversionedInt64Value(cell.AsValue<i64>(), id);
                case NScheme::NTypeIds::Date32:
                    return MakeUnversionedInt64Value(cell.AsValue<i32>(), id);
                case NScheme::NTypeIds::Datetime64:
                case NScheme::NTypeIds::Timestamp64:
                case NScheme::NTypeIds::Interval64:
                    return MakeUnversionedInt64Value(cell.AsValue<i64>(), id);
                case NScheme::NTypeIds::Decimal:
                    buffer = NDataShard::DecimalToString(cell.AsValue<std::pair<ui64, i64>>());
                    return MakeUnversionedStringValue(buffer, id);
                case NScheme::NTypeIds::DyNumber:
                    buffer = NDataShard::DyNumberToString(cell.AsBuf());
                    return MakeUnversionedStringValue(buffer, id);
                case NScheme::NTypeIds::Yson:
                    return MakeUnversionedAnyValue(cell.AsBuf(), id);
                case NScheme::NTypeIds::JsonDocument:
                    buffer = NBinaryJson::SerializeToJson(cell.AsBuf());
                    return MakeUnversionedStringValue(buffer, id);
                default:
                    return MakeUnversionedStringValue(cell.AsBuf(), id);
                }
            } else {
                return MakeUnversionedStringValue(cell.AsBuf(), id);
            }
        }
    }

public:
    explicit TYtBuffer(const TTableColumns& columns, ui64 rowsLimit, ui64 bytesLimit, bool useTypeV3)
        : Columns(MakeColumns(columns))
        , RowsLimit(rowsLimit)
        , BytesLimit(bytesLimit)
        , UseTypeV3(useTypeV3)
        , BytesRead(0)
        , BytesSent(0)
    {
    }

    void ColumnsOrder(const TVector<ui32>& tags) override {
        Y_ABORT_UNLESS(tags.size() == Columns.size());

        OrderedColumns.clear();
        OrderedColumns.reserve(tags.size());

        for (const auto& tag : tags) {
            auto it = Columns.find(tag);
            Y_ABORT_UNLESS(it != Columns.end());
            OrderedColumns.push_back(it);
        }
    }

    bool Collect(const NTable::IScan::TRow& row) override {
        TUnversionedOwningRowBuilder rowBuilder;

        for (ui32 i = 0; i < (*row).size(); ++i) {
            Y_ABORT_UNLESS(i < OrderedColumns.size());
            const auto& column = OrderedColumns[i]->second;
            const auto& cell = (*row)[i];

            TString buffer;
            const auto value = ConvertValue(column.Type, cell, column.Id, UseTypeV3, buffer);

            rowBuilder.AddValue(value);
            BytesRead += cell.Size();
            BytesSent += EstimateRowValueSize(value);
        }

        Buffer.emplace_back(rowBuilder.FinishRow());
        return true;
    }

    IEventBase* PrepareEvent(bool last, IBuffer::TStats& stats) override {
        stats.Rows = Buffer.size();
        stats.BytesRead = BytesRead;
        stats.BytesSent = BytesSent;

        return new TEvBuffer(Flush(), last);
    }

    void Clear() override {
        Flush();
    }

    TVector<TUnversionedOwningRow> Flush() {
        BytesRead = 0;
        BytesSent = 0;
        return std::exchange(Buffer, TVector<TUnversionedOwningRow>());
    }

    bool IsFilled() const override {
        return Buffer.size() >= RowsLimit || BytesSent >= BytesLimit;
    }

    TString GetError() const override {
        Y_ABORT("unreachable");
    }

private:
    const TColumns Columns;
    const ui64 RowsLimit;
    const ui64 BytesLimit;
    const bool UseTypeV3;

    TOrderedColumns OrderedColumns;

    TVector<TUnversionedOwningRow> Buffer;
    ui64 BytesRead;
    ui64 BytesSent;

}; // TYtBuffer

IActor* TYtExport::CreateUploader(const TActorId&, ui64) const {
    const auto& settings = Task.GetYTSettings();
    return new TYtUploader(
        Columns,
        Join(':', settings.GetHost(), settings.GetPort()),
        settings.GetToken(),
        settings.GetTablePattern(),
        settings.GetUseTypeV3(),
        Task.GetNumberOfRetries()
    );
}

IBuffer* TYtExport::CreateBuffer() const {
    const auto& settings = Task.GetYTSettings();
    const auto& scanSettings = Task.GetScanSettings();
    const ui64 maxRows = scanSettings.GetRowsBatchSize() ? scanSettings.GetRowsBatchSize() : Max<ui64>();
    const ui64 maxBytes = scanSettings.GetBytesBatchSize();

    return new TYtBuffer(Columns, maxRows, maxBytes, settings.GetUseTypeV3());
}

void TYtExport::Shutdown() const {
    NYT::Shutdown();
}

} // NYndx
} // NKikimr

#endif // KIKIMR_DISABLE_YT
