#include "yql_ydb_remote_provider_impl.h"

#include <yql/essentials/core/sql_types/block.h>

#include <ydb/library/yql/providers/ydb_remote/expr_nodes/yql_ydb_remote_expr_nodes.h>
#include <ydb/library/yql/providers/native/operation_context.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/providers/common/provider/yql_provider.h>

#include <library/cpp/threading/future/future.h>
#include <util/generic/algorithm.h>

#include <optional>

namespace NYql::NYdbRemote {
namespace {

using namespace NNodes;

bool ParseRead(const TYdbRemoteRead& read, TString& table, TExprContext& ctx) {
    const auto& node = read.Ref();
    if (node.ChildrenSize() < 3 || node.ChildrenSize() > 5) {
        ctx.AddError(TIssue(ctx.GetPosition(read.Pos()), "Native YDB supports a single table read"));
        return false;
    }
    // SQL emits a direct Key in some translation modes, while KQP's external
    // table rewrite and query-mode SQL wrap the same key in MrTableConcat.
    const auto* key = node.Child(2);
    if (key->IsCallable("MrTableConcat")) {
        if (key->ChildrenSize() != 1) {
            ctx.AddError(TIssue(ctx.GetPosition(read.Pos()), "Native YDB supports a single table read"));
            return false;
        }
        key = key->Child(0);
    }
    if (!key->IsCallable("Key") || key->ChildrenSize() != 1 ||
        !key->Head().IsList() || key->Head().ChildrenSize() != 2 ||
        !key->Head().Head().IsAtom("table") || !key->Head().Tail().IsCallable("String") ||
        key->Head().Tail().ChildrenSize() != 1 || !key->Head().Tail().Head().IsAtom()) {
        ctx.AddError(TIssue(ctx.GetPosition(read.Pos()), "Native YDB requires a literal table path; ranges and table functions are unsupported"));
        return false;
    }
    if (node.ChildrenSize() > 4 && (!node.Child(4)->IsList() || node.Child(4)->ChildrenSize())) {
        ctx.AddError(TIssue(ctx.GetPosition(read.Pos()), "Native YDB read settings, views and explicit schemas are not supported yet"));
        return false;
    }
    table = key->Head().Tail().Head().Content();
    if (table.empty() || table.size() > 4096 || table.Contains('\0')) {
        ctx.AddError(TIssue(ctx.GetPosition(read.Pos()), "Native YDB requires a table path of 1 to 4096 bytes"));
        return false;
    }
    return true;
}

struct TMetadataRequest {
    TState::TTableKey Key;
    TMetadataSchema Schema;
};

// Requests are sequential and schemas are size checked. SDK response buffers
// and the cached schemas have no resource-manager reservation in this stage.
class TMetadataBatch final : public std::enable_shared_from_this<TMetadataBatch> {
public:
    TMetadataBatch(TState::TPtr state, TVector<TMetadataRequest> requests)
        : State_(std::move(state))
        , Context_{State_->MetadataDeadline, Cancellation_.Token()}
        , Requests(std::move(requests))
    {
    }

    void Start() {
        StartNextTable();
    }

    void Cancel() {
        // Stop subsequent provider phases. An in-flight Table RPC retains this
        // batch until its callback or the original provider-local RPC deadline.
        Cancellation_.Cancel();
    }

    NThreading::TFuture<void> GetFuture() const {
        return Done_.GetFuture();
    }

private:
    bool Continue() {
        if (Context_.Cancellation.IsCancellationRequested()) {
            Error = "Native YDB metadata cancelled";
        } else if (TInstant::Now() >= Context_.Deadline) {
            Error = "Native YDB metadata deadline exceeded";
        }
        if (Error) {
            Close();
            return false;
        }
        return true;
    }

    TDuration RemainingTimeout(TDuration cap = TDuration::Max()) {
        if (Context_.Cancellation.IsCancellationRequested()) {
            Error = "Native YDB metadata cancelled";
            return TDuration::Zero();
        }
        const auto now = TInstant::Now();
        if (now >= Context_.Deadline) {
            Error = "Native YDB metadata deadline exceeded";
            return TDuration::Zero();
        }
        return Min(Context_.Deadline - now, cap);
    }

    void StartNextTable() {
        if (!Continue()) {
            return;
        }
        if (Index_ == Requests.size()) {
            Done_.TrySetValue();
            return;
        }
        CreateSession();
    }

    void CreateSession() {
        auto self = shared_from_this();
        try {
            const auto& cluster = State_->Clusters.at(Requests[Index_].Key.first);
            const auto credentials = State_->CredentialsFactory->Create(
                State_->Tokens.at(Requests[Index_].Key.first), false);
            Client_ = std::make_shared<NYdb::NTable::TTableClient>(cluster.UseTls ? State_->TlsDriver : State_->Driver,
                NYdb::NTable::TClientSettings()
                    .Database(cluster.Database)
                    .DiscoveryEndpoint(cluster.Endpoint)
                    .DiscoveryMode(NYdb::EDiscoveryMode::Off)
                    .SslCredentials(NYdb::TSslCredentials(cluster.UseTls))
                    .CredentialsProviderFactory(credentials)
                    .SessionPoolSettings(NYdb::NTable::TSessionPoolSettings().MaxActiveSessions(1).MinPoolSize(0).RetryLimit(0)));
            const auto remaining = RemainingTimeout();
            if (!remaining) {
                Close();
                return;
            }
            Client_->CreateSession(NYdb::NTable::TCreateSessionSettings()
                .ClientTimeout(remaining).OperationTimeout(remaining))
                .Subscribe([self](const NYdb::NTable::TAsyncCreateSessionResult& future) {
                    try {
                        const auto& result = future.GetValue();
                        if (result.IsSuccess()) {
                            self->Session_ = result.GetSession();
                        } else {
                            self->Error = TStringBuilder() << "Native YDB metadata session failed: " << result.GetStatus();
                        }
                    } catch (...) {
                        self->Error = "Native YDB metadata session failed";
                    }
                    self->Describe();
                });
        } catch (...) {
            // Credential providers may put credentials in exception text.
            Error = "Native YDB metadata client initialization failed";
            Close();
        }
    }

    void Describe() {
        if (!Continue()) {
            return;
        }
        auto self = shared_from_this();
        try {
            const auto& key = Requests[Index_].Key;
            const auto& cluster = State_->Clusters.at(key.first);
            const TString tablePath = key.second.StartsWith('/') ? key.second : cluster.Database + "/" + key.second;
            const auto remaining = RemainingTimeout();
            if (!remaining) {
                Close();
                return;
            }
            Session_->DescribeTable(tablePath, NYdb::NTable::TDescribeTableSettings()
                .ClientTimeout(remaining).OperationTimeout(remaining))
                .Subscribe([self](const NYdb::NTable::TAsyncDescribeTableResult& future) {
                    try {
                        const auto& result = future.GetValue();
                        if (!self->Context_.Cancellation.IsCancellationRequested() &&
                            TInstant::Now() < self->Context_.Deadline) {
                            if (result.IsSuccess()) {
                                ExtractMetadataSchema(NYdb::TProtoAccessor::GetProto(result.GetTableDescription()),
                                    self->Requests[self->Index_].Schema, self->Error);
                            } else {
                                self->Error = TStringBuilder() << "Native YDB DescribeTable failed: " << result.GetStatus();
                            }
                        }
                    } catch (...) {
                        self->Error = "Native YDB DescribeTable failed";
                    }
                    self->Close();
                });
        } catch (...) {
            Error = "Native YDB DescribeTable failed";
            Close();
        }
    }

    void FinishTable() {
        Session_.reset();
        Client_.reset();
        if (Context_.Cancellation.IsCancellationRequested()) {
            Error = "Native YDB metadata cancelled";
        } else if (TInstant::Now() >= Context_.Deadline) {
            Error = "Native YDB metadata deadline exceeded";
        }
        if (Error) {
            Done_.TrySetValue();
            return;
        }
        ++Index_;
        StartNextTable();
    }

    void Close() {
        if (!Session_ || Context_.Cancellation.IsCancellationRequested() || TInstant::Now() >= Context_.Deadline) {
            // Skip further provider RPCs when cancellation/deadline is observed.
            // The existing SDK may issue its own DeleteSession on destruction;
            // that cleanup has a separate SDK timeout.
            FinishTable();
            return;
        }
        auto self = shared_from_this();
        try {
            const auto remaining = RemainingTimeout(TDuration::Seconds(5));
            if (!remaining) {
                FinishTable();
                return;
            }
            Session_->Close(NYdb::NTable::TCloseSessionSettings()
                .ClientTimeout(remaining).OperationTimeout(remaining))
                .Subscribe([self](const NYdb::TAsyncStatus& future) {
                    try {
                        if (!future.GetValue().IsSuccess() && !self->Error) {
                            self->Error = "Native YDB metadata session cleanup failed";
                        }
                    } catch (...) {
                        if (!self->Error) {
                            self->Error = "Native YDB metadata session cleanup failed";
                        }
                    }
                    self->FinishTable();
                });
        } catch (...) {
            if (!Error) {
                Error = "Native YDB metadata session cleanup failed";
            }
            FinishTable();
        }
    }

    const TState::TPtr State_;
    NThreading::TCancellationTokenSource Cancellation_;
    const NNative::TOperationContext Context_;
    NThreading::TPromise<void> Done_ = NThreading::NewPromise<void>();
    std::shared_ptr<NYdb::NTable::TTableClient> Client_;
    std::optional<NYdb::NTable::TSession> Session_;
    size_t Index_ = 0;

public:
    TVector<TMetadataRequest> Requests;
    TString Error;
};

class TLoadMetadataTransformer final : public TGraphTransformerBase {
public:
    explicit TLoadMetadataTransformer(TState::TPtr state)
        : State_(std::move(state))
    {
    }

    ~TLoadMetadataTransformer() override {
        Rewind();
    }

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) override {
        output = input;
        if (ctx.Step.IsDone(TExprStep::LoadTablesMetadata)) {
            return TStatus::Ok;
        }
        TVector<TMetadataRequest> requests;
        THashSet<TState::TTableKey> keys;
        for (const auto& node : FindReads(input)) {
            const TYdbRemoteRead read(node);
            TString table;
            if (!ParseRead(read, table, ctx)) {
                return TStatus::Error;
            }
            TState::TTableKey key(read.DataSource().Cluster().StringValue(), table);
            if (State_->Tables.contains(key) || !keys.insert(key).second) {
                continue;
            }
            if (keys.size() + State_->Tables.size() > MaxMetadataTables) {
                ctx.AddError(TIssue(ctx.GetPosition(read.Pos()), "Native YDB metadata table limit exceeded"));
                return TStatus::Error;
            }
            requests.push_back({std::move(key), {}});
        }
        if (requests.empty()) {
            return Rewrite(input, output, ctx);
        }
        if (TInstant::Now() >= State_->MetadataDeadline) {
            ctx.AddError(TIssue({}, "Native YDB metadata deadline exceeded"));
            return TStatus::Error;
        }
        Batch_ = std::make_shared<TMetadataBatch>(State_, std::move(requests));
        AsyncFuture_ = Batch_->GetFuture();
        Batch_->Start();
        return TStatus::Async;
    }

    NThreading::TFuture<void> DoGetAsyncFuture(const TExprNode&) override {
        return AsyncFuture_;
    }

    TStatus DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) override {
        output = input;
        AsyncFuture_.GetValue();
        if (Batch_->Error) {
            ctx.AddError(TIssue({}, Batch_->Error));
            Batch_.reset();
            return TStatus::Error;
        }
        for (auto& request : Batch_->Requests) {
            TTable table;
            TVector<const TItemExprType*> items;
            for (auto& [name, type] : request.Schema.Columns) {
                const auto* annotation = ParseColumnType(type, ctx);
                if (!annotation || name == BlockLengthColumnName) {
                    ctx.AddError(TIssue({}, "Native YDB does not support the type or name of a column"));
                    return TStatus::Error;
                }
                if (!table.ColumnTypes.emplace(name, std::move(type)).second) {
                    ctx.AddError(TIssue({}, "Native YDB received duplicate column names"));
                    return TStatus::Error;
                }
                table.ColumnOrder.emplace_back(name);
                items.emplace_back(ctx.MakeType<TItemExprType>(name, annotation));
            }
            table.RowType = ctx.MakeType<TStructExprType>(items);
            State_->Tables.emplace(request.Key, std::move(table));
        }
        Batch_.reset();
        return Rewrite(input, output, ctx);
    }

    void Rewind() override {
        if (Batch_) {
            Batch_->Cancel();
            Batch_.reset();
        }
        AsyncFuture_ = {};
    }

private:
    static TExprNode::TListType FindReads(const TExprNode::TPtr& input) {
        return FindNodes(input, [](const TExprNode::TPtr& node) {
            return TYdbRemoteRead::Match(node.Get()) && node->ChildrenSize() > 1 &&
                TYdbRemoteDataSource::Match(node->Child(1));
        });
    }

    TStatus Rewrite(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
        TNodeOnNodeOwnedMap replacements;
        for (const auto& node : FindReads(input)) {
            const TYdbRemoteRead read(node);
            TString table;
            if (!ParseRead(read, table, ctx)) {
                return TStatus::Error;
            }
            replacements.emplace(node.Get(), Build<TYdbRemoteReadTable>(ctx, read.Pos())
                .World(read.World())
                .DataSource(read.DataSource())
                .Table().Value(table).Build()
                .Columns(read.Ref().ChildrenSize() > 3 ? read.Ref().ChildPtr(3) : ctx.NewCallable(read.Pos(), "Void", {}))
                .Done().Ptr());
        }
        return RemapExpr(input, output, replacements, ctx, TOptimizeExprSettings(nullptr));
    }

    const TState::TPtr State_;
    std::shared_ptr<TMetadataBatch> Batch_;
    NThreading::TFuture<void> AsyncFuture_;
};

} // namespace

bool ExtractMetadataSchema(const Ydb::Table::DescribeTableResult& description,
                           TMetadataSchema& schema, TString& error) {
    if (description.store_type() != Ydb::Table::STORE_TYPE_UNSPECIFIED &&
        description.store_type() != Ydb::Table::STORE_TYPE_ROW) {
        error = "Native YDB currently supports only row tables";
        return false;
    }
    if (!description.columns_size() || static_cast<ui64>(description.columns_size()) > MaxMetadataColumns) {
        error = "Native YDB metadata column limit exceeded";
        return false;
    }
    ui64 schemaBytes = 0;
    ui64 nameBytes = 0;
    for (const auto& column : description.columns()) {
        nameBytes += column.name().size();
        schemaBytes += column.name().size() + column.type().ByteSizeLong();
        if (column.name().empty() || column.name().size() > 4096 || schemaBytes > MaxMetadataSchemaBytes ||
            nameBytes > MaxMetadataSchemaBytes / 2) {
            error = "Native YDB metadata schema limit exceeded";
            return false;
        }
        const auto& type = column.type();
        const auto& item = type.has_optional_type() ? type.optional_type().item() : type;
        if (!item.has_type_id()) {
            error = "Native YDB does not support the type of a column";
            return false;
        }
    }
    // Bound the provider-owned copy. SDK protobuf decoding has already happened
    // and is not covered by these schema limits.
    schema.Columns.reserve(description.columns_size());
    for (const auto& column : description.columns()) {
        const auto& type = column.type();
        schema.Columns.emplace_back(column.name(),
            column.not_null() && type.has_optional_type() ? type.optional_type().item() : type);
    }
    return true;
}

THolder<IGraphTransformer> CreateLoadMetadataTransformer(TState::TPtr state) {
    return MakeHolder<TLoadMetadataTransformer>(std::move(state));
}

} // namespace NYql::NYdbRemote
