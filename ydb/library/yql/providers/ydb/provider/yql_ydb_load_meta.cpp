#include "yql_ydb_provider_impl.h"

#include <yql/essentials/core/sql_types/block.h>

#include <ydb/library/yql/providers/ydb/expr_nodes/yql_ydb_expr_nodes.h>
#include <ydb/library/yql/providers/native/operation_context.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/providers/common/provider/yql_provider.h>

#include <library/cpp/threading/future/future.h>
#include <util/generic/guid.h>

#include <chrono>
#include <list>
#include <mutex>
#include <optional>

namespace NYql {
namespace {

// The baseline SDK keeps STOP callbacks until their database state is destroyed.
// Give each cache entry its own state: evicting and recreating a client must not
// append callbacks to a state held alive by another client or a query stream.
class TMetadataCredentialsFactory final : public ::NYdb::ICredentialsProviderFactory {
public:
    explicit TMetadataCredentialsFactory(std::shared_ptr<::NYdb::ICredentialsProviderFactory> inner)
        : Inner_(std::move(inner))
        , Identity_(std::string("ydb-external-metadata:") + std::string(CreateGuidAsString()))
    {
    }

    ::NYdb::TCredentialsProviderPtr CreateProvider() const override {
        return Inner_->CreateProvider();
    }

    ::NYdb::TCredentialsProviderPtr CreateProvider(std::weak_ptr<::NYdb::ICoreFacility> facility) const override {
        return Inner_->CreateProvider(std::move(facility));
    }

    std::string GetClientIdentity() const override {
        return Identity_;
    }

private:
    const std::shared_ptr<::NYdb::ICredentialsProviderFactory> Inner_;
    const std::string Identity_;
};

class TMetadataClientCache final : public IYdbMetadataClientCache {
public:
    TMetadataClientCache(const ::NYdb::TDriver& driver, const ::NYdb::TDriver& tlsDriver,
                        size_t maxEntries, TDuration idleTimeout)
        : Driver_(driver)
        , TlsDriver_(tlsDriver)
        , MaxEntries_(maxEntries)
        , IdleTimeout_(idleTimeout)
    {
    }

    std::shared_ptr<::NYdb::NTable::TTableClient> GetClient(
        const TString& endpoint, const TString& database, bool useTls,
        const TString& structuredToken, IStructuredTokenCredentialsFactory::TPtr credentialsFactory) override {
        Y_ENSURE(credentialsFactory, "Ydb metadata credentials factory is missing");
        // Exact token equality isolates rotated secrets, including a new value
        // behind the same secret reference. Keys never leave this bounded cache.
        const TKey key{endpoint, database, structuredToken, useTls, std::move(credentialsFactory)};
        std::list<TEntry> retired;
        {
            std::lock_guard lock(Mutex_);
            Expire(retired);
            if (auto client = Find(key)) {
                return client;
            }
        }

        // Credential providers and SDK construction may execute callbacks. Do
        // not hold the cache lock across either them or client destruction.
        auto innerCredentials = key.CredentialsFactory->Create(structuredToken, false);
        Y_ENSURE(innerCredentials, "Ydb metadata credentials could not be initialized");
        auto credentials = std::make_shared<TMetadataCredentialsFactory>(std::move(innerCredentials));
        auto client = std::make_shared<::NYdb::NTable::TTableClient>(useTls ? TlsDriver_ : Driver_,
            ::NYdb::NTable::TClientSettings()
                .Database(database)
                .DiscoveryEndpoint(endpoint)
                .DiscoveryMode(::NYdb::EDiscoveryMode::Off)
                .SslCredentials(::NYdb::TSslCredentials(useTls))
                .CredentialsProviderFactory(std::move(credentials))
                .SessionPoolSettings(::NYdb::NTable::TSessionPoolSettings().MaxActiveSessions(1).MinPoolSize(0).RetryLimit(0)));
        const auto keyBytes = key.Bytes();
        if (!MaxEntries_ || keyBytes > MaxKeyBytes) {
            return client;
        }
        {
            std::lock_guard lock(Mutex_);
            Expire(retired);
            if (auto existing = Find(key)) {
                return existing;
            }
            while (!Entries_.empty() && (Entries_.size() >= MaxEntries_ || KeyBytes_ + keyBytes > MaxKeyBytes)) {
                Retire(Entries_.begin(), retired);
            }
            Entries_.push_back({key, client, std::chrono::steady_clock::now()});
            KeyBytes_ += keyBytes;
        }
        return client;
    }

private:
    struct TKey {
        TString Endpoint;
        TString Database;
        TString Token;
        bool UseTls;
        IStructuredTokenCredentialsFactory::TPtr CredentialsFactory;

        bool operator==(const TKey&) const = default;

        size_t Bytes() const {
            return Endpoint.size() + Database.size() + Token.size();
        }
    };

    struct TEntry {
        TKey Key;
        std::shared_ptr<::NYdb::NTable::TTableClient> Client;
        std::chrono::steady_clock::time_point LastUsed;
    };

    std::shared_ptr<::NYdb::NTable::TTableClient> Find(const TKey& key) {
        for (auto it = Entries_.begin(); it != Entries_.end(); ++it) {
            if (it->Key == key) {
                it->LastUsed = std::chrono::steady_clock::now();
                auto client = it->Client;
                Entries_.splice(Entries_.end(), Entries_, it);
                return client;
            }
        }
        return {};
    }

    void Retire(std::list<TEntry>::iterator it, std::list<TEntry>& retired) {
        KeyBytes_ -= it->Key.Bytes();
        retired.splice(retired.end(), Entries_, it);
    }

    void Expire(std::list<TEntry>& retired) {
        const auto now = std::chrono::steady_clock::now();
        while (!Entries_.empty() && static_cast<ui64>(std::chrono::duration_cast<std::chrono::microseconds>(
                now - Entries_.front().LastUsed).count()) >= IdleTimeout_.MicroSeconds()) {
            Retire(Entries_.begin(), retired);
        }
    }

    static constexpr size_t MaxKeyBytes = 1 << 20;
    const ::NYdb::TDriver Driver_;
    const ::NYdb::TDriver TlsDriver_;
    const size_t MaxEntries_;
    const TDuration IdleTimeout_;
    std::mutex Mutex_;
    std::list<TEntry> Entries_;
    size_t KeyBytes_ = 0;
};

} // namespace

std::shared_ptr<IYdbMetadataClientCache> CreateYdbMetadataClientCache(
    const ::NYdb::TDriver& driver, const ::NYdb::TDriver& tlsDriver, size_t maxEntries, TDuration idleTimeout) {
    return std::make_shared<TMetadataClientCache>(driver, tlsDriver, maxEntries, idleTimeout);
}

} // namespace NYql

namespace NYql::NYdb {
namespace {

using namespace NNodes;

TString ColumnTypeName(const Ydb::Type& type) {
    const bool optional = type.has_optional_type();
    const auto& item = optional ? type.optional_type().item() : type;
    TString name;
    if (item.has_type_id()) {
        name = Ydb::Type::PrimitiveTypeId_Name(item.type_id());
        if (name.empty()) {
            name = TStringBuilder() << "primitive type " << static_cast<int>(item.type_id());
        }
    } else if (item.has_decimal_type()) {
        name = TStringBuilder() << "Decimal(" << item.decimal_type().precision() << "," << item.decimal_type().scale() << ")";
    } else if (item.has_pg_type()) {
        name = TStringBuilder() << "Pg(oid=" << item.pg_type().oid() << ")";
    } else {
        name = TStringBuilder() << "type kind " << static_cast<int>(item.type_case());
    }
    if (optional) {
        return TStringBuilder() << "Optional<" << name << ">";
    }
    return name;
}

TString UnsupportedColumn(const TString& name, const Ydb::Type& type) {
    return TStringBuilder() << "Ydb does not support column '" << name << "' of type "
        << ColumnTypeName(type) << "; all table columns must have supported types";
}

bool ParseRead(const TYdbRead& read, TString& table, TExprContext& ctx) {
    const auto& node = read.Ref();
    if (node.ChildrenSize() < 3 || node.ChildrenSize() > 5) {
        ctx.AddError(TIssue(ctx.GetPosition(read.Pos()), "Ydb supports a single table read"));
        return false;
    }
    // SQL emits a direct Key in some translation modes, while KQP's external
    // table rewrite and query-mode SQL wrap the same key in MrTableConcat.
    const auto* key = node.Child(2);
    if (key->IsCallable("MrTableConcat")) {
        if (key->ChildrenSize() != 1) {
            ctx.AddError(TIssue(ctx.GetPosition(read.Pos()), "Ydb supports a single table read"));
            return false;
        }
        key = key->Child(0);
    }
    if (!key->IsCallable("Key") || key->ChildrenSize() != 1 ||
        !key->Head().IsList() || key->Head().ChildrenSize() != 2 ||
        !key->Head().Head().IsAtom("table") || !key->Head().Tail().IsCallable("String") ||
        key->Head().Tail().ChildrenSize() != 1 || !key->Head().Tail().Head().IsAtom()) {
        ctx.AddError(TIssue(ctx.GetPosition(read.Pos()), "Ydb requires a literal table path; ranges and table functions are unsupported"));
        return false;
    }
    if (node.ChildrenSize() > 4 && (!node.Child(4)->IsList() || node.Child(4)->ChildrenSize())) {
        ctx.AddError(TIssue(ctx.GetPosition(read.Pos()), "Ydb read settings, views and explicit schemas are not supported yet"));
        return false;
    }
    table = key->Head().Tail().Head().Content();
    if (table.empty() || table.size() > 4096 || table.Contains('\0')) {
        ctx.AddError(TIssue(ctx.GetPosition(read.Pos()), "Ydb requires a table path of 1 to 4096 bytes"));
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
            Error = "Ydb metadata cancelled";
        } else if (TInstant::Now() >= Context_.Deadline) {
            Error = "Ydb metadata deadline exceeded";
        }
        if (Error) {
            FinishTable();
            return false;
        }
        return true;
    }

    TDuration RemainingTimeout() {
        if (Context_.Cancellation.IsCancellationRequested()) {
            Error = "Ydb metadata cancelled";
            return TDuration::Zero();
        }
        const auto now = TInstant::Now();
        if (now >= Context_.Deadline) {
            Error = "Ydb metadata deadline exceeded";
            return TDuration::Zero();
        }
        return Context_.Deadline - now;
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
            const auto cache = State_->MetadataClientCacheFactory();
            Y_ENSURE(cache, "Ydb metadata client cache is missing");
            Client_ = cache->GetClient(cluster.Endpoint, cluster.Database, cluster.UseTls,
                State_->Tokens.at(Requests[Index_].Key.first), State_->CredentialsFactory);
            const auto remaining = RemainingTimeout();
            if (!remaining) {
                FinishTable();
                return;
            }
            Client_->CreateSession(::NYdb::NTable::TCreateSessionSettings()
                .ClientTimeout(remaining).OperationTimeout(remaining))
                .Subscribe([self](const ::NYdb::NTable::TAsyncCreateSessionResult& future) {
                    try {
                        const auto& result = future.GetValue();
                        if (result.IsSuccess()) {
                            self->Session_ = result.GetSession();
                        } else {
                            self->Error = TStringBuilder() << "Ydb metadata session failed: " << result.GetStatus();
                        }
                    } catch (...) {
                        self->Error = "Ydb metadata session failed";
                    }
                    self->Describe();
                });
        } catch (...) {
            // Credential providers may put credentials in exception text.
            Error = "Ydb metadata client initialization failed";
            FinishTable();
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
                FinishTable();
                return;
            }
            Session_->DescribeTable(tablePath, ::NYdb::NTable::TDescribeTableSettings()
                .ClientTimeout(remaining).OperationTimeout(remaining))
                .Subscribe([self](const ::NYdb::NTable::TAsyncDescribeTableResult& future) {
                    try {
                        const auto& result = future.GetValue();
                        if (!self->Context_.Cancellation.IsCancellationRequested() &&
                            TInstant::Now() < self->Context_.Deadline) {
                            if (result.IsSuccess()) {
                                ExtractMetadataSchema(::NYdb::TProtoAccessor::GetProto(result.GetTableDescription()),
                                    self->Requests[self->Index_].Schema, self->Error);
                            } else {
                                self->Error = TStringBuilder() << "Ydb DescribeTable failed: " << result.GetStatus();
                            }
                        }
                    } catch (...) {
                        self->Error = "Ydb DescribeTable failed";
                    }
                    self->FinishTable();
                });
        } catch (...) {
            Error = "Ydb DescribeTable failed";
            FinishTable();
        }
    }

    void FinishTable() {
        // CreateSession returns a standalone session. Its SDK deleter already
        // sends DeleteSession with a separate bounded cleanup timeout. Waiting
        // for another Close here both duplicated that RPC and failed successful
        // metadata reads when cleanup was slow or unavailable.
        Session_.reset();
        Client_.reset();
        if (Context_.Cancellation.IsCancellationRequested()) {
            Error = "Ydb metadata cancelled";
        } else if (TInstant::Now() >= Context_.Deadline) {
            Error = "Ydb metadata deadline exceeded";
        }
        if (Error) {
            Done_.TrySetValue();
            return;
        }
        ++Index_;
        StartNextTable();
    }

    const TState::TPtr State_;
    NThreading::TCancellationTokenSource Cancellation_;
    const NNative::TOperationContext Context_;
    NThreading::TPromise<void> Done_ = NThreading::NewPromise<void>();
    std::shared_ptr<::NYdb::NTable::TTableClient> Client_;
    std::optional<::NYdb::NTable::TSession> Session_;
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
            const TYdbRead read(node);
            TString table;
            if (!ParseRead(read, table, ctx)) {
                return TStatus::Error;
            }
            TState::TTableKey key(read.DataSource().Cluster().StringValue(), table);
            if (State_->Tables.contains(key) || !keys.insert(key).second) {
                continue;
            }
            if (keys.size() + State_->Tables.size() > MaxMetadataTables) {
                ctx.AddError(TIssue(ctx.GetPosition(read.Pos()), "Ydb metadata table limit exceeded"));
                return TStatus::Error;
            }
            requests.push_back({std::move(key), {}});
        }
        if (requests.empty()) {
            return Rewrite(input, output, ctx);
        }
        if (TInstant::Now() >= State_->MetadataDeadline) {
            ctx.AddError(TIssue({}, "Ydb metadata deadline exceeded"));
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
                if (!annotation) {
                    ctx.AddError(TIssue({}, UnsupportedColumn(name, type)));
                    return TStatus::Error;
                }
                if (name == BlockLengthColumnName) {
                    ctx.AddError(TIssue({}, TStringBuilder() << "Ydb column '" << name << "' uses a reserved name"));
                    return TStatus::Error;
                }
                if (!table.ColumnTypes.emplace(name, std::move(type)).second) {
                    ctx.AddError(TIssue({}, "Ydb received duplicate column names"));
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
            return TYdbRead::Match(node.Get()) && node->ChildrenSize() > 1 &&
                TYdbDataSource::Match(node->Child(1));
        });
    }

    TStatus Rewrite(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
        TNodeOnNodeOwnedMap replacements;
        for (const auto& node : FindReads(input)) {
            const TYdbRead read(node);
            TString table;
            if (!ParseRead(read, table, ctx)) {
                return TStatus::Error;
            }
            replacements.emplace(node.Get(), Build<TYdbReadTable>(ctx, read.Pos())
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
        error = "Ydb currently supports only row tables";
        return false;
    }
    if (!description.columns_size() || static_cast<ui64>(description.columns_size()) > MaxMetadataColumns) {
        error = "Ydb metadata column limit exceeded";
        return false;
    }
    ui64 schemaBytes = 0;
    ui64 nameBytes = 0;
    for (const auto& column : description.columns()) {
        nameBytes += column.name().size();
        schemaBytes += column.name().size() + column.type().ByteSizeLong();
        if (column.name().empty() || column.name().size() > 4096 || schemaBytes > MaxMetadataSchemaBytes ||
            nameBytes > MaxMetadataSchemaBytes / 2) {
            error = "Ydb metadata schema limit exceeded";
            return false;
        }
        const auto& type = column.type();
        const auto& item = type.has_optional_type() ? type.optional_type().item() : type;
        if (!item.has_type_id()) {
            error = UnsupportedColumn(column.name(), type);
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

} // namespace NYql::NYdb
