#include "idx_test.h"
#include "idx_test_common.h"
#include "idx_test_data_provider.h"

#include <ydb/public/lib/yson_value/ydb_yson_value.h>
#include <ydb/public/sdk/cpp/client/ydb_params/params.h>

#include <util/generic/map.h>
#include <util/string/printf.h>
#include <util/generic/hash_set.h>
#include <util/generic/list.h>
#include <util/system/mutex.h>

using namespace NYdb;
using namespace NYdb::NTable;

namespace {

NThreading::TFuture<TStatus> FinishTxAsync(const TDataQueryResult& in) {
    auto cb = [](TAsyncCommitTransactionResult future) {
        auto result = future.ExtractValue();
        return NThreading::MakeFuture<TStatus>(result);
    };
    if (in.GetTransaction()->IsActive()) {
        return in.GetTransaction()->Commit().Apply(cb);
    } else {
        return NThreading::MakeFuture<TStatus>(in);
    }
}

TString SetPragmas(const TString& in) {
    return "--!syntax_v1\n" + in;
}

const static TRetryOperationSettings retryOperationSettings = TRetryOperationSettings()
    .MaxRetries(10)
    .FastBackoffSettings(NYdb::NTable::TBackoffSettings().SlotDuration(TDuration::MilliSeconds(5)).Ceiling(6))
    .SlowBackoffSettings(NYdb::NTable::TBackoffSettings().SlotDuration(TDuration::MilliSeconds(1000)).Ceiling(6));
}

namespace NIdxTest {

class IWorkTask {
public:
    using TPtr = std::unique_ptr<IWorkTask>;
public:
    virtual TAsyncStatus Run() = 0;
    virtual IWorkLoader::ELoadCommand GetTaskId() const = 0;
    virtual ~IWorkTask() = default;
};

static TAsyncStatus RunOperation(TTableClient client, const TString& programText, NYdb::TParams params)
{
    return client.RetryOperation([programText, params](TSession session) {
        return session.PrepareDataQuery(programText).Apply(
            [params, programText](TAsyncPrepareQueryResult result) -> NYdb::TAsyncStatus {
                auto prepareResult = result.ExtractValue();
                if (!prepareResult.IsSuccess()) {
                    Cerr << "Prepare failed" << Endl;
                    return NThreading::MakeFuture<TStatus>(prepareResult);
                }

                auto dataQuery = prepareResult.GetQuery();
                static const TTxControl txControl = TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx();
                return dataQuery.Execute(txControl, params).Apply(
                    [](TAsyncDataQueryResult asyncResult) {
                        auto result = asyncResult.ExtractValue();
                        if (!result.IsSuccess()) {
                            Cerr << "Execute err: " << result.GetStatus() << " " << result.GetIssues().ToString() <<  Endl;
                        }
                        return NThreading::MakeFuture<TStatus>(result);
                    });
            });
    }, retryOperationSettings);
}

static const ui8 KEYRANGELIMIT = 20;

static ui8 TableDescriptionToShardsPower(const TTableDescription& tableDescription) {
    ui32 ranges = tableDescription.GetKeyRanges().size();
    ui8 result = 0;
    while (ranges >>= 1) {
        result++;
    }

    return result;
}

static bool IsIndexedType(const TType& type) {
    NYdb::TTypeParser parser(type);
    if (parser.GetKind() == TTypeParser::ETypeKind::Optional) {
        parser.OpenOptional();
    }

    if (parser.GetKind() == TTypeParser::ETypeKind::Primitive) {
        switch (parser.GetPrimitive()) {
            case EPrimitiveType::Bool:
            case EPrimitiveType::Int8:
            case EPrimitiveType::Uint8:
            case EPrimitiveType::Int32:
            case EPrimitiveType::Uint32:
            case EPrimitiveType::Int64:
            case EPrimitiveType::Uint64:
            case EPrimitiveType::String:
            case EPrimitiveType::Utf8:
                return true;
            break;
            default:
                return false;
            break;
        }
    }

    return false;
}

template<typename T>
static bool HasColumn(const T& container, const TString& col) {
    return Find(container.begin(), container.end(), col) != container.end();
}

class TAlterTableAddIndexTask
    : public IWorkTask
    , public TRandomValueProvider {
public:
    TAlterTableAddIndexTask(
            TTableDescription tableDescription,
            const TString& tableName,
            TTableClient& client,
            bool withDataColumn)
        : TableDescription_(tableDescription)
        , TableName_(tableName)
        , Client_(client)
        , WithDataColumn_(withDataColumn)
    {
        ChooseColumnsForIndex();
    }

    TAsyncStatus Run() override {
        if (!Description_) {
            // Nothing to add, skip
            return NThreading::MakeFuture<TStatus>(TStatus(EStatus::SUCCESS, NYql::TIssues()));
        }

        ChooseColumnsForIndex();

        return Client_.RetryOperation([this](TSession session) {
            auto settings = TAlterTableSettings()
                .AppendAddIndexes(Description_.GetRef())
                .ClientTimeout(TDuration::Seconds(5));
            return session.AlterTable(TableName_, settings)
                .Apply([](TAsyncStatus future) {
                    const auto& status = future.GetValue();
                    // status BAD_REQUEST - index already exists (realy?), treat it as success
                    // TODO: improve this check
                    if (status.GetStatus() == EStatus::BAD_REQUEST) {
                        return NThreading::MakeFuture<TStatus>(TStatus(EStatus::SUCCESS, NYql::TIssues()));
                    }
                    return future;
                });
        });
        return NThreading::MakeFuture<TStatus>(TStatus(EStatus::SUCCESS, NYql::TIssues()));
    }

    IWorkLoader::ELoadCommand GetTaskId() const override {
        return WithDataColumn_ ? IWorkLoader::LC_ALTER_ADD_INDEX_WITH_DATA_COLUMN : IWorkLoader::LC_ALTER_ADD_INDEX;
    }
private:
    void ChooseColumnsForIndex() {
        TVector<TString> columns;

        bool hasIndexedType = false;

        do {
            for (const TTableColumn& col : TableDescription_.GetTableColumns()) {
                if (IsIndexedType(col.Type)) {
                    hasIndexedType = true;
                    if (RandomBool()) {
                        columns.emplace_back(col.Name);
                    }
                }
            }
        // repeat if we have something to build index but for all columns random returns false
        } while (hasIndexedType && columns.empty());

        if (columns.empty()) {
            return;
        }

        TVector<TString> dataColumn;
        if (WithDataColumn_) {
            for (const TTableColumn& col : TableDescription_.GetTableColumns()) {
                if (HasColumn(TableDescription_.GetPrimaryKeyColumns(), col.Name)) {
                    continue;
                }
                if (HasColumn(columns, col.Name)) {
                    continue;
                }
                dataColumn.emplace_back(col.Name);
            }
        }

        TString indexName;
        ui64 i = 0;
        for (const auto& name : columns) {
            indexName += name;
            if (++i == columns.size()) {
                indexName += "_index";
            } indexName += "_";
        }

        Description_ = NYdb::NTable::TIndexDescription(indexName, columns, dataColumn);
    }
    TMaybe<NYdb::NTable::TIndexDescription> Description_;
    TTableDescription TableDescription_;
    TString TableName_;
    TTableClient Client_;
    const bool WithDataColumn_;
};

class TSelectAndUpsertIfUniqTask
    : public IWorkTask
    , public TRandomValueProvider {
public:
    TSelectAndUpsertIfUniqTask(
            TTableDescription tableDescription,
            const TString& tableName,
            TTableClient& client)
        : TRandomValueProvider(TableDescriptionToShardsPower(tableDescription), KEYRANGELIMIT)
        , TableDescription_(tableDescription)
        , TableName_(tableName)
        , Client_(client)
    {
        CreateProgram();
    }

    TAsyncStatus Run() override {
        return Client_.RetryOperation([this](TSession session) mutable {
            return GetCheckIndexUniq()(1, session, TMaybe<TTransaction>(), THashMap<TString, NYdb::TValue>())
                .Apply([this](NThreading::TFuture<IndexValues> future) {
                    auto result = future.ExtractValue();
                    const auto& status = result.Status;
                    if (status.GetStatus() == EStatus::PRECONDITION_FAILED) {
                        // Client emulated status
                        return result.Tx->Commit().Apply([](const auto& future) {
                            return NThreading::MakeFuture<TStatus>(future.GetValue());
                        });
                    } else if (status.GetStatus() == EStatus::SUCCESS) {
                        // start upsert
                        const auto& program = Programs_[0];
                        const auto& programText = program.first;

                        NYdb::TValueBuilder value;
                        value.BeginStruct();
                        for (const auto& x : program.second) {
                            auto it = result.Values.find(x.first.Name);
                            if (it != result.Values.end()) {
                                value.AddMember(x.first.Name, it->second);
                            } else {
                                value.AddMember(x.first.Name, ::NIdxTest::CreateValue(x.first, *this));
                            }
                        }
                        value.EndStruct();
                        TVector<NYdb::TValue> batch;
                        batch.push_back(value.Build());

                        Y_ABORT_UNLESS(result.Tx);
                        const auto params = ::NIdxTest::CreateParamsAsList(batch, ParamName_);

                        return result.Tx->GetSession().ExecuteDataQuery(
                            programText, TTxControl::Tx(result.Tx.GetRef()), std::move(params),
                            TExecDataQuerySettings().KeepInQueryCache(true).ClientTimeout(TDuration::Seconds(5)))
                                .Apply([](TAsyncDataQueryResult future){
                            auto result = future.ExtractValue();
                            if (result.IsSuccess()) {
                                return FinishTxAsync(result);
                            } else {
                                return NThreading::MakeFuture<TStatus>(result);
                            }

                        });
                    } else {
                        return NThreading::MakeFuture<TStatus>(status);
                    }
            });
        }, retryOperationSettings);

    }

    IWorkLoader::ELoadCommand GetTaskId() const override {
        return IWorkLoader::LC_UPSERT_IF_UNIQ;
    }
private:
    struct IndexValues {
        TStatus Status;
        THashMap<TString, NYdb::TValue> Values;
        TMaybe<TTransaction> Tx;
    };
    using TCheckIndexCb = std::function<NThreading::TFuture<IndexValues>(
        size_t i,
        TSession session,
        TMaybe<TTransaction>,
        THashMap<TString, NYdb::TValue>)>;

    TCheckIndexCb GetCheckIndexUniq() {
        return [this] (
            size_t i,
            TSession session,
            TMaybe<TTransaction> tx,
            THashMap<TString, NYdb::TValue>&& checked) mutable
        {
            if (i == Programs_.size()) {
                return NThreading::MakeFuture<IndexValues>(
                        {
                            TStatus(EStatus::SUCCESS, NYql::TIssues()),
                            checked,
                            tx
                        }
                    );
            }

            const auto& p = Programs_[i];

            TVector<NYdb::TValue> values;
            TVector<TString> paramNames;
            for (const auto& col : p.second) {
                const auto val = ::NIdxTest::CreateValue(col.first, *this);

                checked.insert({col.first.Name, val});
                values.push_back(val);
                paramNames.push_back(col.second);
            }

            const auto params = ::NIdxTest::CreateParamsAsItems(values, paramNames);

            TTxControl txctrl = tx ? TTxControl::Tx(tx.GetRef()) : TTxControl::BeginTx(TTxSettings::SerializableRW());

            return session.ExecuteDataQuery(
                    p.first,
                    txctrl,
                    params,
                    TExecDataQuerySettings().KeepInQueryCache(true).ClientTimeout(TDuration::Seconds(5)))
                        .Apply([this, i, session, checked{std::move(checked)}](TAsyncDataQueryResult future) mutable {
                    auto result = future.ExtractValue();
                    // non success call - return status
                    if (!result.IsSuccess()) {
                        return NThreading::MakeFuture<IndexValues>(
                                {
                                    result,
                                    THashMap<TString, NYdb::TValue>(),
                                    TMaybe<TTransaction>()
                                }
                            );
                    }

                    // success call and no record found (index uniq) - check next one
                    if (result.GetResultSet(0).RowsCount() == 0) {
                        return GetCheckIndexUniq()(i + 1, session, result.GetTransaction(), std::move(checked));
                    } else {
                        return NThreading::MakeFuture<IndexValues>(
                                {
                                    TStatus(EStatus::PRECONDITION_FAILED, NYql::TIssues()),
                                    THashMap<TString, NYdb::TValue>(),
                                    result.GetTransaction()
                                }
                            );
                    }
                });
        };
    }


    void CreateProgram() {
        const auto columns = TableDescription_.GetColumns();
        const auto pkColNames = TableDescription_.GetPrimaryKeyColumns();

        // Used to find column description by column Name
        THashMap<TString, std::pair<TColumn, TString>> colHash;

        TVector<std::pair<TColumn, TString>> upsertInput;

        for (const auto& col : columns) {
            auto pkType = NIdxTest::CreateValue(col, *this);
            auto typeString = NYdb::FormatType(pkType.GetType());
            colHash.insert({col.Name, {col, typeString}});
            upsertInput.push_back({col, ""});
        }

        {
            // Create UPSERT program
            auto rowType = NIdxTest::CreateRow(columns, *this);
            auto rowsTypeString = NYdb::FormatType(rowType.GetType());

            TString sqlTemplate = "DECLARE %s AS List<%s>; UPSERT INTO `%s` SELECT * FROM AS_TABLE(%s);";
            auto program = Sprintf(sqlTemplate.c_str(),
                ParamName_.c_str(),
                rowsTypeString.c_str(),
                TableName_.c_str(),
                ParamName_.c_str());

            Programs_.push_back({SetPragmas(program), upsertInput});
        }

        TString pkColumns;
        {
            size_t id = 0;
            for (const auto& col : pkColNames) {
                pkColumns += col;
                if (++id != pkColNames.size())
                    pkColumns += ", ";
            }
        }

        for (const auto& indexDesc : TableDescription_.GetIndexDescriptions()) {
            // Create select pk program for each index
            size_t id = 0;
            TString indexPredicate;
            TString declare;
            TVector<std::pair<TColumn, TString>> predicates;
            for (const auto& col : indexDesc.GetIndexColumns()) {
                const TString paramName = Sprintf("$items_%zu", id);
                declare += Sprintf("DECLARE %s AS %s;\n",
                    paramName.c_str(), colHash.find(col)->second.second.c_str());
                predicates.push_back({colHash.find(col)->second.first, paramName});
                indexPredicate += Sprintf(" %s = %s ", col.c_str(), paramName.c_str());
                if (++id != pkColNames.size()) {
                    indexPredicate += " AND ";
                }
            }

            TString sql;
            sql += declare;
            sql += Sprintf("SELECT %s FROM `%s` VIEW %s WHERE %s", pkColumns.c_str(),
                TableName_.c_str(), indexDesc.GetIndexName().c_str(), indexPredicate.c_str());

            Programs_.push_back({SetPragmas(sql), predicates});
        }
    }

    const TString ParamName_ = "$items";
    TTableDescription TableDescription_;
    TString TableName_;
    TTableClient Client_;

    TVector<std::pair<TString, TVector<std::pair<TColumn, TString>>>> Programs_;

    mutable TString Err_;
};


class TSelectAndCompareTask
    : public IWorkTask
    , public TRandomValueProvider {
public:
    TSelectAndCompareTask(
            TTableDescription tableDescription,
            const TString& tableName,
            TTableClient& client)
        : TRandomValueProvider(TableDescriptionToShardsPower(tableDescription), KEYRANGELIMIT)
        , TableDescription_(tableDescription)
        , TableName_(tableName)
        , Client_(client)
    {
        CreateProgram();
    }

    TAsyncStatus Run() override {
        if (Programs_.empty())
           throw yexception() << "no program to run";

        TString err;
        with_lock(Mtx_) {
            if (Err_)
                err = Err_;
        }
        if (err)
            throw yexception() << err;

        const auto& program = Programs_[0];

        TVector<NYdb::TValue> values;
        TVector<TString> paramNames;
        for (const auto& col : program.second) {
            values.push_back(::NIdxTest::CreateOptionalValue(col.first, *this));
            paramNames.push_back(col.second);
        }

        const auto params = ::NIdxTest::CreateParamsAsItems(values, paramNames);

        return Client_.RetryOperation([this, params](TSession session) mutable {
            const auto& mainTableQuery = Programs_[0].first;
            return session.ExecuteDataQuery(
                mainTableQuery,
                TTxControl::BeginTx(TTxSettings::SerializableRW()),
                params,
                TExecDataQuerySettings().KeepInQueryCache(true).ClientTimeout(TDuration::Seconds(5)))
                    .Apply([this, session](TAsyncDataQueryResult future) mutable {
                        auto result = future.ExtractValue();
                        if (result.GetResultSets().empty()) {
                            if (result.IsSuccess()) {
                                return FinishTxAsync(result);
                            } else {
                                return NThreading::MakeFuture<TStatus>(result);
                            }
                        }

                        const auto& mainResultSet = NYdb::FormatResultSetYson(result.GetResultSet(0));;
                        return GetCheckIndexOp()(1, session, result, mainResultSet, "");
                    });
        }).Apply([this](TAsyncStatus future) {
            TString err;
            with_lock(Mtx_) {
                if (Err_)
                    err = Err_;
            }
            if (err)
                throw yexception() << err;
             return future;
        });
    }

    IWorkLoader::ELoadCommand GetTaskId() const override {
        return IWorkLoader::LC_SELECT;
    }

private:
    using TCheckIndexCb = std::function<TAsyncStatus(
        size_t i,
        TSession session,
        TDataQueryResult in,
        const TString& mainResultSet,
        const TString& err)>;

    TCheckIndexCb GetCheckIndexOp() {
        return [this] (
            size_t i,
            TSession session,
            TDataQueryResult in,
            const TString& mainResultSet,
            const TString& err) mutable
        {
            TResultSetParser rsParser(in.GetResultSet(0));
            if (!rsParser.TryNextRow()) {
                return FinishTxAsync(in);
            }


            if (i == Programs_.size()) {
                return in.GetTransaction()->Commit()
                    .Apply([this, err](TAsyncCommitTransactionResult future) {
                        auto result = future.ExtractValue();
                        if (err && (result.GetStatus() != EStatus::ABORTED)) {
                            with_lock(Mtx_) {
                                Err_ += err;
                            }
                        }
                        return NThreading::MakeFuture<TStatus>(result);
                    });
            }

            const auto& p = Programs_[i];

            TVector<NYdb::TValue> val;
            TVector<TString> parNames;
            for (const auto& col : p.second) {
                val.push_back(rsParser.GetValue(col.first.Name));
                auto vp = TValueParser(val.back());
                vp.OpenOptional();
                if (vp.IsNull()) {
                    //Cerr << "Null value found, skip check.." << Endl;
                    return NThreading::MakeFuture<TStatus>(TStatus(EStatus::SUCCESS, NYql::TIssues()));
                }
                parNames.push_back(col.second);
            }
            const auto params = ::NIdxTest::CreateParamsAsItems(val, parNames);

            return session.ExecuteDataQuery(
                    p.first,
                    TTxControl::Tx(*in.GetTransaction()),
                    params,
                    TExecDataQuerySettings().KeepInQueryCache(true).ClientTimeout(TDuration::Seconds(5)))
                .Apply([this, i, session, in, mainResultSet, val](TAsyncDataQueryResult future) mutable {
                    auto result = future.ExtractValue();
                    with_lock(Mtx_) {
                        if (Err_) {
                            return FinishTxAsync(result);
                        }
                    }

                    if (!result.IsSuccess()) {
                        return NThreading::MakeFuture<TStatus>(result);
                    }

                    try {
                        auto fromIndexReq = NYdb::FormatResultSetYson(result.GetResultSet(0));;
                        TString err;
                        if (fromIndexReq != mainResultSet) {
                            TStringStream ss;
                            ss << "result set missmatch, index: " << fromIndexReq << " and main: " << mainResultSet << Endl;
                            err = ss.Str();
                        }

                        return GetCheckIndexOp()(i + 1, session, in, mainResultSet, err);
                    } catch (...) {
                        Y_ABORT("Unexpected exception");
                        return FinishTxAsync(result);
                    }
                });
        };
    }


    void CreateProgram() {
        const auto columns = TableDescription_.GetColumns();
        const auto pkColNames = TableDescription_.GetPrimaryKeyColumns();

        THashMap<TString, std::pair<TColumn, TString>> colHash;
        size_t id = 0;

        TString allColumns;
        for (const auto& col : columns) {
            auto pkType = NIdxTest::CreateOptionalValue(col, *this);
            auto typeString = NYdb::FormatType(pkType.GetType());
            colHash.insert({col.Name, {col, typeString}});
            allColumns += col.Name;
            if (++id != columns.size())
                allColumns += ", ";
        }

        id = 0;
        TString pkPredicate;
        TString select1;
        TVector<std::pair<TColumn, TString>> pkPredicates;
        for (const TString& str : pkColNames) {
            const TString paramName = Sprintf("$items_%zu", id);
            select1 += Sprintf("DECLARE %s AS %s;\n", paramName.c_str(), colHash.find(str)->second.second.c_str());
            pkPredicates.push_back({colHash.find(str)->second.first, paramName});
            pkPredicate += Sprintf(" %s = %s ", str.c_str(), paramName.c_str());
            if (++id != pkColNames.size()) {
                pkPredicate += " AND ";
            }
        }

        select1 += Sprintf("SELECT %s FROM `%s` WHERE %s", allColumns.c_str(), TableName_.c_str(), pkPredicate.c_str());
        Programs_.push_back({SetPragmas(select1), pkPredicates});

        THashMap<TString, std::pair<TString,
                                    TVector<std::pair<TColumn,
                                                      TString>>>> selectUsingIndexSql;
        for (const auto& indexDesc : TableDescription_.GetIndexDescriptions()) {
            id = 0;
            TString indexPredicate;
            TString declare;
            TVector<std::pair<TColumn, TString>> predicates;
            for (const auto& col : indexDesc.GetIndexColumns()) {
                const TString paramName = Sprintf("$items_%zu", id);
                declare += Sprintf("DECLARE %s AS %s;\n",
                    paramName.c_str(), colHash.find(col)->second.second.c_str());
                predicates.push_back({colHash.find(col)->second.first, paramName});
                indexPredicate += Sprintf(" %s = %s ", col.c_str(), paramName.c_str());
                indexPredicate += " AND ";
                id++;
            }
            // Add key column to request to handle non uniq index
            for (const TString& str : pkColNames) {
                const TString paramName = Sprintf("$items_%zu", id);
                declare += Sprintf("DECLARE %s AS %s;\n",
                    paramName.c_str(), colHash.find(str)->second.second.c_str());
                predicates.push_back({colHash.find(str)->second.first, paramName});
                indexPredicate += Sprintf(" %s = %s ", str.c_str(), paramName.c_str());
                if (++id != pkColNames.size() + indexDesc.GetIndexColumns().size()) {
                    pkPredicate += " AND ";
                }
            }

            auto& sql = selectUsingIndexSql[indexDesc.GetIndexName()];
            sql.first += declare;
            sql.first += Sprintf("SELECT %s FROM `%s` VIEW %s WHERE %s", allColumns.c_str(),
                TableName_.c_str(), indexDesc.GetIndexName().c_str(), indexPredicate.c_str());

            Programs_.push_back({SetPragmas(sql.first), predicates});
        }
    }

    TTableDescription TableDescription_;
    TString TableName_;
    TTableClient Client_;

    TVector<std::pair<TString, TVector<std::pair<TColumn, TString>>>> Programs_;

    mutable TString Err_;
    TMutex Mtx_;
};

class TUpdateViaParamItemsTask
    : public IWorkTask
    , public TRandomValueProvider {
public:
    TUpdateViaParamItemsTask(
            TTableDescription tableDescription,
            const TString& tableName,
            TTableClient& client,
            IWorkLoader::ELoadCommand stmt,
            size_t opsPerTx)
        : TRandomValueProvider(TableDescriptionToShardsPower(tableDescription), KEYRANGELIMIT)
        , TableDescription_(tableDescription)
        , TableName_(tableName)
        , Client_(client)
        , Stmt_(stmt)
        , OpsPerTx_(opsPerTx)
    {
        CreatePrograms();
    }

    TAsyncStatus Run() override {
        auto rnd = RandomNumber<ui32>(Programs_.size());
        const auto& program = Programs_[rnd];
        const auto& programText = program.first;

        TVector<NYdb::TValue> values;
        TVector<TString> paramNames;
        for (const auto& col : program.second) {
            values.push_back(::NIdxTest::CreateOptionalValue(col.first, *this));
            paramNames.push_back(col.second);
        }

        const auto params = ::NIdxTest::CreateParamsAsItems(values, paramNames);
        return RunOperation(Client_, programText, params);
    }

    std::pair<TString, TVector<std::pair<TColumn, TString>>> CreateProgram(const TString& indexName, bool allColumns = false) {
        const auto columns = TableDescription_.GetColumns();
        const auto pkColNames = TableDescription_.GetPrimaryKeyColumns();

        TVector<TColumn> pkCol;
        THashSet<TString> pkColHash;
        for (const auto& pk : pkColNames) {
            pkColHash.insert(pk);
        }
        for (const auto& col : columns) {
            if (pkColHash.contains(col.Name)) {
                pkCol.push_back(col);
            }
        }

        TVector<TColumn> valueCol;
        THashSet<TString> indexColumns;
        for (const auto& indexDesc : TableDescription_.GetIndexDescriptions()) {
            if (indexName.empty() || indexDesc.GetIndexName() == indexName) {
                for (const auto& col : indexDesc.GetIndexColumns()) {
                    indexColumns.insert(col);
                }
            }
        }

        for (const auto& col : columns) {
            if (indexName.empty()) {
                // only columns withount indexes or all columns
                if (allColumns || !indexColumns.contains(col.Name)) {
                    if (!pkColHash.contains(col.Name)) {
                        valueCol.push_back(col);
                    }
                }
            } else {
                // only columns with given index
                if (indexColumns.contains(col.Name)) {
                    if (!pkColHash.contains(col.Name)) {
                        valueCol.push_back(col);
                    }
                }
            }
        }

        TVector<TString> pkTypeStrings;
        for (const auto& pk : pkCol) {
            auto pkType = NIdxTest::CreateOptionalValue(pk, *this);
            pkTypeStrings.push_back(NYdb::FormatType(pkType.GetType()));
        }

        TVector<TString> valueTypeStrings;
        for (const auto& v : valueCol) {
            auto val = NIdxTest::CreateOptionalValue(v, *this);
            valueTypeStrings.push_back(NYdb::FormatType(val.GetType()));
        }

        TString sql;
        TVector<TString> predicates;
        predicates.reserve(OpsPerTx_);
        TVector<TString> updates;
        updates.reserve(OpsPerTx_);
        TVector<std::pair<TColumn, TString>> resultColumns;
        Y_ABORT_UNLESS(Stmt_ == IWorkLoader::LC_DELETE || Stmt_ == IWorkLoader::LC_UPDATE);

        for (size_t opIndex = 0, paramId = 0; opIndex < OpsPerTx_; ++opIndex) {
            TString predicate;

            for (size_t pkId = 0; pkId < pkTypeStrings.size(); ++pkId) {
                const TString paramName = Sprintf("$items_%zu", paramId++);
                sql += Sprintf("DECLARE %s AS %s;\n", paramName.c_str(), pkTypeStrings[pkId].c_str());
                resultColumns.push_back({pkCol[pkId], paramName});
                predicate += Sprintf(" %s = %s ", pkCol[pkId].Name.c_str(), paramName.c_str());

                if (pkId + 1 != pkTypeStrings.size()) {
                    predicate += " AND ";
                }
            }

            predicates.emplace_back(std::move(predicate));

            if (Stmt_ == IWorkLoader::LC_UPDATE) {
                TString update = " SET ";

                for (size_t dataId = 0; dataId < valueTypeStrings.size(); ++dataId) {
                    const TString paramName = Sprintf("$items_%zu", paramId++);
                    sql += Sprintf("DECLARE %s AS %s;\n", paramName.c_str(), valueTypeStrings[dataId].c_str());
                    resultColumns.push_back({valueCol[dataId], paramName});
                    update += Sprintf(" %s = %s ", valueCol[dataId].Name.c_str(), paramName.c_str());

                    if (dataId + 1 != valueTypeStrings.size()) {
                        update += ", ";
                    }
                }

                updates.emplace_back(std::move(update));
            }
        }


        for (size_t opIndex = 0; opIndex < OpsPerTx_; ++opIndex) {
            if (Stmt_ == IWorkLoader::LC_DELETE) {
                sql += Sprintf("DELETE FROM `%s` WHERE ", TableName_.c_str());
                sql += predicates[opIndex];
                sql += ";\n";
            } else {
                sql += Sprintf("UPDATE `%s` ", TableName_.c_str());
                sql += updates[opIndex];
                sql += " WHERE ";
                sql += predicates[opIndex];
                sql += ";\n";
            }
        }

        return {SetPragmas(sql), resultColumns};
    }

    IWorkLoader::ELoadCommand GetTaskId() const override {
        return Stmt_;
    }

private:
    void CreatePrograms() {
        if (Stmt_ == IWorkLoader::LC_DELETE) {
            Programs_.emplace_back(CreateProgram("", false));
        } else {
            // All columns
            Programs_.emplace_back(CreateProgram("", true));
            // Columns with given index (only if more then 1 index)
            if (TableDescription_.GetIndexDescriptions().size() > 1) {
                for (const auto& indexDesc : TableDescription_.GetIndexDescriptions()) {
                    Programs_.emplace_back(CreateProgram(indexDesc.GetIndexName()));
                }
            }
        }
    }

    TTableDescription TableDescription_;
    TString TableName_;
    TTableClient Client_;
    const IWorkLoader::ELoadCommand Stmt_;
    const size_t OpsPerTx_;
    // program, columns
    TVector<std::pair<TString, TVector<std::pair<TColumn, TString>>>> Programs_;
};

class TUpdateViaParamListTask
    : public IWorkTask
    , public TRandomValueProvider {
public:
    TUpdateViaParamListTask(
            TTableDescription tableDescription,
            const TString& tableName,
            TTableClient& client,
            IWorkLoader::ELoadCommand stmt,
            size_t opsPerTx)
        : TRandomValueProvider(TableDescriptionToShardsPower(tableDescription), KEYRANGELIMIT)
        , TableDescription_(tableDescription)
        , TableName_(tableName)
        , Client_(client)
        , Stmt_(stmt)
        , OpsPerTx_(opsPerTx)
    {
        CreatePrograms();
    }

    TAsyncStatus Run() override {
        auto rnd = RandomNumber<ui32>(Programs_.size());
        const auto& program = Programs_[rnd];
        const auto& programText = program.first;

        NYdb::TParamsBuilder params;
        for (size_t opIndex = 0; opIndex < OpsPerTx_; ++opIndex) {
            const TString paramName = Sprintf("$items_%zu", opIndex);
            TVector<NYdb::TValue> batch;
            batch.emplace_back(::NIdxTest::CreateRow(program.second, *this));
            ::NIdxTest::AddParamsAsList(params, batch, paramName);
        }

        return RunOperation(Client_, programText, params.Build());
    }

    IWorkLoader::ELoadCommand GetTaskId() const override {
        return Stmt_;
    }
private:

    // indexName - create program to touch this indexName only
    // indexName empty && !allColumns - touch pk avd value only
    // indexName empty && allColumns - touch all columns
    std::pair<TString, TVector<TColumn>> CreateUpsertProgram(const TString& indexName, bool allColumns = false) {
        const auto columns = TableDescription_.GetColumns();
        TVector<TColumn> resCol;

        THashSet<TString> indexColumns;
        for (const auto& indexDesc : TableDescription_.GetIndexDescriptions()) {
            for (const auto& col : indexDesc.GetIndexColumns()) {
                indexColumns.insert(col);
            }
        }

        THashSet<TString> pks;
        for (const auto& pk : TableDescription_.GetPrimaryKeyColumns()) {
            pks.insert(pk);
            indexColumns.insert(pk);
        }

        for (const auto& col : columns) {
            if (indexName.empty()) {
                // only columns withount indexes or all columns
                if (allColumns) {
                    resCol.push_back(col);
                } else {
                    if (pks.contains(col.Name)) {
                        resCol.push_back(col);
                    }
                }
            } else {
                // only columns with given index
                if (indexColumns.contains(col.Name)) {
                    resCol.push_back(col);
                }
            }
        }

        auto rowType = NIdxTest::CreateRow(resCol, *this);
        auto rowsTypeString = NYdb::FormatType(rowType.GetType());

        TString program;
        for (size_t opIndex = 0; opIndex < OpsPerTx_; ++opIndex) {
            const TString paramName = Sprintf("$items_%zu", opIndex);
            program += Sprintf("DECLARE %s AS List<%s>;\n", paramName.c_str(), rowsTypeString.c_str());
        }

        for (size_t opIndex = 0; opIndex < OpsPerTx_; ++opIndex) {
            TString sql;
            const TString paramName = Sprintf("$items_%zu", opIndex);

            switch (Stmt_) {
                case IWorkLoader::LC_UPSERT:
                    sql = "UPSERT INTO `%s` SELECT * FROM AS_TABLE(%s);\n";
                    break;
                case IWorkLoader::LC_REPLACE:
                    sql = "REPLACE INTO `%s` SELECT * FROM AS_TABLE(%s);\n";
                    break;
                case IWorkLoader::LC_INSERT:
                    sql = "INSERT INTO `%s` SELECT * FROM AS_TABLE(%s);\n";
                    break;
                case IWorkLoader::LC_UPDATE_ON:
                    sql = "UPDATE `%s` ON SELECT * FROM AS_TABLE(%s);\n";
                    break;
                case IWorkLoader::LC_DELETE_ON:
                    sql = "DELETE FROM `%s` ON SELECT * FROM AS_TABLE(%s);\n";
                    break;
                default:
                    ythrow yexception() << "unsupported statement";
                    break;
            }

            program += Sprintf(sql.c_str(), TableName_.c_str(), paramName.c_str());
        }

        return {SetPragmas(program), resCol};
    }

    void CreatePrograms() {
        // Only columns without indes
        //Programs_.emplace_back(CreateUpsertProgram("", false));
        // All columns
        Programs_.emplace_back(CreateUpsertProgram("", true));
        // Columns with given index (only if more then 1 index)
        if (TableDescription_.GetIndexDescriptions().size() > 1) {
            for (const auto& indexDesc : TableDescription_.GetIndexDescriptions()) {
                Programs_.emplace_back(CreateUpsertProgram(indexDesc.GetIndexName()));
            }
        }
    }
    TTableDescription TableDescription_;
    TString TableName_;
    TTableClient Client_;
    const IWorkLoader::ELoadCommand Stmt_;
    const size_t OpsPerTx_;
    // program, columns
    TVector<std::pair<TString, TVector<TColumn>>> Programs_;
};

class TTimingTracker {
public:
    struct TStat {
        TDuration Min;
        TDuration Max;
        TDuration Total;
        ui64 Count = 0;
        ui64 Errors = 0;
    };

    void Start(IWorkLoader::ELoadCommand cmd) {
        CurStat_ = &Stats_[cmd];
        CurTime_ = TInstant::Now();
    }

    void Finish(bool success) {
        auto time = TInstant::Now() - CurTime_;
        CurStat_->Total += time;

        if (CurStat_->Count) {
            CurStat_->Min = Min(CurStat_->Min, time);
        } else {
            CurStat_->Min = time;
        }
        CurStat_->Count++;
        CurStat_->Max = Max(CurStat_->Max, time);
        if (!success) {
            CurStat_->Errors++;
        }
    }

    const THashMap<IWorkLoader::ELoadCommand, TStat>& GetStat() const {
        return Stats_;
    }

private:
    THashMap<IWorkLoader::ELoadCommand, TStat> Stats_;
    TStat* CurStat_ = nullptr;
    TInstant CurTime_;
};


NJson::TJsonValue GetQueryLabels(IWorkLoader::ELoadCommand queryId) {
    NJson::TJsonValue labels(NJson::JSON_MAP);
    TStringStream ss;
    ss << queryId;
    labels.InsertValue("query", ss.Str());
    return labels;
}

NJson::TJsonValue GetSensorValue(TStringBuf sensor, const TDuration& value, IWorkLoader::ELoadCommand queryId) {
    NJson::TJsonValue sensorValue(NJson::JSON_MAP);
    sensorValue.InsertValue("sensor", sensor);
    sensorValue.InsertValue("value", value.MilliSeconds());
    sensorValue.InsertValue("labels", GetQueryLabels(queryId));
    return sensorValue;
}

NJson::TJsonValue GetSensorValue(TStringBuf sensor, double value, IWorkLoader::ELoadCommand queryId) {
    NJson::TJsonValue sensorValue(NJson::JSON_MAP);
    sensorValue.InsertValue("sensor", sensor);
    sensorValue.InsertValue("value", value);
    sensorValue.InsertValue("labels", GetQueryLabels(queryId));
    return sensorValue;
}


class TWorkLoader : public IWorkLoader {
    using TWorkTaskEntry = std::pair<IWorkTask::TPtr, ui32>;
public:
    TWorkLoader(TTableClient&& client, IProgressTracker::TPtr&& progressTracker)
        : Client_(std::move(client))
        , ProgressTracker_(std::move(progressTracker))
    {}

    NJson::TJsonValue Run(const TString& tableName, ui32 loadCommands, const TRunSettings& settings) override {
        if (!loadCommands)
            return {};

        const auto runLimit = settings.RunLimit;

        if (ProgressTracker_) {
            ProgressTracker_->Start("reqests processed", "Run work loader on table " + tableName);
        }

        auto tableDescription = DescribeTable(tableName, Client_);

        if (!tableDescription)
            throw yexception() << "Unable to describe table " << tableName << Endl;

        TVector<TWorkTaskEntry> tasks;
        for (ui64 i = 1; i < Max<ui32>(); i <<= 1) {
            switch (i & loadCommands) {
                case IWorkLoader::LC_UPSERT:
                    tasks.emplace_back(std::make_pair(std::make_unique<TUpdateViaParamListTask>(
                        tableDescription.GetRef(), tableName, Client_, IWorkLoader::LC_UPSERT, settings.OpsPerTx), 1000));
                break;
                case IWorkLoader::LC_INSERT:
                    tasks.emplace_back(std::make_pair(std::make_unique<TUpdateViaParamListTask>(
                        tableDescription.GetRef(), tableName, Client_, IWorkLoader::LC_INSERT, settings.OpsPerTx), 1000));
                break;
                case IWorkLoader::LC_UPDATE:
                    tasks.emplace_back(std::make_pair(std::make_unique<TUpdateViaParamItemsTask>(
                        tableDescription.GetRef(), tableName, Client_, IWorkLoader::LC_UPDATE, settings.OpsPerTx), 1000));
                break;
                case IWorkLoader::LC_UPDATE_ON:
                    tasks.emplace_back(std::make_pair(std::make_unique<TUpdateViaParamListTask>(
                        tableDescription.GetRef(), tableName, Client_, IWorkLoader::LC_UPDATE_ON, settings.OpsPerTx), 1000));
                break;
                case IWorkLoader::LC_REPLACE:
                    tasks.emplace_back(std::make_pair(std::make_unique<TUpdateViaParamListTask>(
                        tableDescription.GetRef(), tableName, Client_, IWorkLoader::LC_REPLACE, settings.OpsPerTx), 1000));
                break;
                case IWorkLoader::LC_DELETE:
                    tasks.emplace_back(std::make_pair(std::make_unique<TUpdateViaParamItemsTask>(
                        tableDescription.GetRef(), tableName, Client_, IWorkLoader::LC_DELETE, settings.OpsPerTx), 1000));
                break;
                case IWorkLoader::LC_DELETE_ON:
                    tasks.emplace_back(std::make_pair(std::make_unique<TUpdateViaParamListTask>(
                        tableDescription.GetRef(), tableName, Client_, IWorkLoader::LC_DELETE_ON, settings.OpsPerTx), 1000));
                break;
                case IWorkLoader::LC_SELECT:
                    tasks.emplace_back(std::make_pair(std::make_unique<TSelectAndCompareTask>(
                        tableDescription.GetRef(), tableName, Client_), 1000));
                break;
                case IWorkLoader::LC_UPSERT_IF_UNIQ:
                    tasks.emplace_back(std::make_pair(std::make_unique<TSelectAndUpsertIfUniqTask>(
                        tableDescription.GetRef(), tableName, Client_), 1000));
                break;
                case IWorkLoader::LC_ALTER_ADD_INDEX:
                    tasks.emplace_back(std::make_pair(std::make_unique<TAlterTableAddIndexTask>(
                        tableDescription.GetRef(), tableName, Client_, false), 1));
                break;
                case IWorkLoader::LC_ALTER_ADD_INDEX_WITH_DATA_COLUMN:
                    tasks.emplace_back(std::make_pair(std::make_unique<TAlterTableAddIndexTask>(
                        tableDescription.GetRef(), tableName, Client_, true), 1));
                break;
            }
        }

        WeightPrecalc(tasks);

        TVector<TAsyncStatus> results;
        TAtomic curAtomicRunLimit = runLimit - settings.Infly; //infly already "counted" by initiall call
        auto pt = ProgressTracker_.get();

        std::function<TAsyncStatus(TTimingTracker*)> runAsync;
        runAsync = [&tasks, &curAtomicRunLimit, &runAsync, pt, runLimit] (TTimingTracker* tt) -> TAsyncStatus {
            auto task = GetRandomTask(tasks);
            if (pt) {
                pt->Update(runLimit - AtomicGet(curAtomicRunLimit));
            }
            tt->Start(task->GetTaskId());
            return task->Run().Apply([&curAtomicRunLimit, &runAsync, tt](TAsyncStatus status) {
                int newValue = AtomicDecrement(curAtomicRunLimit);
                if (!status.HasException() && status.GetValue().IsSuccess()) {
                    tt->Finish(true);
                } else {
                    tt->Finish(false);
                }
                if (status.HasException() || newValue < 0 ||
                    (!status.GetValue().IsSuccess() && status.GetValue().GetStatus() != EStatus::PRECONDITION_FAILED)) {
                    Cerr << "finished with status: " << status.GetValue().GetStatus() << Endl;
                    return status;
                } else {
                    if (!status.GetValue().IsSuccess())
                        Cerr << "cont with status: " << status.GetValue().GetStatus() << Endl;
                    return runAsync(tt);
                }
            });
        };

        TVector<TTimingTracker> timing;
        timing.resize(settings.Infly);
        for (size_t i = 0; i < settings.Infly; i++) {
            results.push_back(runAsync(&timing[i]));
        }

        TString err;
        for (size_t i = 0; i < settings.Infly; i++) {
            try {
                if (!results[i].GetValueSync().IsSuccess()) {
                    TStringStream ss;
                    ss << "non success status: ";
                    ss << results[i].GetValueSync().GetStatus();
                    ss << " issues: " << results[i].GetValueSync().GetIssues().ToString();
                    ss << "\n";
                    err += ss.Str();
                }
            } catch (const std::exception& ex) {
                err += ex.what();
            }
        }

        if (ProgressTracker_) {
            ProgressTracker_->Finish("Done.");
        }

        auto result = AggregateTiming(timing);

        NJson::TJsonValue jsonReport(NJson::JSON_ARRAY);

        for (const auto& queryRes : result) {
             jsonReport.AppendValue(GetSensorValue("MinTime", queryRes.second.Min, queryRes.first));
             jsonReport.AppendValue(GetSensorValue("MaxTime", queryRes.second.Max, queryRes.first));
             jsonReport.AppendValue(GetSensorValue("MeanTime", queryRes.second.Total / (double)queryRes.second.Count, queryRes.first));
             jsonReport.AppendValue(GetSensorValue("VisiableErrorsCount", queryRes.second.Errors, queryRes.first));
        }

        if (err)
            throw yexception() << err;

        return jsonReport;
    }

private:
    static THashMap<IWorkLoader::ELoadCommand, TTimingTracker::TStat> AggregateTiming(const TVector<TTimingTracker>& timings) {
        THashMap<IWorkLoader::ELoadCommand, TTimingTracker::TStat> result;
        bool first = true;

        for (const auto& x : timings) {
            const THashMap<IWorkLoader::ELoadCommand, TTimingTracker::TStat>& t = x.GetStat();
            for (const auto& pair : t) {
                TTimingTracker::TStat& stat = result[pair.first];

                stat.Max = Max(stat.Max, pair.second.Max);
                if (first) {
                    stat.Min = pair.second.Min;
                } else {
                    stat.Min = Min(stat.Min, pair.second.Min);
                }
                stat.Count += pair.second.Count;
                stat.Total += pair.second.Total;
                stat.Errors += pair.second.Errors;
            }
        }
        return result;
    }

    static bool CompareValueWorkTaskEntry(ui32 a, const TWorkTaskEntry& b) {
        return a < b.second;
    }

    static void WeightPrecalc(TVector<TWorkTaskEntry>& tasks) {
        ui32 s = 0;
        for (auto& t : tasks) {
            s += t.second;
            t.second = s;
        }
    }

    static IWorkTask* GetRandomTask(TVector<TWorkTaskEntry>& tasks) {
        auto rnd = RandomNumber<ui32>(tasks.back().second);
        // First element that is greater than rnd
        const auto upper = std::upper_bound(tasks.begin(), tasks.end(), rnd, &CompareValueWorkTaskEntry);
        return upper->first.get();
    }

    TTableClient Client_;
    IProgressTracker::TPtr ProgressTracker_;
};

IWorkLoader::TPtr CreateWorkLoader(TDriver& driver, IProgressTracker::TPtr&& progressTracker) {
    auto settings = TClientSettings()
                        .SessionPoolSettings(
                            TSessionPoolSettings()
                                .MaxActiveSessions(2000));
    return std::make_unique<TWorkLoader>(TTableClient(driver, settings), std::move(progressTracker));
}

} // namespace NIdxTest
