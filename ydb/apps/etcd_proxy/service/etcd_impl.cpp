#include "etcd_impl.h"
#include "etcd_shared.h"
#include "etcd_events.h"

#include <ydb/apps/etcd_proxy/proto/rpc.grpc.pb.h>

#include <ydb/core/grpc_services/rpc_scheme_base.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/tx.h>

#include <ydb/library/actors/core/executor_thread.h>

namespace NEtcd {

using namespace NYdb::NQuery;

namespace {

std::string GetNameWithIndex(const std::string_view& name, const size_t* counter) {
    auto param = std::string(1U, '$') += name;
    if (counter)
        param += std::to_string(*counter);
    return param;
}

std::string GetParamName(const std::string_view& name, size_t* counter = nullptr) {
    auto param = std::string(1U, '$') += name;
    if (counter)
        param += std::to_string((*counter)++);
    return param;
}

struct TOperation {
    size_t ResultIndex = 0ULL;
};

void MakeSlice(const std::string_view& where, std::ostream& sql, NYdb::TParamsBuilder& params, size_t* paramsCounter, const i64 revision) {
    if (revision) {
        sql << "select * from (select max_by(TableRow(), `modified`) from `history`" << where;
        sql << " and " << AddParam("Rev", params, revision, paramsCounter) << " >= `modified`";
        sql << " group by `key`) flatten columns where"; ;
    } else {
        sql << "select * from `current`" << where << " and";
    }
    sql << " 0L < `version`";
}

struct TRange : public TOperation {
    std::string Key, RangeEnd;
    bool KeysOnly, CountOnly, Serializable;
    ui64 Limit;
    i64 KeyRevision;
    i64 MinCreateRevision, MaxCreateRevision;
    i64 MinModificateRevision, MaxModificateRevision;
    std::optional<bool> SortOrder;
    size_t SortTarget;

    static constexpr std::string_view Fields[] = {"key"sv, "version"sv, "created"sv, "modified"sv, "value"sv};

    std::ostream& Dump(std::ostream& out) const {
        out << (RangeEnd.empty() ? (CountOnly ? "Has" : "Get") : (CountOnly ? "Count" : "Range")) << '(';
        DumpKeyRange(out, Key, RangeEnd);
        if (KeyRevision)
            out << ",revision=" << KeyRevision;
        if (MinCreateRevision)
            out << ",min_create_rev=" << MinCreateRevision;
        if (MaxCreateRevision)
            out << ",max_create_rev=" << MaxCreateRevision;
        if (MinModificateRevision)
            out << ",min_mod_rev=" << MinModificateRevision;
        if (MaxModificateRevision)
            out << ",max_mod_rev=" << MaxModificateRevision;
        if (const auto sort = SortOrder)
            out << ",by " << Fields[SortTarget]  << ' ' << (*sort ? "asc" : "desc");
        if (KeysOnly)
            out << ",keys";
        if (Serializable)
            out << ",serializable";
        if (Limit)
            out << ",limit=" << Limit;
        out << ')';
        return out;
    }

    std::string Parse(const etcdserverpb::RangeRequest& rec) {
        Key = rec.key();
        RangeEnd = DecrementKey(rec.range_end());
        KeysOnly = rec.keys_only();
        CountOnly = rec.count_only();
        Limit = rec.limit();
        KeyRevision = rec.revision();
        MinCreateRevision = rec.min_create_revision();
        MaxCreateRevision = rec.max_create_revision();
        MinModificateRevision = rec.min_mod_revision();
        MaxModificateRevision = rec.max_mod_revision();
        SortTarget = rec.sort_target();
        Serializable = rec.serializable();
        switch (rec.sort_order()) {
            case etcdserverpb::RangeRequest_SortOrder_ASCEND: SortOrder = true; break;
            case etcdserverpb::RangeRequest_SortOrder_DESCEND: SortOrder = false; break;
            default: break;
        }

        if (Key.empty())
            return "key is not provided";

        return {};
    }

    void MakeSimpleQueryWithParams(std::ostream& sql, const std::string_view& keyFilter, NYdb::TParamsBuilder& params, size_t* paramsCounter, size_t* resultsCounter, const std::string_view& txnFilter) {
        if (resultsCounter)
            ResultIndex = (*resultsCounter)++;

        const auto& resultName = GetNameWithIndex("Output", resultsCounter);
        sql << resultName << " = select `key`,`created`,`modified`,`version`,`lease`,";
        if (KeysOnly)
            sql << "'' as ";
        sql << "`value` from (" << std::endl << '\t';
        MakeSlice(keyFilter, sql, params, paramsCounter, KeyRevision);
        sql << std::endl << ") where " << (txnFilter.empty() ? "true" : txnFilter);

        if (MinCreateRevision) {
            sql << std::endl << '\t' << "and `created` >= " << AddParam("MinCreateRevision", params, MinCreateRevision, paramsCounter);
        }

        if (MaxCreateRevision) {
            sql << std::endl << '\t' << "and `created` <= " << AddParam("MaxCreateRevision", params, MaxCreateRevision, paramsCounter);
        }

        if (MinModificateRevision) {
            sql << std::endl << '\t' << "and `modified` >= " << AddParam("MinModificateRevision", params, MinModificateRevision, paramsCounter);
        }

        if (MaxModificateRevision) {
            sql << std::endl << '\t' << "and `modified` <= " << AddParam("MaxModificateRevision", params, MaxModificateRevision, paramsCounter);
        }

        sql << ';' << std::endl;
        sql << "select count(*) from " << resultName << ';' << std::endl;

        if (!CountOnly) {
            if (resultsCounter)
                ++(*resultsCounter);

            sql << "select * from " << resultName;

            if (SortOrder) {
                sql << std::endl << "order by `" << Fields[SortTarget] << "` " << (*SortOrder ? "asc" : "desc");
            }

            if (Limit) {
                sql << std::endl << "limit " << AddParam<ui64>("Limit", params, Limit, paramsCounter);
            }
            sql << ';' << std::endl;
        }
    }

    void MakeQueryWithParams(std::ostream& sql, NYdb::TParamsBuilder& params, size_t* paramsCounter = nullptr, size_t* resultsCounter = nullptr, const std::string_view& txnFilter = {}) {
        std::ostringstream where;
        where << " where ";
        MakeSimplePredicate(Key, RangeEnd, where, params, paramsCounter);
        MakeSimpleQueryWithParams(sql, where.view(), params, paramsCounter, resultsCounter, txnFilter);
    }

    etcdserverpb::RangeResponse MakeResponse(i64 revision, const NYdb::TResultSets& results) const {
        etcdserverpb::RangeResponse response;
        response.mutable_header()->set_revision(revision);

        ui64 count = 0ULL;
        if (auto parser = NYdb::TResultSetParser(results[ResultIndex]); parser.TryNextRow()) {
            count = NYdb::TValueParser(parser.GetValue(0)).GetUint64();
            response.set_count(count);
        }

        if (!CountOnly) {
            const auto& output = results[ResultIndex + 1U];
            if (output.RowsCount() < count)
                response.set_more(true);

            size_t size = 0ULL, rows = 0ULL;
            for (auto parser = NYdb::TResultSetParser(output); size < DataSizeLimit && parser.TryNextRow(); ++rows) {
                const auto kvs = response.add_kvs();
                kvs->set_key(NYdb::TValueParser(parser.GetValue("key")).GetString());
                kvs->set_value(NYdb::TValueParser(parser.GetValue("value")).GetString());
                kvs->set_mod_revision(NYdb::TValueParser(parser.GetValue("modified")).GetInt64());
                kvs->set_create_revision(NYdb::TValueParser(parser.GetValue("created")).GetInt64());
                kvs->set_version(NYdb::TValueParser(parser.GetValue("version")).GetInt64());
                kvs->set_lease(NYdb::TValueParser(parser.GetValue("lease")).GetInt64());
                size += kvs->key().size() + kvs->value().size();
            }

            if (rows < count)
                response.set_more(true);
        }
        return response;
    }
};

using TGrpcError = std::pair<grpc::StatusCode, std::string>;

struct TPut : public TOperation {
    std::string Key, Value;
    i64 Lease = 0LL;
    bool GetPrevious = false;
    bool IgnoreValue = false;
    bool IgnoreLease = false;

    std::ostream& Dump(std::ostream& out) const {
        out << "Put(" << Key;
        if (IgnoreValue)
            out << ",ignore value";
        else
            out << ",size=" << Value.size();
        if (IgnoreLease)
            out << ",ignore lease";
        else if (Lease)
            out << ",lease=" << Lease;
        if (GetPrevious)
            out << ",previous";
        out << ')';
        return out;
    }

    std::string Parse(const etcdserverpb::PutRequest& rec) {
        Key = rec.key();
        Value = rec.value();
        Lease = rec.lease();
        GetPrevious = rec.prev_kv();
        IgnoreValue = rec.ignore_value();
        IgnoreLease = rec.ignore_lease();

        if (Key.empty())
            return "key is not provided";

        if (IgnoreValue && !Value.empty())
            return "value is provided";

        if (IgnoreLease && Lease)
            return "lease is provided";

        return {};
    }

    void MakeSimpleQueryWithParams(std::ostream& sql, const std::string_view& keyParamName, const std::string_view& keyFilter, NYdb::TParamsBuilder& params, size_t* paramsCounter, size_t* resultsCounter, const std::string_view& txnFilter) {
        const auto& valueParamName = IgnoreValue ? std::string("NULL") : AddParam("Value", params, Value, paramsCounter);
        const auto& leaseParamName = IgnoreLease ? std::string("NULL") : AddParam("Lease", params, Lease, paramsCounter);

        const auto& oldResultSetName = GetNameWithIndex("Old", resultsCounter);
        const auto& newResultSetName = GetNameWithIndex("New", resultsCounter);

        sql << oldResultSetName << " = select * from `current`" << keyFilter << " and 0L < `version`;" << std::endl;
        sql << newResultSetName << " = select" << std::endl;
        sql << '\t' << keyParamName << " as `key`," << std::endl;
        sql << '\t' << "if(`version` > 0L, `created`, $Revision) as `created`," << std::endl;
        sql << '\t' << "$Revision as `modified`," << std::endl;
        sql << '\t' << "`version` + 1L as `version`," << std::endl;
        sql << '\t' << "nvl(" << valueParamName << ",`value`) as `value`," << std::endl;
        sql << '\t' << "nvl(" << leaseParamName << ",`lease`) as `lease`" << std::endl;
        sql << '\t' << "from ";
        if (!txnFilter.empty())
            sql << "(select * from ";

        const bool update = IgnoreValue || IgnoreLease;
        if (update)
            sql << oldResultSetName;
        else
            sql << "(select * from " << oldResultSetName <<" union all select * from as_table([<|`key`:" << keyParamName << ", `created`:0L, `modified`: 0L, `version`:0L, `value`:'', `lease`:0L|>]) order by `created` desc limit 1)";

        if (!txnFilter.empty())
            sql << " where " << txnFilter << ')';
        sql << ';' << std::endl;

        sql << (update ? "update `current` on" : "upsert into `current`") << " select * from " << newResultSetName << ';' << std::endl;
        sql << "insert into `history` select * from " << newResultSetName << ';' << std::endl;

        if (GetPrevious || update) {
            if (resultsCounter)
                ResultIndex = (*resultsCounter)++;
            sql << "select `value`, `created`, `modified`, `version`, `lease` from " << oldResultSetName << " where `version` > 0L;" << std::endl;
        }
    }

    void MakeQueryWithParams(std::ostream& sql, NYdb::TParamsBuilder& params, size_t* paramsCounter = nullptr, size_t* resultsCounter = nullptr, const std::string_view& txnFilter = {}) {
        std::ostringstream keyFilter;
        keyFilter << " where ";
        const auto& keyParamName = MakeSimplePredicate(Key, {}, keyFilter, params, paramsCounter);
        MakeSimpleQueryWithParams(sql, keyParamName, keyFilter.view(), params, paramsCounter, resultsCounter, txnFilter);
    }

    std::variant<etcdserverpb::PutResponse, TGrpcError>
    MakeResponse(i64 revision, const NYdb::TResultSets& results) const {
        etcdserverpb::PutResponse response;
        response.mutable_header()->set_revision(revision);

        if (GetPrevious || IgnoreValue || IgnoreValue) {
            if (auto parser = NYdb::TResultSetParser(results[ResultIndex]); parser.TryNextRow() && 5ULL == parser.ColumnsCount()) {
                if (GetPrevious) {
                    const auto prev = response.mutable_prev_kv();
                    prev->set_key(Key);
                    prev->set_value(NYdb::TValueParser(parser.GetValue("value")).GetString());
                    prev->set_mod_revision(NYdb::TValueParser(parser.GetValue("modified")).GetInt64());
                    prev->set_create_revision(NYdb::TValueParser(parser.GetValue("created")).GetInt64());
                    prev->set_version(NYdb::TValueParser(parser.GetValue("version")).GetInt64());
                    prev->set_lease(NYdb::TValueParser(parser.GetValue("lease")).GetInt64());
                }
            } else if (IgnoreValue || IgnoreValue) {
                return std::make_pair(grpc::StatusCode::NOT_FOUND, std::string("key not found"));
            }
        }
        return response;
    }
};

struct TDeleteRange : public TOperation {
    std::string Key, RangeEnd;
    bool GetPrevious = false;

    std::ostream& Dump(std::ostream& out) const {
        out << "Delete(";
        DumpKeyRange(out, Key, RangeEnd);
        if (GetPrevious)
            out << ",previous";
        out << ')';
        return out;
    }

    std::string Parse(const etcdserverpb::DeleteRangeRequest& rec) {
        Key = rec.key();
        RangeEnd = DecrementKey(rec.range_end());
        GetPrevious = rec.prev_kv();

        if (Key.empty())
            return "key is not provided";

        if (!RangeEnd.empty() && Endless != RangeEnd && RangeEnd < Key)
            return "invalid range end";

        return {};
    }

    void MakeSimpleQueryWithParams(std::ostream& sql, const std::string_view& keyFilter, size_t* resultsCounter, const std::string_view& txnFilter) {
        if (resultsCounter)
            ResultIndex = (*resultsCounter)++;

        const auto& oldResultSetName = GetNameWithIndex("Old", resultsCounter);
        sql << oldResultSetName << " = select * from `current`" << keyFilter << " and 0L < `version`";
        if (!txnFilter.empty())
            sql << " and " << txnFilter;
        sql << ';' << std::endl;

        sql << "insert into `history`" << std::endl;
        sql << "select `key`, `created`, $Revision as `modified`, 0L as `version`, `value`, `lease` from " << oldResultSetName << ';' << std::endl;
        sql << "select count(*) from " << oldResultSetName << ';' << std::endl;

        if (GetPrevious) {
            if (resultsCounter)
                ++(*resultsCounter);
            sql << "select `key`, `value`, `created`, `modified`, `version`, `lease` from " << oldResultSetName << ';' << std::endl;
        }

        sql << "delete from `current`" << keyFilter;
        if (!txnFilter.empty())
            sql << " and " << txnFilter;
        sql << ';' << std::endl;
    }

    void MakeQueryWithParams(std::ostream& sql, NYdb::TParamsBuilder& params, size_t* paramsCounter = nullptr, size_t* resultsCounter = nullptr, const std::string_view& txnFilter = {}) {
        std::ostringstream keyFilter;
        keyFilter << " where ";
        MakeSimplePredicate(Key, RangeEnd, keyFilter, params, paramsCounter);
        MakeSimpleQueryWithParams(sql, keyFilter.view(), resultsCounter, txnFilter);
    }

    etcdserverpb::DeleteRangeResponse MakeResponse(i64 revision, const NYdb::TResultSets& results) const {
        etcdserverpb::DeleteRangeResponse response;

        ui64 deleted  = 0ULL;
        if (auto parser = NYdb::TResultSetParser(results[ResultIndex]); parser.TryNextRow()) {
            deleted = NYdb::TValueParser(parser.GetValue(0)).GetUint64();
            response.set_deleted(deleted);
        }

        response.mutable_header()->set_revision(deleted ? revision : revision - 1LL);

        if (GetPrevious) {
            for (auto parser = NYdb::TResultSetParser(results[ResultIndex + 1U]); parser.TryNextRow();) {
                const auto kvs = response.add_prev_kvs();
                kvs->set_key(NYdb::TValueParser(parser.GetValue("key")).GetString());
                kvs->set_value(NYdb::TValueParser(parser.GetValue("value")).GetString());
                kvs->set_mod_revision(NYdb::TValueParser(parser.GetValue("modified")).GetInt64());
                kvs->set_create_revision(NYdb::TValueParser(parser.GetValue("created")).GetInt64());
                kvs->set_version(NYdb::TValueParser(parser.GetValue("version")).GetInt64());
                kvs->set_lease(NYdb::TValueParser(parser.GetValue("lease")).GetInt64());
            }
        }
        return response;
    }
};

struct TCompare {
    std::string Key, RangeEnd;

    std::variant<i64, std::string> Value;

    size_t Result, Target;

    std::string Parse(const etcdserverpb::Compare& rec) {
        Key = rec.key();
        RangeEnd = DecrementKey(rec.range_end());
        Result = rec.result();
        Target = rec.target();
        switch (rec.target()) {
            case etcdserverpb::Compare_CompareTarget_VERSION:
                Value = rec.version();
                break;
            case etcdserverpb::Compare_CompareTarget_CREATE:
                Value = rec.create_revision();
                break;
            case etcdserverpb::Compare_CompareTarget_MOD:
                Value = rec.mod_revision();
                break;
            case etcdserverpb::Compare_CompareTarget_VALUE:
                Value = rec.value();
                break;
            case etcdserverpb::Compare_CompareTarget_LEASE:
                Value = rec.lease();
                break;
            default:
                break;
        }

        if (Key.empty())
            return "key is not provided";

        if (!RangeEnd.empty() && Endless != RangeEnd && RangeEnd < Key)
            return "invalid range end";

        return {};
    }

    static constexpr std::string_view Fields[] = {"version"sv, "created"sv, "modified"sv, "value"sv, "lease"sv};
    static constexpr std::string_view Comparator[] = {"="sv, ">"sv, "<"sv, "!="sv};
    static constexpr std::string_view Inverted[] = {"!="sv, "<="sv, ">="sv, "="sv};

    std::ostream& Dump(std::ostream& out) const {
        out << Fields[Target] << '(';
        DumpKeyRange(out, Key, RangeEnd);
        out << ')' << Comparator[Result];
        if (const auto val = std::get_if<std::string>(&Value))
            out << *val;
        else if (const auto val = std::get_if<i64>(&Value))
            out << *val;
        out << ')';
        return out;
    }

    // return default value if key is absent.
    bool MakeQueryWithParams(std::ostream& positive, std::ostream& negative, NYdb::TParamsBuilder& params, size_t* paramsCounter) const {
        positive << '`' << Fields[Target] << '`' << ' ' << Comparator[Result] << ' ';
        negative << '`' << Fields[Target] << '`' << ' ' << Inverted[Result] << ' ';
        if (const auto val = std::get_if<std::string>(&Value)) {
            const auto& valueParamName = AddParam("Value", params, *val, paramsCounter);
            positive << valueParamName;
            negative << valueParamName;
        } else if (const auto val = std::get_if<i64>(&Value)) {
            const auto& argParamName = AddParam("Arg", params, *val, paramsCounter);
            positive << argParamName;
            negative << argParamName;
            return !*val && Target < 3U;
        }
        return false;
    }
};

struct TTxn : public TOperation {
    using TRequestOp = std::variant<TRange, TPut, TDeleteRange, TTxn>;

    std::vector<TCompare> Compares;
    std::vector<TRequestOp> Success, Failure;

    std::ostream& Dump(std::ostream& out) const {
        const auto dump = [](const std::vector<TRequestOp>& operations, std::ostream& out) {
            for (const auto& operation : operations) {
                if (const auto oper = std::get_if<TRange>(&operation))
                   oper->Dump(out);
                else if (const auto oper = std::get_if<TPut>(&operation))
                   oper->Dump(out);
                else if (const auto oper = std::get_if<TDeleteRange>(&operation))
                   oper->Dump(out);
                else if (const auto oper = std::get_if<TTxn>(&operation))
                   oper->Dump(out);
            }
        };

        out << "Txn(";
        if (!Compares.empty()) {
            out << "if ";
            for (const auto& cmp : Compares)
                cmp.Dump(out);
        }
        if (!Success.empty()) {
            out << " then ";
            dump(Success, out);
        }
        if (!Failure.empty()) {
            out << " else ";
            dump(Failure, out);
        }
        out << ')';
        return out;
    }

    bool IsReadOnly() const {
        for (const auto& operation : Success) {
            if (const auto oper = std::get_if<TTxn>(&operation)) {
                if (!oper->IsReadOnly())
                    return false;
            } else if (!std::get_if<TRange>(&operation))
                return false;
        }

        for (const auto& operation : Failure) {
            if (const auto oper = std::get_if<TTxn>(&operation)) {
                if (!oper->IsReadOnly())
                    return false;
            } else if (!std::get_if<TRange>(&operation))
                return false;
        }

        return true;
    }

    TKeysSet GetKeys() const {
        TKeysSet keys;
        for (const auto& compare : Compares)
            keys.emplace(compare.Key, compare.RangeEnd);

        const auto get = [](const std::vector<TRequestOp>& operations, TKeysSet& keys) {
            for (const auto& operation : operations) {
                if (const auto oper = std::get_if<TRange>(&operation))
                    keys.emplace(oper->Key, oper->RangeEnd);
                else if (const auto oper = std::get_if<TPut>(&operation))
                    keys.emplace(oper->Key, std::string());
                else if (const auto oper = std::get_if<TDeleteRange>(&operation))
                    keys.emplace(oper->Key, oper->RangeEnd);
                else if (const auto oper = std::get_if<TTxn>(&operation))
                    keys.merge(oper->GetKeys());
            }
        };
        get(Success, keys);
        get(Failure, keys);
        return keys;
    }

    template<class TOperation, class TSrc>
    static std::string Parse(std::vector<TRequestOp>& operations, const TSrc& src) {
        TOperation op;
        if (const auto& error = op.Parse(src); !error.empty())
            return error;
        operations.emplace_back(std::move(op));
        return {};
    }

    std::string Parse(const etcdserverpb::TxnRequest& rec) {
        for (const auto& comp : rec.compare()) {
            Compares.emplace_back();
            if (const auto& error = Compares.back().Parse(comp); !error.empty())
                return error;
        }

        const auto fill = [](std::vector<TRequestOp>& operations, const auto& fields) -> std::string {
            for (const auto& op : fields) {
                switch (op.request_case()) {
                    case etcdserverpb::RequestOp::RequestCase::kRequestRange: {
                        if (const auto& error = Parse<TRange>(operations, op.request_range()); !error.empty())
                            return error;
                        break;
                    }
                    case etcdserverpb::RequestOp::RequestCase::kRequestPut: {
                        if (const auto& error = Parse<TPut>(operations, op.request_put()); !error.empty())
                            return error;
                        break;
                    }
                    case etcdserverpb::RequestOp::RequestCase::kRequestDeleteRange: {
                        if (const auto& error = Parse<TDeleteRange>(operations, op.request_delete_range()); !error.empty())
                            return error;
                        break;
                    }
                    case etcdserverpb::RequestOp::RequestCase::kRequestTxn: {
                        if (const auto& error = Parse<TTxn>(operations, op.request_txn()); !error.empty())
                            return error;
                        break;
                    }
                    default:
                        return "invalid operation";
                }
            }
            return {};
        };

        return fill(Success, rec.success()) + fill(Failure, rec.failure());
    }

    void MakeSimpleQueryWithParams(std::ostream& sql, const std::string_view& keyParamName, bool singleKey, const std::string_view& keyFilter, NYdb::TParamsBuilder& params, size_t* paramsCounter, size_t* resultsCounter, const std::string_view& txnFilter = {}) {
        ResultIndex = (*resultsCounter)++;

        const auto make = [&sql, &params](std::vector<TRequestOp>& operations, size_t* paramsCounter, size_t* resultsCounter, const std::string_view& keyFilter, const std::string_view& keyParamName, bool singleKey, const std::string_view& txnFilter) {
            for (auto& operation : operations) {
                if (const auto oper = std::get_if<TRange>(&operation))
                    oper->MakeSimpleQueryWithParams(sql, keyFilter, params, paramsCounter, resultsCounter, txnFilter);
                else if (const auto oper = std::get_if<TPut>(&operation))
                    oper->MakeSimpleQueryWithParams(sql, keyParamName, keyFilter, params, paramsCounter, resultsCounter, txnFilter);
                else if (const auto oper = std::get_if<TDeleteRange>(&operation))
                    oper->MakeSimpleQueryWithParams(sql, keyFilter, resultsCounter, txnFilter);
                else if (const auto oper = std::get_if<TTxn>(&operation))
                    oper->MakeSimpleQueryWithParams(sql, keyParamName, singleKey, keyFilter, params, paramsCounter, resultsCounter, txnFilter);
            }
        };

        std::ostringstream thenFilter, elseFilter;
        bool def = true;
        for (auto j = Compares.cbegin(); Compares.cend() != j; ++j) {
            if (Compares.cbegin() != j) {
                thenFilter << " and ";
                elseFilter << " or ";
            }
            def = j->MakeQueryWithParams(thenFilter, elseFilter, params, paramsCounter) && def;
        }

        if (Compares.empty()) {
            sql << "select true;" << std::endl;
            make(Success, paramsCounter, resultsCounter, keyFilter, keyParamName, singleKey, txnFilter);
        } else {
            std::ostringstream thenExtra, elseExtra;
            if (!txnFilter.empty()) {
                thenExtra << txnFilter << " and ";
                elseExtra << txnFilter << " and ";
            }
            thenExtra << '(' << thenFilter.view() << ')';
            elseExtra << '(' << elseFilter.view() << ')';

            if (!singleKey)
                 sql << "select bool_and(`cmp`) as`cmp` from (";
            sql << "select nvl(" << (def ? '0' : '1') << "UL = count(*), " << (def ? "true" : "false") << ") as `cmp` from `current`" << keyFilter << " and " << (def ? elseFilter : thenFilter).view();
            if (!singleKey)
                sql << " group by `key`)";
            sql << ';' << std::endl;

            make(Success, paramsCounter, resultsCounter, keyFilter, keyParamName, singleKey, thenExtra.view());
            make(Failure, paramsCounter, resultsCounter, keyFilter, keyParamName, singleKey, elseExtra.view());
        }
    }

    void MakeQueryWithParams(std::ostream& sql, NYdb::TParamsBuilder& params, size_t* paramsCounter = nullptr, size_t* resultsCounter = nullptr, const std::string_view& txnFilter = {}) {
        ResultIndex = (*resultsCounter)++;

        const auto make = [&sql, &params](std::vector<TRequestOp>& operations, size_t* paramsCounter, size_t* resultsCounter, const std::string_view& txnFilter) {
            for (auto& operation : operations) {
                if (const auto oper = std::get_if<TRange>(&operation))
                    oper->MakeQueryWithParams(sql, params, paramsCounter, resultsCounter, txnFilter);
                else if (const auto oper = std::get_if<TPut>(&operation))
                    oper->MakeQueryWithParams(sql, params, paramsCounter, resultsCounter, txnFilter);
                else if (const auto oper = std::get_if<TDeleteRange>(&operation))
                    oper->MakeQueryWithParams(sql, params, paramsCounter, resultsCounter, txnFilter);
                else if (const auto oper = std::get_if<TTxn>(&operation))
                    oper->MakeQueryWithParams(sql, params, paramsCounter, resultsCounter, txnFilter);
            }
        };

        if (Compares.empty()) {
            sql << "select true;" << std::endl;
            make(Success, paramsCounter, resultsCounter, "");
        } else {
            std::unordered_map<std::pair<std::string, std::string>, std::vector<TCompare>> map(Compares.size());
            for (const auto& compare : Compares)
                map[std::make_pair(compare.Key, compare.RangeEnd)].emplace_back(compare);
            const bool manyRanges = map.size() > 1U;

            const auto& cmpResultSetName = GetNameWithIndex("Cmp", resultsCounter);
            sql << cmpResultSetName << " = ";

            if (manyRanges)
                sql << "select nvl(bool_and(`cmp`), false) as `cmp` from (" << std::endl;

            for (auto i = map.cbegin(); map.cend() != i; ++i) {
                if (map.cbegin() != i)
                    sql << std::endl << "union all" << std::endl;

                sql << "select nvl(bool_and(";
                const auto& compares = i->second;
                bool def = true;
                for (auto j = compares.cbegin(); compares.cend() != j; ++j) {
                    if (compares.cbegin() != j)
                        sql << " and ";
                    std::ostringstream stub;
                    def = j->MakeQueryWithParams(sql, stub, params, paramsCounter) && def;
                }
                sql << "), " << (def ? "true" : "false") << ") as `cmp` from `current` where ";
                MakeSimplePredicate(i->first.first, i->first.second, sql, params, paramsCounter);
            }

            if (manyRanges)
                sql << std::endl << ')';

            sql << ';' << std::endl;
            sql << "select * from " << cmpResultSetName << ';' << std::endl;


            const auto& scalarSuccessName = GetNameWithIndex("Success", resultsCounter);
            const auto& scalarFailureName = GetNameWithIndex("Failure", resultsCounter);

            if (txnFilter.empty()) {
                if (!Success.empty())
                    sql << scalarSuccessName << " = select " << cmpResultSetName << ';' << std::endl;
                if (!Failure.empty())
                    sql << scalarFailureName << " = select not " << cmpResultSetName << ';' << std::endl;
            } else {
                if (!Success.empty())
                    sql << scalarSuccessName << " = select " << txnFilter << " and " << cmpResultSetName << ';' << std::endl;
                if (!Failure.empty())
                    sql << scalarFailureName << " = select " << txnFilter << " and not " << cmpResultSetName << ';' << std::endl;
            }

            make(Success, paramsCounter, resultsCounter, scalarSuccessName);
            make(Failure, paramsCounter, resultsCounter, scalarFailureName);
        }
    }

    std::variant<etcdserverpb::TxnResponse, TGrpcError>
    MakeResponse(i64 revision, const NYdb::TResultSets& results) const {
        etcdserverpb::TxnResponse response;
        response.mutable_header()->set_revision(revision);

        if (auto parser = NYdb::TResultSetParser(results[ResultIndex]); parser.TryNextRow()) {
            const bool succeeded = NYdb::TValueParser(parser.GetValue(0)).GetBool();
            response.set_succeeded(succeeded);
            for (const auto& operation : succeeded ? Success : Failure) {
                const auto resp = response.add_responses();
                if (const auto oper = std::get_if<TRange>(&operation)) {
                    *resp->mutable_response_range() = oper->MakeResponse(revision, results);
                } else if (const auto oper = std::get_if<TPut>(&operation)) {
                    const auto& res = oper->MakeResponse(revision, results);
                    if (const auto good = std::get_if<etcdserverpb::PutResponse>(&res))
                        *resp->mutable_response_put() = *good;
                    else if (const auto bad = std::get_if<TGrpcError>(&res))
                        return *bad;
                } else if (const auto oper = std::get_if<TDeleteRange>(&operation)) {
                    *resp->mutable_response_delete_range() = oper->MakeResponse(revision, results);
                } else if (const auto oper = std::get_if<TTxn>(&operation)) {
                    const auto& res = oper->MakeResponse(revision, results);
                    if (const auto good = std::get_if<etcdserverpb::TxnResponse>(&res))
                        *resp->mutable_response_txn() = *good;
                    else if (const auto bad = std::get_if<TGrpcError>(&res))
                        return *bad;
                }
            }
        }
        return response;
    }
};

using namespace NActors;

class TBaseEtcdRequest {
protected:
    virtual std::string ParseGrpcRequest() = 0;
    virtual void MakeQueryWithParams(std::ostream& sql, NYdb::TParamsBuilder& params) = 0;
    virtual void ReplyWith(const NYdb::TResultSets& results, const TActorContext& ctx) = 0;
    virtual std::optional<bool> RequiredNextRevision() const { return std::nullopt; }
    virtual TKeysSet GetAffectedKeysSet() const { return {}; }

    i64 Revision = 0LL;
};

using namespace NKikimr::NGRpcService;

template <typename TDerived, typename TRequest, bool ReadOnly = false>
class TEtcdRequestGrpc
    : public TActorBootstrapped<TEtcdRequestGrpc<TDerived, TRequest, ReadOnly>>
    , public TBaseEtcdRequest
{
    friend class TBaseEtcdRequest;
public:
    TEtcdRequestGrpc(std::unique_ptr<NKikimr::NGRpcService::IRequestCtx> request, TSharedStuff::TPtr stuff)
        : Request_(std::move(request)), Stuff(std::move(stuff))
    {}

    void Bootstrap(const TActorContext& ctx) {
        if (const auto& error = this->ParseGrpcRequest(); !error.empty()) {
            this->Request_->ReplyWithRpcStatus(grpc::StatusCode::INVALID_ARGUMENT, TString(error));
            this->Die(ctx);
        } else if (const auto reqRev = this->RequiredNextRevision()) {
            if (*reqRev)
                RequestNextRevision(ctx);
            else
                RequestNextLease(ctx);
        } else {
            SendDatabaseRequest();
        }
    }
protected:
    void RequestNextRevision(const TActorContext& ctx) {
        this->Become(&TEtcdRequestGrpc::WaitRevisionFunc);
        ctx.Send(Stuff->MainGate, new TEvRequestRevision(this->GetAffectedKeysSet()));
    }
private:
    void RequestNextLease(const TActorContext& ctx) {
        this->Become(&TEtcdRequestGrpc::WaitRevisionFunc);
        ctx.Send(Stuff->HolderHouse, new TEvRequestRevision(this->GetAffectedKeysSet()));
    }

    TGuard Guard;

    static std::string GetRequestName() {
        return TRequest::TRequest::descriptor()->name();
    }

    virtual std::ostream& Dump(std::ostream& out) const = 0;

    TQueryClient::TQueryResultFunc GetQueryResultFunc() {
        std::ostringstream sql;
        NYdb::TParamsBuilder params;
        sql << "-- " << GetRequestName() << " >>>>" << std::endl;
        sql << Stuff->TablePrefix;
        const auto reqRev = this->RequiredNextRevision();
        if (reqRev && *reqRev)
            AddParam("Revision", params, Revision);
        this->MakeQueryWithParams(sql, params);
        if (!reqRev)
            sql << "select nvl(max(`revision`), 0L) from `commited`;" << std::endl;
        else if (!Guard && *reqRev)
            sql << "insert into `commited` (`revision`,`timestamp`) values ($Revision,CurrentUtcTimestamp());" << std::endl;
        sql << "-- " << GetRequestName() << " <<<<" << std::endl;
//      std::cout << std::endl << sql.view() << std::endl;

        return [query = sql.str(), args = params.Build()](TQueryClient::TSession session) -> TAsyncExecuteQueryResult {
            return session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx(), args);
        };
    }

    void SendDatabaseRequest() {
        this->Become(&TEtcdRequestGrpc::WaitResultFunc);
        Stuff->Client->RetryQuery(GetQueryResultFunc()).Subscribe([my = this->SelfId(), stuff = TSharedStuff::TWeakPtr(Stuff)](const auto& future) {
            if (const auto lock = stuff.lock()) {
                if (const auto res = future.GetValue(); res.IsSuccess())
                    lock->ActorSystem->Send(my, new NEtcd::TEvQueryResult(res.GetResultSets()));
                else
                    lock->ActorSystem->Send(my, new NEtcd::TEvQueryError(res.GetIssues()));
            }
        });
    }

    STFUNC(WaitRevisionFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvReturnRevision, Handle);
            HFunc(TEvQueryError, Handle);
        }
    }

    STFUNC(WaitResultFunc) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvQueryResult, Handle);
            HFunc(TEvQueryError, Handle);
        }
    }

    void Handle(TEvReturnRevision::TPtr &ev) {
        Revision = ev->Get()->Revision;
        Guard = std::move(ev->Get()->Guard);
        SendDatabaseRequest();
    }

    void Handle(NEtcd::TEvQueryResult::TPtr &ev, const TActorContext& ctx) {
        if (!this->RequiredNextRevision()) {
            if (auto parser = NYdb::TResultSetParser(ev->Get()->Results.back()); parser.TryNextRow()) {
                Revision = NYdb::TValueParser(parser.GetValue(0)).GetInt64();
            }
        }
        if (Revision && !Guard)
            Stuff->UpdateRevision(Revision);
        ReplyWith(ev->Get()->Results, ctx);
    }

    void Handle(NEtcd::TEvQueryError::TPtr &ev, const TActorContext& ctx) {
        std::ostringstream err;
        Dump(err) << " SQL error received:" << std::endl << ev->Get()->Issues.ToString() << std::endl;
        std::cout << err.view();
        Reply(grpc::StatusCode::INTERNAL, err.view(), ctx);
    }
protected:
    const typename TRequest::TRequest* GetProtoRequest() const {
        return TRequest::GetProtoRequest(Request_);
    }

    void Reply(typename TRequest::TResponse& resp, const TActorContext& ctx) {
        this->Request_->Reply(&resp);
        this->Die(ctx);
    }

    void Reply(grpc::StatusCode code, const std::string_view& error, const TActorContext& ctx) {
        this->Request_->ReplyWithRpcStatus(code, TString(error));
        this->Die(ctx);
    }

    const std::unique_ptr<NKikimr::NGRpcService::IRequestCtx> Request_;
    const TSharedStuff::TPtr Stuff;
};

using TEvRangeKVRequest = TGrpcRequestNoOperationCall<etcdserverpb::RangeRequest, etcdserverpb::RangeResponse>;
using TEvPutKVRequest = TGrpcRequestNoOperationCall<etcdserverpb::PutRequest, etcdserverpb::PutResponse>;
using TEvDeleteRangeKVRequest = TGrpcRequestNoOperationCall<etcdserverpb::DeleteRangeRequest, etcdserverpb::DeleteRangeResponse>;
using TEvTxnKVRequest = TGrpcRequestNoOperationCall<etcdserverpb::TxnRequest, etcdserverpb::TxnResponse>;
using TEvCompactKVRequest = TGrpcRequestNoOperationCall<etcdserverpb::CompactionRequest, etcdserverpb::CompactionResponse>;

using TEvLeaseGrantRequest = TGrpcRequestNoOperationCall<etcdserverpb::LeaseGrantRequest, etcdserverpb::LeaseGrantResponse>;
using TEvLeaseRevokeRequest = TGrpcRequestNoOperationCall<etcdserverpb::LeaseRevokeRequest, etcdserverpb::LeaseRevokeResponse>;
using TEvLeaseTimeToLiveRequest = TGrpcRequestNoOperationCall<etcdserverpb::LeaseTimeToLiveRequest, etcdserverpb::LeaseTimeToLiveResponse>;
using TEvLeaseLeasesRequest = TGrpcRequestNoOperationCall<etcdserverpb::LeaseLeasesRequest, etcdserverpb::LeaseLeasesResponse>;

class TRangeRequest
    : public TEtcdRequestGrpc<TRangeRequest, TEvRangeKVRequest, true> {
public:
    using TBase = TEtcdRequestGrpc<TRangeRequest, TEvRangeKVRequest, true>;
    using TBase::TBase;
private:
    std::string ParseGrpcRequest() final {
        return Range.Parse(*GetProtoRequest());
    }

    void MakeQueryWithParams(std::ostream& sql, NYdb::TParamsBuilder& params) final {
        return Range.MakeQueryWithParams(sql, params);
    }

    void ReplyWith(const NYdb::TResultSets& results, const TActorContext& ctx) final {
        auto response = Range.MakeResponse(Revision, results);
        Dump(std::cout) << '=' << response.count() << std::endl;
        return Reply(response, ctx);
    }

    std::ostream& Dump(std::ostream& out) const final {
        return Range.Dump(out);
    }

    TKeysSet GetAffectedKeysSet() const final {
        return {{Range.Key, Range.RangeEnd}};
    }

    TRange Range;
};

class TPutRequest
    : public TEtcdRequestGrpc<TPutRequest, TEvPutKVRequest> {
public:
    using TBase = TEtcdRequestGrpc<TPutRequest, TEvPutKVRequest>;
    using TBase::TBase;
private:
    std::string ParseGrpcRequest() final {
        if (const auto& error = Put.Parse(*GetProtoRequest()); !error.empty())
            return error;
        return {};
    }

    void MakeQueryWithParams(std::ostream& sql, NYdb::TParamsBuilder& params) final {
        return Put.MakeQueryWithParams(sql, params);
    }

    void ReplyWith(const NYdb::TResultSets& results, const TActorContext& ctx) final {
        auto response = Put.MakeResponse(Revision, results);
        Dump(std::cout) << '=';
        if (const auto good = std::get_if<etcdserverpb::PutResponse>(&response)) {
            std::cout << "ok" << std::endl;
            return Reply(*good, ctx);
        } else if (const auto bad = std::get_if<TGrpcError>(&response)) {
            std::cout << bad->second << std::endl;
            return Reply(bad->first, bad->second, ctx);
        }
    }

    std::optional<bool> RequiredNextRevision() const final {
        return true;
    }

    TKeysSet GetAffectedKeysSet() const final {
        return {{Put.Key, {}}};
    }

    std::ostream& Dump(std::ostream& out) const final {
        return Put.Dump(out);
    }

    TPut Put;
};

class TDeleteRangeRequest
    : public TEtcdRequestGrpc<TDeleteRangeRequest, TEvDeleteRangeKVRequest> {
public:
    using TBase = TEtcdRequestGrpc<TDeleteRangeRequest, TEvDeleteRangeKVRequest>;
    using TBase::TBase;
private:
    std::string ParseGrpcRequest() final {
        if (const auto& error = DeleteRange.Parse(*GetProtoRequest()); !error.empty())
            return error;
        return {};
    }

    void MakeQueryWithParams(std::ostream& sql, NYdb::TParamsBuilder& params) final {
        return DeleteRange.MakeQueryWithParams(sql, params);
    }

    void ReplyWith(const NYdb::TResultSets& results, const TActorContext& ctx) final {
        auto response = DeleteRange.MakeResponse(Revision, results);
        Dump(std::cout) << '=' << response.deleted() << std::endl;
        return Reply(response, ctx);
    }

    std::optional<bool> RequiredNextRevision() const final {
        return true;
    }

    TKeysSet GetAffectedKeysSet() const final {
        return {{DeleteRange.Key, DeleteRange.RangeEnd}};
    }

    std::ostream& Dump(std::ostream& out) const final {
        return DeleteRange.Dump(out);
    }

    TDeleteRange DeleteRange;
};

class TTxnRequest
    : public TEtcdRequestGrpc<TTxnRequest, TEvTxnKVRequest> {
public:
    using TBase = TEtcdRequestGrpc<TTxnRequest, TEvTxnKVRequest>;
    using TBase::TBase;
private:
    std::string ParseGrpcRequest() final {
        if (const auto& error = Txn.Parse(*GetProtoRequest()); !error.empty())
            return error;
        return {};
    }

    void MakeQueryWithParams(std::ostream& sql, NYdb::TParamsBuilder& params) final {
        size_t resultsCounter = 0U, paramsCounter = 0U;
        if (const auto& keys = Txn.GetKeys(); keys.empty()) {
            return Txn.MakeSimpleQueryWithParams(sql, {}, true, {}, params, &resultsCounter, &paramsCounter);
        } else if (1U == keys.size()) {
            std::ostringstream where;
            where << " where ";
            const auto& keyParamName = MakeSimplePredicate(keys.cbegin()->first, keys.cbegin()->second, where, params);
            return Txn.MakeSimpleQueryWithParams(sql, keyParamName, keys.cbegin()->second.empty(), where.view(), params, &resultsCounter, &paramsCounter);
        };

        return Txn.MakeQueryWithParams(sql, params, &resultsCounter, &paramsCounter);
    }

    void ReplyWith(const NYdb::TResultSets& results, const TActorContext& ctx) final {
        auto response = Txn.MakeResponse(Revision, results);
        Dump(std::cout) << '=';
        if (const auto good = std::get_if<etcdserverpb::TxnResponse>(&response)) {
            std::cout << (good->succeeded() ? "success" : "failure") << std::endl;
            return Reply(*good, ctx);
        } else if (const auto bad = std::get_if<TGrpcError>(&response)) {
            std::cout << bad->second << std::endl;
            return Reply(bad->first, bad->second, ctx);
        }
    }

    std::optional<bool> RequiredNextRevision() const final {
        if (Txn.IsReadOnly())
            return std::nullopt;
        else
            return true;
    }

    TKeysSet GetAffectedKeysSet() const final {
        return Txn.GetKeys();
    }

    std::ostream& Dump(std::ostream& out) const final {
        return Txn.Dump(out);
    }

    TTxn Txn;
};

class TCompactRequest
    : public TEtcdRequestGrpc<TCompactRequest, TEvCompactKVRequest> {
public:
    using TBase = TEtcdRequestGrpc<TCompactRequest, TEvCompactKVRequest>;
    using TBase::TBase;
private:
    std::string ParseGrpcRequest() final {
        const auto &rec = *GetProtoRequest();
        KeyRevision = rec.revision();
        Physical = rec.physical();
        if (!KeyRevision)
            return std::string("invalid revision:" ) += std::to_string(KeyRevision);
        return {};
    }

    TQueryClient::TQueryResultFunc GetQueryAsyncResultFunc() const {
        std::ostringstream sql;
        NYdb::TParamsBuilder params;
        const auto& paramName = AddParam("Revision", params, KeyRevision);
        sql << Stuff->TablePrefix;
        sql << "$Trash = select c.key as key, c.modified as modified from `history` as c inner join (" << std::endl;
        sql << "select max_by((`key`, `modified`), `modified`) as pair from `history`" << std::endl;
        sql << "where `modified` < " << paramName << " and 0L = `version` group by `key`" << std::endl;
        sql << ") as keys on keys.pair.0 = c.key where c.modified <= keys.pair.1;" << std::endl;
        sql << "delete from `history` on select * from $Trash;" << std::endl;
        sql << "delete from `commited` where `revision` < " << paramName << ';' << std::endl;
//      std::cout << std::endl << sql.view() << std::endl;

        return [query = sql.str(), args = params.Build()](TQueryClient::TSession session) -> TAsyncExecuteQueryResult {
            return session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx(), args);
        };
    }

    void SendBackgrondRequest() const {
        Stuff->Client->RetryQuery(GetQueryAsyncResultFunc()).Subscribe([](const auto& future) {
            if (const auto res = future.GetValue(); res.IsSuccess())
                std::cout << "Async compaction finished succesfully." << std::endl;
            else
                std::cout << "Async compaction finished with errors: " << res.GetIssues().ToString() << std::endl;
        });
    }

    void MakeQueryWithParams(std::ostream& sql, NYdb::TParamsBuilder& params) final {
        if (Physical) {
            const auto& paramName = AddParam("Revision", params, KeyRevision);
            sql << "$Trash = select c.key as key, c.modified as modified from `history` as c inner join (" << std::endl;
            sql << "select max_by((`key`, `modified`), `modified`) as pair from `history`" << std::endl;
            sql << "where `modified` < " << paramName << " and 0L = `version` group by `key`" << std::endl;
            sql << ") as keys on keys.pair.0 = c.key where c.modified <= keys.pair.1;" << std::endl;
            sql << "delete from `history` on select * from $Trash;" << std::endl;
            sql << "delete from `commited` where `revision` < " << paramName << ';' << std::endl;
            sql << "select count(*) from $Trash;" << std::endl;
        }
    }

    void ReplyWith(const NYdb::TResultSets& results, const TActorContext& ctx) final {
        if (KeyRevision >= Revision)
            return Reply(grpc::StatusCode::INVALID_ARGUMENT, std::string("invalid revision:" ) += std::to_string(KeyRevision), ctx);

        Dump(std::cout);
        if (Physical) {
            auto parser = NYdb::TResultSetParser(results.front());
            const auto erased = parser.TryNextRow() ? NYdb::TValueParser(parser.GetValue(0)).GetUint64() : 0ULL;
            std::cout << '=' << erased << std::endl;
        } else {
            std::cout << " is executing asynchronously." << std::endl;
            SendBackgrondRequest();
        }

        etcdserverpb::CompactionResponse response;
        response.mutable_header()->set_revision(Revision);
        return Reply(response, ctx);
    }

    std::ostream& Dump(std::ostream& out) const final {
        out << "Compact(" << KeyRevision;
        if (Physical)
            out << ",physical";
        return out << ')';
    }

    i64 KeyRevision;
    bool Physical;
};

class TLeaseGrantRequest
    : public TEtcdRequestGrpc<TLeaseGrantRequest, TEvLeaseGrantRequest> {
public:
    using TBase = TEtcdRequestGrpc<TLeaseGrantRequest, TEvLeaseGrantRequest>;
    using TBase::TBase;
private:
    std::string ParseGrpcRequest() final {
        const auto &rec = *GetProtoRequest();
        TTL = rec.ttl();

        if (rec.id())
            return "requested id isn't supported";
        return {};
    }

    void MakeQueryWithParams(std::ostream& sql, NYdb::TParamsBuilder& params) final {
        Lease = Revision;
        Revision = Stuff->Revision.load();
        sql << "insert into `leases` (`id`,`ttl`,`created`,`updated`) values (" << AddParam("Lease", params, Lease) << ',' << AddParam("TimeToLive", params, TTL) << ",CurrentUtcDatetime(),CurrentUtcDatetime());" << std::endl;
    }

    void ReplyWith(const NYdb::TResultSets&, const TActorContext& ctx) final {
        etcdserverpb::LeaseGrantResponse response;
        response.mutable_header()->set_revision(Revision);
        response.set_id(Lease);
        response.set_ttl(TTL);
        Dump(std::cout) << '=' << response.id() << ',' << response.ttl() << std::endl;
        return Reply(response, ctx);
    }

    std::optional<bool> RequiredNextRevision() const final {
        return false;
    }

    std::ostream& Dump(std::ostream& out) const final {
        return out << "Grant(" << TTL << ')';
    }

    i64 Lease;
    i64 TTL;
};

class TLeaseRevokeRequest
    : public TEtcdRequestGrpc<TLeaseRevokeRequest, TEvLeaseRevokeRequest> {
public:
    using TBase = TEtcdRequestGrpc<TLeaseRevokeRequest, TEvLeaseRevokeRequest>;
    using TBase::TBase;
private:
    std::string ParseGrpcRequest() final {
        const auto &rec = *GetProtoRequest();
        Lease = rec.id();
        if (!Lease)
            return "lease id isn't set";
        return {};
    }

    void MakeQueryWithParams(std::ostream& sql, NYdb::TParamsBuilder& params) final {
        const auto& leaseParamName = AddParam("Lease", params, Lease);
        if (EmptyLease) {
            sql << "$Revoked = select `id` as `lease` from `leases` where `id`= " << leaseParamName << ';' << std::endl;
            sql << "$Keys = select `ex` from (select 0UL = count(*) as `ex`, " << leaseParamName << " as `lease` from `current` view `lease` as c" << std::endl;
            sql << "left semi join $Revoked as l using(`lease`)) as k left semi join $Revoked as l using(`lease`);" << std::endl;
            sql << "delete from `leases` where `id` = " << leaseParamName << " and $Keys;" << std::endl;
            sql << "select $Keys;" << std::endl;
        } else {
            sql << "select count(*) > 0UL from `leases` where " << leaseParamName << " = `id`;" << std::endl;
            sql << "$Victims = select `key`, `value`, `created`, `modified`, `version`, `lease` from `current` view `lease` where " << leaseParamName << " = `lease`;" << std::endl;
            sql << "insert into `history`" << std::endl;
            sql << "select `key`, `created`, $Revision as `modified`, 0L as `version`, `value`, `lease` from $Victims;" << std::endl;
            sql << "delete from `current` on select `key` from $Victims;" << std::endl;
            sql << "delete from `leases` where " << leaseParamName << " = `id`;" << std::endl;
        }
    }

    void ReplyWith(const NYdb::TResultSets& results, const TActorContext& ctx) final {
        if (EmptyLease) {
            if (auto parser = NYdb::TResultSetParser(results.front()); parser.TryNextRow()) {
                if (const auto value = NYdb::TValueParser(parser.GetValue(0)).GetOptionalBool(); !value)
                    return Reply(grpc::StatusCode::NOT_FOUND, "requested lease not found", ctx);
                else
                    EmptyLease = *value;
                if (!EmptyLease)
                    return RequestNextRevision(ctx);
            }
        } else {
            if (auto parser = NYdb::TResultSetParser(results.front()); parser.TryNextRow()) {
                if (!NYdb::TValueParser(parser.GetValue(0)).GetBool()) {
                    return Reply(grpc::StatusCode::NOT_FOUND, "requested lease not found", ctx);
                }
            }
        }

        etcdserverpb::LeaseRevokeResponse response;
        response.mutable_header()->set_revision(Revision);
        Dump(std::cout) << std::endl;
        return Reply(response, ctx);
    }

    std::optional<bool> RequiredNextRevision() const final {
        if (EmptyLease)
            return std::nullopt;
        else
            return true;
    }

    std::ostream& Dump(std::ostream& out) const final {
        return out << "Revoke(" << Lease << ')';
    }

    i64 Lease;
    bool EmptyLease = true;
};

class TLeaseTimeToLiveRequest
    : public TEtcdRequestGrpc<TLeaseTimeToLiveRequest, TEvLeaseTimeToLiveRequest> {
public:
    using TBase = TEtcdRequestGrpc<TLeaseTimeToLiveRequest, TEvLeaseTimeToLiveRequest>;
    using TBase::TBase;
private:
    std::string ParseGrpcRequest() final {
        const auto &rec = *GetProtoRequest();
        Lease = rec.id();
        Keys = rec.keys();
        if (!Lease)
            return "lease id isn't set";
        return {};
    }

    void MakeQueryWithParams(std::ostream& sql, NYdb::TParamsBuilder& params) final {
        const auto& leaseParamName = AddParam("Lease", params, Lease);

        sql << "select `ttl`, `ttl` - unwrap(cast(CurrentUtcDatetime(`id`) - `updated` as Int64) / 1000000L) as `granted` from `leases` where " << leaseParamName << " = `id`;" << std::endl;
        if (Keys) {
            sql << "select `key` from `current` where " << leaseParamName << " = `lease` and 0L < `version`;" << std::endl;
        }
    }

    void ReplyWith(const NYdb::TResultSets& results, const TActorContext& ctx) final {
        etcdserverpb::LeaseTimeToLiveResponse response;
        response.mutable_header()->set_revision(Revision);

        response.set_id(Lease);
        auto parser = NYdb::TResultSetParser(results.front());
        const bool exists = parser.TryNextRow();
        response.set_ttl(exists ? NYdb::TValueParser(parser.GetValue("ttl")).GetInt64() : -1LL);
        response.set_grantedttl(exists ? NYdb::TValueParser(parser.GetValue("granted")).GetInt64() : 0LL);

        if (Keys) {
            for (auto parser = NYdb::TResultSetParser(results[1U]); parser.TryNextRow();) {
                response.add_keys(NYdb::TValueParser(parser.GetValue(0)).GetString());
            }
        }

        Dump(std::cout) << '=' << response.ttl() << ',' << response.grantedttl() << std::endl;
        return Reply(response, ctx);
    }

    std::ostream& Dump(std::ostream& out) const final {
        return out << "TimeToLive(" << Lease << ')';
    }

    i64 Lease = 0LL;
    bool Keys = false;
};

class TLeaseLeasesRequest
    : public TEtcdRequestGrpc<TLeaseLeasesRequest, TEvLeaseLeasesRequest> {
public:
    using TBase = TEtcdRequestGrpc<TLeaseLeasesRequest, TEvLeaseLeasesRequest>;
    using TBase::TBase;
private:
    std::string ParseGrpcRequest() final {
        return {};
    }

    void MakeQueryWithParams(std::ostream& sql, NYdb::TParamsBuilder&) final {
        sql << "select `id` from `leases`;" << std::endl;
    }

    void ReplyWith(const NYdb::TResultSets& results, const TActorContext& ctx) final {
        etcdserverpb::LeaseLeasesResponse response;
        response.mutable_header()->set_revision(Revision);

        for (auto parser = NYdb::TResultSetParser(results.front()); parser.TryNextRow();) {
            response.add_leases()->set_id(NYdb::TValueParser(parser.GetValue(0)).GetInt64());
        }

        Dump(std::cout) << '=' << response.leases().size() << std::endl;
        return Reply(response, ctx);
    }

    std::ostream& Dump(std::ostream& out) const final {
        return out << "Leases()";
    }
};

}

template<typename TValueType>
std::string AddParam(const std::string_view& name, NYdb::TParamsBuilder& params, const TValueType& value, size_t* counter) {
    const auto param = GetParamName(name, counter);
    if constexpr (std::is_same<TValueType, std::string_view>::value) {
        params.AddParam(param).String(std::string(value)).Build();
    } else if constexpr (std::is_same<TValueType, std::string>::value) {
        params.AddParam(param).String(value).Build();
    } else if constexpr (std::is_same<TValueType, i64>::value) {
        if (!value)
            return "0L";
        params.AddParam(param).Int64(value).Build();
    } else if constexpr (std::is_same<TValueType, ui64>::value) {
        if (!value)
            return "0UL";
        params.AddParam(param).Uint64(value).Build();
    }
    return param;
}

template std::string AddParam<i64>(const std::string_view& name, NYdb::TParamsBuilder& params, const i64& value, size_t* counter);
template std::string AddParam<ui64>(const std::string_view& name, NYdb::TParamsBuilder& params, const ui64& value, size_t* counter);

std::string MakeSimplePredicate(const std::string_view& key, const std::string_view& rangeEnd, std::ostream& sql, NYdb::TParamsBuilder& params, size_t* paramsCounter) {
    if (key.empty())
        return {};

    const auto& keyParamName = AddParam("Key", params, key, paramsCounter);
    if (rangeEnd.empty())
        sql << keyParamName << " = `key`";
    else if (Endless == rangeEnd)
        sql << keyParamName << " <= `key`";
    else if (rangeEnd == key)
        sql << "startswith(`key`, " << keyParamName << ')';
    else
        sql << "`key` between " << keyParamName << " and " << AddParam("RangeEnd", params, rangeEnd, paramsCounter);
    return keyParamName;
}

NActors::IActor* MakeRange(std::unique_ptr<NKikimr::NGRpcService::IRequestCtx> p, TSharedStuff::TPtr stuff) {
    return new TRangeRequest(std::move(p), std::move(stuff));
}

NActors::IActor* MakePut(std::unique_ptr<NKikimr::NGRpcService::IRequestCtx> p, TSharedStuff::TPtr stuff) {
    return new TPutRequest(std::move(p), std::move(stuff));
}

NActors::IActor* MakeDeleteRange(std::unique_ptr<NKikimr::NGRpcService::IRequestCtx> p, TSharedStuff::TPtr stuff) {
    return new TDeleteRangeRequest(std::move(p), std::move(stuff));
}

NActors::IActor* MakeTxn(std::unique_ptr<NKikimr::NGRpcService::IRequestCtx> p, TSharedStuff::TPtr stuff) {
    return new TTxnRequest(std::move(p), std::move(stuff));
}

NActors::IActor* MakeCompact(std::unique_ptr<NKikimr::NGRpcService::IRequestCtx> p, TSharedStuff::TPtr stuff) {
    return new TCompactRequest(std::move(p), std::move(stuff));
}

NActors::IActor* MakeLeaseGrant(std::unique_ptr<NKikimr::NGRpcService::IRequestCtx> p, TSharedStuff::TPtr stuff) {
    return new TLeaseGrantRequest(std::move(p), std::move(stuff));
}

NActors::IActor* MakeLeaseRevoke(std::unique_ptr<NKikimr::NGRpcService::IRequestCtx> p, TSharedStuff::TPtr stuff) {
    return new TLeaseRevokeRequest(std::move(p), std::move(stuff));
}

NActors::IActor* MakeLeaseTimeToLive(std::unique_ptr<NKikimr::NGRpcService::IRequestCtx> p, TSharedStuff::TPtr stuff) {
    return new TLeaseTimeToLiveRequest(std::move(p), std::move(stuff));
}

NActors::IActor* MakeLeaseLeases(std::unique_ptr<NKikimr::NGRpcService::IRequestCtx> p, TSharedStuff::TPtr stuff) {
    return new TLeaseLeasesRequest(std::move(p), std::move(stuff));
}

} // namespace NEtcd
