#include "etcd_impl.h"
#include "etcd_shared.h"
#include "etcd_events.h"

#include <ydb/apps/etcd_proxy/proto/rpc.grpc.pb.h>

#include <ydb/core/grpc_services/rpc_scheme_base.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/tx.h>

#include <ydb/library/actors/core/executor_thread.h>

namespace NEtcd {

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

void FillHeader(i64 revision, etcdserverpb::ResponseHeader& header) {
    header.set_revision(revision);
    header.set_cluster_id(0ULL);
    header.set_member_id(0ULL);
    header.set_raft_term(0ULL);
}

struct TOperation {
    size_t ResultIndex = 0ULL;
};

struct TRange : public TOperation {
    std::string Key, RangeEnd;
    bool KeysOnly, CountOnly;
    ui64 Limit;
    i64 KeyRevision;
    i64 MinCreateRevision, MaxCreateRevision;
    i64 MinModificateRevision, MaxModificateRevision;
    std::optional<bool> SortOrder;
    size_t SortTarget;

    static constexpr std::string_view Fields[] = {"key"sv, "version"sv, "created"sv, "modified"sv, "value"sv};

    std::ostream& Dump(std::ostream& out) const {
        out << (RangeEnd.empty() ? "Get" : "Range") << '(';
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
        if (CountOnly)
            out << ",count";
        if (KeysOnly)
            out << ",keys";
        if (Limit)
            out << ",limit=" << Limit;
        out << ')';
        return out;
    }

    bool Parse(const etcdserverpb::RangeRequest& rec) {
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
        switch (rec.sort_order()) {
            case etcdserverpb::RangeRequest_SortOrder_ASCEND: SortOrder = true; break;
            case etcdserverpb::RangeRequest_SortOrder_DESCEND: SortOrder = false; break;
            default: break;
        }
        return !Key.empty() && (RangeEnd.empty() || Key <= RangeEnd);
    }

    void MakeQueryWithParams(std::ostream& sql, NYdb::TParamsBuilder& params, size_t* paramsCounter = nullptr, size_t* resultsCounter = nullptr, const std::string_view& txnFilter = {}) {
        if (resultsCounter)
            ResultIndex = (*resultsCounter)++;
        sql << "select ";
        if (CountOnly)
            sql << "count(*)";
        else if (KeysOnly)
            sql << "`key`";
        else
            sql << "`key`,`value`,`created`,`modified`,`version`,`lease`";
        sql << std::endl << "from ";
        const bool fromHistory = KeyRevision || MinCreateRevision || MaxCreateRevision || MinModificateRevision || MaxModificateRevision;
        sql << '`' << (fromHistory ? "verhaal" : "huidig") << '`' << std::endl;
        sql << "where ";

        if (!txnFilter.empty())
            sql << txnFilter << " and ";

        MakeSimplePredicate(Key, RangeEnd, sql, params, paramsCounter);
        if (KeyRevision) {
            sql << std::endl << '\t' << "and `modified` = " << AddParam("Revision", params, KeyRevision, paramsCounter);
        }

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

        if (SortOrder) {
            sql << std::endl << "order by `" << Fields[SortTarget] << "` " << (*SortOrder ? "asc" : "desc");
        }

        if (Limit) {
            sql << std::endl << "limit " << AddParam<ui64>("Limit", params, Limit, paramsCounter);
        }

        sql << ';' << std::endl;
    }

    etcdserverpb::RangeResponse MakeResponse(i64 revision, const NYdb::TResultSets& results) const {
        etcdserverpb::RangeResponse response;
        FillHeader(revision, *response.mutable_header());

        if (!results.empty()) {
            if (CountOnly) {
                if (auto parser = NYdb::TResultSetParser(results[ResultIndex]); parser.TryNextRow()) {
                    response.set_count(NYdb::TValueParser(parser.GetValue(0)).GetUint64());
                }
            } else {
                for (auto parser = NYdb::TResultSetParser(results[ResultIndex]); parser.TryNextRow();) {
                    if (KeysOnly)
                        response.add_kvs()->set_key(NYdb::TValueParser(parser.GetValue(0)).GetString());
                    else {
                        const auto kvs = response.add_kvs();
                        kvs->set_key(NYdb::TValueParser(parser.GetValue("key")).GetString());
                        kvs->set_value(NYdb::TValueParser(parser.GetValue("value")).GetString());
                        kvs->set_mod_revision(NYdb::TValueParser(parser.GetValue("modified")).GetInt64());
                        kvs->set_create_revision(NYdb::TValueParser(parser.GetValue("created")).GetInt64());
                        kvs->set_version(NYdb::TValueParser(parser.GetValue("version")).GetInt64());
                        kvs->set_lease(NYdb::TValueParser(parser.GetValue("lease")).GetInt64());
                    }
                }
            }
        }
        return response;
    }
};

using TNotifier = std::function<void(std::string&&, NEtcd::TData&&, NEtcd::TData&&)>;

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

    bool Parse(const etcdserverpb::PutRequest& rec) {
        Key = rec.key();
        Value = rec.value();
        Lease = rec.lease();
        GetPrevious = rec.prev_kv();
        IgnoreValue = rec.ignore_value();
        IgnoreLease = rec.ignore_lease();
        return !Key.empty();
    }

    void MakeQueryWithParams(std::ostream& sql, NYdb::TParamsBuilder& params, size_t* paramsCounter = nullptr, size_t* resultsCounter = nullptr, const std::string_view& txnFilter = {}) {
        const auto& keyParamName = AddParam("Key", params, Key, paramsCounter);
        const auto& valueParamName = IgnoreValue ? std::string("NULL") : AddParam("Value", params, Value, paramsCounter);
        const auto& leaseParamName = IgnoreLease ? std::string("NULL") : AddParam("Lease", params, Lease, paramsCounter);

        const auto& oldResultSetName = GetNameWithIndex("Old", resultsCounter);
        const auto& newResultSetName = GetNameWithIndex("New", resultsCounter);

        sql << oldResultSetName << " = select `key`, `created`, `modified`, `version`, `value`, `lease` from `verhaal` where ";
        if (!txnFilter.empty())
            sql << txnFilter << " and ";
        sql << "`key` = " << keyParamName << " order by `modified` desc limit 1;" << std::endl;

        sql << newResultSetName << " = select" << std::endl;
        sql << '\t' << keyParamName << " as `key`," << std::endl;
        sql << '\t' << "if(`version` > 0L, `created`, $Revision) as `created`," << std::endl;
        sql << '\t' << "$Revision as `modified`," << std::endl;
        sql << '\t' << "`version` + 1L as `version`," << std::endl;
        sql << '\t' << "nvl(" << valueParamName << ",`value`) as `value`," << std::endl;
        sql << '\t' << "nvl(" << leaseParamName << ",`lease`) as `lease`" << std::endl;
        sql << '\t' << "from ";

        const bool update = IgnoreValue || IgnoreLease;
        if (update)
            sql << oldResultSetName;
        else
            sql << "(select * from " << oldResultSetName <<" union all select * from as_table([<|`key`:'', `created`:0L, `modified`: 0L, `version`:0L, `value`:'', `lease`:0L|>]) order by `created` desc limit 1)";
        if (!txnFilter.empty())
            sql << " where " << txnFilter;
        sql << ';' << std::endl;

        sql << "insert into `verhaal` select * from " << newResultSetName << ';' << std::endl;
        sql << (update ? "update `huidig` on" : "upsert into `huidig`") << " select * from " << newResultSetName << ';' << std::endl;

        if (GetPrevious || NotifyWatchtower) {
            if (resultsCounter)
                ResultIndex = (*resultsCounter)++;
            sql << "select `value`, `created`, `modified`, `version`, `lease` from " << oldResultSetName << " where `version` > 0L;" << std::endl;
        }
        if constexpr (NotifyWatchtower) {
            if (resultsCounter)
                ++(*resultsCounter);
            sql << "select `value`, `created`, `modified`, `version`, `lease` from " << newResultSetName << ';' << std::endl;
        }
    }

    etcdserverpb::PutResponse MakeResponse(i64 revision, const NYdb::TResultSets& results, const TNotifier& notifier) const {
        etcdserverpb::PutResponse response;
        FillHeader(revision, *response.mutable_header());

        if (GetPrevious) {
            if (auto parser = NYdb::TResultSetParser(results[ResultIndex]); parser.TryNextRow() && 5ULL == parser.ColumnsCount()) {
                const auto prev = response.mutable_prev_kv();
                prev->set_key(Key);
                prev->set_value(NYdb::TValueParser(parser.GetValue("value")).GetString());
                prev->set_mod_revision(NYdb::TValueParser(parser.GetValue("modified")).GetInt64());
                prev->set_create_revision(NYdb::TValueParser(parser.GetValue("created")).GetInt64());
                prev->set_version(NYdb::TValueParser(parser.GetValue("version")).GetInt64());
                prev->set_lease(NYdb::TValueParser(parser.GetValue("lease")).GetInt64());
            }
        }
        if (NotifyWatchtower && notifier) {
            NEtcd::TData oldData, newData;
            if (auto parser = NYdb::TResultSetParser(results[ResultIndex]); parser.TryNextRow() && 5ULL == parser.ColumnsCount()) {
                oldData.Value = NYdb::TValueParser(parser.GetValue("value")).GetString();
                oldData.Created = NYdb::TValueParser(parser.GetValue("created")).GetInt64();
                oldData.Modified = NYdb::TValueParser(parser.GetValue("modified")).GetInt64();
                oldData.Version = NYdb::TValueParser(parser.GetValue("version")).GetInt64();
                oldData.Lease = NYdb::TValueParser(parser.GetValue("lease")).GetInt64();
            }
            if (auto parser = NYdb::TResultSetParser(results[ResultIndex + 1U]); parser.TryNextRow() && 5ULL == parser.ColumnsCount()) {
                newData.Value = NYdb::TValueParser(parser.GetValue("value")).GetString();
                newData.Created = NYdb::TValueParser(parser.GetValue("created")).GetInt64();
                newData.Modified = NYdb::TValueParser(parser.GetValue("modified")).GetInt64();
                newData.Version = NYdb::TValueParser(parser.GetValue("version")).GetInt64();
                newData.Lease = NYdb::TValueParser(parser.GetValue("lease")).GetInt64();
                notifier(std::string(Key), std::move(oldData), std::move(newData));
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

    bool Parse(const etcdserverpb::DeleteRangeRequest& rec) {
        Key = rec.key();
        RangeEnd = DecrementKey(rec.range_end());
        GetPrevious = rec.prev_kv();
        return !Key.empty() && (RangeEnd.empty() || Key <= RangeEnd);
    }

    void MakeQueryWithParams(std::ostream& sql, NYdb::TParamsBuilder& params, size_t* paramsCounter = nullptr, size_t* resultsCounter = nullptr, const std::string_view& txnFilter = {}) {
        if (resultsCounter)
            ResultIndex = (*resultsCounter)++;

        std::ostringstream where;
        where << "where ";
        if (!txnFilter.empty())
            where << txnFilter << " and ";
        MakeSimplePredicate(Key, RangeEnd, where, params, paramsCounter);

        const auto& oldResultSetName = GetNameWithIndex("Old", resultsCounter);

        sql << oldResultSetName << " = select `key`, `value`, `created`, `modified`, `version`, `lease` from `huidig` " << where.str() << ';' << std::endl;
        sql << "insert into `verhaal`" << std::endl;
        sql << "select `key`, `created`, $Revision as `modified`, 0L as `version`, `value`, `lease` from " << oldResultSetName << ';' << std::endl;

        sql << "select count(*) from " << oldResultSetName << ';' << std::endl;
        if (GetPrevious || NotifyWatchtower) {
            if (resultsCounter)
                ++(*resultsCounter);
            sql << "select `key`, `value`, `created`, `modified`, `version`, `lease` from " << oldResultSetName << ';' << std::endl;
        }

        sql << "delete from `huidig` " << where.str() << ';' << std::endl;
    }

    etcdserverpb::DeleteRangeResponse MakeResponse(i64 revision, const NYdb::TResultSets& results, const TNotifier& notifier) const {
        etcdserverpb::DeleteRangeResponse response;
        FillHeader(revision, *response.mutable_header());

        if (auto parser = NYdb::TResultSetParser(results[ResultIndex]); parser.TryNextRow()) {
            response.set_deleted(NYdb::TValueParser(parser.GetValue(0)).GetUint64());
        }

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

        if (NotifyWatchtower && notifier) {
            for (auto parser = NYdb::TResultSetParser(results[ResultIndex + 1U]); parser.TryNextRow();) {
                NEtcd::TData oldData {
                    .Value = NYdb::TValueParser(parser.GetValue("value")).GetString(),
                    .Created = NYdb::TValueParser(parser.GetValue("created")).GetInt64(),
                    .Modified = NYdb::TValueParser(parser.GetValue("modified")).GetInt64(),
                    .Version = NYdb::TValueParser(parser.GetValue("version")).GetInt64(),
                    .Lease = NYdb::TValueParser(parser.GetValue("lease")).GetInt64()
                };
                auto key = NYdb::TValueParser(parser.GetValue("key")).GetString();
                notifier(std::move(key), std::move(oldData), {});
            }
        }
        return response;
    }
};

struct TCompare {
    std::string Key, RangeEnd;

    std::variant<i64, std::string> Value;

    size_t Result, Target;

    bool Parse(const etcdserverpb::Compare& rec) {
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

        return !Key.empty() && (RangeEnd.empty() || Key <= RangeEnd);
    }

    static constexpr std::string_view Fields[] = {"version"sv, "created"sv, "modified"sv, "value"sv, "lease"sv};
    static constexpr std::string_view Comparator[] = {"="sv, ">"sv, "<"sv, "!="sv};

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
    bool MakeQueryWithParams(std::ostream& sql, NYdb::TParamsBuilder& params, size_t* paramsCounter) const {
        sql << '`' << Fields[Target] << '`' << ' ' << Comparator[Result] << ' ';
        if (const auto val = std::get_if<std::string>(&Value))
            sql << AddParam("Value", params, *val, paramsCounter);
        else if (const auto val = std::get_if<i64>(&Value)) {
            sql << AddParam("Arg", params, *val, paramsCounter);
            return !*val && Target < 3U;
        }
        return false;
    }
};

struct TTxn : public TOperation {
    using TRequestOp = std::variant<TRange, TPut, TDeleteRange, TTxn>;

    std::vector<TCompare> Compares;
    std::vector<TRequestOp> Success, Failure;

    using TKeysSet = std::unordered_set<std::pair<std::string, std::string>>;

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

        for (const auto& cmp : Compares)
            cmp.Dump(out);
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

    void GetKeys(TKeysSet& keys) const {
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
                    oper->GetKeys(keys);
            }
        };
        get(Success, keys);
        get(Failure, keys);
    }

    template<class TOperation, class TSrc>
    static bool Parse(std::vector<TRequestOp>& operations, const TSrc& src) {
        TOperation op;
        if (!op.Parse(src))
            return false;
        operations.emplace_back(std::move(op));
        return true;
    }

    bool Parse(const etcdserverpb::TxnRequest& rec) {
        for (const auto& comp : rec.compare()) {
            Compares.emplace_back();
            if (!Compares.back().Parse(comp))
                return false;
        }

        const auto fill = [](std::vector<TRequestOp>& operations, const auto& fields) {
            for (const auto& op : fields) {
                switch (op.request_case()) {
                    case etcdserverpb::RequestOp::RequestCase::kRequestRange: {
                        if (!Parse<TRange>(operations, op.request_range()))
                            return false;
                        break;
                    }
                    case etcdserverpb::RequestOp::RequestCase::kRequestPut: {
                        if (!Parse<TPut>(operations, op.request_put()))
                            return false;
                        break;
                    }
                    case etcdserverpb::RequestOp::RequestCase::kRequestDeleteRange: {
                        if (!Parse<TDeleteRange>(operations, op.request_delete_range()))
                            return false;
                        break;
                    }
                    case etcdserverpb::RequestOp::RequestCase::kRequestTxn: {
                        if (!Parse<TTxn>(operations, op.request_txn()))
                            return false;
                        break;
                    }
                    default:
                        return false;
                }
            }
            return true;
        };

        return !Compares.empty() && fill(Success, rec.success()) && fill(Failure, rec.failure());
    }

    void MakeQueryWithParams(std::ostream& sql, NYdb::TParamsBuilder& params, size_t* paramsCounter = nullptr, size_t* resultsCounter = nullptr, const std::string_view& txnFilter = {}) {
        ResultIndex = (*resultsCounter)++;

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
                def = j->MakeQueryWithParams(sql, params, paramsCounter) && def;
            }
            sql << "), " << (def ? "true" : "false") << ") as `cmp` from `huidig` where ";
            MakeSimplePredicate(i->first.first, i->first.second, sql, params, paramsCounter);
        }

        if (manyRanges)
            sql << std::endl << ')';
        sql << ';' << std::endl;

        sql << "select * from " << cmpResultSetName << ';' << std::endl;


        const auto& scalarBoolOneName = GetNameWithIndex("One", resultsCounter);
        const auto& scalarBoolTwoName = GetNameWithIndex("Two", resultsCounter);

        if (txnFilter.empty()) {
            sql << scalarBoolOneName << " = select " << cmpResultSetName << ';' << std::endl;
            sql << scalarBoolTwoName << " = select not " << cmpResultSetName << ';' << std::endl;
        } else {
            sql << scalarBoolOneName << " = select " << txnFilter << " and " << cmpResultSetName << ';' << std::endl;
            sql << scalarBoolTwoName << " = select " << txnFilter << " and not " << cmpResultSetName << ';' << std::endl;
        }

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

        make(Success, paramsCounter, resultsCounter, scalarBoolOneName);
        make(Failure, paramsCounter, resultsCounter, scalarBoolTwoName);
    }

    etcdserverpb::TxnResponse MakeResponse(i64 revision, const NYdb::TResultSets& results, const TNotifier& notifier) const {
        etcdserverpb::TxnResponse response;
        FillHeader(revision, *response.mutable_header());

        if (auto parser = NYdb::TResultSetParser(results[ResultIndex]); parser.TryNextRow()) {
            const bool succeeded = NYdb::TValueParser(parser.GetValue(0)).GetBool();
            response.set_succeeded(succeeded);
            for (const auto& operation : succeeded ? Success : Failure) {
                const auto resp = response.add_responses();
                if (const auto oper = std::get_if<TRange>(&operation))
                    *resp->mutable_response_range() = oper->MakeResponse(revision, results);
                else if (const auto oper = std::get_if<TPut>(&operation))
                    *resp->mutable_response_put() = oper->MakeResponse(revision, results, notifier);
                else if (const auto oper = std::get_if<TDeleteRange>(&operation))
                    *resp->mutable_response_delete_range() = oper->MakeResponse(revision, results, notifier);
                else if (const auto oper = std::get_if<TTxn>(&operation))
                    *resp->mutable_response_txn() = oper->MakeResponse(revision, results, notifier);
            }
        }
        return response;
    }
};

using namespace NActors;

class TBaseEtcdRequest {
protected:
    virtual bool ParseGrpcRequest() = 0;
    virtual void MakeQueryWithParams(std::ostream& sql, NYdb::TParamsBuilder& params) = 0;
    virtual void ReplyWith(const NYdb::TResultSets& results, const TActorContext& ctx) = 0;

    i64 Revision = 0LL;
};

using namespace NKikimr::NGRpcService;

template <typename TDerived, typename TRequest>
class TEtcdRequestGrpc
    : public TActorBootstrapped<TEtcdRequestGrpc<TDerived, TRequest>>
    , public TBaseEtcdRequest
{
    friend class TBaseEtcdRequest;
public:
    TEtcdRequestGrpc(std::unique_ptr<IRequestCtx> request, TSharedStuff::TPtr stuff)
        : Request_(std::move(request)), Stuff(std::move(stuff))
    {}

    void Bootstrap(const TActorContext&) {
        this->ParseGrpcRequest();
        this->Become(&TEtcdRequestGrpc::StateFunc);
        SendDatabaseRequest();
    }
private:
    void SendDatabaseRequest() {
        std::ostringstream sql;
        NYdb::TParamsBuilder params;
        sql << "-- " << TRequest::TRequest::descriptor()->name() << " >>>>" << std::endl;
        this->MakeQueryWithParams(sql, params);
        sql << "-- " << TRequest::TRequest::descriptor()->name() << " <<<<" << std::endl;
        std::cout << std::endl << sql.str() << std::endl;
        const auto my = this->SelfId();
        const auto ass = NActors::TlsActivationContext->ExecutorThread.ActorSystem;
        Stuff->Client->ExecuteQuery(sql.str(), NYdb::NQuery::TTxControl::BeginTx().CommitTx(), params.Build()).Subscribe([my, ass](const auto& future) {
            if (const auto res = future.GetValueSync(); res.IsSuccess())
                ass->Send(my, new NEtcd::TEvQueryResult(res.GetResultSets()));
            else
                ass->Send(my, new NEtcd::TEvQueryError(res.GetIssues()));
        });
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NEtcd::TEvQueryResult, Handle);
            hFunc(NEtcd::TEvQueryError, Handle);
        }
    }

    void Handle(NEtcd::TEvQueryResult::TPtr &ev, const TActorContext& ctx) {
        this->ReplyWith(ev->Get()->Results, ctx);
    }

    void Handle(NEtcd::TEvQueryError::TPtr &ev) {
        std::cerr << __func__ << ' ' << ev->Get()->Issues.ToString() << std::endl;
    }
protected:
    const typename TRequest::TRequest* GetProtoRequest() const {
        return TRequest::GetProtoRequest(Request_);
    }

    void Reply(typename TRequest::TResponse& resp, const TActorContext& ctx) {
        this->Request_->Reply(&resp);
        this->Die(ctx);
    }

    const std::unique_ptr<IRequestCtx> Request_;
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
    : public TEtcdRequestGrpc<TRangeRequest, TEvRangeKVRequest> {
public:
    using TBase = TEtcdRequestGrpc<TRangeRequest, TEvRangeKVRequest>;
    using TBase::TBase;
private:
    bool ParseGrpcRequest() final {
        Revision = Stuff->Revision.load();
        return Range.Parse(*GetProtoRequest());
    }

    void MakeQueryWithParams(std::ostream& sql, NYdb::TParamsBuilder& params) final {
        Range.Dump(std::cout) << std::endl;
        return Range.MakeQueryWithParams(sql, params);
    }

    void ReplyWith(const NYdb::TResultSets& results, const TActorContext& ctx) final {
        auto response = Range.MakeResponse(Revision, results);
        return this->Reply(response, ctx);
    }

    TRange Range;
};

class TPutRequest
    : public TEtcdRequestGrpc<TPutRequest, TEvPutKVRequest> {
public:
    using TBase = TEtcdRequestGrpc<TPutRequest, TEvPutKVRequest>;
    using TBase::TBase;
private:
    bool ParseGrpcRequest() final {
        Revision = Stuff->Revision.fetch_add(1L);
        return Put.Parse(*GetProtoRequest());
    }

    void MakeQueryWithParams(std::ostream& sql, NYdb::TParamsBuilder& params) final {
        Put.Dump(std::cout) << std::endl;
        AddParam("Revision", params, Revision);
        return Put.MakeQueryWithParams(sql, params);
    }

    void ReplyWith(const NYdb::TResultSets& results, const TActorContext& ctx) final {
        const auto watcher = Stuff->Watchtower;
        const auto notifier = [&watcher, &ctx](std::string&& key, NEtcd::TData&& oldData, NEtcd::TData&& newData) {
            ctx.Send(watcher, std::make_unique<NEtcd::TEvChange>(std::move(key), std::move(oldData), std::move(newData)));
        };

        auto response = Put.MakeResponse(Revision, results, notifier);
        return this->Reply(response, ctx);
    }

    TPut Put;
};

class TDeleteRangeRequest
    : public TEtcdRequestGrpc<TDeleteRangeRequest, TEvDeleteRangeKVRequest> {
public:
    using TBase = TEtcdRequestGrpc<TDeleteRangeRequest, TEvDeleteRangeKVRequest>;
    using TBase::TBase;
private:
    bool ParseGrpcRequest() final {
        Revision = Stuff->Revision.fetch_add(1L);
        return DeleteRange.Parse(*GetProtoRequest());
    }

    void MakeQueryWithParams(std::ostream& sql, NYdb::TParamsBuilder& params) final {
        DeleteRange.Dump(std::cout) << std::endl;
        AddParam("Revision", params, Revision);
        return DeleteRange.MakeQueryWithParams(sql, params);
    }

    void ReplyWith(const NYdb::TResultSets& results, const TActorContext& ctx) final {
        const auto watcher = Stuff->Watchtower;
        const auto notifier = [&watcher, &ctx](std::string&& key, NEtcd::TData&& oldData, NEtcd::TData&& newData) {
            ctx.Send(watcher, std::make_unique<NEtcd::TEvChange>(std::move(key), std::move(oldData), std::move(newData)));
        };

        auto response = DeleteRange.MakeResponse(Revision, results, notifier);
        return this->Reply(response, ctx);
    }

    TDeleteRange DeleteRange;
};

class TTxnRequest
    : public TEtcdRequestGrpc<TTxnRequest, TEvTxnKVRequest> {
public:
    using TBase = TEtcdRequestGrpc<TTxnRequest, TEvTxnKVRequest>;
    using TBase::TBase;
private:
    bool ParseGrpcRequest() final {
        Revision = Stuff->Revision.fetch_add(1L);
        return Txn.Parse(*GetProtoRequest());
    }

    void MakeQueryWithParams(std::ostream& sql, NYdb::TParamsBuilder& params) final {
        Txn.Dump(std::cout) << std::endl;
        AddParam("Revision", params, Revision);

        TTxn::TKeysSet keys;
        Txn.GetKeys(keys);
//        std::cerr << __func__ << " keys: " << keys.size() << std::endl;
     //   const bool singleKey = keys.size() < 2U;

        size_t resultsCounter = 0U, paramsCounter = 0U;
        return Txn.MakeQueryWithParams(sql, params, &resultsCounter, &paramsCounter);
    }

    void ReplyWith(const NYdb::TResultSets& results, const TActorContext& ctx) final {
        const auto watcher = Stuff->Watchtower;
        const auto notifier = [&watcher, &ctx](std::string&& key, NEtcd::TData&& oldData, NEtcd::TData&& newData) {
            ctx.Send(watcher, std::make_unique<NEtcd::TEvChange>(std::move(key), std::move(oldData), std::move(newData)));
        };

        auto response = Txn.MakeResponse(Revision, results, notifier);
        return this->Reply(response, ctx);
    }

    TTxn Txn;
};

class TCompactRequest
    : public TEtcdRequestGrpc<TCompactRequest, TEvCompactKVRequest> {
public:
    using TBase = TEtcdRequestGrpc<TCompactRequest, TEvCompactKVRequest>;
    using TBase::TBase;
private:
    bool ParseGrpcRequest() final {
        Revision = Stuff->Revision.load();

        const auto &rec = *GetProtoRequest();
        KeyRevision = rec.revision();
        return KeyRevision > 0LL && KeyRevision < Revision;
    }

    void MakeQueryWithParams(std::ostream& sql, NYdb::TParamsBuilder& params) final {
        std::cout << "Compact(" << KeyRevision << ')' << std::endl;
        sql << "delete from `verhaal` where `modified` < " << AddParam("Revision", params, KeyRevision) << ';' << std::endl;
    }

    void ReplyWith(const NYdb::TResultSets&, const TActorContext& ctx) final {
        etcdserverpb::CompactionResponse response;
        FillHeader(Revision, *response.mutable_header());
        return this->Reply(response, ctx);
    }

    i64 KeyRevision;
};

class TLeaseGrantRequest
    : public TEtcdRequestGrpc<TLeaseGrantRequest, TEvLeaseGrantRequest> {
public:
    using TBase = TEtcdRequestGrpc<TLeaseGrantRequest, TEvLeaseGrantRequest>;
    using TBase::TBase;
private:
    bool ParseGrpcRequest() final {
        Revision = Stuff->Revision.load();
        Lease = Stuff->Lease.fetch_add(1L);

        const auto &rec = *GetProtoRequest();
        TTL = rec.ttl();
        return !rec.id();
    }

    void MakeQueryWithParams(std::ostream& sql, NYdb::TParamsBuilder& params) final {
        std::cout << "Grant(" << TTL << ")=" << Lease << std::endl;
        sql << "insert into `leases` (`id`,`ttl`,`created`,`updated`)" << std::endl;
        sql << '\t' << "values (" << AddParam("Lease", params, Lease) << ',' << AddParam("TimeToLive", params, TTL) << ",CurrentUtcDatetime(),CurrentUtcDatetime());" << std::endl;
    }

    void ReplyWith(const NYdb::TResultSets&, const TActorContext& ctx) final {
        etcdserverpb::LeaseGrantResponse response;
        FillHeader(Revision, *response.mutable_header());
        response.set_id(Lease);
        response.set_ttl(TTL);
        return this->Reply(response, ctx);
    }

    i64 Lease, TTL;
};

class TLeaseRevokeRequest
    : public TEtcdRequestGrpc<TLeaseRevokeRequest, TEvLeaseRevokeRequest> {
public:
    using TBase = TEtcdRequestGrpc<TLeaseRevokeRequest, TEvLeaseRevokeRequest>;
    using TBase::TBase;
private:
    bool ParseGrpcRequest() final {
        Revision = Stuff->Revision.fetch_add(1LL);

        const auto &rec = *GetProtoRequest();
        Lease = rec.id();
        return Lease != 0LL;
    }

    void MakeQueryWithParams(std::ostream& sql, NYdb::TParamsBuilder& params) final {
        std::cout << "Revoke(" << Lease << ')' << std::endl;

        const auto& revisionParamName = AddParam("Revision", params, Revision);
        const auto& leaseParamName = AddParam("Lease", params, Lease);

        sql << "$Victims = select `key`, `value`, `created`, `modified`, `version`, `lease` from `huidig` where " << leaseParamName << " = `lease`;" << std::endl;
        sql << "insert into `verhaal`" << std::endl;
        sql << "select `key`, `created`, " << revisionParamName << " as `modified`, 0L as `version`, `value`, `lease` from $Victims;" << std::endl;

        if constexpr (NotifyWatchtower) {
            sql << "select `key`, `value`, `created`, `modified`, `version`, `lease` from $Victims;" << std::endl;
        }

        sql << "delete from `huidig` where " << leaseParamName << " = `lease`;" << std::endl;
        sql << "delete from `leases` where " << leaseParamName << " = `id`;" << std::endl;
    }

    void ReplyWith(const NYdb::TResultSets& results, const TActorContext& ctx) final {
        if constexpr (NotifyWatchtower) {
            i64 deleted = 0ULL;
            for (auto parser = NYdb::TResultSetParser(results.front()); parser.TryNextRow(); ++deleted) {
                NEtcd::TData oldData;
                oldData.Value = NYdb::TValueParser(parser.GetValue("value")).GetString();
                oldData.Created = NYdb::TValueParser(parser.GetValue("created")).GetInt64();
                oldData.Modified = NYdb::TValueParser(parser.GetValue("modified")).GetInt64();
                oldData.Version = NYdb::TValueParser(parser.GetValue("version")).GetInt64();
                oldData.Lease = NYdb::TValueParser(parser.GetValue("lease")).GetInt64();
                auto key = NYdb::TValueParser(parser.GetValue("key")).GetString();

                ctx.Send(Stuff->Watchtower, std::make_unique<NEtcd::TEvChange>(std::move(key), std::move(oldData)));
            }

            if (!deleted) {
                auto expected = Revision + 1U;
                Stuff->Revision.compare_exchange_strong(expected, Revision);
            }
        }

        etcdserverpb::LeaseRevokeResponse response;
        FillHeader(Revision, *response.mutable_header());
        return this->Reply(response, ctx);
    }

    i64 Lease;
};

class TLeaseTimeToLiveRequest
    : public TEtcdRequestGrpc<TLeaseTimeToLiveRequest, TEvLeaseTimeToLiveRequest> {
public:
    using TBase = TEtcdRequestGrpc<TLeaseTimeToLiveRequest, TEvLeaseTimeToLiveRequest>;
    using TBase::TBase;
private:
    bool ParseGrpcRequest() final {
        Revision = Stuff->Revision.load();

        const auto &rec = *GetProtoRequest();
        Lease = rec.id();
        Keys = rec.keys();
        return Lease != 0LL;
    }

    void MakeQueryWithParams(std::ostream& sql, NYdb::TParamsBuilder& params) final {
        std::cout << "TimeToLive(" << Lease << ')' << std::endl;

        const auto& leaseParamName = AddParam("Lease", params, Lease);

        sql << "select `ttl`, `ttl` - unwrap(cast(CurrentUtcDatetime() - `updated` as Int64) / 1000000L) as `granted` from `leases` where " << leaseParamName << " = `id`;" << std::endl;
        if (Keys) {
            sql << "select `key` from `huidig` where " << leaseParamName << " = `lease`;" << std::endl;
        }
    }

    void ReplyWith(const NYdb::TResultSets& results, const TActorContext& ctx) final {
        etcdserverpb::LeaseTimeToLiveResponse response;
        FillHeader(Revision, *response.mutable_header());

        response.set_id(Lease);
        auto parser = NYdb::TResultSetParser(results.front());
        const bool exists = parser.TryNextRow();
        response.set_ttl(exists ? NYdb::TValueParser(parser.GetValue("ttl")).GetInt64() : -1LL);
        response.set_grantedttl(exists ? NYdb::TValueParser(parser.GetValue("granted")).GetInt64() : 0LL);

        if (Keys) {
            for (auto parser = NYdb::TResultSetParser(results.back()); parser.TryNextRow();) {
                response.add_keys(NYdb::TValueParser(parser.GetValue(0)).GetString());
            }
        }

        return this->Reply(response, ctx);
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
    bool ParseGrpcRequest() final {
        Revision = Stuff->Revision.load();
        return true;
    }

    void MakeQueryWithParams(std::ostream& sql, NYdb::TParamsBuilder&) final {
        std::cout << "Leases()" << std::endl;
        sql << "select `id` from `leases`;" << std::endl;
    }

    void ReplyWith(const NYdb::TResultSets& results, const TActorContext& ctx) final {
        etcdserverpb::LeaseLeasesResponse response;
        FillHeader(Revision, *response.mutable_header());

        for (auto parser = NYdb::TResultSetParser(results.back()); parser.TryNextRow();) {
            response.add_leases()->set_id(NYdb::TValueParser(parser.GetValue(0)).GetInt64());
        }

        return this->Reply(response, ctx);
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
        params.AddParam(param).Int64(value).Build();
    } else if constexpr (std::is_same<TValueType, ui64>::value) {
        params.AddParam(param).Uint64(value).Build();
    }
    return param;
}

template std::string AddParam<i64>(const std::string_view& name, NYdb::TParamsBuilder& params, const i64& value, size_t* counter);
template std::string AddParam<ui64>(const std::string_view& name, NYdb::TParamsBuilder& params, const ui64& value, size_t* counter);

void MakeSimplePredicate(const std::string_view& key, const std::string_view& rangeEnd, std::ostream& sql, NYdb::TParamsBuilder& params, size_t* paramsCounter) {
    if (rangeEnd.empty())
        sql << "`key` = " << AddParam("Key", params, key, paramsCounter);
    else if (rangeEnd == key)
        sql << "startswith(`key`, " << AddParam("Key", params, key, paramsCounter) << ')';
    else
        sql << "`key` between " << AddParam("Key", params, key, paramsCounter) << " and " << AddParam("RangeEnd", params, rangeEnd, paramsCounter);
}

NActors::IActor* MakeRange(std::unique_ptr<IRequestCtx> p, TSharedStuff::TPtr stuff) {
    return new TRangeRequest(std::move(p), std::move(stuff));
}

NActors::IActor* MakePut(std::unique_ptr<IRequestCtx> p, TSharedStuff::TPtr stuff) {
    return new TPutRequest(std::move(p), std::move(stuff));
}

NActors::IActor* MakeDeleteRange(std::unique_ptr<IRequestCtx> p, TSharedStuff::TPtr stuff) {
    return new TDeleteRangeRequest(std::move(p), std::move(stuff));
}

NActors::IActor* MakeTxn(std::unique_ptr<IRequestCtx> p, TSharedStuff::TPtr stuff) {
    return new TTxnRequest(std::move(p), std::move(stuff));
}

NActors::IActor* MakeCompact(std::unique_ptr<IRequestCtx> p, TSharedStuff::TPtr stuff) {
    return new TCompactRequest(std::move(p), std::move(stuff));
}

NActors::IActor* MakeLeaseGrant(std::unique_ptr<IRequestCtx> p, TSharedStuff::TPtr stuff) {
    return new TLeaseGrantRequest(std::move(p), std::move(stuff));
}

NActors::IActor* MakeLeaseRevoke(std::unique_ptr<IRequestCtx> p, TSharedStuff::TPtr stuff) {
    return new TLeaseRevokeRequest(std::move(p), std::move(stuff));
}

NActors::IActor* MakeLeaseTimeToLive(std::unique_ptr<IRequestCtx> p, TSharedStuff::TPtr stuff) {
    return new TLeaseTimeToLiveRequest(std::move(p), std::move(stuff));
}

NActors::IActor* MakeLeaseLeases(std::unique_ptr<IRequestCtx> p, TSharedStuff::TPtr stuff) {
    return new TLeaseLeasesRequest(std::move(p), std::move(stuff));
}

} // namespace NEtcd
