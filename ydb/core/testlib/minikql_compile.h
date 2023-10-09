#pragma once

#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/client/minikql_compile/db_key_resolver.h>
#include <ydb/core/client/minikql_compile/yql_expr_minikql.h>
#include <library/cpp/threading/future/future.h>
#include <util/thread/pool.h>
#include <library/cpp/testing/unittest/registar.h>

class TMockDbSchemeResolver : public NYql::IDbSchemeResolver {
public:
    TMockDbSchemeResolver()
    {
        MtpQueue.Start(2);
    }

    template <typename Func>
    NThreading::TFuture<NThreading::TFutureType<::TFunctionResult<Func>>> Async(Func&& func, IThreadPool& queue) {
        auto promise = NThreading::NewPromise<NThreading::TFutureType<::TFunctionResult<Func>>>();
        auto lambda = [promise, func = std::forward<Func>(func)]() mutable {
            NThreading::NImpl::SetValue(promise, func);
        };
        queue.SafeAddFunc(std::move(lambda));
        return promise.GetFuture();
    }

    virtual NThreading::TFuture<TTableResults> ResolveTables(const TVector<TTable>& tables) override {
        TTableResults results;
        results.reserve(tables.size());
        for (auto& table : tables) {
            TTableResult result(TTableResult::Ok);
            auto data = Tables.FindPtr(table.TableName);
            if (!data) {
                result.Status = TTableResult::Error;
                result.Reason = TStringBuilder() << "Table " << table.TableName << " not found";
            }
            else {
                result.Table = table;
                result.TableId.Reset(new NKikimr::TTableId(*data->TableId));
                result.KeyColumnCount = data->KeyColumnCount;

                for (auto& column : table.ColumnNames) {
                    auto columnInfo = data->Columns.FindPtr(column);
                    Y_ABORT_UNLESS(column);

                    auto insertResult = result.Columns.insert(std::make_pair(column, *columnInfo));
                    Y_ABORT_UNLESS(insertResult.second);
                }
            }

            results.push_back(result);
        }

        return Async([results]() {
            return results;
        }, MtpQueue);
    }

    virtual void ResolveTables(const TVector<TTable>& tables, NActors::TActorId responseTo) override {
        Y_UNUSED(tables);
        Y_UNUSED(responseTo);
        ythrow yexception() << "Not implemented";
    }

    void AddTable(const IDbSchemeResolver::TTableResult& table) {
        Y_ENSURE(!!table.TableId, "TableId must be defined");
        if (!Tables.insert({ table.Table.TableName, table }).second) {
            ythrow yexception() << "Table " << table.Table.TableName << " is already registered";
        }
    }

private:
    TThreadPool MtpQueue;
    THashMap<TString, IDbSchemeResolver::TTableResult> Tables;
};

namespace NYql {

inline TExprContainer::TPtr ParseText(const TString& programText) {
    TAstParseResult astRes = ParseAst(programText);
    astRes.Issues.PrintTo(Cerr);
    UNIT_ASSERT(astRes.IsOk());

    TExprContainer::TPtr expr(new TExprContainer());
    bool isOk = CompileExpr(*astRes.Root, expr->Root, expr->Context, nullptr, nullptr);
    expr->Context.IssueManager.GetIssues().PrintTo(Cerr);
    UNIT_ASSERT(isOk);
    return expr;
}

}

