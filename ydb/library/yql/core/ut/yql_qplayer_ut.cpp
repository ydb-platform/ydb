#include "yql_execution.h"

#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/providers/yt/provider/yql_yt_provider.h>
#include <ydb/library/yql/providers/yt/gateway/file/yql_yt_file.h>
#include <ydb/library/yql/providers/yt/gateway/file/yql_yt_file_services.h>
#include <ydb/library/yql/core/facade/yql_facade.h>
#include <ydb/library/yql/core/qplayer/storage/memory/yql_qstorage_memory.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/user.h>

using namespace NYql;

bool RunProgram(const TString& sourceCode, const TQContext& qContext, bool isSql) {
    auto functionRegistry = NKikimr::NMiniKQL::CreateFunctionRegistry(NKikimr::NMiniKQL::CreateBuiltinRegistry());
    auto yqlNativeServices = NFile::TYtFileServices::Make(functionRegistry.Get(), {}, {}, "");
    auto ytGateway = CreateYtFileGateway(yqlNativeServices);

    TVector<TDataProviderInitializer> dataProvidersInit;
    dataProvidersInit.push_back(GetYtNativeDataProviderInitializer(ytGateway));
    TProgramFactory factory(true, functionRegistry.Get(), 0ULL, dataProvidersInit, "ut");

    TProgramPtr program = factory.Create("-stdin-", sourceCode);
    program->SetQContext(qContext);
    if (isSql) {
        if (!program->ParseSql()) {
            program->PrintErrorsTo(Cerr);
            return false;
        } 
    } else if (!program->ParseYql()) {
        program->PrintErrorsTo(Cerr);
        return false;
    }

    if (!program->Compile(GetUsername())) {
        program->PrintErrorsTo(Cerr);
        return false;
    }

    TProgram::TStatus status = program->Optimize(GetUsername());
    if (status == TProgram::TStatus::Error) {
        program->PrintErrorsTo(Cerr);
    }

    return status == TProgram::TStatus::Ok;
}

void CheckProgram(const TString& sql, bool isSql = true) {
    auto qStorage = MakeMemoryQStorage();
    TQContext savingCtx(qStorage->MakeWriter("foo"));
    UNIT_ASSERT(RunProgram(sql, savingCtx, isSql));
    savingCtx.GetWriter()->Commit().GetValueSync();
    TQContext loadingCtx(qStorage->MakeReader("foo"));
    UNIT_ASSERT(RunProgram("", loadingCtx, isSql));
}

Y_UNIT_TEST_SUITE(QPlayerTests) {
    Y_UNIT_TEST(SimpleYql) {
        auto s = R"(
(
(let world (block '(
  (let output (block '(
    (let select (block '(
      (let core (AsList (AsStruct)))
      (let core (PersistableRepr (block '(
        (let projectCoreType (TypeOf core))
        (let core (OrderedSqlProject core '((SqlProjectItem projectCoreType '"" (lambda '(row) (block '(
          (let res (Int32 '"1"))
          (return res)
        ))) '('('autoName))))))
        (return core)
      ))))
      (return core)
    )))
    (return select)
  )))
  (let output (Unordered output))
  (let world (block '(
    (let result_sink (DataSink 'result))
    (let world (Write! world result_sink (Key) output '('('type) '('autoref) '('columns '('('auto))) '('unordered))))
    (return (Commit! world result_sink))
  )))
  (return world)
)))
(let world (block '(
  (let world (CommitAll! world))
  (return world)
)))
(return world)
)
        )";
        
        CheckProgram(s, false);
    }

    Y_UNIT_TEST(SimpleSql) {
        auto s = "select 1";
        CheckProgram(s);
    }
}
