#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/providers/yt/provider/yql_yt_provider.h>
#include <ydb/library/yql/providers/yt/gateway/file/yql_yt_file.h>
#include <ydb/library/yql/providers/yt/gateway/file/yql_yt_file_services.h>
#include <ydb/library/yql/core/facade/yql_facade.h>
#include <ydb/library/yql/core/qplayer/storage/memory/yql_qstorage_memory.h>
#include <ydb/library/yql/providers/common/udf_resolve/yql_simple_udf_resolver.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/user.h>
#include <util/system/tempfile.h>

using namespace NYql;

template <typename F>
void WithTables(const F&& f) {
    static const TStringBuf KSV_ATTRS =
        "{\"_yql_row_spec\" = {\"Type\" = [\"StructType\";["
        "[\"key\";[\"DataType\";\"String\"]];"
        "[\"subkey\";[\"DataType\";\"String\"]];"
        "[\"value\";[\"DataType\";\"String\"]]"
        "]]}}"
        ;

    TTempFileHandle inputFile;
    TTempFileHandle inputFileAttrs(inputFile.Name() + ".attr");

    TStringBuf data =
        "{\"key\"=\"075\";\"subkey\"=\".\";\"value\"=\"abc\"};\n"
        "{\"key\"=\"800\";\"subkey\"=\".\";\"value\"=\"ddd\"};\n"
        "{\"key\"=\"020\";\"subkey\"=\".\";\"value\"=\"q\"};\n"
        "{\"key\"=\"150\";\"subkey\"=\".\";\"value\"=\"qzz\"};\n"sv
        ;

    inputFile.Write(data.data(), data.size());
    inputFile.FlushData();
    inputFileAttrs.Write(KSV_ATTRS.data(), KSV_ATTRS.size());
    inputFileAttrs.FlushData();

    THashMap<TString, TString> tables;
    tables["yt.plato.Input"] = inputFile.Name();
    f(tables);
}

bool RunProgram(bool optimizeOnly, const TString& sourceCode, const TQContext& qContext, bool isSql, bool withUdfs,
    const THashMap<TString, TString>& tables) {
    auto functionRegistry = NKikimr::NMiniKQL::CreateFunctionRegistry(NKikimr::NMiniKQL::CreateBuiltinRegistry());
    if (withUdfs) {
        auto cloned = functionRegistry->Clone();
        NKikimr::NMiniKQL::FillStaticModules(*cloned);
        functionRegistry = cloned;
    }

    auto yqlNativeServices = NFile::TYtFileServices::Make(functionRegistry.Get(), tables, {}, "");
    auto ytGateway = CreateYtFileGateway(yqlNativeServices);

    TVector<TDataProviderInitializer> dataProvidersInit;
    dataProvidersInit.push_back(GetYtNativeDataProviderInitializer(ytGateway));
    TProgramFactory factory(true, functionRegistry.Get(), 0ULL, dataProvidersInit, "ut");
    factory.SetUdfResolver(NCommon::CreateSimpleUdfResolver(functionRegistry.Get()));

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

    TProgram::TStatus status = optimizeOnly ? 
        program->Optimize(GetUsername()) :
        program->Run(GetUsername());
    if (status == TProgram::TStatus::Error) {
        program->PrintErrorsTo(Cerr);
    }

    return status == TProgram::TStatus::Ok;
}

void CheckProgram(const TString& sql, const THashMap<TString, TString>& tables, bool isSql = true) {
    auto qStorage = MakeMemoryQStorage();
    TQContext savingCtx(qStorage->MakeWriter("foo"));
    UNIT_ASSERT(RunProgram(false, sql, savingCtx, isSql, true, tables));
    savingCtx.GetWriter()->Commit().GetValueSync();
    TQContext loadingCtx(qStorage->MakeReader("foo"));
    UNIT_ASSERT(RunProgram(true, "", loadingCtx, isSql, false, tables));
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
        
        CheckProgram(s, {}, false);
    }

    Y_UNIT_TEST(SimpleSql) {
        auto s = "select 1";
        CheckProgram(s, {});
    }

    Y_UNIT_TEST(Udf) {
        auto s = "select String::AsciiToUpper('a')";
        CheckProgram(s, {});
    }
    
    Y_UNIT_TEST(YtGetFolder) {
        auto s = "select * from plato.folder('','_yql_row_spec')";
        WithTables([&](const auto& tables) {
            CheckProgram(s, tables);
        });
    }
}
