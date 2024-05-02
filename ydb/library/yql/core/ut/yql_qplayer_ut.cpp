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

struct TRunSettings {
    bool IsSql = true;
    THashMap<TString, TString> Tables;
};

bool RunProgram(bool replay, const TString& query, const TQContext& qContext, const TRunSettings& runSettings) {
    auto functionRegistry = NKikimr::NMiniKQL::CreateFunctionRegistry(NKikimr::NMiniKQL::CreateBuiltinRegistry());
    if (!replay) {
        auto cloned = functionRegistry->Clone();
        NKikimr::NMiniKQL::FillStaticModules(*cloned);
        functionRegistry = cloned;
    }

    auto yqlNativeServices = NFile::TYtFileServices::Make(functionRegistry.Get(), runSettings.Tables, {}, "");
    auto ytGateway = CreateYtFileGateway(yqlNativeServices);

    TVector<TDataProviderInitializer> dataProvidersInit;
    dataProvidersInit.push_back(GetYtNativeDataProviderInitializer(ytGateway));
    TProgramFactory factory(true, functionRegistry.Get(), 0ULL, dataProvidersInit, "ut");
    factory.SetUdfResolver(NCommon::CreateSimpleUdfResolver(functionRegistry.Get()));

    TProgramPtr program = factory.Create("-stdin-", query);
    program->SetQContext(qContext);
    if (runSettings.IsSql) {
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

    TProgram::TStatus status = replay ? 
        program->Optimize(GetUsername()) :
        program->Run(GetUsername());
    if (status == TProgram::TStatus::Error) {
        program->PrintErrorsTo(Cerr);
    }

    return status == TProgram::TStatus::Ok;
}

void CheckProgram(const TString& query, const TRunSettings& runSettings) {
    auto qStorage = MakeMemoryQStorage();
    TQContext savingCtx(qStorage->MakeWriter("foo", {}));
    UNIT_ASSERT(RunProgram(false, query, savingCtx, runSettings));
    savingCtx.GetWriter()->Commit().GetValueSync();
    TQContext loadingCtx(qStorage->MakeReader("foo", {}));
    UNIT_ASSERT(RunProgram(true, "", loadingCtx, runSettings));
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
        
        TRunSettings runSettings;
        runSettings.IsSql = false;
        CheckProgram(s, runSettings);
    }

    Y_UNIT_TEST(SimpleSql) {
        auto s = "select 1";
        TRunSettings runSettings;
        CheckProgram(s, runSettings);
    }

    Y_UNIT_TEST(Udf) {
        auto s = "select String::AsciiToUpper('a')";
        TRunSettings runSettings;
        CheckProgram(s, runSettings);
    }
    
    Y_UNIT_TEST(YtGetFolder) {
        auto s = "select * from plato.folder('','_yql_row_spec')";
        WithTables([&](const auto& tables) {
            TRunSettings runSettings;
            runSettings.Tables = tables;
            CheckProgram(s, runSettings);
        });
    }

    Y_UNIT_TEST(YtGetTableInfo) {
        auto s = "select * from plato.Input";
        WithTables([&](const auto& tables) {
            TRunSettings runSettings;
            runSettings.Tables = tables;
            CheckProgram(s, runSettings);
        });
    }
}
