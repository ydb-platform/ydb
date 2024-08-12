#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/providers/yt/provider/yql_yt_provider.h>
#include <ydb/library/yql/providers/yt/gateway/file/yql_yt_file.h>
#include <ydb/library/yql/providers/yt/gateway/file/yql_yt_file_services.h>
#include <ydb/library/yql/core/facade/yql_facade.h>
#include <ydb/library/yql/core/qplayer/storage/memory/yql_qstorage_memory.h>
#include <ydb/library/yql/providers/common/udf_resolve/yql_simple_udf_resolver.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/core/file_storage/file_storage.h>
#include <ydb/library/yql/core/services/mounts/yql_mounts.h>

#include <library/cpp/yson/node/node_io.h>

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
    bool IsPg = false;
    THashMap<TString, TString> Tables;
    TMaybe<TString> ParametersYson;
    THashMap<TString, TString> StaticFiles, DynamicFiles;
};

TUserDataTable MakeUserTables(const THashMap<TString, TString>& map) {
    TUserDataTable userDataTable;
    for (const auto& f : map) {
        TUserDataBlock block;
        block.Type = EUserDataType::RAW_INLINE_DATA;
        block.Data = f.second;
        userDataTable[TUserDataKey::File(GetDefaultFilePrefix() + f.first)] = block;
    }

    return userDataTable;
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

    TExprContext modulesCtx;
    IModuleResolver::TPtr moduleResolver;
    if (!GetYqlDefaultModuleResolver(modulesCtx, moduleResolver)) {
        Cerr << "Errors loading default YQL libraries:" << Endl;
        modulesCtx.IssueManager.GetIssues().PrintTo(Cerr);
        return false;
    }
    TExprContext::TFreezeGuard freezeGuard(modulesCtx);

    TProgramFactory factory(true, functionRegistry.Get(), 0ULL, dataProvidersInit, "ut");
    factory.SetUdfResolver(NCommon::CreateSimpleUdfResolver(functionRegistry.Get()));
    factory.SetModules(moduleResolver);

    if (!replay && (!runSettings.StaticFiles.empty() || !runSettings.DynamicFiles.empty())) {
        TFileStorageConfig fsConfig;
        LoadFsConfigFromResource("fs.conf", fsConfig);
        auto storage = WithAsync(CreateFileStorage(fsConfig));
        factory.SetFileStorage(storage);
    }

    if (!replay && !runSettings.StaticFiles.empty()) {
        factory.AddUserDataTable(MakeUserTables(runSettings.StaticFiles));
    }

    TProgramPtr program = factory.Create("-stdin-", query, "", EHiddenMode::Disable, qContext);
    if (!replay && runSettings.ParametersYson) {
        program->SetParametersYson(*runSettings.ParametersYson);
    }

    if (!replay && !runSettings.DynamicFiles.empty()) {
        program->AddUserDataTable(MakeUserTables(runSettings.DynamicFiles));
    }

    if (runSettings.IsSql || runSettings.IsPg) {
        NSQLTranslation::TTranslationSettings settings;
        settings.PgParser = runSettings.IsPg;
        if (!replay) {
            settings.ClusterMapping["plato"] = TString(YtProviderName);
        }

        if (!program->ParseSql(settings)) {
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

    Y_UNIT_TEST(SimplePg) {
        auto s = "select 1::text";
        TRunSettings runSettings;
        runSettings.IsPg = true;
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

    Y_UNIT_TEST(Parameters) {
        auto s = "declare $x as String; select $x";
        TRunSettings runSettings;
        runSettings.ParametersYson = NYT::NodeToYsonString(NYT::TNode()
            ("$x", NYT::TNode()("Data", "value")));
        CheckProgram(s, runSettings);
    }

    Y_UNIT_TEST(Files) {
        auto s = "select FileContent('a'), FileContent('b')";
        TRunSettings runSettings;
        runSettings.StaticFiles["a"] = "1";
        runSettings.DynamicFiles["b"] = "2";
        CheckProgram(s, runSettings);
    }

    Y_UNIT_TEST(Libraries) {
        auto s = "pragma library('a.sql'); import a symbols $f; select $f(1)";
        TRunSettings runSettings;
        runSettings.StaticFiles["a.sql"] = "$f = ($x)->($x+1); export $f";
        CheckProgram(s, runSettings);
    }

    Y_UNIT_TEST(Evaluation) {
        auto s = "$c = select count(*) from plato.Input; select EvaluateExpr($c)";
        WithTables([&](const auto& tables) {
            TRunSettings runSettings;
            runSettings.Tables = tables;
            CheckProgram(s, runSettings);
        });
    }

    Y_UNIT_TEST(WalkFolders) {
        auto s = R"(
$postHandler = ($nodes, $state, $level) -> {
    $tables = ListFilter($nodes, ($x)->($x.Type = "table"));
    return ListExtend($state, ListExtract($tables, "Path"));
};

SELECT State FROM plato.WalkFolders(``, $postHandler AS PostHandler);
    )";

        WithTables([&](const auto& tables) {
            TRunSettings runSettings;
            runSettings.Tables = tables;
            CheckProgram(s, runSettings);
        });
    }
}
