#include "yql_execution.h"

#include <ydb/library/yql/core/ut_common/yql_ut_common.h>

#include <ydb/library/yql/ast/yql_ast_annotation.h>
#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/core/type_ann/type_ann_core.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/core/yql_opt_proposed_by_data.h>
#include <ydb/library/yql/core/yql_opt_rewrite_io.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/schema/parser/yql_type_parser.h>
#include <ydb/library/yql/providers/result/provider/yql_result_provider.h>

#include <ydb/library/yql/core/facade/yql_facade.h>
#include <ydb/library/yql/providers/yt/provider/yql_yt_provider.h>
#include <ydb/library/yql/providers/yt/gateway/file/yql_yt_file.h>
#include <ydb/library/yql/providers/yt/gateway/file/yql_yt_file_services.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/user.h>
#include <util/system/tempfile.h>
#include <util/system/defaults.h>
#include <util/system/fstat.h>
#include <util/folder/path.h>
#include <util/folder/tempdir.h>
#include <util/string/cast.h>
#include <util/string/builder.h>
#include <util/system/sanitizers.h>

namespace NYql {

    static TString BuildFileNameForTmpTable(TStringBuf table, TStringBuf tmpDir) {
        return TStringBuilder() << tmpDir << LOCSLASH_C << table.substr(4) << ".tmp";
    }

    struct TRunSingleProgram {
        TString Src;
        TString TmpDir;
        TString Parameters;
        IOutputStream& Err;
        TVector<TString> Res;
        THashMap<TString, TString> Tables;

        TRunSingleProgram(const TString& src, IOutputStream& err)
            : Src(src)
            , Err(err)
        {
        }

        bool Run(
            const NKikimr::NMiniKQL::IFunctionRegistry* funcReg
        ) {
            auto yqlNativeServices = NFile::TYtFileServices::Make(funcReg, Tables, {}, TmpDir);
            auto ytGateway = CreateYtFileGateway(yqlNativeServices);

            TVector<TDataProviderInitializer> dataProvidersInit;
            dataProvidersInit.push_back(GetYtNativeDataProviderInitializer(ytGateway));
            TProgramFactory factory(true, funcReg, 0ULL, dataProvidersInit, "ut");

            TProgramPtr program = factory.Create("-stdin-", Src);
            program->ConfigureYsonResultFormat(NYson::EYsonFormat::Text);
            if (!Parameters.empty()) {
                program->SetParametersYson(Parameters);
            }

            if (!program->ParseYql() || !program->Compile(GetUsername())) {
                program->PrintErrorsTo(Err);
                return false;
            }

            TProgram::TStatus status = program->Run(GetUsername());
            if (status == TProgram::TStatus::Error) {
                program->PrintErrorsTo(Err);
            }
            Res = program->Results();
            return status == TProgram::TStatus::Ok;
        }

        void AddResults(TVector<TString>& res) const {
            res.insert(res.end(), Res.begin(), Res.end());
        }

        bool Finished() const {
            return true;
        }
    };

    struct TRunMultiplePrograms: public TRunSingleProgram {
        TVector<TString> Srcs;
        size_t Curr;

        TRunMultiplePrograms(const TVector<TString>& srcs, IOutputStream& err)
            : TRunSingleProgram(TString(), err)
            , Srcs(srcs)
            , Curr(0)
        {
        }

        bool Run(
            const NKikimr::NMiniKQL::IFunctionRegistry* funcReg
        ) {
            TString origTmpDir = TmpDir;
            if (TmpDir) {
                TFsPath newTmp = TFsPath(TmpDir) / ToString(Curr);
                newTmp.MkDirs();
                TmpDir = newTmp.GetPath();
            }
            Src = Srcs[Curr];
            if (!TRunSingleProgram::Run(funcReg)) {
                return false;
            }
            ui32 idx = 0;
            for (auto& resStr: Res) {
                NYT::TNode res;
                if (!NCommon::ParseYson(res, resStr, Err)) {
                    return false;
                }
                if (!res.IsMap() || !res.HasKey("Write") || !res["Write"].IsList()) {
                    Err << "Invalid result: " << resStr << Endl;
                    return false;
                }
                for (auto& elem: res["Write"].AsList()) {
                    if (!elem.IsMap()) {
                        Err << "Invalid result element in result: " << resStr << Endl;
                        return false;
                    }
                    if (elem.HasKey("Ref")) {
                        if (!elem["Ref"].IsList()) {
                            Err << "Invalid reference in result: " << resStr << Endl;
                            return false;
                        }
                        for (auto& refElem: elem["Ref"].AsList()) {
                            if (!refElem.IsMap() || !refElem.HasKey("Reference")) {
                                Err << "Invalid reference in result: " << resStr << Endl;
                                return false;
                            }
                            if (!refElem["Remove"].AsBool()) {
                                continue;
                            }
                            const auto& ref = refElem["Reference"].AsList();
                            TStringStream name;
                            name << ref[0].AsString() << "." << ref[1].AsString() << ".Result" << Curr << "_" << idx;
                            Tables[name.Str()] = BuildFileNameForTmpTable(ref[2].AsString(), TmpDir);
                            ++idx;
                        }
                    }
                }
            }
            ++Curr;
            origTmpDir.swap(TmpDir);
            return true;
        }

        bool Finished() const {
            return Curr == Srcs.size();
        }
    };

    template <typename TDriver>
    TVector<TString> Run(TDriver& driver) {
        auto functionRegistry = NKikimr::NMiniKQL::CreateFunctionRegistry(NKikimr::NMiniKQL::CreateBuiltinRegistry());

        TVector<TString> res;
        do {
            const bool runRes = driver.Run(functionRegistry.Get());
            UNIT_ASSERT(runRes);

            driver.AddResults(res);
        } while (!driver.Finished());
        return res;
    }

    TVector<TString> RunProgram(const TString& programSrc, const THashMap<TString, TString>& tables, const TString& tmpDir = TString(), const TString& params = TString()) {
        TRunSingleProgram driver(programSrc, Cerr);
        driver.Tables = tables;
        driver.TmpDir = tmpDir;
        driver.Parameters = params;
        return Run(driver);
    }

    static const TStringBuf KSV_ATTRS =
        "{\"_yql_row_spec\" = {\"Type\" = [\"StructType\";["
        "[\"key\";[\"DataType\";\"String\"]];"
        "[\"subkey\";[\"DataType\";\"String\"]];"
        "[\"value\";[\"DataType\";\"String\"]]"
        "]]}}"
        ;

    Y_UNIT_TEST_SUITE(ExecutionYqlExpr) {
        Y_UNIT_TEST(WriteToResultUsingIsolatedGraph) {
            auto s = "(\n"
                "(let res_sink (DataSink 'result))\n"
                "(let data (AsList (String 'x)))\n"
                "(let world (Write! world res_sink (Key) data '()))\n"
                "(let world (Commit! world res_sink))\n"
                "(return world)\n"
                ")\n";

            auto res = RunProgram(s, THashMap<TString, TString>());
            UNIT_ASSERT_VALUES_EQUAL(res.size(), 1);
            UNIT_ASSERT_NO_DIFF("{\"Write\"=[{\"Data\"=[\"x\"]}]}", res[0]);
        }

        Y_UNIT_TEST(WriteToResultTableOutput) {
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

            auto s = "(\n"
                "(let mr_source (DataSource 'yt 'plato))\n"
                "(let x (Read! world mr_source (Key '('table (String 'Input))) '('key 'subkey 'value) '()))\n"
                "(let world (Left! x))\n"
                "(let table1 (Right! x))\n"
                "(let res_sink (DataSink 'result))\n"
                "(let data (AsList (String 'x)))\n"
                "(let world (Write! world res_sink (Key) table1 '()))\n"
                "(let world (Commit! world res_sink))\n"
                "(return world)\n"
                ")\n";

            auto res = RunProgram(s, tables);
            UNIT_ASSERT_VALUES_EQUAL(res.size(), 1);
            UNIT_ASSERT_NO_DIFF(
                "{\"Write\"=[{\"Data\"=["
                "[\"075\";\".\";\"abc\"];"
                "[\"800\";\".\";\"ddd\"];"
                "[\"020\";\".\";\"q\"];"
                "[\"150\";\".\";\"qzz\"]"
                "]}]}",
                res[0]
            );
        }

        Y_UNIT_TEST(WriteToResultTransformedTable) {
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

            auto s = "(\n"
                "(let mr_source (DataSource 'yt 'plato))\n"
                "(let x (Read! world mr_source (Key '('table (String 'Input))) '('key 'subkey 'value) '()))\n"
                "(let world (Left! x))\n"
                "(let table1 (Right! x))\n"
                "(let table1low (FlatMap table1 (lambda '(item) (block '(\n"
                "  (let intValueOpt (FromString (Member item 'key) 'Int32))\n"
                "  (let ret (FlatMap intValueOpt (lambda '(item2) (block '(\n"
                "    (return (ListIf (< item2 (Int32 '100)) item))\n"
                "  )))))"
                "  (return ret)"
                ")))))"
                "(let res_sink (DataSink 'result))\n"
                "(let data (AsList (String 'x)))\n"
                "(let world (Write! world res_sink (Key) table1low '()))\n"
                "(let world (Commit! world res_sink))\n"
                "(return world)\n"
                ")\n";

            auto res = RunProgram(s, tables);
            UNIT_ASSERT_VALUES_EQUAL(res.size(), 1);
            UNIT_ASSERT_NO_DIFF(
                "{\"Write\"=[{\"Data\"=["
                "[\"075\";\".\";\"abc\"];"
                "[\"020\";\".\";\"q\"]"
                "]}]}",
                res[0]
            );
        }

        Y_UNIT_TEST(DropTable) {
            TTempFileHandle outputFile;
            TTempFileHandle outputFileAttrs(outputFile.Name() + ".attr");

            TStringBuf data =
                "{\"key\"=\"075\";\"subkey\"=\".\";\"value\"=\"abc\"};\n"sv
                ;

            outputFile.Write(data.data(), data.size());
            outputFile.FlushData();
            outputFile.Close();
            outputFileAttrs.Write(KSV_ATTRS.data(), KSV_ATTRS.size());
            outputFileAttrs.FlushData();
            outputFileAttrs.Close();
            UNIT_ASSERT(TFileStat(outputFile.Name()).IsFile());

            THashMap<TString, TString> tables;
            tables["yt.plato.Output"] = outputFile.Name();

            auto s = "(\n"
                "(let mr_sink (DataSink 'yt 'plato))\n"
                "(let world (Write! world mr_sink (Key '('table (String 'Output))) (Void) '('('mode 'drop))))\n"
                "(let world (Commit! world mr_sink))\n"
                "(return world)\n"
                ")\n";

            RunProgram(s, tables);

            UNIT_ASSERT(!TFileStat(outputFile.Name()).IsFile());
        }

        Y_UNIT_TEST(WriteToResultTableByRef) {
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

            auto s = "(\n"
                "(let mr_source (DataSource 'yt 'plato))\n"
                "(let x (Read! world mr_source (Key '('table (String 'Input))) '('key 'subkey 'value) '()))\n"
                "(let world (Left! x))\n"
                "(let table1 (Right! x))\n"
                "(let res_sink (DataSink 'result))\n"
                "(let mr_sink (DataSink 'yt 'plato))\n"
                "(let world (Write! world res_sink (Key) table1 '('('ref))))\n"
                "(let world (Commit! world mr_sink))\n"
                "(let world (Commit! world res_sink))\n"
                "(return world)\n"
                ")\n";

            auto res = RunProgram(s, tables);
            UNIT_ASSERT_VALUES_EQUAL(res.size(), 1);
            UNIT_ASSERT_NO_DIFF(
                "{\"Write\"=[{\"Ref\"=["
                "{\"Reference\"=[\"yt\";\"plato\";\"Input\"];\"Columns\"=[\"key\";\"subkey\";\"value\"];\"Remove\"=%false}"
                "]}]}",
                res[0]
                );
        }

        Y_UNIT_TEST(WriteToResultTransformedTableByRef) {
            TTempFileHandle inputFile;
            TTempFileHandle inputFileAttrs(inputFile.Name() + ".attr");
            TTempDir tmpDir;

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

            TVector<TString> progs;
            progs.push_back(
                "(\n"
                "(let mr_source (DataSource 'yt 'plato))\n"
                "(let x (Read! world mr_source (Key '('table (String 'Input))) '('key 'subkey 'value) '()))\n"
                "(let world (Left! x))\n"
                "(let table1 (Right! x))\n"
                "(let table1low (FlatMap table1 (lambda '(item) (block '(\n"
                "  (let intValueOpt (FromString (Member item 'key) 'Int32))\n"
                "  (let ret (FlatMap intValueOpt (lambda '(item2) (block '(\n"
                "    (return (ListIf (< item2 (Int32 '100)) item))\n"
                "  )))))"
                "  (return ret)"
                ")))))"
                "(let res_sink (DataSink 'result))\n"
                "(let mr_sink (DataSink 'yt 'plato))\n"
                "(let world (Write! world res_sink (Key) table1low '('('ref))))\n"
                "(let world (Commit! world mr_sink))\n"
                "(let world (Commit! world res_sink))\n"
                "(return world)\n"
                ")\n"
            );

            progs.push_back(
                "(\n"
                "(let mr_source (DataSource 'yt 'plato))\n"
                "(let x (Read! world mr_source (Key '('table (String 'Result0_0))) '('key 'subkey 'value) '()))\n"
                "(let world (Left! x))\n"
                "(let table1 (Right! x))\n"
                "(let res_sink (DataSink 'result))\n"
                "(let data (AsList (String 'x)))\n"
                "(let world (Write! world res_sink (Key) table1 '()))\n"
                "(let world (Commit! world res_sink))\n"
                "(return world)\n"
                ")\n"
            );

            TRunMultiplePrograms driver(progs, Cerr);
            driver.Tables["yt.plato.Input"] = inputFile.Name();
            driver.TmpDir = tmpDir.Name();

            auto res = Run(driver);

            UNIT_ASSERT_VALUES_EQUAL(res.size(), 2);
            UNIT_ASSERT_NO_DIFF(
                "{\"Write\"=[{\"Ref\"=["
                "{\"Reference\"=[\"yt\";\"plato\";\"tmp/bb686f68-2245bd5f-2318fa4e-1\"];\"Columns\"=[\"key\";\"subkey\";\"value\"];\"Remove\"=%true}"
                "]}]}",
                res[0]
            );
            UNIT_ASSERT_NO_DIFF(
                "{\"Write\"=[{\"Data\"=["
                "[\"075\";\".\";\"abc\"];"
                "[\"020\";\".\";\"q\"]"
                "]}]}",
                res[1]
            );
        }

        Y_UNIT_TEST(WriteAndTakeResult) {
            TTempFileHandle inputFile;
            TTempFileHandle inputFileAttrs(inputFile.Name() + ".attr");
            TTempDir tmpDir;

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

            TVector<TString> progs;
            progs.push_back(
                "(\n"
                "(let mr_source (DataSource 'yt 'plato))\n"
                "(let x (Read! world mr_source (Key '('table (String 'Input))) '('key 'subkey 'value) '()))\n"
                "(let world (Left! x))\n"
                "(let table (Right! x))\n"
                "(let result (Map table (lambda '(item) (block '("
                "  (let res (Struct))"
                "  (let res (AddMember res 'k (Member item 'key)))"
                "  (let res (AddMember res 's (Member item 'subkey)))"
                "  (let res (AddMember res 'v (Member item 'value)))"
                "  (return res)"
                ")))))"
                "(let res_sink (DataSink 'result))\n"
                "(let mr_sink (DataSink 'yt 'plato))\n"
                "(let world (Write! world res_sink (Key) result '('('ref))))\n"
                "(let world (Commit! world mr_sink))\n"
                "(let world (Commit! world res_sink))\n"
                "(return world)\n"
                ")\n"
            );

            progs.push_back(
                "(\n"
                "(let mr_source (DataSource 'yt 'plato))\n"
                "(let x (Read! world mr_source (Key '('table (String 'Result0_0))) '('k 's 'v) '()))\n"
                "(let world (Left! x))\n"
                "(let table (Right! x))\n"
                "(let result (Take table (Uint64 '2)))"
                "(let res_sink (DataSink 'result))\n"
                "(let mr_sink (DataSink 'yt 'plato))\n"
                "(let world (Write! world res_sink (Key) result '('('type))))\n"
                "(let world (Commit! world mr_sink))\n"
                "(let world (Commit! world res_sink))\n"
                "(return world)\n"
                ")\n"
            );

            TRunMultiplePrograms driver(progs, Cerr);
            driver.Tables["yt.plato.Input"] = inputFile.Name();
            driver.TmpDir = tmpDir.Name();

            auto res = Run(driver);

            UNIT_ASSERT_VALUES_EQUAL(res.size(), 2);

            //~ Cerr << res[0] << Endl;
            //~ Cerr << res[1] << Endl;
            UNIT_ASSERT_NO_DIFF(
                "{\"Write\"=[{\"Ref\"=["
                "{\"Reference\"=[\"yt\";\"plato\";\"tmp/bb686f68-2245bd5f-2318fa4e-1\"];\"Columns\"=[\"k\";\"s\";\"v\"];\"Remove\"=%true}"
                "]}]}",
                res[0]
            );
            UNIT_ASSERT_NO_DIFF(
                "{\"Write\"=[{"
                "\"Type\"=[\"ListType\";[\"StructType\";["
                "[\"k\";[\"DataType\";\"String\"]];[\"s\";[\"DataType\";\"String\"]];[\"v\";[\"DataType\";\"String\"]]"
                "]]];"
                "\"Data\"=["
                "[\"075\";\".\";\"abc\"];"
                "[\"800\";\".\";\"ddd\"]"
                "]}"
                "]}",
                res[1]
            );
        }

        Y_UNIT_TEST(WriteAndReadScheme) {
            TTempFileHandle inputFile;
            TTempFileHandle inputFileAttrs(inputFile.Name() + ".attr");
            TTempDir tmpDir;

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

            TVector<TString> progs;
            progs.push_back(
                "(\n"
                "(let mr_source (DataSource 'yt 'plato))\n"
                "(let x (Read! world mr_source (Key '('table (String 'Input))) '('key 'subkey 'value) '()))\n"
                "(let world (Left! x))\n"
                "(let table (Right! x))\n"
                "(let result0 (Map table (lambda '(item) (block '("
                "  (return (AsStruct '('bar (Coalesce (FromString (Member item 'key) 'Uint64) (Uint64 '0)))))"
                ")))))"
                "(let result1 (Map result0 (lambda '(item) (block '("
                "  (return (AddMember (Struct) 'foo item))"
                ")))))"
                "(let res_sink (DataSink 'result))\n"
                "(let mr_sink (DataSink 'yt 'plato))\n"
                "(let world (Write! world res_sink (Key) result0 '('('ref))))\n"
                "(let world (Write! world res_sink (Key) result1 '('('ref))))\n"
                "(let world (Commit! world mr_sink))\n"
                "(let world (Commit! world res_sink))\n"
                "(return world)\n"
                ")\n"
            );

            progs.push_back(
                "(\n"
                "(let mr_source (DataSource 'yt 'plato))\n"
                "(let x (Read! world mr_source (Key '('tablescheme (String 'Result0_0))) (Void) '()))\n"
                "(let world (Left! x))\n"
                "(let scheme (Right! x))\n"
                "(let res_sink (DataSink 'result))\n"
                "(let mr_sink (DataSink 'yt 'plato))\n"
                "(let world (Write! world res_sink (Key) scheme '('('type))))\n"
                "(let world (Commit! world mr_sink))\n"
                "(let world (Commit! world res_sink))\n"
                "(return world)\n"
                ")\n"
            );

            progs.push_back(
                "(\n"
                "(let mr_source (DataSource 'yt 'plato))\n"
                "(let x (Read! world mr_source (Key '('tablescheme (String 'Result0_1))) (Void) '()))\n"
                "(let world (Left! x))\n"
                "(let scheme (Right! x))\n"
                "(let res_sink (DataSink 'result))\n"
                "(let mr_sink (DataSink 'yt 'plato))\n"
                "(let world (Write! world res_sink (Key) scheme '('('type))))\n"
                "(let world (Commit! world mr_sink))\n"
                "(let world (Commit! world res_sink))\n"
                "(return world)\n"
                ")\n"
            );

            TRunMultiplePrograms driver(progs, Cerr);
            driver.Tables["yt.plato.Input"] = inputFile.Name();
            driver.TmpDir = tmpDir.Name();

            auto res = Run(driver);

            UNIT_ASSERT_VALUES_EQUAL(res.size(), 3);

            //~ Cerr << res[0] << Endl;
            //~ Cerr << res[1] << Endl;
            //~ Cerr << res[2] << Endl;
            UNIT_ASSERT_NO_DIFF(
                "{\"Write\"=["
                "{\"Ref\"=[{\"Reference\"=[\"yt\";\"plato\";\"tmp/bb686f68-2245bd5f-2318fa4e-1\"];\"Columns\"=[\"bar\"];\"Remove\"=%true}]};"
                "{\"Ref\"=[{\"Reference\"=[\"yt\";\"plato\";\"tmp/7ae6459a-7382d1e7-7935c08e-2\"];\"Columns\"=[\"foo\"];\"Remove\"=%true}]}"
                "]}",
                res[0]
            );
            UNIT_ASSERT(res[1].find("\"Fields\"=[{\"Name\"=\"bar\"") != TString::npos);
            UNIT_ASSERT(res[2].find("\"Fields\"=[{\"Name\"=\"foo\"") != TString::npos);
        }

        Y_UNIT_TEST(ExtendSortedWithNonSortedAndRead) {
            TTempFileHandle inputFile;
            TTempFileHandle inputFileAttrs(inputFile.Name() + ".attr");
            TTempFileHandle outputFile;
            TTempFile outputFileAttr(outputFile.Name() + ".attr");
            TTempDir tmpDir;

            TStringBuf data =
                "{\"key\"=\"foo\";\"subkey\"=\"wat\";\"value\"=\"222\"};\n"
                "{\"key\"=\"bar\";\"subkey\"=\"wat\";\"value\"=\"111\"};\n"
                "{\"key\"=\"jar\";\"subkey\"=\"wat\";\"value\"=\"333\"};\n"sv
            ;

            inputFile.Write(data.data(), data.size());
            inputFile.FlushData();
            inputFileAttrs.Write(KSV_ATTRS.data(), KSV_ATTRS.size());
            inputFileAttrs.FlushData();

            TVector<TString> progs;
            progs.push_back(
                "(\n"
                "(let source (DataSource 'yt 'plato))\n"
                "(let x (Read! world source (Key '('table (String 'Input))) '('key 'value) '()))\n"
                "(let world (Left! x))\n"
                "(let table (Right! x))\n"
                "(let sorted (Sort table (Bool 'true) (lambda '(item) (Member item 'value))))\n"
                "(let result (Extend table sorted))\n"
                "(let sink (DataSink 'yt 'plato))\n"
                "(let world (Write! world sink (Key '('table (String 'Output))) result '()))\n"
                "(let world (Commit! world sink))\n"
                "(return world)\n"
                ")\n"
            );

            progs.push_back(
                "(\n"
                "(let mr_source (DataSource 'yt 'plato))\n"
                "(let x (Read! world mr_source (Key '('table (String 'Output))) '('key 'value) '()))\n"
                "(let world (Left! x))\n"
                "(let result (Right! x))\n"
                "(let res_sink (DataSink 'result))\n"
                "(let mr_sink (DataSink 'yt 'plato))\n"
                "(let world (Write! world res_sink (Key) result '('('type))))\n"
                "(let world (Commit! world mr_sink))\n"
                "(let world (Commit! world res_sink))\n"
                "(return world)\n"
                ")\n"
            );

            TRunMultiplePrograms driver(progs, Cerr);
            driver.TmpDir = tmpDir.Name();
            driver.Tables["yt.plato.Input"] = inputFile.Name();
            driver.Tables["yt.plato.Output"] = outputFile.Name();

            auto res = Run(driver);

            UNIT_ASSERT_VALUES_EQUAL(res.size(), 1);

            //~ Cerr << res[0] << Endl;
            UNIT_ASSERT_NO_DIFF(
                "{\"Write\"=[{"
                "\"Type\"=[\"ListType\";[\"StructType\";[[\"key\";[\"DataType\";\"String\"]];[\"value\";[\"DataType\";\"String\"]]]]];"
                "\"Data\"=["
                "[\"foo\";\"222\"];[\"bar\";\"111\"];[\"jar\";\"333\"];[\"foo\";\"222\"];[\"bar\";\"111\"];[\"jar\";\"333\"]"
                "]}]}",
                res[0]
            );
        }

        Y_UNIT_TEST(OrderedExtendSortedWithNonSortedAndRead) {
            TTempFileHandle inputFile;
            TTempFileHandle inputFileAttrs(inputFile.Name() + ".attr");
            TTempFileHandle outputFile;
            TTempFile outputFileAttr(outputFile.Name() + ".attr");
            TTempDir tmpDir;

            TStringBuf data =
                "{\"key\"=\"foo\";\"subkey\"=\"wat\";\"value\"=\"222\"};\n"
                "{\"key\"=\"bar\";\"subkey\"=\"wat\";\"value\"=\"111\"};\n"
                "{\"key\"=\"jar\";\"subkey\"=\"wat\";\"value\"=\"333\"};\n"sv
            ;

            inputFile.Write(data.data(), data.size());
            inputFile.FlushData();
            inputFileAttrs.Write(KSV_ATTRS.data(), KSV_ATTRS.size());
            inputFileAttrs.FlushData();

            TVector<TString> progs;
            progs.push_back(
                "(\n"
                "(let source (DataSource 'yt 'plato))\n"
                "(let x (Read! world source (Key '('table (String 'Input))) '('key 'value) '()))\n"
                "(let world (Left! x))\n"
                "(let table (Right! x))\n"
                "(let sorted (Sort table (Bool 'true) (lambda '(item) (Member item 'value))))\n"
                "(let result (OrderedExtend table sorted))\n"
                "(let sink (DataSink 'yt 'plato))\n"
                "(let world (Write! world sink (Key '('table (String 'Output))) result '()))\n"
                "(let world (Commit! world sink))\n"
                "(return world)\n"
                ")\n"
            );

            progs.push_back(
                "(\n"
                "(let mr_source (DataSource 'yt 'plato))\n"
                "(let x (Read! world mr_source (Key '('table (String 'Output))) '('key 'value) '()))\n"
                "(let world (Left! x))\n"
                "(let result (Right! x))\n"
                "(let res_sink (DataSink 'result))\n"
                "(let mr_sink (DataSink 'yt 'plato))\n"
                "(let world (Write! world res_sink (Key) result '('('type))))\n"
                "(let world (Commit! world mr_sink))\n"
                "(let world (Commit! world res_sink))\n"
                "(return world)\n"
                ")\n"
            );

            TRunMultiplePrograms driver(progs, Cerr);
            driver.TmpDir = tmpDir.Name();
            driver.Tables["yt.plato.Input"] = inputFile.Name();
            driver.Tables["yt.plato.Output"] = outputFile.Name();

            auto res = Run(driver);

            UNIT_ASSERT_VALUES_EQUAL(res.size(), 1);

            //~ Cerr << res[0] << Endl;
            UNIT_ASSERT_NO_DIFF(
                "{\"Write\"=[{"
                "\"Type\"=[\"ListType\";[\"StructType\";[[\"key\";[\"DataType\";\"String\"]];[\"value\";[\"DataType\";\"String\"]]]]];"
                "\"Data\"=["
                "[\"foo\";\"222\"];[\"bar\";\"111\"];[\"jar\";\"333\"];[\"bar\";\"111\"];[\"foo\";\"222\"];[\"jar\";\"333\"]"
                "]}]}",
                res[0]
            );
        }

        Y_UNIT_TEST(TestParametersEvaluation) {
            auto s = "(\n"
                "(let res_sink (DataSink 'result))\n"
                "(let data (Parameter '\"$foo\" (ParseType '\"Tuple<String, Int32 ? , List<Uint32>, Dict<Int32, Bool>, Struct<a : Void, b : Double>, Variant<Int32, Bool>>\")))\n"
                "(let world (Write! world res_sink (Key) data '('('type))))\n"
                "(let world (Commit! world res_sink))\n"
                "(return world)\n"
                ")\n";

            auto params = R"__(
{"$foo"={Data=[
    bar;
    "33";
    ["1";"2";"3"];
    [["7";%true];["12";%false]];
    [#;"-1.7"];
    ["0";"8"];
]}}
            )__";

            auto res = RunProgram(s, THashMap<TString, TString>(), "", params);
            UNIT_ASSERT_VALUES_EQUAL(res.size(), 1);
            UNIT_ASSERT_NO_DIFF(R"__({"Write"=[{"Type"=["TupleType";[["DataType";"String"];["OptionalType";["DataType";"Int32"]];["ListType";["DataType";"Uint32"]];["DictType";["DataType";"Int32"];["DataType";"Bool"]];["StructType";[["a";["VoidType"]];["b";["DataType";"Double"]]]];["VariantType";["TupleType";[["DataType";"Int32"];["DataType";"Bool"]]]]]];"Data"=["bar";["33"];["1";"2";"3"];[["7";%true];["12";%false]];["Void";"-1.7"];["0";"8"]]}]})__", res[0]);
        }
    }

} // namespace NYql
