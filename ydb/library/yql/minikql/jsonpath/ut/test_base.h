#pragma once

#include <ydb/library/yql/core/issue/protos/issue_id.pb.h>
#include <ydb/library/yql/minikql/jsonpath/jsonpath.h>
#include <ydb/library/yql/minikql/dom/json.h>

#include <ydb/library/yql/minikql/computation/mkql_value_builder.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/minikql/mkql_mem_info.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/mkql_alloc.h>
#include <ydb/library/yql/minikql/mkql_node.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/yexception.h>

using namespace NYql;
using namespace NYql::NDom;
using namespace NYql::NUdf;
using namespace NYql::NJsonPath;
using namespace NJson;
using namespace NKikimr::NMiniKQL;

class TJsonPathTestBase: public TTestBase {
public:
    TJsonPathTestBase();

protected:
    const TVector<TStringBuf> LAX_MODES = {"", "lax "};
    const TVector<TStringBuf> STRICT_MODES = {"strict "};
    const TVector<TStringBuf> ALL_MODES = {"", "lax ", "strict "};

    TIntrusivePtr<IFunctionRegistry> FunctionRegistry;
    TScopedAlloc Alloc;
    TTypeEnvironment Env;
    TMemoryUsageInfo MemInfo;
    THolderFactory HolderFactory;
    TDefaultValueBuilder ValueBuilder;

    const int MAX_PARSE_ERRORS = 100;

    TIssueCode C(TIssuesIds::EIssueCode code);

    TUnboxedValue ParseJson(TStringBuf raw);

    struct TMultiOutputTestCase {
        TString Json;
        TString JsonPath;
        TVector<TString> Result;
    };

    void RunTestCase(const TString& rawJson, const TString& rawJsonPath, const TVector<TString>& expectedResult);

    void RunParseErrorTestCase(const TString& rawJsonPath);

    struct TRuntimeErrorTestCase {
        TString Json;
        TString JsonPath;
        TIssueCode Error;
    };

    void RunRuntimeErrorTestCase(const TString& rawJson, const TString& rawJsonPath, TIssueCode error);

    struct TVariablesTestCase {
        TString Json;
        THashMap<TStringBuf, TStringBuf> Variables;
        TString JsonPath;
        TVector<TString> Result;
    };

    void RunVariablesTestCase(const TString& rawJson, const THashMap<TStringBuf, TStringBuf>& variables, const TString& rawJsonPath, const TVector<TString>& expectedResult);
};
