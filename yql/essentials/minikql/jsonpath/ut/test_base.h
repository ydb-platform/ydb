#pragma once

#include <yql/essentials/core/issue/protos/issue_id.pb.h>
#include <yql/essentials/minikql/jsonpath/jsonpath.h>
#include <yql/essentials/minikql/dom/json.h>

#include <yql/essentials/minikql/computation/mkql_value_builder.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_mem_info.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/mkql_node.h>

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
    const TVector<TStringBuf> LaxModes_ = {"", "lax "};
    const TVector<TStringBuf> StrictModes_ = {"strict "};
    const TVector<TStringBuf> AllModes_ = {"", "lax ", "strict "};

    TIntrusivePtr<IFunctionRegistry> FunctionRegistry_;
    TScopedAlloc Alloc_;
    TTypeEnvironment Env_;
    TMemoryUsageInfo MemInfo_;
    THolderFactory HolderFactory_;
    TDefaultValueBuilder ValueBuilder_;

    const int MaxParseErrors_ = 100;

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
