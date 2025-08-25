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

#include <yql/essentials/types/binary_json/read.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/yexception.h>

using namespace NYql;
using namespace NYql::NDom;
using namespace NYql::NUdf;
using namespace NYql::NJsonPath;
using namespace NJson;
using namespace NKikimr::NMiniKQL;
using namespace NKikimr::NBinaryJson;

class TBinaryJsonTestBase: public TTestBase {
public:
    TBinaryJsonTestBase();

    TString EntryToJsonText(const TEntryCursor& cursor);

    TString ContainerToJsonText(const TContainerCursor& cursor);

protected:
    TIntrusivePtr<IFunctionRegistry> FunctionRegistry_;
    TScopedAlloc Alloc_;
    TTypeEnvironment Env_;
    TMemoryUsageInfo MemInfo_;
    THolderFactory HolderFactory_;
    TDefaultValueBuilder ValueBuilder_;
};
