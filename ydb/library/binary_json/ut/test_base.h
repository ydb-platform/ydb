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

#include <ydb/library/binary_json/read.h>

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
    TIntrusivePtr<IFunctionRegistry> FunctionRegistry;
    TScopedAlloc Alloc;
    TTypeEnvironment Env;
    TMemoryUsageInfo MemInfo;
    THolderFactory HolderFactory;
    TDefaultValueBuilder ValueBuilder;
};
