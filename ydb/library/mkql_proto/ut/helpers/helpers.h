#pragma once

#include <ydb/library/yql/minikql/mkql_program_builder.h>

#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <ydb/library/yql/minikql/comp_nodes/mkql_factories.h>

#include <google/protobuf/text_format.h>
#include <library/cpp/testing/unittest/registar.h>

#include <functional>

namespace NKikimr::NMiniKQL {

template <typename TProtoType>
void TestExportType(std::function<TRuntimeNode(TProgramBuilder& pgmBuilder)> setup, const TString& expectedString) {
    auto functionRegistry = CreateFunctionRegistry(CreateBuiltinRegistry());
    TScopedAlloc alloc(__LOCATION__);
    TTypeEnvironment env(alloc);
    TProgramBuilder pgmBuilder(env, *functionRegistry);

    auto pgmReturn = setup(pgmBuilder);

    TProtoType res;
    ExportTypeToProto(pgmReturn.GetStaticType(), res);
    using ::google::protobuf::TextFormat;
    TString str;
    TextFormat::PrintToString(res, &str);
    UNIT_ASSERT_NO_DIFF(str, expectedString);
}

template <typename TProtoValue>
void TestExportValue(std::function<TRuntimeNode(TProgramBuilder& pgmBuilder)> setup, const TString& expectedString, const TVector<ui32>* columnOrder = nullptr) {
    auto functionRegistry = CreateFunctionRegistry(CreateBuiltinRegistry());
    auto randomProvider = CreateDeterministicRandomProvider(1);
    auto timeProvider = CreateDeterministicTimeProvider(1);
    TScopedAlloc alloc(__LOCATION__);
    TTypeEnvironment env(alloc);
    TProgramBuilder pgmBuilder(env, *functionRegistry);

    auto pgmReturn = setup(pgmBuilder);

    TExploringNodeVisitor explorer;
    explorer.Walk(pgmReturn.GetNode(), env);
    TComputationPatternOpts opts(alloc.Ref(), env, GetBuiltinFactory(),
                                 functionRegistry.Get(), NUdf::EValidateMode::None, NUdf::EValidatePolicy::Exception,
                                 "OFF", EGraphPerProcess::Multi);
    auto pattern = MakeComputationPattern(explorer, pgmReturn, {}, opts);
    auto graph = pattern->Clone(opts.ToComputationOptions(*randomProvider, *timeProvider));
    TProtoValue res;
    ExportValueToProto(pgmReturn.GetStaticType(), graph->GetValue(), res, columnOrder);
    using ::google::protobuf::TextFormat;
    TString str;
    TextFormat::PrintToString(res, &str);
    UNIT_ASSERT_NO_DIFF(str, expectedString);
}

template <typename TParam, typename TType, typename TValue>
void TestImportParams(const TString& type, const TString& value) {
    auto functionRegistry = CreateFunctionRegistry(CreateBuiltinRegistry());
    auto randomProvider = CreateDeterministicRandomProvider(1);
    auto timeProvider = CreateDeterministicTimeProvider(1);

    TParam protoParam;
    TType protoType;
    TValue protoValue;
    ::google::protobuf::TextFormat::ParseFromString(type, &protoType);
    ::google::protobuf::TextFormat::ParseFromString(value, &protoValue);
    protoParam.MutableType()->CopyFrom(protoType);
    protoParam.MutableValue()->CopyFrom(protoValue);

    TScopedAlloc alloc(__LOCATION__);
    TTypeEnvironment env(alloc);
    auto runtimeNode = ImportValueFromProto(protoParam, env);
    UNIT_ASSERT(runtimeNode.HasValue());

    TValue res;

    TExploringNodeVisitor explorer;
    explorer.Walk(runtimeNode.GetValue(), env);
    TComputationPatternOpts opts(alloc.Ref(), env, GetBuiltinFactory(),
                                 functionRegistry.Get(), NUdf::EValidateMode::None, NUdf::EValidatePolicy::Exception,
                                 "OFF", EGraphPerProcess::Multi);
    auto pattern = MakeComputationPattern(explorer, runtimeNode, {}, opts);

    auto graph = pattern->Clone(opts.ToComputationOptions(*randomProvider, *timeProvider));
    ExportValueToProto(runtimeNode.GetStaticType(), graph->GetValue(), res);
    using ::google::protobuf::TextFormat;
    TString str;
    TextFormat::PrintToString(res, &str);

    UNIT_ASSERT_NO_DIFF(str, value);
}

}
