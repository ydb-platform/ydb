#include "manifest.h"
#include <ydb/public/lib/udf/manifest/manifest.h>
#include "registry_helpers.h"

#include <library/cpp/json/json_reader.h>
#include <yql/essentials/ast/yql_type_string.h>
#include <yql/essentials/ast/yql_expr.h>
#include <util/string/cast.h>
#include <util/string/strip.h>

#include <util/generic/hash_set.h>
#include <util/generic/yexception.h>
#include <util/string/builder.h>

#include <array>
#include <memory>

namespace NKikimr::NUdfStore::NWasm {

namespace {

constexpr ui32 MaxManifestTypeDepth = 32;
// Keep malformed input from making the recursive YQL parser recurse too deeply.
// This is deliberately much larger than the semantic type depth, while counting
// all syntax delimiters that can make the parser recurse: <, (, [, and -> return
// type chains.
constexpr ui32 MaxManifestLexicalNesting = 256;

using namespace NYql;

// ParseType produces quoted atoms/lists for type parameters and members.
const TAstNode& Unquote(const TAstNode& node) {
    Y_ENSURE(node.IsList() && node.GetChildrenCount() == 2
        && node.GetChild(0)->IsAtom() && node.GetChild(0)->GetContent() == "quote");
    return *node.GetChild(1);
}

struct TParsedType {
    TWasmTypeNodePtr Node;
    const TTypeAnnotationNode* Annotation;
};

void ValidateTypeLexicalNesting(TStringBuf type) {
    struct TFrame {
        char Opening = 0;
        ui32 Arrows = 0;
    };
    std::array<TFrame, MaxManifestLexicalNesting + 1> frames;
    size_t nesting = 0;
    size_t total = 0;
    bool quoted = false;
    for (size_t i = 0; i < type.size(); ++i) {
        const char c = type[i];
        if (quoted) {
            if (c == '\\' && i + 1 < type.size()) {
                ++i;
            } else if (c == '\'') {
                quoted = false;
            }
        } else if (c == '\'') {
            quoted = true;
        } else if (c == '<' || c == '(' || c == '[') {
            Y_ENSURE(total < MaxManifestLexicalNesting,
                "Type lexical nesting exceeds " << MaxManifestLexicalNesting << " levels");
            frames[++nesting].Opening = c;
            frames[nesting].Arrows = 0;
            ++total;
        } else if (c == '>' || c == ')' || c == ']') {
            // In the type grammar, the '>' in the callable arrow is not a
            // closing delimiter.
            if (c == '>' && i > 0 && type[i - 1] == '-') {
                continue;
            }
            if (nesting) {
                const char opening = frames[nesting].Opening;
                if ((c == '>' && opening == '<') ||
                    (c == ')' && opening == '(') ||
                    (c == ']' && opening == '[')) {
                    total -= 1 + frames[nesting].Arrows;
                    --nesting;
                }
            }
        } else if (c == ',') {
            total -= frames[nesting].Arrows;
            frames[nesting].Arrows = 0;
        } else if (c == '-' && i + 1 < type.size() && type[i + 1] == '>') {
            ++i;
            Y_ENSURE(total < MaxManifestLexicalNesting,
                "Type lexical nesting exceeds " << MaxManifestLexicalNesting << " levels");
            ++frames[nesting].Arrows;
            ++total;
        }
    }
}

TParsedType ConvertType(const TAstNode& ast, TExprContext& ctx, ui32 depth = 0) {
    Y_ENSURE(depth <= MaxManifestTypeDepth, "Type nesting exceeds 32 levels");
    Y_ENSURE(ast.IsList() && ast.GetChildrenCount());
    const auto kind = ast.GetChild(0)->GetContent();
    auto node = std::make_shared<TWasmTypeNode>();
    const TTypeAnnotationNode* annotation = nullptr;
    if (kind == "DataType") {
        const TString name(Unquote(*ast.GetChild(1)).GetContent());
        static const THashMap<TString, EUdfValueType> Leaves = {
            {"Bool", EUdfValueType::Boolean}, {"Int32", EUdfValueType::Int32},
            {"Uint32", EUdfValueType::Uint32}, {"Int64", EUdfValueType::Int64},
            {"Uint64", EUdfValueType::Uint64}, {"Float", EUdfValueType::Float},
            {"Double", EUdfValueType::Double}, {"String", EUdfValueType::String},
            {"Utf8", EUdfValueType::Utf8}, {"Date", EUdfValueType::Date},
            {"Datetime", EUdfValueType::Datetime}, {"Timestamp", EUdfValueType::Timestamp},
            {"Decimal", EUdfValueType::Decimal},
        };
        const auto* leaf = Leaves.FindPtr(name);
        Y_ENSURE(leaf, "Unsupported bridge type: " << name);
        node->Leaf = *leaf;
        if (*leaf == EUdfValueType::Decimal) {
            const auto precision = Unquote(*ast.GetChild(2)).GetContent();
            const auto scale = Unquote(*ast.GetChild(3)).GetContent();
            auto* decimal = ctx.MakeType<TDataExprParamsType>(EDataSlot::Decimal, precision, scale);
            Y_ENSURE(decimal->Validate(ast.GetPosition(), ctx), ctx.IssueManager.GetIssues().ToString());
            node->Precision = FromString<ui8>(precision);
            node->Scale = FromString<ui8>(scale);
            annotation = decimal;
        } else {
            annotation = ctx.MakeType<TDataExprType>(NUdf::GetDataSlot(name));
        }
    } else if (kind == "NullType") {
        annotation = ctx.MakeType<TNullExprType>();
    } else if (kind == "OptionalType" || kind == "ListType") {
        auto item = ConvertType(*ast.GetChild(1), ctx, depth + 1);
        node->Item = item.Node;
        node->Kind = kind == "OptionalType" ? TWasmTypeNode::EKind::Optional : TWasmTypeNode::EKind::List;
        annotation = kind == "OptionalType"
            ? static_cast<const TTypeAnnotationNode*>(ctx.MakeType<TOptionalExprType>(item.Annotation))
            : ctx.MakeType<TListExprType>(item.Annotation);
    } else if (kind == "DictType") {
        auto key = ConvertType(*ast.GetChild(1), ctx, depth + 1);
        auto payload = ConvertType(*ast.GetChild(2), ctx, depth + 1);
        node->Kind = TWasmTypeNode::EKind::Dict;
        node->Key = key.Node;
        node->Payload = payload.Node;
        auto* dict = ctx.MakeType<TDictExprType>(key.Annotation, payload.Annotation);
        Y_ENSURE(dict->Validate(ast.GetPosition(), ctx), ctx.IssueManager.GetIssues().ToString());
        annotation = dict;
    } else if (kind == "TupleType" || kind == "StructType" || kind == "VariantType") {
        const bool variant = kind == "VariantType";
        const auto& members = variant ? *ast.GetChild(1) : ast;
        const bool named = members.GetChild(0)->GetContent() == "StructType";
        node->Kind = variant ? TWasmTypeNode::EKind::Variant
            : named ? TWasmTypeNode::EKind::Struct : TWasmTypeNode::EKind::Tuple;
        node->NamedVariant = variant && named;
        TVector<const TTypeAnnotationNode*> elements;
        TVector<const TItemExprType*> fields;
        for (ui32 i = 1; i < members.GetChildrenCount(); ++i) {
            const auto& member = named ? Unquote(*members.GetChild(i)) : *members.GetChild(i);
            const TString name = named ? TString(Unquote(*member.GetChild(0)).GetContent()) : TString();
            auto type = ConvertType(named ? *member.GetChild(1) : member, ctx, depth + 1);
            node->Members.push_back({name, type.Node});
            if (named) {
                auto* field = ctx.MakeType<TItemExprType>(name, type.Annotation);
                Y_ENSURE(field->Validate(ast.GetPosition(), ctx), ctx.IssueManager.GetIssues().ToString());
                fields.push_back(field);
            } else {
                elements.push_back(type.Annotation);
            }
        }
        if (named) {
            auto* type = ctx.MakeType<TStructExprType>(fields);
            Y_ENSURE(type->Validate(ast.GetPosition(), ctx), ctx.IssueManager.GetIssues().ToString());
            annotation = type;
        } else {
            auto* type = ctx.MakeType<TTupleExprType>(elements);
            Y_ENSURE(type->Validate(ast.GetPosition(), ctx), ctx.IssueManager.GetIssues().ToString());
            annotation = type;
        }
        if (variant) {
            auto* type = ctx.MakeType<TVariantExprType>(annotation);
            Y_ENSURE(type->Validate(ast.GetPosition(), ctx), ctx.IssueManager.GetIssues().ToString());
            annotation = type;
        }
    } else if (kind == "ResourceType") {
        node->Kind = TWasmTypeNode::EKind::Resource;
        node->Tag = Unquote(*ast.GetChild(1)).GetContent();
        annotation = ctx.MakeType<TResourceExprType>(node->Tag);
    } else if (kind == "CallableType") {
        Y_ENSURE(Unquote(*ast.GetChild(1)).GetChildrenCount() == 0,
            "Callable optional arguments and payload are not supported");
        node->Kind = TWasmTypeNode::EKind::Callable;
        auto result = ConvertType(*Unquote(*ast.GetChild(2)).GetChild(0), ctx, depth + 1);
        node->CallableReturns = result.Node;
        TVector<TCallableExprType::TArgumentInfo> args;
        for (ui32 i = 3; i < ast.GetChildrenCount(); ++i) {
            const auto& arg = Unquote(*ast.GetChild(i));
            Y_ENSURE(arg.GetChildrenCount() == 1, "Callable argument names and flags are not supported");
            auto type = ConvertType(*arg.GetChild(0), ctx, depth + 1);
            node->Members.push_back({{}, type.Node});
            args.push_back({type.Annotation, {}, 0});
        }
        annotation = ctx.MakeType<TCallableExprType>(result.Annotation, args, 0, TStringBuf());
    } else {
        ythrow yexception() << "Unsupported bridge type: " << kind;
    }
    return {std::move(node), annotation};
}

TWasmTypeNodePtr ParseTypeNode(const NJson::TJsonValue& value, TStringBuf where) {
    try {
        Y_ENSURE(value.IsString() && !Strip(value.GetString()).empty(), "Expected a non-empty YQL type string");
        ValidateTypeLexicalNesting(value.GetString());
        TMemoryPool pool(4096);
        TIssues issues;
        auto* ast = NYql::ParseType(value.GetString(), pool, issues, {1, 1});
        Y_ENSURE(ast, issues.ToString());
        TExprContext ctx;
        return ConvertType(*ast, ctx).Node;
    } catch (const yexception& ex) {
        ythrow yexception() << where << ": " << ex.what();
    }
}

TVector<TWasmTypeNodePtr> ParseArgumentTypeNodes(const NJson::TJsonValue& node, TStringBuf where) {
    TVector<TWasmTypeNodePtr> result;
    if (!node.Has("argument_types")) {
        return result;
    }
    const auto& args = node["argument_types"];
    Y_ENSURE(args.IsArray(), where << ".argument_types must be an array");
    for (const auto& arg : args.GetArray()) {
        result.push_back(ParseTypeNode(arg, TStringBuilder() << where << ".argument_types[" << result.size() << "]"));
    }
    return result;
}

EWasmUdfBinding ParseBinding(const NJson::TJsonValue& node) {
    if (!node.Has("yql_binding")) {
        return EWasmUdfBinding::Plain;
    }
    const auto binding = node["yql_binding"].GetString();
    if (binding == "plain") {
        return EWasmUdfBinding::Plain;
    }
    if (binding == "type_config_callable") {
        return EWasmUdfBinding::TypeConfigCallable;
    }
    ythrow yexception() << "Unsupported yql_binding in wasm manifest: " << binding;
}

TWasmUdfDescriptor ParseFunctionDescriptor(
    const NJson::TJsonValue& functionNode,
    TStringBuf where)
{
    if (!functionNode.IsMap()) {
        ythrow yexception() << "Each function entry in wasm manifest must be an object";
    }
    if (!functionNode.Has("name")) {
        ythrow yexception() << "Missing function name in wasm manifest";
    }
    if (!functionNode.Has("result_type")) {
        ythrow yexception() << "Missing result_type in wasm manifest";
    }

    TWasmUdfDescriptor descriptor;
    descriptor.Name = functionNode["name"].GetString();
    descriptor.ArgTypes = ParseArgumentTypeNodes(functionNode, where);
    descriptor.ResultType = ParseTypeNode(functionNode["result_type"], TStringBuilder() << where << ".result_type");
    descriptor.Binding = ParseBinding(functionNode);
    if (functionNode.Has("export")) {
        descriptor.ExportName = functionNode["export"].GetString();
    }
    if (descriptor.Binding == EWasmUdfBinding::TypeConfigCallable) {
        ythrow yexception()
            << "type_config_callable is only supported under objects[].methods, not functions[]";
    }
    return descriptor;
}

TWasmObjectMethodDescriptor ParseObjectMethod(const NJson::TJsonValue& methodNode, TStringBuf where) {
    if (!methodNode.IsMap()) {
        ythrow yexception() << "Each objects[].methods entry must be an object";
    }
    if (!methodNode.Has("name")) {
        ythrow yexception() << "Missing method name in objects[].methods";
    }
    if (!methodNode.Has("export")) {
        ythrow yexception() << "Missing export in objects[].methods";
    }
    if (!methodNode.Has("result_type")) {
        ythrow yexception() << "Missing result_type in objects[].methods";
    }

    TWasmObjectMethodDescriptor method;
    method.Name = methodNode["name"].GetString();
    method.Export = methodNode["export"].GetString();
    method.ArgTypes = ParseArgumentTypeNodes(methodNode, where);
    method.ResultType = ParseTypeNode(methodNode["result_type"], TStringBuilder() << where << ".result_type");
    method.Binding = methodNode.Has("yql_binding")
        ? ParseBinding(methodNode)
        : EWasmUdfBinding::TypeConfigCallable;
    return method;
}

TWasmObjectDescriptor ParseObjectDescriptor(const NJson::TJsonValue& objectNode, TStringBuf where) {
    if (!objectNode.IsMap()) {
        ythrow yexception() << "Each objects[] entry must be an object";
    }
    if (!objectNode.Has("name")) {
        ythrow yexception() << "Missing objects[].name";
    }
    if (!objectNode.Has("create_export")) {
        ythrow yexception() << "Missing objects[].create_export";
    }
    if (!objectNode.Has("methods") || !objectNode["methods"].IsArray() || objectNode["methods"].GetArray().empty())
    {
        ythrow yexception() << "objects[].methods must be a non-empty array";
    }

    TWasmObjectDescriptor object;
    object.Name = objectNode["name"].GetString();
    object.CreateExport = objectNode["create_export"].GetString();
    if (objectNode.Has("destroy_export")) {
        object.DestroyExport = objectNode["destroy_export"].GetString();
    }
    for (const auto& methodNode : objectNode["methods"].GetArray()) {
        object.Methods.push_back(ParseObjectMethod(methodNode, TStringBuilder() << where << ".methods[" << object.Methods.size() << "] (" << methodNode["name"].GetString() << ")"));
    }
    return object;
}

void ExpandObjectsIntoFunctions(TWasmManifest& manifest)
{
    THashSet<TString> knownNames;
    for (const auto& function : manifest.Functions) {
        knownNames.insert(function.Name);
    }

    for (const auto& object : manifest.Objects) {
        if (!object.CreateExport.empty()) {
            // Prefer "New" for the first free slot.
            // Additional objects get New{ObjectName} so each stays YQL-visible.
            TString ctorName = "New";
            if (!knownNames.insert(ctorName).second) {
                ctorName = TString("New") + object.Name;
                if (!knownNames.insert(ctorName).second) {
                    ythrow yexception()
                        << "Cannot synthesize constructor for object '" << object.Name
                        << "': YQL names 'New' and '" << ctorName << "' are already taken";
                }
            }
            TWasmUdfDescriptor createFn;
            createFn.Name = std::move(ctorName);
            createFn.ExportName = object.CreateExport;
            createFn.Binding = EWasmUdfBinding::Plain;
            createFn.ResultType = MakeLeafTypeNode(EUdfValueType::Uint64);
            createFn.CreateExport = object.CreateExport;
            createFn.IsObjectConstructor = true;
            manifest.Functions.push_back(std::move(createFn));
        }

        for (const auto& method : object.Methods) {
            if (!knownNames.insert(method.Name).second) {
                ythrow yexception()
                    << "Duplicate YQL function name '" << method.Name
                    << "' from objects[].methods (names must be unique across functions/objects)";
            }
            TWasmUdfDescriptor descriptor;
            descriptor.Name = method.Name;
            descriptor.ArgTypes = method.ArgTypes;
            descriptor.ResultType = method.ResultType;
            descriptor.Binding = method.Binding;
            descriptor.CreateExport = object.CreateExport;
            descriptor.CallExport = method.Export;
            descriptor.DestroyExport = object.DestroyExport;
            if (descriptor.Binding == EWasmUdfBinding::Plain) {
                descriptor.ExportName = method.Export;
            }
            if (descriptor.Binding == EWasmUdfBinding::TypeConfigCallable && descriptor.CreateExport.empty())
            {
                ythrow yexception()
                    << "type_config_callable method '" << method.Name
                    << "' requires objects[].create_export";
            }
            manifest.Functions.push_back(std::move(descriptor));
        }
    }
}

} // namespace

TWasmManifest ParseManifest(TStringBuf manifestJson) {
    const auto common = NYdb::NUdfManifest::Parse(manifestJson);
    if (common.Type != NYdb::NUdfManifest::EModuleType::Module || common.Kind != NYdb::NUdfManifest::EModuleKind::Wasm) {
        ythrow yexception() << "Expected a WASM module manifest";
    }
    NJson::TJsonValue root;
    if (!NJson::ReadJsonTree(manifestJson, &root, true)) {
        ythrow yexception() << "Failed to parse wasm manifest JSON";
    }
    if (!root.IsMap()) {
        ythrow yexception() << "Wasm manifest must be a JSON object";
    }
    if (!root.Has("module_name")) {
        ythrow yexception() << "Wasm manifest is missing module_name";
    }

    TWasmManifest manifest;
    manifest.ModuleName = root["module_name"].GetString();
    manifest.ModuleExtension = root.Has("module_extension")
        ? root["module_extension"].GetString()
        : TString("wasm");
    if (root.Has("required_libraries")) {
        const auto& libraries = root["required_libraries"];
        if (!libraries.IsArray()) {
            ythrow yexception() << "required_libraries must be an array in wasm manifest";
        }
        for (const auto& libraryNode : libraries.GetArray()) {
            if (!libraryNode.IsString()) {
                ythrow yexception() << "Each required_libraries entry must be a string";
            }
            manifest.RequiredLibraries.push_back(libraryNode.GetString());
        }
    }

    if (root.Has("functions")) {
        const auto& functions = root["functions"];
        if (!functions.IsArray()) {
            ythrow yexception() << "Wasm manifest functions must be an array";
        }
        // A repeat inside functions[] used to be kept: BuildModuleStateFromManifest
        // maps the name to whichever declaration came last while pushing the name
        // onto its order twice, so one declaration was silently shadowed and the
        // name reached the YQL function sink two times.
        THashSet<TString> declaredNames;
        for (const auto& functionNode : functions.GetArray()) {
            auto descriptor = ParseFunctionDescriptor(functionNode, TStringBuilder() << "functions[" << manifest.Functions.size() << "] (" << functionNode["name"].GetString() << ")");
            if (!declaredNames.insert(descriptor.Name).second) {
                ythrow yexception()
                    << "Duplicate YQL function name '" << descriptor.Name
                    << "' in functions[] (names must be unique across functions/objects)";
            }
            manifest.Functions.push_back(std::move(descriptor));
        }
    }

    if (root.Has("objects")) {
        const auto& objects = root["objects"];
        if (!objects.IsArray()) {
            ythrow yexception() << "Wasm manifest objects must be an array";
        }
        for (const auto& objectNode : objects.GetArray()) {
            manifest.Objects.push_back(ParseObjectDescriptor(objectNode, TStringBuilder() << "objects[" << manifest.Objects.size() << "]"));
        }
        ExpandObjectsIntoFunctions(manifest);
    }

    if (manifest.Functions.empty()) {
        ythrow yexception()
            << "Wasm manifest must declare non-empty functions[] and/or objects[]";
    }
    return manifest;
}

} // namespace NKikimr::NUdfStore::NWasm
