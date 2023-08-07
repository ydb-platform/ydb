#include "yson_serializable.h"

#include <yt/yt/core/yson/consumer.h>

#include <yt/yt/core/yson/consumer.h>
#include <yt/yt/core/yson/token_writer.h>

#include <yt/yt/core/ytree/ephemeral_node_factory.h>
#include <yt/yt/core/ytree/node.h>
#include <yt/yt/core/ytree/ypath_detail.h>

#include <util/generic/algorithm.h>

namespace NYT::NYTree {

using namespace NYPath;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TYsonSerializableLite::TYsonSerializableLite()
{ }

IMapNodePtr TYsonSerializableLite::GetUnrecognized() const
{
    return Unrecognized;
}

IMapNodePtr TYsonSerializableLite::GetRecursiveUnrecognized() const
{
    return GetUnrecognizedRecursively();
}

IMapNodePtr TYsonSerializableLite::GetUnrecognizedRecursively() const
{
    // Take a copy of `Unrecognized` and add parameter->GetUnrecognizedRecursively()
    // for all parameters that are TYsonSerializable's themselves.
    auto result = Unrecognized ? ConvertTo<IMapNodePtr>(Unrecognized) : GetEphemeralNodeFactory()->CreateMap();
    for (const auto& [name, parameter] : Parameters) {
        auto unrecognized = parameter->GetUnrecognizedRecursively();
        if (unrecognized && unrecognized->AsMap()->GetChildCount() > 0) {
            result->AddChild(name, unrecognized);
        }
    }
    return result;
}

void TYsonSerializableLite::SetUnrecognizedStrategy(EUnrecognizedStrategy strategy)
{
    UnrecognizedStrategy = strategy;
    if (strategy == EUnrecognizedStrategy::KeepRecursive) {
        for (const auto& [name, parameter] : Parameters) {
            parameter->SetKeepUnrecognizedRecursively();
        }
    }
}

THashSet<TString> TYsonSerializableLite::GetRegisteredKeys() const
{
    THashSet<TString> result(Parameters.size());
    for (const auto& [name, parameter] : Parameters) {
        result.insert(name);
        for (const auto& alias : parameter->GetAliases()) {
            result.insert(alias);
        }
    }
    return result;
}

void TYsonSerializableLite::Load(
    INodePtr node,
    bool postprocess,
    bool setDefaults,
    const TYPath& path)
{
    YT_VERIFY(node);

    if (setDefaults) {
        SetDefaults();
    }

    auto mapNode = node->AsMap();
    for (const auto& [name, parameter] : Parameters) {
        TString key = name;
        auto child = mapNode->FindChild(name); // can be NULL
        for (const auto& alias : parameter->GetAliases()) {
            auto otherChild = mapNode->FindChild(alias);
            if (child && otherChild && !AreNodesEqual(child, otherChild)) {
                THROW_ERROR_EXCEPTION("Different values for aliased parameters %Qv and %Qv", key, alias)
                    << TErrorAttribute("main_value", child)
                    << TErrorAttribute("aliased_value", otherChild);
            }
            if (!child && otherChild) {
                child = otherChild;
                key = alias;
            }
        }
        auto childPath = path + "/" + key;
        parameter->Load(child, childPath);
    }

    if (UnrecognizedStrategy != EUnrecognizedStrategy::Drop) {
        auto registeredKeys = GetRegisteredKeys();
        if (!Unrecognized) {
            Unrecognized = GetEphemeralNodeFactory()->CreateMap();
        }
        for (const auto& [key, child] : mapNode->GetChildren()) {
            if (registeredKeys.find(key) == registeredKeys.end()) {
                Unrecognized->RemoveChild(key);
                YT_VERIFY(Unrecognized->AddChild(key, ConvertToNode(child)));
            }
        }
    }

    if (postprocess) {
        Postprocess(path);
    }
}

void TYsonSerializableLite::Load(
    TYsonPullParserCursor* cursor,
    bool postprocess,
    bool setDefaults,
    const TYPath& path)
{
    YT_VERIFY(cursor);

    if (setDefaults) {
        SetDefaults();
    }

    THashMap<TStringBuf, IParameter*> keyToParameter;
    THashSet<IParameter*> pendingParameters;
    for (const auto& [key, parameter] : Parameters) {
        EmplaceOrCrash(keyToParameter, key, parameter.Get());
        for (const auto& alias : parameter->GetAliases()) {
            EmplaceOrCrash(keyToParameter, alias, parameter.Get());
        }
        InsertOrCrash(pendingParameters, parameter.Get());
    }

    THashMap<TString, TString> aliasedData;

    auto processPossibleAlias = [&] (
        IParameter* parameter,
        TStringBuf key,
        TYsonPullParserCursor* cursor)
    {
        TStringStream ss;
        {
            TCheckedInDebugYsonTokenWriter writer(&ss);
            cursor->TransferComplexValue(&writer);
        }
        auto data = std::move(ss.Str());
        const auto& canonicalKey = parameter->GetKey();
        auto aliasedDataIt = aliasedData.find(canonicalKey);
        if (aliasedDataIt != aliasedData.end()) {
            auto firstNode = ConvertTo<INodePtr>(TYsonStringBuf(aliasedDataIt->second));
            auto secondNode = ConvertTo<INodePtr>(TYsonStringBuf(data));
            if (!AreNodesEqual(firstNode, secondNode)) {
                THROW_ERROR_EXCEPTION("Different values for aliased parameters %Qv and %Qv", canonicalKey, key)
                    << TErrorAttribute("main_value", firstNode)
                    << TErrorAttribute("aliased_value", secondNode);
            }
            return;
        }
        {
            TStringInput input(data);
            TYsonPullParser parser(&input, NYson::EYsonType::Node);
            TYsonPullParserCursor newCursor(&parser);
            auto childPath = path + "/" + key;
            parameter->Load(&newCursor, childPath);
        }
        EmplaceOrCrash(aliasedData, canonicalKey, std::move(data));
    };

    auto processUnrecognized = [&, this] (const TString& key, TYsonPullParserCursor* cursor) {
        if (UnrecognizedStrategy == EUnrecognizedStrategy::Drop) {
            cursor->SkipComplexValue();
            return;
        }
        if (!Unrecognized) {
            Unrecognized = GetEphemeralNodeFactory()->CreateMap();
        }
        Unrecognized->RemoveChild(key);
        auto added = Unrecognized->AddChild(key, ExtractTo<INodePtr>(cursor));
        YT_VERIFY(added);
    };

    cursor->ParseMap([&] (TYsonPullParserCursor* cursor) {
        auto key = ExtractTo<TString>(cursor);
        auto it = keyToParameter.find(key);
        if (it == keyToParameter.end()) {
            processUnrecognized(key, cursor);
            return;
        }

        auto parameter = it->second;
        if (parameter->GetAliases().empty()) {
            auto childPath = path + "/" + key;
            parameter->Load(cursor, childPath);
        } else {
            processPossibleAlias(parameter, key, cursor);
        }
        // NB: Key may be missing in case of aliasing.
        pendingParameters.erase(parameter);
    });

    for (auto parameter : pendingParameters) {
        auto childPath = path + "/" + parameter->GetKey();
        parameter->Load(/* cursor */ nullptr, childPath);
    }

    if (postprocess) {
        Postprocess(path);
    }
}

void TYsonSerializableLite::Save(IYsonConsumer* consumer) const
{
    consumer->OnBeginMap();

    for (const auto& [name, parameter] : SortHashMapByKeys(Parameters)) {
        if (!parameter->CanOmitValue()) {
            consumer->OnKeyedItem(name);
            parameter->Save(consumer);
        }
    }

    if (Unrecognized) {
        auto children = Unrecognized->GetChildren();
        SortByFirst(children);
        for (const auto& [key, child] : children) {
            consumer->OnKeyedItem(key);
            Serialize(child, consumer);
        }
    }

    consumer->OnEndMap();
}

void TYsonSerializableLite::Postprocess(const TYPath& path) const
{
    for (const auto& [name, parameter] : Parameters) {
        parameter->Postprocess(path + "/" + name);
    }

    try {
        for (const auto& postprocessor : Postprocessors) {
            postprocessor();
        }
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Postprocess failed at %v",
            path.empty() ? "root" : path)
            << ex;
    }
}

void TYsonSerializableLite::SetDefaults()
{
    for (const auto& [name, parameter] : Parameters) {
        parameter->SetDefaults();
    }
    for (const auto& initializer : Preprocessors) {
        initializer();
    }
}

void TYsonSerializableLite::RegisterPreprocessor(const TPreprocessor& func)
{
    func();
    Preprocessors.push_back(func);
}

void TYsonSerializableLite::RegisterPostprocessor(const TPostprocessor& func)
{
    Postprocessors.push_back(func);
}

void TYsonSerializableLite::SaveParameter(const TString& key, IYsonConsumer* consumer) const
{
    GetParameter(key)->Save(consumer);
}

void TYsonSerializableLite::LoadParameter(const TString& key, const NYTree::INodePtr& node, EMergeStrategy mergeStrategy) const
{
    const auto& parameter = GetParameter(key);
    auto validate = [&] () {
        parameter->Postprocess("/" + key);
        try {
            for (const auto& postprocessor : Postprocessors) {
                postprocessor();
            }
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION(
                "Postprocess failed while loading parameter %Qv from value %Qv",
                key,
                ConvertToYsonString(node, EYsonFormat::Text))
                << ex;
        }
    };
    parameter->SafeLoad(node, /* path */ "", validate, mergeStrategy);
}

void TYsonSerializableLite::ResetParameter(const TString& key) const
{
    GetParameter(key)->SetDefaults();
}

TYsonSerializableLite::IParameterPtr TYsonSerializableLite::GetParameter(const TString& keyOrAlias) const
{
    auto it = Parameters.find(keyOrAlias);
    if (it != Parameters.end()) {
        return it->second;
    }

    for (const auto& [_, parameter] : Parameters) {
        if (Count(parameter->GetAliases(), keyOrAlias) > 0) {
            return parameter;
        }
    }
    THROW_ERROR_EXCEPTION("Key or alias %Qv not found in yson serializable", keyOrAlias);
}

int TYsonSerializableLite::GetParameterCount() const
{
    return Parameters.size();
}

std::vector<TString> TYsonSerializableLite::GetAllParameterAliases(const TString& key) const
{
    auto parameter = GetParameter(key);
    auto result = parameter->GetAliases();
    result.push_back(parameter->GetKey());
    return result;
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TYsonSerializableLite& value, IYsonConsumer* consumer)
{
    value.Save(consumer);
}

void Deserialize(TYsonSerializableLite& value, INodePtr node)
{
    value.Load(node);
}

void Deserialize(TYsonSerializableLite& value, NYson::TYsonPullParserCursor* cursor)
{
    value.Load(cursor);
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(TYsonSerializable)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree


namespace NYT {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TBinaryYsonSerializer::Save(TStreamSaveContext& context, const TYsonSerializableLite& obj)
{
    auto str = ConvertToYsonString(obj);
    NYT::Save(context, str);
}

void TBinaryYsonSerializer::Load(TStreamLoadContext& context, TYsonSerializableLite& obj)
{
    auto str = NYT::Load<TYsonString>(context);
    auto node = ConvertTo<INodePtr>(str);
    obj.Load(node);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
