#include "yson_struct.h"

#include <yt/yt/core/yson/consumer.h>

#include <yt/yt/core/ytree/node.h>
#include <yt/yt/core/ytree/ypath_detail.h>

#include <util/generic/algorithm.h>

#include <util/system/platform.h>

namespace NYT::NYTree {

using namespace NYPath;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TYsonStructFinalClassHolder::TYsonStructFinalClassHolder(std::type_index typeIndex)
    : FinalType_(typeIndex)
{ }

#ifdef _win_

// This constructor is not actually called.
// This dummy implementation is only provided for MSVC
// as the latter fails to link the binary in debug mode unless it is implemented.
// If we just delete it, the default constructor of TYsonStructLite
// will be implicitly deleted as well and compilation will fail.
TYsonStructFinalClassHolder::TYsonStructFinalClassHolder()
    : FinalType_{typeid(void)}
{ }

#endif

////////////////////////////////////////////////////////////////////////////////

TYsonStructBase::TYsonStructBase()
{
    TYsonStructRegistry::Get()->OnBaseCtorCalled();
}

IMapNodePtr TYsonStructBase::GetLocalUnrecognized() const
{
    return LocalUnrecognized_;
}

IMapNodePtr TYsonStructBase::GetRecursiveUnrecognized() const
{
    return Meta_->GetRecursiveUnrecognized(this);
}

void TYsonStructBase::SetUnrecognizedStrategy(EUnrecognizedStrategy strategy)
{
    InstanceUnrecognizedStrategy_ = strategy;
}

THashSet<std::string> TYsonStructBase::GetRegisteredKeys() const
{
    return Meta_->GetRegisteredKeys();
}

void TYsonStructBase::Load(
    INodePtr node,
    bool postprocess,
    bool setDefaults,
    const std::function<NYPath::TYPath()>& pathGetter)
{
    Meta_->LoadStruct(this, std::move(node), postprocess, setDefaults, pathGetter);
}

void TYsonStructBase::Load(
    TYsonPullParserCursor* cursor,
    bool postprocess,
    bool setDefaults,
    const std::function<NYPath::TYPath()>& pathGetter)
{
    Meta_->LoadStruct(this, cursor, postprocess, setDefaults, pathGetter);
}

void TYsonStructBase::Load(
    INodePtr node,
    bool postprocess,
    bool setDefaults,
    const NYPath::TYPath& path)
{
    Load(std::move(node), postprocess, setDefaults, [&] {
        return path;
    });
}

void TYsonStructBase::Load(
    TYsonPullParserCursor* cursor,
    bool postprocess,
    bool setDefaults,
    const NYPath::TYPath& path)
{
    Load(cursor, postprocess, setDefaults, [&] {
        return path;
    });
}

void TYsonStructBase::Load(IInputStream* input)
{
    NYT::TStreamLoadContext context(input);
    NYT::TBinaryYsonStructSerializer::Load(context, *this);
}

void TYsonStructBase::Save(IYsonConsumer* consumer) const
{
    consumer->OnBeginMap();
    SaveAsMapFragment(consumer);
    consumer->OnEndMap();
}

void TYsonStructBase::SaveRecognizedAsMapFragment(NYson::IYsonConsumer* consumer) const
{
    for (const auto& [name, parameter] : Meta_->GetParameterSortedList()) {
        if (!parameter->CanOmitValue(this)) {
            consumer->OnKeyedItem(name);
            parameter->Save(this, consumer);
        }
    }
}

void TYsonStructBase::SaveAsMapFragment(NYson::IYsonConsumer* consumer) const
{
    if (!LocalUnrecognized_) {
        // Fast path.
        return SaveRecognizedAsMapFragment(consumer);
    }

    const auto& recognizedList = Meta_->GetParameterSortedList();
    auto recognizedIt = recognizedList.begin();

    auto unrecognizedList = LocalUnrecognized_->GetChildren();
    SortBy(unrecognizedList, [] (const auto& item) { return item.first; });
    auto unrecognizedIt = unrecognizedList.begin();

    auto saveRecognized = [&recognizedIt, this] (auto* consumer) {
        const auto& parameter = recognizedIt->second;
        if (!parameter->CanOmitValue(this)) {
            consumer->OnKeyedItem(recognizedIt->first);
            parameter->Save(this, consumer);
        }
        ++recognizedIt;
    };

    auto saveUnrecognized = [&unrecognizedIt] (auto* consumer) {
        consumer->OnKeyedItem(unrecognizedIt->first);
        Serialize(unrecognizedIt->second, consumer);
        ++unrecognizedIt;
    };

    while (recognizedIt != recognizedList.end() && unrecognizedIt != unrecognizedList.end()) {
        if (recognizedIt->first < unrecognizedIt->first) {
            saveRecognized(consumer);
        } else {
            saveUnrecognized(consumer);
        }
    }

    while (recognizedIt != recognizedList.end()) {
        saveRecognized(consumer);
    }

    while (unrecognizedIt != unrecognizedList.end()) {
        saveUnrecognized(consumer);
    }
}

void TYsonStructBase::Save(IOutputStream* output) const
{
    NYT::TStreamSaveContext context(output);
    NYT::TBinaryYsonStructSerializer::Save(context, *this);
    context.Finish();
}

void TYsonStructBase::Postprocess(const std::function<NYPath::TYPath()>& pathGetter)
{
    Meta_->PostprocessStruct(this, pathGetter);
}

void TYsonStructBase::SetDefaults()
{
    YT_VERIFY(Meta_);
    Meta_->SetDefaultsOfInitializedStruct(this);
}

void TYsonStructBase::SaveParameter(const std::string& key, IYsonConsumer* consumer) const
{
    Meta_->GetParameter(key)->Save(this, consumer);
}

void TYsonStructBase::LoadParameter(const std::string& key, const NYTree::INodePtr& node)
{
    Meta_->LoadParameter(this, key, node);
}

void TYsonStructBase::ResetParameter(const std::string& key)
{
    Meta_->GetParameter(key)->SetDefaultsInitialized(this);
}

int TYsonStructBase::GetParameterCount() const
{
    return Meta_->GetParameterMap().size();
}

std::vector<std::string> TYsonStructBase::GetAllParameterAliases(const std::string& key) const
{
    auto parameter = Meta_->GetParameter(key);
    auto result = parameter->GetAliases();
    result.push_back(parameter->GetKey());
    return result;
}

void TYsonStructBase::WriteSchema(IYsonConsumer* consumer) const
{
    return Meta_->WriteSchema(this, consumer);
}

bool TYsonStructBase::IsEqual(const TYsonStructBase& rhs) const
{
    return Meta_->CompareStructs(this, &rhs);
}

const IYsonStructMeta* TYsonStructBase::GetMeta() const
{
    return Meta_;
}

////////////////////////////////////////////////////////////////////////////////

void TYsonStruct::InitializeRefCounted()
{
    TYsonStructRegistry::Get()->OnFinalCtorCalled();
    if (!TYsonStructRegistry::InitializationInProgress()) {
        SetDefaults();
    }
}

bool TYsonStruct::IsSet(const std::string& key) const
{
    return SetFields_[Meta_->GetParameter(key)->GetFieldIndex()];
}

TCompactBitmap* TYsonStruct::GetSetFieldsBitmap()
{
    return &SetFields_;
}

////////////////////////////////////////////////////////////////////////////////

TCompactBitmap* TYsonStructLite::GetSetFieldsBitmap()
{
    return nullptr;
}

////////////////////////////////////////////////////////////////////////////////

bool TYsonStructLiteWithFieldTracking::IsSet(const std::string& key) const
{
    return SetFields_[Meta_->GetParameter(key)->GetFieldIndex()];
}

TCompactBitmap* TYsonStructLiteWithFieldTracking::GetSetFieldsBitmap()
{
    return &SetFields_;
}

TYsonStructLiteWithFieldTracking::TYsonStructLiteWithFieldTracking(const TYsonStructLiteWithFieldTracking& other)
    : TYsonStructFinalClassHolder(other.FinalType_)
    , TYsonStructLite(other)
{
    SetFields_.CopyFrom(other.SetFields_, GetParameterCount());
}

TYsonStructLiteWithFieldTracking& TYsonStructLiteWithFieldTracking::operator=(const TYsonStructLiteWithFieldTracking& other)
{
    TYsonStructLite::operator=(other);

    SetFields_.CopyFrom(other.SetFields_, GetParameterCount());
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_THREAD_LOCAL(IYsonStructMeta*, CurrentlyInitializingYsonMeta, nullptr);
YT_DEFINE_THREAD_LOCAL(i64, YsonMetaRegistryDepth, 0);

////////////////////////////////////////////////////////////////////////////////

TYsonStructRegistry* TYsonStructRegistry::Get()
{
    return LeakySingleton<TYsonStructRegistry>();
}

bool TYsonStructRegistry::InitializationInProgress()
{
    return CurrentlyInitializingYsonMeta() != nullptr;
}

void TYsonStructRegistry::OnBaseCtorCalled()
{
    if (CurrentlyInitializingYsonMeta() != nullptr) {
        ++YsonMetaRegistryDepth();
    }
}

void TYsonStructRegistry::OnFinalCtorCalled()
{
    if (CurrentlyInitializingYsonMeta() != nullptr) {
        --YsonMetaRegistryDepth();
    }
}

TYsonStructRegistry::TForbidCachedDynamicCastGuard::TForbidCachedDynamicCastGuard(TYsonStructBase* target)
    : Target_(target)
{
    Target_->CachedDynamicCastAllowed_ = false;
}

TYsonStructRegistry::TForbidCachedDynamicCastGuard::~TForbidCachedDynamicCastGuard()
{
    Target_->CachedDynamicCastAllowed_ = true;
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TYsonStructBase& value, IYsonConsumer* consumer)
{
    value.Save(consumer);
}

void Deserialize(TYsonStructBase& value, INodePtr node)
{
    value.Load(node);
}

void Deserialize(TYsonStructBase& value, TYsonPullParserCursor* cursor)
{
    value.Load(cursor);
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(TYsonStruct)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree

namespace NYT {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TBinaryYsonStructSerializer::Save(TStreamSaveContext& context, const TYsonStructBase& obj)
{
    auto str = ConvertToYsonString(obj);
    NYT::Save(context, str);
}

void TBinaryYsonStructSerializer::Load(TStreamLoadContext& context, TYsonStructBase& obj)
{
    auto str = NYT::Load<TYsonString>(context);
    auto node = ConvertTo<INodePtr>(str);
    obj.Load(node);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
