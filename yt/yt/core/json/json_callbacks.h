#pragma once

#include "public.h"

#include <yt/yt/core/ytree/public.h>
#include <yt/yt/core/ytree/tree_builder.h>

#include <yt/yt/core/json/config.h>
#include <yt/yt/core/misc/utf8_decoder.h>

#include <library/cpp/yt/small_containers/compact_vector.h>

#include <queue>

namespace NYT::NJson {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EJsonCallbacksNodeType,
    (List)
    (Map)
);

class TJsonCallbacks
{
public:
    virtual void OnStringScalar(TStringBuf value) = 0;
    virtual void OnInt64Scalar(i64 value) = 0;
    virtual void OnUint64Scalar(ui64 value) = 0;
    virtual void OnDoubleScalar(double value) = 0;
    virtual void OnBooleanScalar(bool value) = 0;
    virtual void OnEntity() = 0;
    virtual void OnBeginList() = 0;
    virtual void OnEndList() = 0;
    virtual void OnBeginMap() = 0;
    virtual void OnKeyedItem(TStringBuf key) = 0;
    virtual void OnEndMap() = 0;

    virtual ~TJsonCallbacks()
    { }
};

////////////////////////////////////////////////////////////////////////////////

class TJsonCallbacksBuildingNodesImpl
    : public TJsonCallbacks
{
public:
    TJsonCallbacksBuildingNodesImpl(
        NYson::IYsonConsumer* consumer,
        NYson::EYsonType ysonType,
        const TUtf8Transcoder& utf8Transcoder,
        i64 memoryLimit,
        int nestingLevelLimit,
        NJson::EJsonAttributesMode attributesMode);

    void OnStringScalar(TStringBuf value) override;
    void OnInt64Scalar(i64 value) override;
    void OnUint64Scalar(ui64 value) override;
    void OnDoubleScalar(double value) override;
    void OnBooleanScalar(bool value) override;
    void OnEntity() override;
    void OnBeginList() override;
    void OnEndList() override;
    void OnBeginMap() override;
    void OnKeyedItem(TStringBuf key) override;
    void OnEndMap() override;

private:
    // Memory accounted approximately
    void AccountMemory(i64 memory);
    void OnItemStarted();
    void OnItemFinished();

    void ConsumeNode(NYTree::INodePtr node, int nestingLevel);
    void ConsumeNode(NYTree::IMapNodePtr map, int nestingLevel);
    void ConsumeNode(NYTree::IListNodePtr list, int nestingLevel);
    void ConsumeMapFragment(NYTree::IMapNodePtr map, int nestingLevel);

    NYson::IYsonConsumer* Consumer_;
    NYson::EYsonType YsonType_;
    TUtf8Transcoder Utf8Transcoder_;
    i64 ConsumedMemory_ = 0;
    const i64 MemoryLimit_;
    const int NestingLevelLimit_;
    const NJson::EJsonAttributesMode AttributesMode_;

    TCompactVector<EJsonCallbacksNodeType, 4> Stack_;

    const std::unique_ptr<NYTree::ITreeBuilder> TreeBuilder_;
};

////////////////////////////////////////////////////////////////////////////////

class TJsonCallbacksForwardingImpl
    : public TJsonCallbacks
{
public:
    TJsonCallbacksForwardingImpl(
        NYson::IYsonConsumer* consumer,
        NYson::EYsonType ysonType,
        const TUtf8Transcoder& utf8Transcoder);

    void OnStringScalar(TStringBuf value) override;
    void OnInt64Scalar(i64 value) override;
    void OnUint64Scalar(ui64 value) override;
    void OnDoubleScalar(double value) override;
    void OnBooleanScalar(bool value) override;
    void OnEntity() override;
    void OnBeginList() override;
    void OnEndList() override;
    void OnBeginMap() override;
    void OnKeyedItem(TStringBuf key) override;
    void OnEndMap() override;

private:
    void OnItemStarted();
    void OnItemFinished();

    NYson::IYsonConsumer* Consumer_;
    NYson::EYsonType YsonType_;
    TUtf8Transcoder Utf8Transcoder_;

    TCompactVector<EJsonCallbacksNodeType, 4> Stack_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJson
