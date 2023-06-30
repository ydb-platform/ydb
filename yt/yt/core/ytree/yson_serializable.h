#pragma once

#include "public.h"
#include "node.h"
#include "yson_serialize_common.h"

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/mpl.h>
#include <yt/yt/core/misc/property.h>

#include <yt/yt/core/yson/public.h>

#include <functional>
#include <optional>

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

class TYsonSerializableLite
    : private TNonCopyable
{
public:
    typedef std::function<void()> TPostprocessor;
    typedef std::function<void()> TPreprocessor;

    struct IParameter
        : public TRefCounted
    {
        virtual void Load(
            NYTree::INodePtr node,
            const NYPath::TYPath& path,
            std::optional<EMergeStrategy> mergeStrategy = std::nullopt) = 0;

        virtual void SafeLoad(
            NYTree::INodePtr node,
            const NYPath::TYPath& path,
            const std::function<void()>& validate,
            std::optional<EMergeStrategy> mergeStrategy = std::nullopt) = 0;

        virtual void Load(
            NYson::TYsonPullParserCursor* cursor,
            const NYPath::TYPath& path,
            std::optional<EMergeStrategy> mergeStrategy = std::nullopt) = 0;

        virtual void SafeLoad(
            NYson::TYsonPullParserCursor* cursor,
            const NYPath::TYPath& path,
            const std::function<void()>& validate,
            std::optional<EMergeStrategy> mergeStrategy = std::nullopt) = 0;

        virtual void Postprocess(const NYPath::TYPath& path) const = 0;
        virtual void SetDefaults() = 0;
        virtual void Save(NYson::IYsonConsumer* consumer) const = 0;
        virtual bool CanOmitValue() const = 0;
        virtual const TString& GetKey() const = 0;
        virtual const std::vector<TString>& GetAliases() const = 0;
        virtual IMapNodePtr GetUnrecognizedRecursively() const = 0;
        virtual void SetKeepUnrecognizedRecursively() = 0;
    };

    typedef TIntrusivePtr<IParameter> IParameterPtr;

    template <class T>
    class TParameter
        : public IParameter
    {
    public:
        typedef std::function<void(const T&)> TPostprocessor;
        typedef typename TOptionalTraits<T>::TValue TValueType;

        TParameter(TString key, T& parameter);

        void Load(
            NYTree::INodePtr node,
            const NYPath::TYPath& path,
            std::optional<EMergeStrategy> mergeStrategy = std::nullopt) override;

        void SafeLoad(
            NYTree::INodePtr node,
            const NYPath::TYPath& path,
            const std::function<void()>& validate,
            std::optional<EMergeStrategy> mergeStrategy = std::nullopt) override;

        void Load(
            NYson::TYsonPullParserCursor* cursor,
            const NYPath::TYPath& path,
            std::optional<EMergeStrategy> mergeStrategy = std::nullopt) override;

        void SafeLoad(
            NYson::TYsonPullParserCursor* cursor,
            const NYPath::TYPath& path,
            const std::function<void()>& validate,
            std::optional<EMergeStrategy> mergeStrategy = std::nullopt) override;

        void Postprocess(const NYPath::TYPath& path) const override;
        void SetDefaults() override;
        void Save(NYson::IYsonConsumer* consumer) const override;
        bool CanOmitValue() const override;
        const TString& GetKey() const override;
        const std::vector<TString>& GetAliases() const override;
        IMapNodePtr GetUnrecognizedRecursively() const override;
        void SetKeepUnrecognizedRecursively() override;

    public:
        TParameter& Optional();
        TParameter& Default(const T& defaultValue = T());
        TParameter& DontSerializeDefault();
        TParameter& CheckThat(TPostprocessor validator);
        TParameter& GreaterThan(TValueType value);
        TParameter& GreaterThanOrEqual(TValueType value);
        TParameter& LessThan(TValueType value);
        TParameter& LessThanOrEqual(TValueType value);
        TParameter& InRange(TValueType lowerBound, TValueType upperBound);
        TParameter& NonEmpty();
        TParameter& Alias(const TString& name);
        TParameter& MergeBy(EMergeStrategy strategy);

        template <class... TArgs>
        TParameter& DefaultNew(TArgs&&... args);

    private:
        TString Key;
        T& Parameter;
        std::optional<T> DefaultValue;
        bool SerializeDefault = true;
        std::vector<TPostprocessor> Postprocessors;
        std::vector<TString> Aliases;
        EMergeStrategy MergeStrategy;
        bool KeepUnrecognizedRecursively = false;
    };

public:
    TYsonSerializableLite();

    void Load(
        NYTree::INodePtr node,
        bool postprocess = true,
        bool setDefaults = true,
        const NYPath::TYPath& path = "");

    void Load(
        NYson::TYsonPullParserCursor* cursor,
        bool postprocess = true,
        bool setDefaults = true,
        const NYPath::TYPath& path = "");

    void Postprocess(const NYPath::TYPath& path = "") const;

    void SetDefaults();

    void Save(NYson::IYsonConsumer* consumer) const;

    IMapNodePtr GetUnrecognized() const;
    IMapNodePtr GetUnrecognizedRecursively() const;
    IMapNodePtr GetRecursiveUnrecognized() const;

    void SetUnrecognizedStrategy(EUnrecognizedStrategy strategy);

    THashSet<TString> GetRegisteredKeys() const;
    int GetParameterCount() const;

    void SaveParameter(const TString& key, NYson::IYsonConsumer* consumer) const;
    void LoadParameter(const TString& key, const NYTree::INodePtr& node, EMergeStrategy mergeStrategy) const;
    void ResetParameter(const TString& key) const;

    std::vector<TString> GetAllParameterAliases(const TString& key) const;

protected:
    template <class T>
    TParameter<T>& RegisterParameter(
        TString parameterName,
        T& value);

    void RegisterPreprocessor(const TPreprocessor& func);
    void RegisterPostprocessor(const TPostprocessor& func);

private:
    template <class T>
    friend class TParameter;

    THashMap<TString, IParameterPtr> Parameters;

    NYTree::IMapNodePtr Unrecognized;
    EUnrecognizedStrategy UnrecognizedStrategy = EUnrecognizedStrategy::Drop;

    std::vector<TPreprocessor> Preprocessors;
    std::vector<TPostprocessor> Postprocessors;

    IParameterPtr GetParameter(const TString& keyOrAlias) const;
};

////////////////////////////////////////////////////////////////////////////////

class TYsonSerializable
    : public TRefCounted
    , public TYsonSerializableLite
{ };

////////////////////////////////////////////////////////////////////////////////

template <class T>
TIntrusivePtr<T> CloneYsonSerializable(const TIntrusivePtr<T>& obj);
template <class T>
std::vector<TIntrusivePtr<T>> CloneYsonSerializables(const std::vector<TIntrusivePtr<T>>& objs);
template <class T>
THashMap<TString, TIntrusivePtr<T>> CloneYsonSerializables(const THashMap<TString, TIntrusivePtr<T>>& objs);

void Serialize(const TYsonSerializableLite& value, NYson::IYsonConsumer* consumer);
void Deserialize(TYsonSerializableLite& value, NYTree::INodePtr node);
void Deserialize(TYsonSerializableLite& value, NYson::TYsonPullParserCursor* cursor);

template <class T>
TIntrusivePtr<T> UpdateYsonSerializable(
    const TIntrusivePtr<T>& obj,
    const NYTree::INodePtr& patch);

template <class T>
TIntrusivePtr<T> UpdateYsonSerializable(
    const TIntrusivePtr<T>& obj,
    const NYson::TYsonString& patch);

template <class T>
bool ReconfigureYsonSerializable(
    const TIntrusivePtr<T>& config,
    const NYson::TYsonString& newConfigYson);

template <class T>
bool ReconfigureYsonSerializable(
    const TIntrusivePtr<T>& config,
    const TIntrusivePtr<T>& newConfig);

template <class T>
bool ReconfigureYsonSerializable(
    const TIntrusivePtr<T>& config,
    const NYTree::INodePtr& newConfigNode);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree


namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TBinaryYsonSerializer
{
    static void Save(TStreamSaveContext& context, const NYTree::TYsonSerializableLite& obj);
    static void Load(TStreamLoadContext& context, NYTree::TYsonSerializableLite& obj);
};

template <class T, class C>
struct TSerializerTraits<
    T,
    C,
    typename std::enable_if_t<std::is_convertible_v<T&, NYTree::TYsonSerializableLite&>>>
{
    typedef TBinaryYsonSerializer TSerializer;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define YSON_SERIALIZABLE_INL_H_
#include "yson_serializable-inl.h"
#undef YSON_SERIALIZABLE_INL_H_
