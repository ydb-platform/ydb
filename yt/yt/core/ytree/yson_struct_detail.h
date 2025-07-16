#pragma once

#include "yson_struct_public.h"

#include <yt/yt/core/yson/public.h>
#include <yt/yt/core/ypath/public.h>
#include <yt/yt/core/ytree/public.h>

#include <library/cpp/yt/misc/optional.h>

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

namespace NPrivate {

// Least common denominator between INodePtr
// and TYsonPullParserCursor.
// Maybe something else in the future.
template <class T>
struct TYsonSourceTraits
{
    static constexpr bool IsValid = false;

    static INodePtr AsNode(T& source)
        requires false;

    static bool IsEmpty(T& source)
        requires false;

    static void Advance(T& source)
        requires false;

    template <CStdVector TVector, class TFiller>
    static void FillVector(T& source, TVector& vector, TFiller filler)
        requires false;

    template <CAnyMap TMap, class TFiller>
    static void FillMap(T& source, TMap& map, TFiller filler)
        requires false;
};

} // namespace NPrivate

////////////////////////////////////////////////////////////////////////////////

template <class TStruct, class TValue>
using TYsonStructField = TValue(TStruct::*);

// This is intended to be used as an equality-only comparator of YsonStruct fields.
// dynamic_cast is used to compare generic #ITypeErasedYsonStructField to
// a concrete #TTypedYsonStructField (see -inl.h).
struct ITypeErasedYsonStructField
    : public TRefCounted
{ };

DECLARE_REFCOUNTED_STRUCT(ITypeErasedYsonStructField);
DEFINE_REFCOUNTED_TYPE(ITypeErasedYsonStructField);

template <class TStruct, class TValue>
ITypeErasedYsonStructFieldPtr CreateTypeErasedYsonStructField(TYsonStructField<TStruct, TValue> field);

////////////////////////////////////////////////////////////////////////////////

struct TLoadParameterOptions
{
    std::function<NYPath::TYPath()> PathGetter;
    std::optional<EUnrecognizedStrategy> RecursiveUnrecognizedRecursively;
};

////////////////////////////////////////////////////////////////////////////////

struct IYsonStructParameter
    : public TRefCounted
{
    virtual void Load(
        TYsonStructBase* self,
        NYTree::INodePtr node,
        const TLoadParameterOptions& options) = 0;

    virtual void Load(
        TYsonStructBase* self,
        NYson::TYsonPullParserCursor* cursor,
        const TLoadParameterOptions& options) = 0;

    virtual void SafeLoad(
        TYsonStructBase* self,
        NYTree::INodePtr node,
        const TLoadParameterOptions& options,
        const std::function<void()>& validate) = 0;

    virtual void Save(const TYsonStructBase* self, NYson::IYsonConsumer* consumer) const = 0;

    virtual void PostprocessParameter(const TYsonStructBase* self, const std::function<NYPath::TYPath()>& pathGetter) const = 0;

    virtual void SetDefaultsInitialized(TYsonStructBase* self) = 0;

    virtual bool CanOmitValue(const TYsonStructBase* self) const = 0;

    virtual bool IsRequired() const = 0;
    virtual const std::string& GetKey() const = 0;
    virtual const std::vector<std::string>& GetAliases() const = 0;
    virtual IMapNodePtr GetRecursiveUnrecognized(const TYsonStructBase* self) const = 0;

    virtual void WriteSchema(const TYsonStructBase* self, NYson::IYsonConsumer* consumer) const = 0;

    virtual bool CompareParameter(const TYsonStructBase* lhsSelf, const TYsonStructBase* rhsSelf) const = 0;

    virtual int GetFieldIndex() const = 0;

    virtual bool HoldsField(ITypeErasedYsonStructFieldPtr erasedField) const = 0;
};

DECLARE_REFCOUNTED_STRUCT(IYsonStructParameter)
DEFINE_REFCOUNTED_TYPE(IYsonStructParameter)

////////////////////////////////////////////////////////////////////////////////

struct IYsonStructMeta
{
    virtual const THashMap<std::string, IYsonStructParameterPtr>& GetParameterMap() const = 0;
    virtual const std::vector<std::pair<std::string, IYsonStructParameterPtr>>& GetParameterSortedList() const = 0;
    virtual void SetDefaultsOfInitializedStruct(TYsonStructBase* target) const = 0;
    virtual const THashSet<std::string>& GetRegisteredKeys() const = 0;
    virtual void PostprocessStruct(TYsonStructBase* target, const std::function<NYPath::TYPath()>& pathGetter) const = 0;
    virtual IYsonStructParameterPtr GetParameter(const std::string& keyOrAlias) const = 0;
    virtual void LoadParameter(TYsonStructBase* target, const std::string& key, const NYTree::INodePtr& node) const = 0;

    virtual void LoadStruct(
        TYsonStructBase* target,
        INodePtr node,
        bool postprocess,
        bool setDefaults,
        const std::function<NYPath::TYPath()>& pathGetter) const = 0;

    virtual void LoadStruct(
        TYsonStructBase* target,
        NYson::TYsonPullParserCursor* cursor,
        bool postprocess,
        bool setDefaults,
        const std::function<NYPath::TYPath()>& pathGetter) const = 0;

    virtual IMapNodePtr GetRecursiveUnrecognized(const TYsonStructBase* target) const = 0;

    virtual void RegisterParameter(std::string key, IYsonStructParameterPtr parameter) = 0;
    virtual void RegisterPreprocessor(std::function<void(TYsonStructBase*)> preprocessor) = 0;
    virtual void RegisterPostprocessor(std::function<void(TYsonStructBase*)> postprocessor) = 0;
    virtual void SetUnrecognizedStrategy(EUnrecognizedStrategy strategy) = 0;

    virtual void WriteSchema(const TYsonStructBase* target, NYson::IYsonConsumer* consumer) const = 0;

    virtual bool CompareStructs(
        const TYsonStructBase* lhs,
        const TYsonStructBase* rhs) const = 0;

    virtual ~IYsonStructMeta() = default;
};

////////////////////////////////////////////////////////////////////////////////

class TYsonStructMeta
    : public IYsonStructMeta
{
public:
    void SetDefaultsOfInitializedStruct(TYsonStructBase* target) const override;

    const THashMap<std::string, IYsonStructParameterPtr>& GetParameterMap() const override;
    const std::vector<std::pair<std::string, IYsonStructParameterPtr>>& GetParameterSortedList() const override;
    const THashSet<std::string>& GetRegisteredKeys() const override;

    IYsonStructParameterPtr GetParameter(const std::string& keyOrAlias) const override;
    void LoadParameter(TYsonStructBase* target, const std::string& key, const NYTree::INodePtr& node) const override;

    void PostprocessStruct(TYsonStructBase* target, const std::function<NYPath::TYPath()>& pathGetter) const override;

    void LoadStruct(
        TYsonStructBase* target,
        INodePtr node,
        bool postprocess,
        bool setDefaults,
        const std::function<NYPath::TYPath()>& pathGetter) const override;

    void LoadStruct(
        TYsonStructBase* target,
        NYson::TYsonPullParserCursor* cursor,
        bool postprocess,
        bool setDefaults,
        const std::function<NYPath::TYPath()>& pathGetter) const override;

    IMapNodePtr GetRecursiveUnrecognized(const TYsonStructBase* target) const override;

    void RegisterParameter(std::string key, IYsonStructParameterPtr parameter) override;
    void RegisterPreprocessor(std::function<void(TYsonStructBase*)> preprocessor) override;
    void RegisterPostprocessor(std::function<void(TYsonStructBase*)> postprocessor) override;
    void SetUnrecognizedStrategy(EUnrecognizedStrategy strategy) override;

    void WriteSchema(const TYsonStructBase* target, NYson::IYsonConsumer* consumer) const override;

    void FinishInitialization(const std::type_info& structType);

    bool CompareStructs(
        const TYsonStructBase* lhs,
        const TYsonStructBase* rhs) const override;

private:
    friend class TYsonStructRegistry;

    const std::type_info* StructType_;

    THashMap<std::string, IYsonStructParameterPtr> Parameters_;
    std::vector<std::pair<std::string, IYsonStructParameterPtr>> SortedParameters_;
    THashSet<std::string> RegisteredKeys_;

    std::vector<std::function<void(TYsonStructBase*)>> Preprocessors_;
    std::vector<std::function<void(TYsonStructBase*)>> Postprocessors_;

    EUnrecognizedStrategy MetaUnrecognizedStrategy_;
};

////////////////////////////////////////////////////////////////////////////////

//! Type erasing interface.
/*! This interface and underlying class is used to erase TStruct type parameter from TYsonStructParameter.
 * Otherwise we would have TYsonStructParameter<TStruct, TValue>
 * and compiler would have to instantiate huge template for each pair <TStruct, TValue>.
 */
template <class TValue>
struct IYsonFieldAccessor
{
    virtual TValue& GetValue(const TYsonStructBase* source) = 0;
    virtual bool HoldsField(ITypeErasedYsonStructFieldPtr erasedField) const = 0;
    virtual ~IYsonFieldAccessor() = default;
};

////////////////////////////////////////////////////////////////////////////////

template <class TStruct, class TValue>
class TYsonFieldAccessor
    : public IYsonFieldAccessor<TValue>
{
public:
    explicit TYsonFieldAccessor(TYsonStructField<TStruct, TValue> field);
    bool HoldsField(ITypeErasedYsonStructFieldPtr erasedField) const override;
    TValue& GetValue(const TYsonStructBase* source) override;

private:
    TYsonStructField<TStruct, TValue> Field_;
};

////////////////////////////////////////////////////////////////////////////////

template <class TStruct, class TValue>
class TUniversalYsonParameterAccessor
    : public IYsonFieldAccessor<TValue>
{
public:
    explicit TUniversalYsonParameterAccessor(std::function<TValue&(TStruct*)> field);
    bool HoldsField(ITypeErasedYsonStructFieldPtr erasedField) const override;
    TValue& GetValue(const TYsonStructBase* source) override;

private:
    std::function<TValue&(TStruct*)> Accessor_;
};

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
class TYsonStructParameter
    : public IYsonStructParameter
{
public:
    using TValidator = std::function<void(const TValue&)>;
    using TValueType = typename TOptionalTraits<TValue>::TValue;

    TYsonStructParameter(
        std::string key,
        std::unique_ptr<IYsonFieldAccessor<TValue>> fieldAccessor,
        int fieldIndex);

    void Load(
        TYsonStructBase* self,
        NYTree::INodePtr node,
        const TLoadParameterOptions& options) override;

    void Load(
        TYsonStructBase* self,
        NYson::TYsonPullParserCursor* cursor,
        const TLoadParameterOptions& options) override;

    void SafeLoad(
        TYsonStructBase* self,
        NYTree::INodePtr node,
        const TLoadParameterOptions& options,
        const std::function<void()>& validate) override;

    void PostprocessParameter(const TYsonStructBase* self, const std::function<NYPath::TYPath()>& pathGetter) const override;
    void SetDefaultsInitialized(TYsonStructBase* self) override;
    void Save(const TYsonStructBase* self, NYson::IYsonConsumer* consumer) const override;
    bool CanOmitValue(const TYsonStructBase* self) const override;
    bool IsRequired() const override;
    const std::string& GetKey() const override;
    const std::vector<std::string>& GetAliases() const override;
    IMapNodePtr GetRecursiveUnrecognized(const TYsonStructBase* self) const override;

    void WriteSchema(const TYsonStructBase* self, NYson::IYsonConsumer* consumer) const override;

    bool CompareParameter(const TYsonStructBase* lhsSelf, const TYsonStructBase* rhsSelf) const override;

    virtual int GetFieldIndex() const override;

    const TValue& GetValue(const TYsonStructBase* source) const;
    bool HoldsField(ITypeErasedYsonStructFieldPtr erasedField) const override;

    // Mark as optional. Field will be default-initialized if `init` is true, initialization is skipped otherwise.
    TYsonStructParameter& Optional(bool init = true);
    // Set default value. It will be copied during instance initialization.
    TYsonStructParameter& Default(TValue defaultValue);
    // Set empty value as default value. It will be created during instance initialization.
    TYsonStructParameter& Default();
    // Register constructor for default value. It will be called during instance initialization.
    TYsonStructParameter& DefaultCtor(std::function<TValue()> defaultCtor);
    // Omit this parameter during serialization if it is equal to default.
    TYsonStructParameter& DontSerializeDefault();
    // Register general purpose validator for parameter. Used by other validators.
    // It is called after deserialization.
    TYsonStructParameter& CheckThat(TValidator validator);
    // Register validator that checks value to be greater than given value.
    TYsonStructParameter& GreaterThan(TValueType value);
    // Register validator that checks value to be greater than or equal to given value.
    TYsonStructParameter& GreaterThanOrEqual(TValueType value);
    // Register validator that checks value to be less than given value.
    TYsonStructParameter& LessThan(TValueType value);
    // Register validator that checks value to be less than or equal to given value.
    TYsonStructParameter& LessThanOrEqual(TValueType value);
    // Register validator that checks value to be in given range.
    TYsonStructParameter& InRange(TValueType lowerBound, TValueType upperBound);
    // Register validator that checks value to be non empty.
    TYsonStructParameter& NonEmpty();
    // Register alias for parameter. Used in deserialization.
    TYsonStructParameter& Alias(const std::string& name);
    // Set field to T() (or suitable analogue) before deserializations.
    TYsonStructParameter& ResetOnLoad();
    // Uses given unrecognized strategy in |Load| if there was no strategy supplied.
    TYsonStructParameter& DefaultUnrecognizedStrategy(EUnrecognizedStrategy strategy);
    // Forces given parameter to ignore unrecognized strategy even if it set to
    // some recursive version. Combination with |DefaultUnrecognizedStrategy| enables
    // behavior which ensures selected default strategy for all fields below.
    TYsonStructParameter& EnforceDefaultUnrecognizedStrategy();

    // Register constructor with parameters as initializer of default value for ref-counted class.
    template <class... TArgs>
    TYsonStructParameter& DefaultNew(TArgs&&... args);

private:
    const std::string Key_;

    std::unique_ptr<IYsonFieldAccessor<TValue>> FieldAccessor_;
    std::optional<std::function<TValue()>> DefaultCtor_;
    bool SerializeDefault_ = true;
    std::vector<TValidator> Validators_;
    std::vector<std::string> Aliases_;
    bool TriviallyInitializedIntrusivePtr_ = false;
    bool Optional_ = false;
    bool ResetOnLoad_ = false;
    std::optional<EUnrecognizedStrategy> DefaultUnrecognizedStrategy_;
    bool EnforceDefaultUnrecognizedStrategy_ = false;
    const int FieldIndex_ = -1;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree

#define YSON_STRUCT_DETAIL_INL_H_
#include "yson_struct_detail-inl.h"
#undef YSON_STRUCT_DETAIL_INL_H_
