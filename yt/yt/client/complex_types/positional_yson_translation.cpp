#include "positional_yson_translation.h"

#include "check_yson_token.h"
#include "common_yson_converters.h"

#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/unversioned_value.h>

#include <yt/yt/core/yson/pull_parser.h>
#include <yt/yt/core/yson/writer.h>

#include <library/cpp/yt/yson/consumer.h>

#include <util/generic/buffer.h>
#include <util/stream/buffer.h>

namespace NYT::NComplexTypes {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NTableClient;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TPositionalDictCursorConverter
{
public:
    TPositionalDictCursorConverter(
        TComplexTypeFieldDescriptor descriptor,
        TYsonCursorConverter keyConverter,
        TYsonCursorConverter valueConverter)
        : Descriptor_(std::move(descriptor))
        , KeyConverter_(std::move(keyConverter))
        , ValueConverter_(std::move(valueConverter))
    { }

    void operator()(TYsonPullParserCursor* cursor, IYsonConsumer* consumer) const
    {
        EnsureYsonToken(Descriptor_, *cursor, EYsonItemType::BeginList);
        cursor->Next();

        consumer->OnBeginList();
        while ((*cursor)->GetType() != EYsonItemType::EndList) {
            EnsureYsonToken(Descriptor_, *cursor, EYsonItemType::BeginList);
            cursor->Next();

            consumer->OnListItem();
            consumer->OnBeginList();

            consumer->OnListItem();
            KeyConverter_(cursor, consumer);

            consumer->OnListItem();
            ValueConverter_(cursor, consumer);

            consumer->OnEndList();

            EnsureYsonToken(Descriptor_, *cursor, EYsonItemType::EndList);
            cursor->Next();
        }
        consumer->OnEndList();

        // Skip end list token.
        cursor->Next();
    }

private:
    const TComplexTypeFieldDescriptor Descriptor_;
    const TYsonCursorConverter KeyConverter_;
    const TYsonCursorConverter ValueConverter_;
};

////////////////////////////////////////////////////////////////////////////////

struct TRetainedStructFieldInfo
{
    TYsonCursorConverter Converter;
    int OldPosition = 0;
    int Position = 0;
    bool IsNullable = false;
};

class TPositionalStructCursorConverter
{
public:
    TPositionalStructCursorConverter(
        TComplexTypeFieldDescriptor descriptor,
        std::vector<TRetainedStructFieldInfo> retainedFieldInfos,
        int fieldCount)
        : Descriptor_(std::move(descriptor))
        , OldPositionToRetainedFieldEntry_(std::invoke([&] {
            auto maxOldPosition = std::ranges::max(
                retainedFieldInfos,
                std::ranges::less{},
                &TRetainedStructFieldInfo::OldPosition)
                .OldPosition;

            std::vector<std::optional<TRetainedFieldEntry>> result(maxOldPosition + 1);
            for (auto& fieldInfo : retainedFieldInfos) {
                result[fieldInfo.OldPosition] = TRetainedFieldEntry{
                    .Converter = std::move(fieldInfo.Converter),
                    .Position = fieldInfo.Position,
                };
            }
            return result;
        }))
        , PositionTable_(std::invoke([&] {
            std::vector<TPositionTableEntry> result(fieldCount);
            for (const auto& fieldInfo : retainedFieldInfos) {
                PositionTable_[fieldInfo.Position].IsNullable = fieldInfo.IsNullable;
            }
            return result;
        }))
        , BufferOutput_(Buffer_)
        , YsonWriter_(&BufferOutput_, EYsonType::ListFragment)
    { }

    // NB: To wrap this object into std::function we must be able to copy it.
    TPositionalStructCursorConverter(const TPositionalStructCursorConverter& other)
        : Descriptor_(other.Descriptor_)
        , OldPositionToRetainedFieldEntry_(other.OldPositionToRetainedFieldEntry_)
        , PositionTable_(other.PositionTable_)
        , Buffer_(other.Buffer_)
        , BufferOutput_(Buffer_)
        , YsonWriter_(&BufferOutput_, EYsonType::ListFragment)
    { }

    void operator()(TYsonPullParserCursor* cursor, IYsonConsumer* consumer)
    {
        YT_ASSERT(YsonWriter_.GetDepth() == 0);

        Buffer_.Clear();
        ResetPositionEntryPresence();

        EnsureYsonToken(Descriptor_, *cursor, EYsonItemType::BeginList);
        cursor->Next();

        for (int oldPosition = 0; (*cursor)->GetType() != EYsonItemType::EndList; ++oldPosition) {
            if (oldPosition >= std::ssize(OldPositionToRetainedFieldEntry_) ||
                !OldPositionToRetainedFieldEntry_[oldPosition].has_value())
            {
                cursor->SkipComplexValue();
                continue;
            }

            const auto& fieldEntry = *OldPositionToRetainedFieldEntry_[oldPosition];
            auto& positionEntry = PositionTable_[fieldEntry.Position];

            auto offset = Buffer_.Size();
            fieldEntry.Converter(cursor, &YsonWriter_);
            YsonWriter_.Flush();

            positionEntry.Offset = offset;
            positionEntry.Size = Buffer_.size() - offset;
            positionEntry.IsPresent = true;
        }

        // Skip list end token.
        cursor->Next();

        consumer->OnBeginList();
        for (int position : std::views::iota(0, std::ssize(PositionTable_))) {
            const auto& positionEntry = PositionTable_[position];
            if (positionEntry.IsPresent) {
                consumer->OnRaw(
                    TStringBuf(Buffer_.Data() + positionEntry.Offset, positionEntry.Size),
                    EYsonType::ListFragment);
            } else if (positionEntry.IsNullable) {
                consumer->OnRaw("#;", EYsonType::ListFragment);
            } else {
                THROW_ERROR_EXCEPTION(
                    "Field (%v) is missing while parsing %Qv",
                    position,
                    Descriptor_.GetDescription());
            }
        }
        consumer->OnEndList();
    }

private:
    const TComplexTypeFieldDescriptor Descriptor_;

    struct TRetainedFieldEntry
    {
        TYsonCursorConverter Converter;
        int Position = 0;
    };
    const std::vector<std::optional<TRetainedFieldEntry>> OldPositionToRetainedFieldEntry_;

    struct TPositionTableEntry
    {
        bool IsNullable = true; // All newly added fields must be nullable.

        bool IsPresent = false;
        ssize_t Offset = 0;
        ssize_t Size = 0;
    };
    std::vector<TPositionTableEntry> PositionTable_;

    TBuffer Buffer_;
    TBufferOutput BufferOutput_;
    TBufferedBinaryYsonWriter YsonWriter_;

    void ResetPositionEntryPresence()
    {
        for (auto& entry : PositionTable_) {
            entry.IsPresent = false;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TRetainedVariantStructFieldInfo
{
    TYsonCursorConverter Converter;
    int OldTag = 0;
    int Tag = 0;
};

class TPositionalVariantStructCursorConverter
{
public:
    TPositionalVariantStructCursorConverter(
        TComplexTypeFieldDescriptor descriptor,
        std::vector<TRetainedVariantStructFieldInfo> retainedFieldInfos)
        : Descriptor_(std::move(descriptor))
        , OldTagToRetainedFieldEntry_(std::invoke([&] {
            auto maxOldTag = std::ranges::max(
                retainedFieldInfos,
                std::ranges::less{},
                &TRetainedVariantStructFieldInfo::OldTag)
                .OldTag;

            std::vector<std::optional<TRetainedFieldEntry>> result(maxOldTag + 1);
            for (auto& retainedFieldInfo : retainedFieldInfos) {
                result[retainedFieldInfo.OldTag] = TRetainedFieldEntry{
                    .Converter = std::move(retainedFieldInfo.Converter),
                    .Tag = retainedFieldInfo.Tag,
                };
            }
            return result;
        }))
    { }

    void operator()(TYsonPullParserCursor* cursor, IYsonConsumer* consumer) const
    {
        EnsureYsonToken(Descriptor_, *cursor, EYsonItemType::BeginList);
        cursor->Next();

        EnsureYsonToken(Descriptor_, *cursor, EYsonItemType::Int64Value);
        auto oldTag = cursor->GetCurrent().UncheckedAsInt64();
        const auto& fieldEntry = GetFieldEntry(oldTag);

        cursor->Next();

        consumer->OnBeginList();

        consumer->OnListItem();
        consumer->OnInt64Scalar(fieldEntry.Tag);

        consumer->OnListItem();
        fieldEntry.Converter(cursor, consumer);

        consumer->OnEndList();

        EnsureYsonToken(Descriptor_, *cursor, EYsonItemType::EndList);
        cursor->Next();
    }

private:
    const TComplexTypeFieldDescriptor Descriptor_;

    struct TRetainedFieldEntry
    {
        TYsonCursorConverter Converter;
        int Tag = 0;
    };
    const std::vector<std::optional<TRetainedFieldEntry>> OldTagToRetainedFieldEntry_;

    const TRetainedFieldEntry& GetFieldEntry(int oldTag) const
    {
        THROW_ERROR_EXCEPTION_IF(
            oldTag < 0,
            "Error while parsing %Qv: encountered negative variant tag (%v)",
            Descriptor_.GetDescription(),
            oldTag);

        THROW_ERROR_EXCEPTION_UNLESS(
            oldTag < std::ssize(OldTagToRetainedFieldEntry_) &&
                OldTagToRetainedFieldEntry_[oldTag].has_value(),
            "Error while parsing %Qv: encountered unknown variant tag (%v)",
            Descriptor_.GetDescription(),
            oldTag);

        return OldTagToRetainedFieldEntry_[oldTag].value();
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTrivialTranslationSpec
{ };

struct TNonTrivialTranslationSpec
{
    TYsonCursorConverter CursorConverter;
};

using TTranslationSpec = std::variant<TTrivialTranslationSpec, TNonTrivialTranslationSpec>;

TYsonCursorConverter GetCursorConverter(TTranslationSpec translationSpec)
{
    return Visit(
        std::move(translationSpec),
        [] (TTrivialTranslationSpec&&) -> TYsonCursorConverter {
            return IdentityYsonCursorConverter;
        },
        [&] (TNonTrivialTranslationSpec&& spec) -> TYsonCursorConverter {
            return std::move(spec).CursorConverter;
        });
}

// Overload for statically-sized tuple of translation specs (used for optional, list, dict).
template <class TConvertersWrapper, std::same_as<TTranslationSpec>... TSpec>
TTranslationSpec WrapTranslationSpecs(
    TConvertersWrapper&& convertersWrapper,
    TSpec&&... translationSpecs)
    requires requires (TYsonCursorConverter converter) {
        { convertersWrapper((translationSpecs, converter)...) }
            -> std::convertible_to<TYsonCursorConverter>;
    }
{
    return std::visit(
        [&] (auto&&... concreteSpecs) -> TTranslationSpec {
            static constexpr auto AreAllTrivial = (... &&
                std::same_as<std::decay_t<decltype(concreteSpecs)>, TTrivialTranslationSpec>);

            if constexpr (AreAllTrivial) {
                return TTrivialTranslationSpec{};
            } else {
                return TNonTrivialTranslationSpec{
                    .CursorConverter = std::invoke(
                        std::forward<TConvertersWrapper>(convertersWrapper),
                        GetCursorConverter(std::move(concreteSpecs))...),
                };
            }
        },
        std::move(translationSpecs)...);
}

// Overload for dynamically-sized list of translation specs (as seen in tuple, struct, etc).
template <CInvocable<TYsonCursorConverter(std::vector<TYsonCursorConverter>)> TConvertersWrapper>
TTranslationSpec WrapTranslationSpecs(
    TConvertersWrapper&& convertersWrapper,
    std::vector<TTranslationSpec>&& translationSpecs)
{
    bool areAllTrivial = std::ranges::all_of(translationSpecs, [] (const auto& spec) {
        return std::holds_alternative<TTrivialTranslationSpec>(spec);
    });
    if (areAllTrivial) {
        return TTrivialTranslationSpec{};
    }

    std::vector<TYsonCursorConverter> converters;
    converters.reserve(translationSpecs.size());
    for (auto& translationSpec : translationSpecs) {
        converters.push_back(GetCursorConverter(std::move(translationSpec)));
    }
    return TNonTrivialTranslationSpec{
        .CursorConverter = std::invoke(
            std::forward<TConvertersWrapper>(convertersWrapper),
            std::move(converters)),
    };
};

////////////////////////////////////////////////////////////////////////////////

TTranslationSpec BuildTranslationSpec(
    const TComplexTypeFieldDescriptor& sourceDescriptor,
    const TComplexTypeFieldDescriptor& targetDescriptor);

template <bool IsVariantTuple>
TTranslationSpec BuildTranslationSpecForTuple(
    const TComplexTypeFieldDescriptor& sourceDescriptor,
    const TComplexTypeFieldDescriptor& targetDescriptor)
{
    const auto& sourceType = sourceDescriptor.GetType();
    const auto& targetType = targetDescriptor.GetType();
    int sourceSize = sourceType->GetElements().size();
    int targetSize = targetType->GetElements().size();

    if constexpr (IsVariantTuple) {
        THROW_ERROR_EXCEPTION_UNLESS(
            sourceSize <= targetSize,
            "Cannot create translator between variant tuples: "
            "source variant tuple at %Qv cannot be larger than "
            "target variant tuple at %Qv, got %v and %v",
            sourceDescriptor.GetDescription(),
            targetDescriptor.GetDescription(),
            sourceSize,
            targetSize);
    } else {
        THROW_ERROR_EXCEPTION_UNLESS(
            sourceSize == targetSize,
            "Cannot create translator between tuples of different sizes %v and %v "
            "at %Qv and %Qv",
            sourceSize,
            targetSize,
            sourceDescriptor.GetDescription(),
            targetDescriptor.GetDescription());
    }

    auto converterConstructor = IsVariantTuple
        ? &CreateVariantTupleYsonCursorConverter
        : &CreateTupleYsonCursorConverter;

    std::vector<TTranslationSpec> elementSpecs;
    elementSpecs.reserve(sourceSize);
    for (auto index : std::views::iota(0, sourceSize)) {
        elementSpecs.push_back(BuildTranslationSpec(
            sourceDescriptor.Element(index),
            targetDescriptor.Element(index)));
    }
    return WrapTranslationSpecs(
        std::bind_front(converterConstructor, sourceDescriptor),
        std::move(elementSpecs));
}

template <bool IsVariantStruct>
TTranslationSpec BuildTranslationSpecForStruct(
    const TComplexTypeFieldDescriptor& sourceDescriptor,
    const TComplexTypeFieldDescriptor& targetDescriptor)
{
    const auto& sourceType = sourceDescriptor.GetType();
    const auto& targetType = targetDescriptor.GetType();

    auto buildStableNameToPosition = [&] (const TLogicalTypePtr& type) {
        THashMap<std::string_view, int> result;
        const auto& fields = type->GetFields();
        for (auto fieldIndex : std::views::iota(0, std::ssize(fields))) {
            EmplaceOrCrash(result, fields[fieldIndex].StableName, fieldIndex);
        }
        return result;
    };

    auto stableNameToSourcePosition = buildStableNameToPosition(sourceType);
    auto stableNameToTargetPosition = buildStableNameToPosition(targetType);

    using TRetainedFieldInfo = std::conditional_t<
        IsVariantStruct,
        TRetainedVariantStructFieldInfo,
        TRetainedStructFieldInfo>;

    bool isTranslationTrivial = true;
    std::vector<TRetainedFieldInfo> retainedFieldInfos;
    for (const auto& [stableName, targetPosition] : stableNameToTargetPosition) {
        auto it = stableNameToSourcePosition.find(stableName);
        if (it == stableNameToSourcePosition.end()) {
            if constexpr(!IsVariantStruct) {
                auto fieldDescriptor = targetDescriptor.Field(targetPosition);
                THROW_ERROR_EXCEPTION_UNLESS(
                    fieldDescriptor.GetType()->IsNullable(),
                    "Cannot create translator between structs at %Qv and %Qv: "
                    "target field %Qv is absent in source and is not nullable",
                    sourceDescriptor.GetDescription(),
                    targetDescriptor.GetDescription(),
                    fieldDescriptor.GetDescription());
            }
            continue;
        }
        auto sourcePosition = it->second;
        if (sourcePosition != targetPosition) {
            isTranslationTrivial = false;
        }

        auto fieldSpec = BuildTranslationSpec(
            sourceDescriptor.Field(sourcePosition),
            targetDescriptor.Field(targetPosition));
        if (std::holds_alternative<TNonTrivialTranslationSpec>(fieldSpec)) {
            isTranslationTrivial = false;
        }

        auto converter = GetCursorConverter(std::move(fieldSpec));
        if constexpr (IsVariantStruct) {
            retainedFieldInfos.push_back({
                .Converter = std::move(converter),
                .OldTag = sourcePosition,
                .Tag = targetPosition,
            });
        } else {
            retainedFieldInfos.push_back({
                .Converter = std::move(converter),
                .OldPosition = sourcePosition,
                .Position = targetPosition,
                .IsNullable = sourceDescriptor.Field(sourcePosition).GetType()->IsNullable(),
            });
        }
    }

    if constexpr (IsVariantStruct) {
        THROW_ERROR_EXCEPTION_IF(
            retainedFieldInfos.empty(),
            "Cannot create translator between variant structs at %Qv and %Qv: no common fields",
            sourceDescriptor.GetDescription(),
            targetDescriptor.GetDescription());
    }

    // Some fields were removed.
    if (retainedFieldInfos.size() < stableNameToSourcePosition.size()) {
        isTranslationTrivial = false;
    }

    if (isTranslationTrivial) {
        return TTrivialTranslationSpec{};
    }

    if constexpr (IsVariantStruct) {
        return TNonTrivialTranslationSpec{
            .CursorConverter = TPositionalVariantStructCursorConverter(
                sourceDescriptor,
                std::move(retainedFieldInfos)),
        };
    } else {
        return TNonTrivialTranslationSpec{
            .CursorConverter = TPositionalStructCursorConverter(
                sourceDescriptor,
                std::move(retainedFieldInfos),
                stableNameToTargetPosition.size()),
        };
    }
}

TTranslationSpec BuildTranslationSpec(
    const TComplexTypeFieldDescriptor& sourceDescriptor,
    const TComplexTypeFieldDescriptor& targetDescriptor)
{
    const auto& sourceType = sourceDescriptor.GetType();
    const auto& targetType = targetDescriptor.GetType();
    auto sourceMetatype = sourceType->GetMetatype();
    auto targetMetatype = targetType->GetMetatype();

    if (sourceMetatype == ELogicalMetatype::Tagged) {
        return BuildTranslationSpec(sourceDescriptor.TaggedElement(), targetDescriptor);
    }
    if (targetMetatype == ELogicalMetatype::Tagged) {
        return BuildTranslationSpec(sourceDescriptor, targetDescriptor.TaggedElement());
    }

    if (sourceMetatype == ELogicalMetatype::Optional &&
        targetMetatype == ELogicalMetatype::Optional)
    {
        return WrapTranslationSpecs(
            std::bind_front(CreateOptionalYsonCursorConverter, sourceDescriptor),
            BuildTranslationSpec(
                sourceDescriptor.OptionalElement(),
                targetDescriptor.OptionalElement()));
    }
    if (sourceMetatype == ELogicalMetatype::Optional) {
        return WrapTranslationSpecs(
            std::bind_front(CreateUnwrappingOptionalYsonCursorConverter, sourceDescriptor),
            BuildTranslationSpec(sourceDescriptor.OptionalElement(), targetDescriptor));
    }
    if (targetMetatype == ELogicalMetatype::Optional) {
        return WrapTranslationSpecs(
            std::bind_front(CreateWrappingOptionalYsonCursorConverter, sourceDescriptor),
            BuildTranslationSpec(sourceDescriptor, targetDescriptor.OptionalElement()));
    }

    THROW_ERROR_EXCEPTION_UNLESS(
        sourceMetatype == targetMetatype,
        "Cannot create translator from %Qlv at %Qv to %Qlv at %Qv: types are incompatible",
        sourceMetatype,
        sourceDescriptor.GetDescription(),
        targetMetatype,
        targetDescriptor.GetDescription());

    switch (sourceMetatype) {
        case ELogicalMetatype::Optional:
        case ELogicalMetatype::Tagged: {
            // NB: Already handled above.
            YT_ABORT();
        }
        case ELogicalMetatype::Simple:
        case ELogicalMetatype::Decimal: {
            return TTrivialTranslationSpec{};
        }
        case ELogicalMetatype::List: {
            return WrapTranslationSpecs(
                std::bind_front(CreateListYsonCursorConverter, sourceDescriptor),
                BuildTranslationSpec(
                    sourceDescriptor.ListElement(),
                    targetDescriptor.ListElement()));
        }
        case ELogicalMetatype::Dict: {
            return WrapTranslationSpecs(
                [&] (auto&&... args) {
                    return TPositionalDictCursorConverter(sourceDescriptor, std::move(args)...);
                },
                BuildTranslationSpec(sourceDescriptor.DictKey(), targetDescriptor.DictKey()),
                BuildTranslationSpec(sourceDescriptor.DictValue(), targetDescriptor.DictValue()));
        }
        case ELogicalMetatype::Tuple: {
            return BuildTranslationSpecForTuple<false>(sourceDescriptor, targetDescriptor);
        }
        case ELogicalMetatype::VariantTuple: {
            return BuildTranslationSpecForTuple<true>(sourceDescriptor, targetDescriptor);
        }
        case ELogicalMetatype::Struct: {
            return BuildTranslationSpecForStruct<false>(sourceDescriptor, targetDescriptor);
        }
        case ELogicalMetatype::VariantStruct: {
            return BuildTranslationSpecForStruct<true>(sourceDescriptor, targetDescriptor);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

TPositionalYsonTranslator CreatePositionalYsonTranslator(
    const TComplexTypeFieldDescriptor& sourceDescriptor,
    const TComplexTypeFieldDescriptor& targetDescriptor)
{
    return Visit(
        BuildTranslationSpec(sourceDescriptor, targetDescriptor),
        [] (TTrivialTranslationSpec&&) -> TPositionalYsonTranslator {
            return std::identity{};
        },
        [] (TNonTrivialTranslationSpec&& translationSpec) {
            return CreateUnversionedValueConverter(std::move(translationSpec).CursorConverter);
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NComplexTypes
