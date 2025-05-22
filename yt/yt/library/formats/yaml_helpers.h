#pragma once

#include "private.h"

#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/ytree/public.h>

#include <contrib/libs/yaml/include/yaml.h>

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

template <class TLibYamlType, void(*Deleter)(TLibYamlType*)>
struct TLibYamlTypeWrapper
    : public TLibYamlType
    , public TNonCopyable
{
    TLibYamlTypeWrapper();
    void Reset();
    ~TLibYamlTypeWrapper();
};

using TLibYamlParser = TLibYamlTypeWrapper<yaml_parser_t, yaml_parser_delete>;
using TLibYamlEmitter = TLibYamlTypeWrapper<yaml_emitter_t, yaml_emitter_delete>;
using TLibYamlEvent = TLibYamlTypeWrapper<yaml_event_t, yaml_event_delete>;

////////////////////////////////////////////////////////////////////////////////

// These enums are counterparts of the enums in the Yaml library.
// Keep them in sync with the library.

DEFINE_ENUM(EYamlErrorType,
    ((NoError)  (YAML_NO_ERROR))
    ((Memory)   (YAML_MEMORY_ERROR))
    ((Reader)   (YAML_READER_ERROR))
    ((Scanner)  (YAML_SCANNER_ERROR))
    ((Parser)   (YAML_PARSER_ERROR))
    ((Composer) (YAML_COMPOSER_ERROR))
    ((Writer)   (YAML_WRITER_ERROR))
    ((Emitter)  (YAML_EMITTER_ERROR))
);

DEFINE_ENUM(EYamlEventType,
    ((NoEvent)          (YAML_NO_EVENT))
    ((StreamStart)      (YAML_STREAM_START_EVENT))
    ((StreamEnd)        (YAML_STREAM_END_EVENT))
    ((DocumentStart)    (YAML_DOCUMENT_START_EVENT))
    ((DocumentEnd)      (YAML_DOCUMENT_END_EVENT))
    ((Alias)            (YAML_ALIAS_EVENT))
    ((Scalar)           (YAML_SCALAR_EVENT))
    ((SequenceStart)    (YAML_SEQUENCE_START_EVENT))
    ((SequenceEnd)      (YAML_SEQUENCE_END_EVENT))
    ((MappingStart)     (YAML_MAPPING_START_EVENT))
    ((MappingEnd)       (YAML_MAPPING_END_EVENT))
);

//! This tag is used for denoting 2-element sequences that represent a YSON node with attributes.
static constexpr std::string_view YTAttrNodeTag = "!yt/attrnode";

//! Thia tag is used upon parsing to denote an integer scalar which should be
//! represented by YT uint64 type. Writer by default omits this tag, but may be
//! configured to force this tag on all uint64 values.
static constexpr std::string_view YTUintTag = "!yt/uint64";

////////////////////////////////////////////////////////////////////////////////

//! We support:
//! - YAML 1.2 Core schema types
//! - YT-specific uint type, for which we introduce a special tag "!yt/uint64".
DEFINE_ENUM(EYamlScalarType,
    (String)
    (Int)
    (Float)
    (Bool)
    (Null)
    (Uint)
);

union TNonStringScalar
{
    i64 Int64;
    ui64 Uint64;
    double Double;
    bool Boolean;
};

//! Extracts a recognized YAML scalar type from a tag.
EYamlScalarType DeduceScalarTypeFromTag(const std::string_view& tag);
//! Guesses a recognized YAML scalar type from a value.
EYamlScalarType DeduceScalarTypeFromValue(const std::string_view& value);
//! Given a recognized YAML type, transforms it into a YT type and,
//! in case of a non-string result, parses a scalar value.
std::pair<NYTree::ENodeType, TNonStringScalar> ParseScalarValue(
    const std::string_view& value,
    EYamlScalarType yamlType);

////////////////////////////////////////////////////////////////////////////////

// Convenience helpers for transforming a weirdly represented (yaml_char_t* ~ unsigned char*)
// YAML string into string_view, also handling the case of a null pointer.

std::string_view YamlLiteralToStringView(const yaml_char_t* literal, size_t length);
std::string_view YamlLiteralToStringView(const yaml_char_t* literal);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats

// Note that ADL requires to put this function in the global namespace since
// yaml_mark_t is defined in the global namespace from C++ POV

void Serialize(const yaml_mark_t& mark, NYT::NYson::IYsonConsumer* consumer);
