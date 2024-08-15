#pragma once

#include "config.h"

#include <yt/yt/core/yson/public.h>

namespace NYT::NJson {

////////////////////////////////////////////////////////////////////////////////

// YSON-to-JSON Mapping Conventions.
// * Attributes are not supported.
// * Mapping:
//      YSON --> JSON
//    * List --> Array
//    * Map  --> Object
//    * Int64  --> Number
//    * Uint64 --> Number
//    * Double (NaNs are not supported) --> Number
//    * String (MUST be UTF-8) --> String
//    * Entity --> null
struct IJsonWriter
    : public NYson::IFlushableYsonConsumer
{
    // Finish writing top-level value and start the next one from new line.
    // No check for value completeness are guaranteed.
    virtual void StartNextValue() = 0;

    virtual ui64 GetWrittenByteCount() const = 0;
};

std::unique_ptr<IJsonWriter> CreateJsonWriter(
    IOutputStream* output,
    bool pretty = false);

////////////////////////////////////////////////////////////////////////////////

// THIS INTERFACE IS YT JSON FORMAT SPECIFIC, IT SHOULD NOT BE USED ELSEWHERE.
//
// YSON-to-JSON Mapping Conventions
//
// * List fragment corresponds to new-line-delimited JSON.
// * Map fragment (which exists in YSON) is not supported.
// * Other types (without attributes) are mapped almost as is:
//      YSON --> JSON
//    * List --> Array
//    * Map  --> Object
//    * Int64 --> Number
//    * Uint64 --> Number
//    * Double --> Number
//    * String (s) --> String (t):
//      * If s is ASCII: t := s
//      * else: t := UTF-8 string with code points corresponding to bytes in s.
//    * Entity --> null
// * Nodes with attributes are mapped to the following JSON map:
//    {
//        '$attributes': (attributes map),
//        '$value': (value, as explained above),
//        '$type': (type name, if type annotation is enabled)
//    }

//! Translates YSON events into a series of calls to underlying |IJsonWriter|
//! thus enabling to transform YSON into JSON.
/*!
 *  \note
 *  Entities are translated to nulls.
 *
 *  Attributes are only supported for entities and maps.
 *  They are written as an inner "$attributes" map.
 *
 *  Explicit #Flush calls should be made when finished writing via the adapter.
 */
struct IJsonConsumer
    : public NYson::IFlushableYsonConsumer
{
    virtual void SetAnnotateWithTypesParameter(bool value) = 0;

    virtual void OnStringScalarWeightLimited(TStringBuf value, std::optional<i64> weightLimit) = 0;
    virtual void OnNodeWeightLimited(TStringBuf yson, std::optional<i64> weightLimit) = 0;
};

// If |type == ListFragment|, additionally call |underlying->StartNextValue| after each complete value.
std::unique_ptr<IJsonConsumer> CreateJsonConsumer(
    IOutputStream* output,
    NYson::EYsonType type = NYson::EYsonType::Node,
    TJsonFormatConfigPtr config = New<TJsonFormatConfig>());

// If |type == ListFragment|, additionally call |underlying->StartNextValue| after each complete value.
std::unique_ptr<IJsonConsumer> CreateJsonConsumer(
    IJsonWriter* underlying,
    NYson::EYsonType type = NYson::EYsonType::Node,
    TJsonFormatConfigPtr config = New<TJsonFormatConfig>());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJson
