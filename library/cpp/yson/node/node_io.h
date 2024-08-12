#pragma once

#include "node.h"
#include <library/cpp/yson/public.h>

namespace NJson {
    class TJsonValue;
} // namespace NJson

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// Parse TNode from string in YSON format
TNode NodeFromYsonString(const TStringBuf input, ::NYson::EYsonType type = ::NYson::EYsonType::Node);

// Serialize TNode to string in one of YSON formats with random order of maps' keys (don't use in tests)
TString NodeToYsonString(const TNode& node, ::NYson::EYsonFormat format = ::NYson::EYsonFormat::Text);

// Same as the latter, but maps' keys are sorted lexicographically (to be used in tests)
TString NodeToCanonicalYsonString(const TNode& node, ::NYson::EYsonFormat format = ::NYson::EYsonFormat::Text);

// Parse TNode from stream in YSON format
TNode NodeFromYsonStream(IInputStream* input, ::NYson::EYsonType type = ::NYson::EYsonType::Node);

// Parse TNode from stream in YSON format.
// NB: This is substantially slower (1.5-2x using the benchmark from `./benchmark/saveload.cpp`) than using
//     the original `NodeFromYsonStream`.
// Stops reading from `input` as soon as some valid YSON was read, leaving the remainder of the stream unread.
// Used in TNode::Load to support cases of saveloading multiple values after the TNode from/to the same stream.
TNode NodeFromYsonStreamNonGreedy(IInputStream* input, ::NYson::EYsonType type = ::NYson::EYsonType::Node);

// Serialize TNode to stream in one of YSON formats with random order of maps' keys (don't use in tests)
void NodeToYsonStream(const TNode& node, IOutputStream* output, ::NYson::EYsonFormat format = ::NYson::EYsonFormat::Text);

// Same as the latter, but maps' keys are sorted lexicographically (to be used in tests)
void NodeToCanonicalYsonStream(const TNode& node, IOutputStream* output, ::NYson::EYsonFormat format = ::NYson::EYsonFormat::Text);

// Parse TNode from string in JSON format
TNode NodeFromJsonString(const TStringBuf input);
bool TryNodeFromJsonString(const TStringBuf input, TNode& dst);

// Parse TNode from string in JSON format using an iterative JSON parser.
// Iterative JSON parsers still use the stack, but allocate it on the heap (instead of using the system call stack).
// Needed to mitigate stack overflow with short stacks on deeply nested JSON strings
//  (e.g. 256kb of stack when parsing "[[[[[[...]]]]]]" crashes the whole binary).
TNode NodeFromJsonStringIterative(const TStringBuf input, ui64 maxDepth = 1024);

// Convert TJsonValue to TNode
TNode NodeFromJsonValue(const ::NJson::TJsonValue& input);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
