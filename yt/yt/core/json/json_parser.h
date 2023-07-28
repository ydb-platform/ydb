#pragma once

#include "public.h"
#include "config.h"

#include <yt/yt/core/yson/consumer.h>

namespace NYT::NJson {

// See json_writer.h for details on how YSON is mapped to JSON.
// This implementation of TJsonParser is DOM-based (and is thus suboptimal).

////////////////////////////////////////////////////////////////////////////////

class TJsonParser
{
public:
    explicit TJsonParser(
        NYson::IYsonConsumer* consumer,
        TJsonFormatConfigPtr config = nullptr,
        NYson::EYsonType type = NYson::EYsonType::Node);
    ~TJsonParser();

    void Read(TStringBuf data);
    void Finish();

    void Parse(IInputStream* input);

private:
    class TImpl;
    const std::unique_ptr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

void ParseJson(
    IInputStream* input,
    NYson::IYsonConsumer* consumer,
    TJsonFormatConfigPtr config = nullptr,
    NYson::EYsonType type = NYson::EYsonType::Node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJson
