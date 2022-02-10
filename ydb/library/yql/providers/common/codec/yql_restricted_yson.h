#pragma once

#include "yql_codec_results.h"

#include <util/generic/strbuf.h>
#include <library/cpp/yson/public.h>

namespace NYT {
    class TNode;
}

namespace NYql {
namespace NCommon {

void EncodeRestrictedYson(
    TYsonResultWriter& writer,
    const TStringBuf& yson);

TString EncodeRestrictedYson(
    const NYT::TNode& node,
    NYson::EYsonFormat format = NYson::EYsonFormat::Binary);

TString DecodeRestrictedYson(
    const TStringBuf& yson,
    NYson::EYsonFormat format = NYson::EYsonFormat::Binary);

}
}
