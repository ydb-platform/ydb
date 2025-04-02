#pragma once

#include <util/generic/fwd.h>

namespace NKikimr::NKqp::NBatchParams {

const TString Header = "%kqp%batch_";
const TString IsInclusiveLeft = Header + "is_inclusive_left";
const TString IsInclusiveRight = Header + "is_inclusive_right";
const TString Begin = Header + "begin_"; // begin_N
const TString End = Header + "end_";     // end_N
const TString BeginPrefixSize = Begin + "prefix_size";
const TString EndPrefixSize = End + "prefix_size";

} // namespace NKikimr::NKqp::NBatchParams
