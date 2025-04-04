#pragma once

#include <util/generic/fwd.h>

namespace NKikimr::NKqp::NBatchParams {

const TString Header = "$_kqp_batch_";
const TString IsFirstQuery = Header + "is_first_query";
const TString IsLastQuery = Header + "is_last_query";
const TString IsInclusiveLeft = Header + "is_inclusive_left";
const TString IsInclusiveRight = Header + "is_inclusive_right";
const TString Begin = Header + "begin_"; // begin_N
const TString End = Header + "end_";     // end_N

} // namespace NKikimr::NKqp::NPartitionedExecuter
