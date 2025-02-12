#pragma once

#include <util/generic/fwd.h>

namespace NKikimr::NKqp::NBatchParams {

const TString IsFirstQuery = "_kqp_batch_is_first_query";
const TString IsLastQuery = "_kqp_batch_is_last_query";
const TString IsInclusive = "_kqp_batch_is_inclusive";
const TString Begin = "_kqp_batch_begin_"; // begin_N
const TString End = "_kqp_batch_end_";     // end_N

} // namespace NKikimr::NKqp::NPartitionedExecuter
