#pragma once
#include <ydb/library/conclusion/result.h>
#include <util/datetime/base.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

namespace NKikimr::NSchemeShard {

TConclusion<TDuration> GetExpireAfter(const NKikimrSchemeOp::TTTLSettings::TEnabled& settings);

}