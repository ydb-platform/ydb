#pragma once

#include <ydb/core/persqueue/public/mlp/mlp.h>

#include <util/generic/string.h>

#include <expected>

namespace NKikimr::NSqsTopic::V1 {

    TString SerializeReceipt(const NPQ::NMLP::TMessageId& pos);
    std::expected<NPQ::NMLP::TMessageId, TString> DeserializeReceipt(TStringBuf receipt);

} // namespace NKikimr::NSqsTopic::V1
