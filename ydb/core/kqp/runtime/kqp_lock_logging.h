#pragma once

#include <ydb/core/protos/data_events.pb.h>

#include <util/string/builder.h>

namespace NKikimr::NKqp {

inline TStringBuilder FormatLocksForLog(const google::protobuf::RepeatedPtrField<NKikimrDataEvents::TLock>& locks) {
    TStringBuilder text;
    for (const auto& lock : locks) {
        text << lock.ShortDebugString();
    }
    return text;
}

}
