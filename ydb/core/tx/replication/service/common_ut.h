#pragma once

#include "worker.h"

namespace NKikimr::NReplication::NService {

struct TRecord: public TEvWorker::TEvData::TRecord {
    explicit TRecord(ui64 offset, const TString& data)
        : TEvWorker::TEvData::TRecord(offset, data, TInstant::Zero(), "MessageGroupId", "ProducerId", 42)
    {}
};

}
