#pragma once

#include <ydb/core/tx/replication/ydb_proxy/topic_message.h>

namespace NKikimr::NReplication::NService {

struct TRecord: public TTopicMessage {
    explicit TRecord(ui64 offset, const TString& data)
        : TTopicMessage(offset, data)
    {}
};

}
