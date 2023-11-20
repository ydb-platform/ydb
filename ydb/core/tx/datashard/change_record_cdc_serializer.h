#pragma once

#include "datashard_user_table.h"

namespace NKikimrClient {
    class TPersQueuePartitionRequest_TCmdWrite;
}

namespace NKikimr::NDataShard {

class TChangeRecord;

class IChangeRecordSerializer {
protected:
    using TCmdWrite = NKikimrClient::TPersQueuePartitionRequest_TCmdWrite;

public:
    virtual ~IChangeRecordSerializer() = default;
    virtual void Serialize(TCmdWrite& cmd, const TChangeRecord& record) = 0;
};

struct TChangeRecordSerializerOpts {
    TUserTable::TCdcStream::EFormat StreamFormat;
    TUserTable::TCdcStream::EMode StreamMode;
    TString AwsRegion;
    bool VirtualTimestamps = false;
    ui64 ShardId = 0;
};

IChangeRecordSerializer* CreateChangeRecordSerializer(const TChangeRecordSerializerOpts& opts);

}
