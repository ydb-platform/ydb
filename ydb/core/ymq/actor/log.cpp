#include "log.h"

#include <ydb/core/ymq/base/queue_path.h>

namespace NKikimr::NSQS {

ui64 RequestIdSample(const TStringBuf& requestId) {
    if (Y_UNLIKELY(requestId.size() < sizeof(ui64))) {
        return 0;
    }
    const ui64 result = *reinterpret_cast<const ui64*>(requestId.data()); // We don't need real hashing,
                                                                          // because logging system will take
                                                                          // murmur hash from returned ui64
    return result;
}

TLogQueueName::TLogQueueName(const TString& userName, const TString& queueName, ui64 shard)
    : UserName(userName)
    , QueueName(queueName)
    , Shard(shard)
{
}

TLogQueueName::TLogQueueName(const TQueuePath& queuePath, ui64 shard)
    : TLogQueueName(queuePath.UserName, queuePath.QueueName, shard)
{
}

void TLogQueueName::OutTo(IOutputStream& out) const {
    out << "["sv << UserName;
    if (QueueName) {
        out << "/"sv << QueueName;
    }
    if (Shard != std::numeric_limits<ui64>::max()) {
        out << "/"sv << Shard;
    }
    out << "]"sv;
}

} // namespace NKikimr::NSQS

template<>
void Out<NKikimr::NSQS::TLogQueueName>(IOutputStream& out,
                                       typename TTypeTraits<NKikimr::NSQS::TLogQueueName>::TFuncParam nameForLogging) {
    nameForLogging.OutTo(out);
}
