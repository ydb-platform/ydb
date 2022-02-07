#include "queries.h"

namespace NKikimr::NSQS {

extern const char* const GetQueueParamsQuery = R"__(
    (
        (let name     (Parameter 'NAME (DataType 'Utf8String)))
        (let userName (Parameter 'USER_NAME (DataType 'Utf8String)))

        (let queuesTable '%1$s/.Queues)

        (let queuesRow '(
            '('Account userName)
            '('QueueName name)))
        (let queuesSelect '(
            'QueueId
            'QueueState
            'FifoQueue
            'Shards
            'Version
            'Partitions))
        (let queuesRead
            (SelectRow queuesTable queuesRow queuesSelect))
        (let exists
            (Exists queuesRead))

        (return (Extend
            (AsList (SetResult 'exists exists))
            (ListIf exists (SetResult 'queue queuesRead))
        ))
    )
)__";

} // namespace NKikimr::NSQS
