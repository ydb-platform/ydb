#include "queries.h"

#include "db_queries_defs.h"

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
            'Partitions
            'TablesFormat))
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

extern const char* const MatchQueueAttributesQuery = R"__(
    (
        (let queueIdNumber             (Parameter 'QUEUE_ID_NUMBER             (DataType 'Uint64)))
        (let queueIdNumberHash         (Parameter 'QUEUE_ID_NUMBER_HASH        (DataType 'Uint64)))
        (let name            (Parameter 'NAME              (DataType 'Utf8String)))
        (let fifo            (Parameter 'FIFO              (DataType 'Bool)))
        (let shards          (Parameter 'SHARDS            (DataType 'Uint64)))
        (let partitions      (Parameter 'PARTITIONS        (DataType 'Uint64)))
        (let expectedVersion (Parameter 'EXPECTED_VERSION  (DataType 'Uint64)))
        (let maxSize         (Parameter 'MAX_SIZE          (DataType 'Uint64)))
        (let delay           (Parameter 'DELAY             (DataType 'Uint64)))
        (let visibility      (Parameter 'VISIBILITY        (DataType 'Uint64)))
        (let retention       (Parameter 'RETENTION         (DataType 'Uint64)))
        (let dlqName         (Parameter 'DLQ_TARGET_NAME   (DataType 'Utf8String)))
        (let maxReceiveCount (Parameter 'MAX_RECEIVE_COUNT (DataType 'Uint64)))
        (let userName        (Parameter 'USER_NAME         (DataType 'Utf8String)))
        (let tags            (Parameter 'TAGS              (DataType 'Utf8String)))

        (let attrsTable ')__" QUEUE_TABLES_FOLDER_PARAM R"__(/Attributes)
        (let queuesTable ')__" ROOT_PARAM R"__(/.Queues)

        (let queuesRange '(
            '('Account userName userName)
            '('QueueName (Utf8String '"") (Void))))
        (let queues
            (Member (SelectRange queuesTable queuesRange '('QueueState) '()) 'List))

        (let queuesRow '(
            '('Account userName)
            '('QueueName name)))
        (let queuesSelect '(
            'QueueState
            'QueueId
            'FifoQueue
            'Shards
            'Partitions
            'DlqName
            'Version
            'Tags))
        (let queuesRead (SelectRow queuesTable queuesRow queuesSelect))

        (let queueExists
            (Coalesce
                (Or
                    (Equal (Uint64 '1) (Member queuesRead 'QueueState))
                    (Equal (Uint64 '3) (Member queuesRead 'QueueState))
                )
                (Bool 'false)))

        (let currentVersion
            (Coalesce
                (Member queuesRead 'Version)
                (Uint64 '0)
            )
        )

        (let sameParams
            (Coalesce
                (And
                    (And
                        (And
                            (And (Equal (Member queuesRead 'Shards) shards)
                                 (Equal (Coalesce (Member queuesRead 'Tags) (Utf8String '"{}")) tags))
                            (Equal (Member queuesRead 'Partitions) partitions))
                        (Equal (Member queuesRead 'FifoQueue) fifo))
                    (Equal  (Coalesce (Member queuesRead 'DlqName) (Utf8String '"")) dlqName))
                (Bool 'false)))

        (let attrRow '(
            )__" ATTRS_KEYS_PARAM R"__(
        ))
        (let attrSelect '(
            'DelaySeconds
            'MaximumMessageSize
            'MessageRetentionPeriod
            'MaxReceiveCount
            'VisibilityTimeout))
        (let attrRead (SelectRow attrsTable attrRow attrSelect))

        (let sameAttributes
            (Coalesce
                (And
                    (And
                        (And (Equal (Member attrRead 'DelaySeconds) delay)
                            (And (Equal (Member attrRead 'MaximumMessageSize) maxSize)
                                (Equal (Member attrRead 'MessageRetentionPeriod) retention)))
                        (Equal (Member attrRead 'VisibilityTimeout) visibility))
                    (Equal (Coalesce (Member attrRead 'MaxReceiveCount) (Uint64 '0)) maxReceiveCount))
                (Bool 'false)))

        (let sameVersion
            (Equal currentVersion expectedVersion))

        (let isSame
            (And
                queueExists
                (And
                    sameVersion
                    (And
                        sameAttributes
                        sameParams))))

        (let existingQueueId
            (Coalesce
                (Member queuesRead 'QueueId)
                (String '"")))

        (return (AsList
            (SetResult 'exists queueExists)
            (SetResult 'sameVersion sameVersion)
            (SetResult 'id existingQueueId)
            (SetResult 'isSame isSame)))
    )
)__";


} // namespace NKikimr::NSQS
