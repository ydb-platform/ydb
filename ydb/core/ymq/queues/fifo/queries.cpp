#include "queries.h"
#include <ydb/core/ymq/base/constants.h>
#include <ydb/core/ymq/queues/common/db_queries_defs.h>


namespace NKikimr::NSQS {
namespace {

const char* const ChangeMessageVisibilityQuery = R"__(
    (
        (let queueIdNumber      (Parameter 'QUEUE_ID_NUMBER (DataType 'Uint64)))
        (let queueIdNumberHash  (Parameter 'QUEUE_ID_NUMBER_HASH (DataType 'Uint64)))
        (let now                (Parameter 'NOW      (DataType 'Uint64)))
        (let groupsReadAttemptIdsPeriod (Parameter 'GROUPS_READ_ATTEMPT_IDS_PERIOD (DataType 'Uint64)))
        (let keys (Parameter 'KEYS
            (ListType (StructType
                '('Offset (DataType 'Uint64))
                '('GroupId (DataType 'String))
                '('ReceiveAttemptId (DataType 'Utf8String))
                '('LockTimestamp (DataType 'Uint64))
                '('NewVisibilityDeadline (DataType 'Uint64))))))

        (let groupTable ')__" QUEUE_TABLES_FOLDER_PARAM R"__(/Groups)
        (let readsTable ')__" QUEUE_TABLES_FOLDER_PARAM R"__(/Reads)

        (let records
            (MapParameter keys (lambda '(item) (block '(
                (let groupRow '(
                    )__" QUEUE_ID_KEYS_PARAM  R"__(
                    '('GroupId (Member item 'GroupId))))
                (let groupSelect '(
                    'Head
                    'LockTimestamp
                    'VisibilityDeadline
                    'ReceiveAttemptId))
                (let groupRead (SelectRow groupTable groupRow groupSelect))

                (let readsRow '(
                    )__" QUEUE_ID_KEYS_PARAM  R"__(
                    '('ReceiveAttemptId (Member item 'ReceiveAttemptId))))
                (let readsSelect '(
                    'Deadline))
                (let readsRead (SelectRow readsTable readsRow readsSelect))

                (let exists (Exists groupRead))

                (let changeCond
                    (IfPresent groupRead
                        (lambda '(x)
                            (Coalesce
                                (And
                                    (Equal (Member x 'Head) (Member item 'Offset))
                                    (LessOrEqual now (Member groupRead 'VisibilityDeadline)))
                             (Bool 'false)))
                    (Bool 'false)))

                (let lockTimestamp (Member item 'LockTimestamp))
                (let readDeadline (Member readsRead 'Deadline))
                (let readCreateTimestamp (Sub readDeadline groupsReadAttemptIdsPeriod))
                (let sameReceiveAttempt
                    (Coalesce
                        (And
                            (Less lockTimestamp readDeadline)
                            (GreaterOrEqual lockTimestamp readCreateTimestamp)
                        )
                        (Bool 'false)
                    )
                )

                (return (AsStruct
                    '('GroupId (Member item 'GroupId))
                    '('Exists exists)
                    '('ChangeCond changeCond)
                    '('NewVisibilityDeadline (Member item 'NewVisibilityDeadline))
                    '('ReceiveAttemptId (Member item 'ReceiveAttemptId))
                    '('SameReceiveAttempt sameReceiveAttempt))))))))

        (let recordsToChange
            (Filter records (lambda '(item) (block '(
                (return (And (Member item 'Exists) (Member item 'ChangeCond)))
        )))))

        (let recordsToEraseReceiveAttempt
            (Filter records (lambda '(item) (block '(
                (return (Member item 'SameReceiveAttempt))
        )))))

        (return (Extend
            (AsList (SetResult 'result records))
            (AsList (SetResult 'result records))
            (AsList (SetResult 'recordsToEraseReceiveAttempt recordsToEraseReceiveAttempt))

            (Map recordsToChange (lambda '(item) (block '(
                (let groupRow '(
                    )__" QUEUE_ID_KEYS_PARAM  R"__(
                    '('GroupId (Member item 'GroupId))))
                (let visibilityUpdate '(
                    '('VisibilityDeadline (Member item 'NewVisibilityDeadline))))
                (return (UpdateRow groupTable groupRow visibilityUpdate))
            ))))

            (Map recordsToEraseReceiveAttempt (lambda '(item) (block '(
                (let readsRow '(
                    )__" QUEUE_ID_KEYS_PARAM  R"__(
                    '('ReceiveAttemptId (Member item 'ReceiveAttemptId))))
                (return (EraseRow readsTable readsRow))
            ))))
        ))
    )
)__";

const char* const PurgeQueueQuery = R"__(
    (
        (let queueIdNumber      (Parameter 'QUEUE_ID_NUMBER (DataType 'Uint64)))
        (let queueIdNumberHash  (Parameter 'QUEUE_ID_NUMBER_HASH (DataType 'Uint64)))
        (let offsetFrom     (Parameter 'OFFSET_FROM (DataType 'Uint64)))
        (let offsetTo       (Parameter 'OFFSET_TO   (DataType 'Uint64)))
        (let now            (Parameter 'NOW         (DataType 'Uint64)))
        (let shard          (Parameter 'SHARD       (DataType 'Uint64)))
        (let batchSize      (Parameter 'BATCH_SIZE  (DataType 'Uint64)))

        (let messageTable ')__" QUEUE_TABLES_FOLDER_PARAM R"__(/Messages)
        (let stateTable   ')__" QUEUE_TABLES_FOLDER_PARAM R"__(/State)

        (let stateRow '(
            )__" STATE_KEYS_PARAM  R"__(
        ))
        (let stateSelect '(
            'CleanupVersion
            'LastModifiedTimestamp))
        (let stateRead
            (SelectRow stateTable stateRow stateSelect))

        (let modifiedTimestamp (Max now (Member stateRead 'LastModifiedTimestamp)))

        (let messageRange '(
            )__" QUEUE_ID_KEYS_RANGE_PARAM  R"__(
            '('Offset offsetFrom offsetTo)))
        (let messageSelect '(
            'SentTimestamp
            'Offset
            'RandomId))

        (let selectResult (SelectRange messageTable messageRange messageSelect '('('"ItemsLimit" batchSize))))

        (let messages (Member selectResult 'List))
        (let truncated (Member selectResult 'Truncated))
        (let newCleanupVersion (Add (Member stateRead 'CleanupVersion) (Uint64 '1)))

        (let stateUpdate '(
            '('LastModifiedTimestamp modifiedTimestamp)
            '('CleanupVersion newCleanupVersion)
        ))

        (return (Extend
            (AsList (SetResult 'messages messages))
            (AsList (SetResult 'truncated truncated))
            (AsList (SetResult 'cleanupVersion newCleanupVersion))
            (AsList (UpdateRow stateTable stateRow stateUpdate))
        ))
    )
)__";

const char* const PurgeQueueStage2Query = R"__(
    (
        (let queueIdNumber      (Parameter 'QUEUE_ID_NUMBER (DataType 'Uint64)))
        (let queueIdNumberHash  (Parameter 'QUEUE_ID_NUMBER_HASH (DataType 'Uint64)))
        (let cleanupVersion (Parameter 'CLEANUP_VERSION (DataType 'Uint64)))
        (let now  (Parameter 'NOW (DataType 'Uint64)))
        (let messages (Parameter 'MESSAGES
            (ListType (StructType
                '('Offset (DataType 'Uint64))
                '('RandomId (DataType 'Uint64))
                '('SentTimestamp (DataType 'Uint64))
        ))))

        (let dataTable    ')__" QUEUE_TABLES_FOLDER_PARAM R"__(/Data)
        (let groupTable   ')__" QUEUE_TABLES_FOLDER_PARAM R"__(/Groups)
        (let messageTable ')__" QUEUE_TABLES_FOLDER_PARAM R"__(/Messages)
        (let sentTsIdx    ')__" QUEUE_TABLES_FOLDER_PARAM R"__(/SentTimestampIdx)
        (let stateTable   ')__" QUEUE_TABLES_FOLDER_PARAM R"__(/State)

        (let records
            (MapParameter messages (lambda '(item) (block '(
                (let messageRow '(
                    )__" QUEUE_ID_KEYS_PARAM  R"__(
                    '('Offset (Member item 'Offset))))
                (let messageSelect '(
                    'Offset
                    'RandomId
                    'GroupId
                    'SentTimestamp
                    'NextOffset
                    'NextRandomId))

                (let msg (SelectRow messageTable messageRow messageSelect))
                (return msg))))))

        (let recordsExisted
            (Filter records (lambda '(item) (block '(
                (return (Exists item))
        )))))

        (let stateRow '(
            )__" STATE_KEYS_PARAM  R"__(
        ))
        (let stateSelect '(
            'MessageCount
            'CleanupVersion
            'LastModifiedTimestamp))

        (let stateRead (SelectRow stateTable stateRow stateSelect))
        (let count (Sub (Member stateRead 'MessageCount) (Length recordsExisted)))
        (let modifiedTimestamp (Max now (Member stateRead 'LastModifiedTimestamp)))

        (let stateUpdate '(
            '('LastModifiedTimestamp modifiedTimestamp)
            '('MessageCount count)))

        (let versionIsSame
            (Coalesce
                (Equal (Member stateRead 'CleanupVersion) cleanupVersion)
                (Bool 'false)
            )
        )

        (return (Extend
            (AsList (SetResult 'versionIsSame versionIsSame))
            (AsList (SetResult 'messagesDeleted
                (If versionIsSame
                    (Length recordsExisted)
                    (Uint64 '0))
            ))
            (AsList (SetResult 'newMessagesCount
                (If versionIsSame
                    count
                    (Member stateRead 'MessageCount))
            ))
            (If versionIsSame
                (AsList (UpdateRow stateTable stateRow stateUpdate))
            (AsList (Void)))

            (If versionIsSame
                (Map recordsExisted (lambda '(item) (block '(
                    (let groupRow '(
                        )__" QUEUE_ID_KEYS_PARAM  R"__(
                        '('GroupId (Member item 'GroupId))))
                    (let update '(
                        '('RandomId (Member item 'NextRandomId))
                        '('Head (Member item 'NextOffset))))

                    # If we delete the last message, we need to delete the empty group
                    (let groupIsEmpty
                        (Coalesce
                            (Equal (Member item 'NextOffset) (Uint64 '0))
                            (Bool 'false)
                        )
                    )

                    (return
                        (If groupIsEmpty
                            (EraseRow groupTable groupRow)
                            (UpdateRow groupTable groupRow update)
                        )
                    )))))
                (AsList (Void)))

            (If versionIsSame
                (Map recordsExisted (lambda '(item) (block '(
                    (let row '(
                        )__" QUEUE_ID_KEYS_PARAM  R"__(
                        '('RandomId (Member item 'RandomId))
                        '('Offset   (Member item 'Offset))))
                    (return (EraseRow dataTable row))))))
                (AsList (Void)))

            (If versionIsSame
                (Map recordsExisted (lambda '(item) (block '(
                    (let row '(
                        )__" QUEUE_ID_KEYS_PARAM  R"__(
                        '('Offset (Member item 'Offset))))
                    (return (EraseRow messageTable row))))))
                (AsList (Void)))

            (If versionIsSame
                (Map recordsExisted (lambda '(item) (block '(
                    (let row '(
                        )__" QUEUE_ID_KEYS_PARAM  R"__(
                        '('SentTimestamp (Member item 'SentTimestamp))
                        '('Offset        (Member item 'Offset))))
                    (return (EraseRow sentTsIdx row))))))
                (AsList (Void)))
        ))
    )
)__";

const char* const DeleteMessageQuery = R"__(
    (
        (let queueIdNumber      (Parameter 'QUEUE_ID_NUMBER (DataType 'Uint64)))
        (let queueIdNumberHash  (Parameter 'QUEUE_ID_NUMBER_HASH (DataType 'Uint64)))
        (let keys (Parameter 'KEYS
            (ListType (StructType
                '('GroupId (DataType 'String))
                '('Offset  (DataType 'Uint64))
                '('LockTimestamp (DataType 'Uint64))
                '('ReceiveAttemptId (DataType 'Utf8String))))))
        (let now  (Parameter 'NOW (DataType 'Uint64)))
        (let groupsReadAttemptIdsPeriod (Parameter 'GROUPS_READ_ATTEMPT_IDS_PERIOD (DataType 'Uint64)))

        (let dataTable    ')__" QUEUE_TABLES_FOLDER_PARAM R"__(/Data)
        (let groupTable   ')__" QUEUE_TABLES_FOLDER_PARAM R"__(/Groups)
        (let messageTable ')__" QUEUE_TABLES_FOLDER_PARAM R"__(/Messages)
        (let sentTsIdx    ')__" QUEUE_TABLES_FOLDER_PARAM R"__(/SentTimestampIdx)
        (let stateTable   ')__" QUEUE_TABLES_FOLDER_PARAM R"__(/State)
        (let readsTable   ')__" QUEUE_TABLES_FOLDER_PARAM R"__(/Reads)

        (let records
            (MapParameter keys (lambda '(item) (block '(
                (let messageRow '(
                    )__" QUEUE_ID_KEYS_PARAM  R"__(
                    '('Offset (Member item 'Offset))))
                (let messageSelect '(
                    'Offset
                    'RandomId
                    'NextOffset
                    'NextRandomId
                    'SentTimestamp))

                (let groupRow '(
                    )__" QUEUE_ID_KEYS_PARAM  R"__(
                    '('GroupId (Member item 'GroupId))))
                (let groupSelect '(
                    'GroupId
                    'Head
                    'LockTimestamp
                    'ReceiveAttemptId))

                (let readsRow '(
                    )__" QUEUE_ID_KEYS_PARAM  R"__(
                    '('ReceiveAttemptId (Member item 'ReceiveAttemptId))))
                (let readsSelect '(
                    'Deadline))
                (let readsRead (SelectRow readsTable readsRow readsSelect))

                (let lockTimestamp (Member item 'LockTimestamp))
                (let readDeadline (Member readsRead 'Deadline))
                (let readCreateTimestamp (Sub readDeadline groupsReadAttemptIdsPeriod))
                (let sameReceiveAttempt
                    (Coalesce
                        (And
                            (Less lockTimestamp readDeadline)
                            (GreaterOrEqual lockTimestamp readCreateTimestamp)
                        )
                        (Bool 'false)
                    )
                )

                (return '(
                    (SelectRow groupTable groupRow groupSelect)
                    (SelectRow messageTable messageRow messageSelect)
                    (Member item 'LockTimestamp)
                    (Member item 'ReceiveAttemptId)
                    sameReceiveAttempt)))))))

        (let valid
            (Filter records (lambda '(item) (block '(
                (let group (Nth item '0))
                (let msg   (Nth item '1))
                (let lockTimestamp (Nth item '2))

                (return
                    (Coalesce
                        (And
                            (Equal  (Member group 'Head) (Member msg 'Offset))
                            (Equal  (Member group 'LockTimestamp) lockTimestamp))
                        (Bool 'false))))))))

        (let validWithReceiveAttemptToDelete
            (Filter records (lambda '(item) (block '(
                (let sameReceiveAttempt (Nth item '4))
                (return sameReceiveAttempt))))))

        (let result
            (Map valid (lambda '(item) (block '(
                (let msg (Nth item '1))

                (return (AsStruct
                    '('Offset (Member msg 'Offset)))))))))

        (let stateRow '(
            )__" STATE_KEYS_PARAM  R"__(
        ))
        (let stateSelect '(
            'MessageCount
            'LastModifiedTimestamp))
        (let stateRead (SelectRow stateTable stateRow stateSelect))

        (let modifiedTimestamp (Max now (Member stateRead 'LastModifiedTimestamp)))
        (let count (Sub (Member stateRead 'MessageCount) (Length valid)))

        (let stateUpdate '(
            '('LastModifiedTimestamp modifiedTimestamp)
            '('MessageCount count)))

        (return (Extend
            (AsList (SetResult 'deleted result))
            (AsList (SetResult 'newMessagesCount count))
            (ListIf (HasItems valid) (UpdateRow stateTable stateRow stateUpdate))

            (Map valid (lambda '(item) (block '(
                (let group (Nth item '0))
                (let msg   (Nth item '1))

                (let row '(
                    )__" QUEUE_ID_KEYS_PARAM  R"__(
                    '('GroupId (Member group 'GroupId))))
                (let update '(
                    '('RandomId (Member msg 'NextRandomId))
                    '('Head (Member msg 'NextOffset))
                    '('LockTimestamp (Uint64 '0))
                    '('VisibilityDeadline (Uint64 '0))))

                (return
                    (If (Coalesce (Equal (Member msg 'NextOffset) (Uint64 '0)) (Bool 'false))
                        (EraseRow  groupTable row)
                        (UpdateRow groupTable row update)))))))

            (Map valid (lambda '(item) (block '(
                (let row '(
                    )__" QUEUE_ID_KEYS_PARAM  R"__(
                    '('RandomId (Member (Nth item '1) 'RandomId))
                    '('Offset   (Member (Nth item '1) 'Offset))))
                (return (EraseRow dataTable row))))))

            (Map valid (lambda '(item) (block '(
                (let row '(
                    )__" QUEUE_ID_KEYS_PARAM  R"__(
                    '('Offset (Member (Nth item '1) 'Offset))))
                (return (EraseRow messageTable row))))))

            (Map valid (lambda '(item) (block '(
                (let row '(
                    )__" QUEUE_ID_KEYS_PARAM  R"__(
                    '('SentTimestamp (Member (Nth item '1) 'SentTimestamp))
                    '('Offset        (Member (Nth item '1) 'Offset))))
                (return (EraseRow sentTsIdx row))))))

            (Map validWithReceiveAttemptToDelete (lambda '(item) (block '(
                (let row '(
                    )__" QUEUE_ID_KEYS_PARAM  R"__(
                    '('ReceiveAttemptId (Nth item '3))))
                (return (EraseRow readsTable row))))))
        ))
    )
)__";

const char* const SetQueueAttributesQuery = R"__(
    (
        (let queueIdNumber             (Parameter 'QUEUE_ID_NUMBER             (DataType 'Uint64)))
        (let queueIdNumberHash         (Parameter 'QUEUE_ID_NUMBER_HASH        (DataType 'Uint64)))
        (let delay                     (Parameter 'DELAY                       (OptionalType (DataType 'Uint64))))
        (let retention                 (Parameter 'RETENTION                   (OptionalType (DataType 'Uint64))))
        (let visibility                (Parameter 'VISIBILITY                  (OptionalType (DataType 'Uint64))))
        (let wait                      (Parameter 'WAIT                        (OptionalType (DataType 'Uint64))))
        (let maxMessageSize            (Parameter 'MAX_MESSAGE_SIZE            (OptionalType (DataType 'Uint64))))
        (let contentBasedDeduplication (Parameter 'CONTENT_BASED_DEDUPLICATION (OptionalType (DataType 'Bool))))
        (let maxReceiveCount           (Parameter 'MAX_RECEIVE_COUNT           (OptionalType (DataType 'Uint64))))
        (let dlqArn                    (Parameter 'DLQ_TARGET_ARN              (OptionalType (DataType 'Utf8String))))
        (let dlqName                   (Parameter 'DLQ_TARGET_NAME             (OptionalType (DataType 'Utf8String))))
        (let userName                  (Parameter 'USER_NAME                   (DataType 'Utf8String)))

        (let attrsTable ')__" QUEUE_TABLES_FOLDER_PARAM R"__(/Attributes)

        (let attrsRow '(
            )__" ATTRS_KEYS_PARAM R"__())
        (let attrsSelect '(
            'DelaySeconds
            'MessageRetentionPeriod
            'ReceiveMessageWaitTime
            'VisibilityTimeout
            'MaximumMessageSize
            'DlqName
            'DlqArn
            'MaxReceiveCount
            'ContentBasedDeduplication))
        (let attrsRead (SelectRow attrsTable attrsRow attrsSelect))

        (let attrsUpdate '(
            '('DelaySeconds (Coalesce delay (Member attrsRead 'DelaySeconds)))
            '('MessageRetentionPeriod (Coalesce retention (Member attrsRead 'MessageRetentionPeriod)))
            '('ReceiveMessageWaitTime (Coalesce wait (Member attrsRead 'ReceiveMessageWaitTime)))
            '('VisibilityTimeout (Coalesce visibility (Member attrsRead 'VisibilityTimeout)))
            '('MaximumMessageSize (Coalesce maxMessageSize (Member attrsRead 'MaximumMessageSize)))
            '('MaxReceiveCount (Coalesce maxReceiveCount (Member attrsRead 'MaxReceiveCount)))
            '('DlqName (Coalesce dlqName (Member attrsRead 'DlqName)))
            '('DlqArn (Coalesce dlqArn (Member attrsRead 'DlqArn)))
            '('ContentBasedDeduplication (Coalesce contentBasedDeduplication (Member attrsRead 'ContentBasedDeduplication)))))

        (let queuesTable ')__" ROOT_PARAM R"__(/.Queues)
        (let queuesRow '(
            '('Account userName)
            '('QueueName (Utf8String '")__" QUEUE_NAME_PARAM R"__("))))

        (let queuesRowSelect '(
            'DlqName))

        (let queuesRowRead (SelectRow queuesTable queuesRow queuesRowSelect))

        (let queuesRowUpdate '(
            '('DlqName (Coalesce dlqName (Member queuesRowRead 'DlqName)))))

        (return (AsList
            (UpdateRow attrsTable attrsRow attrsUpdate)
            (UpdateRow queuesTable queuesRow queuesRowUpdate)))
    )
)__";

const char* const InternalGetQueueAttributesQuery = R"__(
    (
        (let queueIdNumber      (Parameter 'QUEUE_ID_NUMBER (DataType 'Uint64)))
        (let queueIdNumberHash  (Parameter 'QUEUE_ID_NUMBER_HASH (DataType 'Uint64)))
        (let name     (Parameter 'NAME (DataType 'Utf8)))
        (let userName (Parameter 'USER_NAME (DataType 'Utf8)))

        (let attrsTable ')__" QUEUE_TABLES_FOLDER_PARAM R"__(/Attributes)

        (let attrsRow '(
            )__" ATTRS_KEYS_PARAM R"__())
        (let attrsSelect '(
            'ContentBasedDeduplication
            'DelaySeconds
            'FifoQueue
            'MaximumMessageSize
            'MessageRetentionPeriod
            'ReceiveMessageWaitTime
            'MaxReceiveCount
            'DlqName
            'DlqArn
            'VisibilityTimeout
            'ShowDetailedCountersDeadline))

        (let attrsRead (SelectRow attrsTable attrsRow attrsSelect))

        (let tags
            (If (Not (Exists attrsRead))
                (Utf8 '"{}")
                (block '(
                    (let queuesTable ')__" ROOT_PARAM R"__(/.Queues)

                    (let queuesRow '(
                        '('Account userName)
                        '('QueueName (Utf8String '")__" QUEUE_NAME_PARAM R"__("))))

                    (let queuesRowSelect '('QueueName 'Tags))
                    (let queuesRowRead (SelectRow queuesTable queuesRow queuesRowSelect))

                    (let str (Coalesce (Member queuesRowRead 'Tags) (Utf8 '"{}")))
                    (return (If (Equal (Utf8 '"") str)
                                (Utf8 '"{}")
                                str))))))

        (return (AsList
            (SetResult 'queueExists (Exists attrsRead))
            (SetResult 'attrs attrsRead)
            (SetResult 'tags tags)))
    )
)__";

const char* const TagQueueQuery = R"__(
    (
        (let name     (Parameter 'NAME (DataType 'Utf8)))
        (let userName (Parameter 'USER_NAME (DataType 'Utf8)))
        (let tags     (Parameter 'TAGS (DataType 'Utf8)))
        (let oldTags  (Parameter 'OLD_TAGS (DataType 'Utf8)))

        (let queuesTable ')__" ROOT_PARAM R"__(/.Queues)

        (let queuesRow '(
            '('Account userName)
            '('QueueName (Utf8 '")__" QUEUE_NAME_PARAM R"__("))))

        (let queuesRead (SelectRow queuesTable queuesRow '('Tags)))

        (let curTags
            (block '(
                (let str (Coalesce (Member queuesRead 'Tags) (Utf8 '"{}")))
                (return (If (Equal (Utf8 '"") str)
                            (Utf8 '"{}")
                            str)))))

        (let queuesRowUpdate '(
            '('Tags tags)))

        (return
            (If (Equal oldTags curTags)
                (AsList (UpdateRow queuesTable queuesRow queuesRowUpdate)
                        (SetResult 'updated (Bool 'true)))
                (AsList (SetResult 'updated (Bool 'false)))))
    )
)__";

const char* const ListQueuesQuery = R"__(
    (
        (let folderId (Parameter 'FOLDERID  (DataType 'Utf8String)))
        (let userName (Parameter 'USER_NAME (DataType 'Utf8String)))

        (let queuesTable ')__" ROOT_PARAM R"__(/.Queues)

        (let skipFolderIdFilter (Equal folderId (Utf8String '"")))

        (let queuesRange '(
            '('Account userName userName)
            '('QueueName (Utf8String '"") (Void))))
        (let queueSelect '('QueueName 'QueueId 'QueueState 'FifoQueue 'CreatedTimestamp 'CustomQueueName 'FolderId 'MasterTabletId 'Version 'Shards 'TablesFormat))
        (let queues (Member (SelectRange queuesTable queuesRange queueSelect '()) 'List))

        (let filtered (Filter queues (lambda '(item) (block '(
            (return (Coalesce
                (And
                    (Or
                        (Equal (Member item 'QueueState) (Uint64 '1))
                        (Equal (Member item 'QueueState) (Uint64 '3)))
                    (Or
                        (Equal (Member item 'FolderId) folderId)
                        skipFolderIdFilter))
                (Bool 'false)))
        )))))

        (return (AsList
            (SetResult 'queues filtered)))
    )
)__";

const char* const LockGroupsQuery = R"__(
    (
        (let queueIdNumber      (Parameter 'QUEUE_ID_NUMBER      (DataType 'Uint64)))
        (let queueIdNumberHash  (Parameter 'QUEUE_ID_NUMBER_HASH (DataType 'Uint64)))
        (let attemptId          (Parameter 'ATTEMPT_ID           (DataType 'Utf8String)))
        (let now                (Parameter 'NOW                  (DataType 'Uint64)))
        (let count              (Parameter 'COUNT                (DataType 'Uint64)))
        (let visibilityTimeout  (Parameter 'VISIBILITY_TIMEOUT   (DataType 'Uint64)))
        (let groupsReadAttemptIdsPeriod (Parameter 'GROUPS_READ_ATTEMPT_IDS_PERIOD (DataType 'Uint64)))
        (let fromGroup          (Parameter 'FROM_GROUP           (DataType 'String)))
        (let batchSize          (Parameter 'BATCH_SIZE           (DataType 'Uint64)))

        (let groupTable ')__" QUEUE_TABLES_FOLDER_PARAM R"__(/Groups)
        (let readsTable ')__" QUEUE_TABLES_FOLDER_PARAM R"__(/Reads)
        (let sentTsIdx  ')__" QUEUE_TABLES_FOLDER_PARAM R"__(/SentTimestampIdx)

        (let readsRow '(
            )__" QUEUE_ID_KEYS_PARAM  R"__(
            '('ReceiveAttemptId attemptId)))
        (let readsSelect (SelectRow readsTable readsRow '('Deadline)))
        (let readsUpdate '(
            '('Deadline (Add now groupsReadAttemptIdsPeriod))))

        (let sameCond (IfPresent readsSelect (lambda '(x) (Coalesce (Less now (Member x 'Deadline)) (Bool 'false))) (Bool 'false)))

        (let groupRange '(
            )__" QUEUE_ID_KEYS_RANGE_PARAM  R"__(
            '('GroupId fromGroup (Void))))
        (let groupSelect '(
            'GroupId
            'RandomId
            'Head
            'ReceiveAttemptId
            'VisibilityDeadline))
        (let groupsRead (SelectRange groupTable groupRange groupSelect '('('"ItemsLimit" batchSize))))
        (let groups (Member groupsRead 'List))
        (let truncated (Member groupsRead 'Truncated))
        (let lastProcessedGroup (ToOptional (Skip groups (Sub (Length groups) (Uint64 '1)))))

        (let previous (Take (Filter groups (lambda '(item) (block '(
            (return (Coalesce (Equal (Member item 'ReceiveAttemptId) attemptId) (Bool 'false)))
        )))) count))

        (let filtered_1 (Filter groups (lambda '(item) (block '(
            (return (Coalesce (Less (Member item 'VisibilityDeadline) now) (Bool 'false)))
        )))))

        (let filtered (Take (Sort filtered_1 (Bool 'true) (lambda '(item) (block '(
            (return (Member item 'Head))
        )))) count))

        (let update (lambda '(item) (block '(
            (let groupId (Member item 'GroupId))

            (let groupRow '(
                )__" QUEUE_ID_KEYS_PARAM  R"__(
                '('GroupId groupId)))
            (let groupUpdate '(
                '('ReceiveAttemptId attemptId)
                '('LockTimestamp now)
                '('VisibilityDeadline (Add now visibilityTimeout))))
            (return (UpdateRow groupTable groupRow groupUpdate))
        ))))

        (return (Extend
            (AsList (SetResult 'sameCond sameCond))
            (AsList (SetResult 'truncated truncated))
            (AsList (SetResult 'lastProcessedGroup lastProcessedGroup))
            (AsList (If sameCond (SetResult 'offsets previous) (SetResult 'offsets filtered)))
            (ListIf (And (Not sameCond) (HasItems filtered)) (UpdateRow readsTable readsRow readsUpdate))
            (If sameCond (Map previous update) (Map filtered update))))
    )
)__";

const char* const ReadMessageQuery = R"__(
    (
        (let queueIdNumber      (Parameter 'QUEUE_ID_NUMBER      (DataType 'Uint64)))
        (let queueIdNumberHash  (Parameter 'QUEUE_ID_NUMBER_HASH (DataType 'Uint64)))
        (let now                (Parameter 'NOW                  (DataType 'Uint64)))
        (let keys (Parameter 'KEYS
            (ListType (StructType
                '('RandomId (DataType 'Uint64))
                '('Offset   (DataType 'Uint64))))))

        (let dataTable    ')__" QUEUE_TABLES_FOLDER_PARAM R"__(/Data)
        (let messageTable ')__" QUEUE_TABLES_FOLDER_PARAM R"__(/Messages)

        (let unfilteredResult
            (MapParameter keys (lambda '(item) (block '(
                (let dataRow '(
                    )__" QUEUE_ID_KEYS_PARAM  R"__(
                    '('RandomId (Member item 'RandomId))
                    '('Offset   (Member item 'Offset))))
                (let dataFields '(
                    'Offset
                    'DedupId
                    'Attributes
                    'Data
                    'MessageId
                    'SenderId))

                (let messageRow '(
                    )__" QUEUE_ID_KEYS_PARAM  R"__(
                    '('Offset (Member item 'Offset))))
                (let messageFields '(
                    'GroupId
                    'Offset
                    'ReceiveCount
                    'FirstReceiveTimestamp
                    'SentTimestamp))

                (let sourceDataFieldsRead (SelectRow dataTable dataRow dataFields))

                (return (AsStruct
                    '('Valid (Exists sourceDataFieldsRead))
                    '('SourceDataFieldsRead sourceDataFieldsRead)
                    '('SourceMessageFieldsRead (SelectRow messageTable messageRow messageFields)))))))))

        (let result
            (Filter unfilteredResult (lambda '(item) (block '(
                (return (Coalesce (Member item 'Valid) (Bool 'false)))
        )))))

        (return (Extend
            (AsList (SetResult 'dlqExists (Bool 'true)))
            (AsList (SetResult 'result result))
            (AsList (SetResult 'movedMessagesCount (Uint64 '0)))

            (Map result (lambda '(item) (block '(
                (let message (Member item 'SourceMessageFieldsRead))
                (let row '(
                    )__" QUEUE_ID_KEYS_PARAM  R"__(
                    '('Offset (Member message 'Offset))))
                (let receiveTimestamp
                    (If (Coalesce (Equal (Member message 'FirstReceiveTimestamp) (Uint64 '0)) (Bool 'false)) now (Member message 'FirstReceiveTimestamp)))
                (let update '(
                    '('FirstReceiveTimestamp receiveTimestamp)
                    '('ReceiveCount (Add (Member message 'ReceiveCount) (Uint32 '1)))))
                (return (UpdateRow messageTable row update))))))))
    )
)__";

const char* const ReadOrRedriveMessageQuery = R"__(
    (
        (let keys (Parameter 'KEYS
            (ListType (StructType
                '('RandomId (DataType 'Uint64))
                '('GroupId  (DataType 'String))
                '('Index    (DataType 'Uint64))
                '('Offset   (DataType 'Uint64))))))
        (let queueIdNumber      (Parameter 'QUEUE_ID_NUMBER      (DataType 'Uint64)))
        (let queueIdNumberHash  (Parameter 'QUEUE_ID_NUMBER_HASH (DataType 'Uint64)))

        (let dlqIdNumber        (Parameter 'DLQ_ID_NUMBER        (DataType 'Uint64)))
        (let dlqIdNumberHash    (Parameter 'DLQ_ID_NUMBER_HASH   (DataType 'Uint64)))

        (let now                (Parameter 'NOW                  (DataType 'Uint64)))
        (let maxReceiveCount    (Parameter 'MAX_RECEIVE_COUNT    (DataType 'Uint32)))
        (let randomId           (Parameter 'RANDOM_ID            (DataType 'Uint64)))

        (let sourceDataTable    ')__" QUEUE_TABLES_FOLDER_PARAM R"__(/Data)
        (let sourceStateTable   ')__" QUEUE_TABLES_FOLDER_PARAM R"__(/State)
        (let sourceMsgTable     ')__" QUEUE_TABLES_FOLDER_PARAM R"__(/Messages)
        (let sourceGroupTable   ')__" QUEUE_TABLES_FOLDER_PARAM R"__(/Groups)
        (let sourceSentTsIdx    ')__" QUEUE_TABLES_FOLDER_PARAM R"__(/SentTimestampIdx)

        (let dlqDataTable       ')__" DLQ_TABLES_FOLDER_PARAM R"__(/Data)
        (let dlqDedupTable      ')__" DLQ_TABLES_FOLDER_PARAM R"__(/Deduplication)
        (let dlqStateTable      ')__" DLQ_TABLES_FOLDER_PARAM R"__(/State)
        (let dlqMsgTable        ')__" DLQ_TABLES_FOLDER_PARAM R"__(/Messages)
        (let dlqGroupTable      ')__" DLQ_TABLES_FOLDER_PARAM R"__(/Groups)
        (let dlqSentTsIdx       ')__" DLQ_TABLES_FOLDER_PARAM R"__(/SentTimestampIdx)

        (let unfilteredAllMessages
            (MapParameter keys (lambda '(item) (block '(
                (let dataRow '(
                    )__" QUEUE_ID_KEYS_PARAM  R"__(
                    '('RandomId (Member item 'RandomId))
                    '('Offset   (Member item 'Offset))))
                (let dataFields '(
                    'RandomId
                    'Offset
                    'DedupId
                    'Attributes
                    'Data
                    'MessageId
                    'SenderId))

                (let messageRow '(
                    )__" QUEUE_ID_KEYS_PARAM  R"__(
                    '('Offset (Member item 'Offset))))
                (let messageFields '(
                    'GroupId
                    'Offset
                    'ReceiveCount
                    'FirstReceiveTimestamp
                    'NextOffset
                    'NextRandomId
                    'SentTimestamp))

                (let dlqGroupRow '(
                    )__" DLQ_ID_KEYS_PARAM  R"__(
                    '('GroupId (Member item 'GroupId))))
                (let dlqGroupSelect '(
                    'Head
                    'ReceiveAttemptId
                    'Tail))
                (let dlqGroupRead (SelectRow dlqGroupTable dlqGroupRow dlqGroupSelect))
                (let dlqTail (IfPresent dlqGroupRead (lambda '(x) (Coalesce (Member x 'Tail) (Uint64 '0))) (Uint64 '0)))

                (let sourceDataFieldsRead (SelectRow sourceDataTable dataRow dataFields))
                (let sourceMessageFieldsRead (SelectRow sourceMsgTable messageRow messageFields))

                (return (AsStruct
                    '('Valid (Exists sourceDataFieldsRead))
                    '('SourceDataFieldsRead sourceDataFieldsRead)
                    '('SourceMessageFieldsRead sourceMessageFieldsRead)
                    '('DlqGroupRead dlqGroupRead)
                    '('DlqTail dlqTail)
                    '('Attributes (Member sourceDataFieldsRead 'Attributes))
                    '('Data       (Member sourceDataFieldsRead 'Data))
                    '('MessageId  (Member sourceDataFieldsRead 'MessageId))
                    '('SenderId   (Member sourceDataFieldsRead 'SenderId))
                    '('DeduplicationId (Member sourceDataFieldsRead 'DedupId))
                    '('SourceRandomId (Member sourceDataFieldsRead 'RandomId))
                    '('SourceOffset (Member sourceMessageFieldsRead 'Offset))
                    '('SourceNextOffset (Member sourceMessageFieldsRead 'NextOffset))
                    '('SourceNextRandomId (Member sourceMessageFieldsRead 'NextRandomId))
                    '('SourceSentTimestamp (Member sourceMessageFieldsRead 'SentTimestamp))
                    '('GroupId (Member item 'GroupId))
                    '('Delay (Uint64 '0))
                    '('Index (Member item 'Index)))))))))

        (let allMessages
            (Filter unfilteredAllMessages (lambda '(item) (block '(
                (return (Coalesce (Member item 'Valid) (Bool 'false)))
        )))))

        (let messagesToMoveAsStruct
            (Filter allMessages (lambda '(item) (block '(
                (let message (Member item 'SourceMessageFieldsRead))
                (return (Coalesce (GreaterOrEqual (Member message 'ReceiveCount) maxReceiveCount) (Bool 'false)))
        )))))

        (let messagesToReturnAsStruct
            (Filter allMessages (lambda '(item) (block '(
                (let message (Member item 'SourceMessageFieldsRead))
                (return (Coalesce (Less (Member message 'ReceiveCount) maxReceiveCount) (Bool 'false)))
        )))))

        (let dlqStateRow '(
            )__" DLQ_STATE_KEYS_PARAM  R"__(
        ))
        (let dlqStateSelect '(
            'MessageCount
            'WriteOffset
            'LastModifiedTimestamp))
        (let dlqStateRead (SelectRow dlqStateTable dlqStateRow dlqStateSelect))
        (let dlqExists (Exists dlqStateRead))

        (let dlqSentTimestamp (Max now (Member dlqStateRead 'LastModifiedTimestamp)))
        (let dlqStartOffset (Add (Member dlqStateRead 'WriteOffset) (Uint64 '1)))
        (let dlqNewWriteOffset (Add (Member dlqStateRead 'WriteOffset) (Length messagesToMoveAsStruct)))

        (let dlqStateUpdate '(
            '('LastModifiedTimestamp dlqSentTimestamp)
            '('MessageCount (Add (Member dlqStateRead 'MessageCount) (Length messagesToMoveAsStruct)))
            '('WriteOffset dlqNewWriteOffset)))

        (let dlqMessagesInfoWithProperIndexes
            (Enumerate messagesToMoveAsStruct (Coalesce dlqStartOffset (Uint64 '1))))

        (let dlqMessagesInfoWithProperIndexesSorted
            (Sort dlqMessagesInfoWithProperIndexes (Bool 'true) (lambda '(item) (block '(
                (return (Member (Nth item '1) 'Index))
            ))))
        )

        (let sourceStateRow '(
            )__" STATE_KEYS_PARAM  R"__(
        ))
        (let sourceStateSelect '(
            'MessageCount
            'LastModifiedTimestamp))
        (let sourceStateRead (SelectRow sourceStateTable sourceStateRow sourceStateSelect))
        (let newSourceMsgCount (Sub (Member sourceStateRead 'MessageCount) (Length messagesToMoveAsStruct)))
        (let sourceLastModifiedTimestamp (Max now (Member sourceStateRead 'LastModifiedTimestamp)))

        (let sourceStateUpdate '(
            '('LastModifiedTimestamp sourceLastModifiedTimestamp)
            '('MessageCount newSourceMsgCount)))

        (return (Extend
            (AsList (SetResult 'dlqExists dlqExists))
            (AsList (SetResult 'result messagesToReturnAsStruct))
            (AsList (SetResult 'movedMessagesCount (Length messagesToMoveAsStruct)))
            (AsList (SetResult 'movedMessages messagesToMoveAsStruct))
            (AsList (SetResult 'newMessagesCount newSourceMsgCount))
            (ListIf (And (HasItems messagesToMoveAsStruct) dlqExists) (UpdateRow dlqStateTable dlqStateRow dlqStateUpdate))
            (ListIf (And (HasItems messagesToMoveAsStruct) dlqExists) (UpdateRow sourceStateTable sourceStateRow sourceStateUpdate))

            # copy messages to dlq
            (If dlqExists (Map dlqMessagesInfoWithProperIndexesSorted (lambda '(item) (block '(
                (let dlqDataRow '(
                    )__" DLQ_ID_KEYS_PARAM  R"__(
                    '('RandomId randomId)
                    '('Offset (Nth item '0))))
                (let dlqDataUpdate '(
                    '('Data (Member (Nth item '1) 'Data))
                    '('DedupId (Member (Nth item '1) 'DeduplicationId))
                    '('Attributes (Member (Nth item '1) 'Attributes))
                    '('SenderId (Member (Nth item '1) 'SenderId))
                    '('MessageId (Member (Nth item '1) 'MessageId))))
                (return (UpdateRow dlqDataTable dlqDataRow dlqDataUpdate))))))
                (AsList (Void)))

            (If dlqExists (Map dlqMessagesInfoWithProperIndexesSorted (lambda '(item) (block '(
                (let dlqMsgRow '(
                    )__" DLQ_ID_KEYS_PARAM  R"__(
                    '('Offset (Nth item '0))))
                (let dlqMessageUpdate '(
                    '('RandomId randomId)
                    '('GroupId (Member (Nth item '1) 'GroupId))
                    '('NextOffset (Uint64 '0))
                    '('NextRandomId (Uint64 '0))
                    '('ReceiveCount (Uint32 '0))
                    '('FirstReceiveTimestamp (Uint64 '0))
                    '('SentTimestamp dlqSentTimestamp)))
                (return (UpdateRow dlqMsgTable dlqMsgRow dlqMessageUpdate))))))
                (AsList (Void)))

            (If dlqExists (Map dlqMessagesInfoWithProperIndexesSorted (lambda '(item) (block '(
                (let dlqSentTsRow '(
                    )__" DLQ_ID_KEYS_PARAM  R"__(
                    '('SentTimestamp dlqSentTimestamp)
                    '('Offset (Nth item '0))))
                (let delay (Member (Nth item '1) 'Delay))
                (let delayDeadline (Uint64 '0))
                (let dlqSentTsUpdate '(
                    '('RandomId randomId)
                    '('DelayDeadline delayDeadline)
                    '('GroupId (Member (Nth item '1) 'GroupId))))
                (return (UpdateRow dlqSentTsIdx dlqSentTsRow dlqSentTsUpdate))))))
                (AsList (Void)))

            (If dlqExists (Map dlqMessagesInfoWithProperIndexesSorted (lambda '(item) (block '(
                (let dlqGroupRow '(
                    )__" DLQ_ID_KEYS_PARAM  R"__(
                    '('GroupId (Member (Nth item '1) 'GroupId))))
                (let delay (Member (Nth item '1) 'Delay))
                (let delayDeadline (Uint64 '0))
                (let dlqGroupInsert '(
                    '('RandomId randomId)
                    '('Head (Nth item '0))
                    '('Tail (Nth item '0))
                    '('LockTimestamp (Uint64 '0))
                    '('VisibilityDeadline  (Uint64 '0))))
                (let dlqGroupRead (Member (Nth item '1) 'DlqGroupRead))
                (let dlqGroupUpdate '(
                    '('Head (Member dlqGroupRead 'Head))
                    '('Tail (Nth item '0))))
                (let dlqTail (Member (Nth item '1) 'DlqTail))
                (return
                    (If (Equal dlqTail (Uint64 '0))
                        (UpdateRow dlqGroupTable dlqGroupRow dlqGroupInsert)
                        (UpdateRow dlqGroupTable dlqGroupRow dlqGroupUpdate)
                    )
                )))))
                (AsList (Void)))

            (If dlqExists (Map dlqMessagesInfoWithProperIndexesSorted (lambda '(item) (block '(
                (let dlqTail (Member (Nth item '1) 'DlqTail))
                (let dlqPrevMessageRow '(
                    )__" DLQ_ID_KEYS_PARAM  R"__(
                    '('Offset dlqTail)))
                (let dlqPrevMessageUpdate '(
                    '('NextOffset (Nth item '0))
                    '('NextRandomId randomId)))
                (return
                    (If (NotEqual dlqTail (Uint64 '0))
                        (UpdateRow dlqMsgTable dlqPrevMessageRow dlqPrevMessageUpdate)
                        (Void))
                )))))
                (AsList (Void)))

            # remove dead letters' content from source queue
            (If dlqExists (Map dlqMessagesInfoWithProperIndexesSorted (lambda '(item) (block '(
                (let row '(
                    )__" QUEUE_ID_KEYS_PARAM  R"__(
                    '('RandomId (Member (Nth item '1) 'SourceRandomId))
                    '('Offset   (Member (Nth item '1) 'SourceOffset))))
                (return (EraseRow sourceDataTable row))))))
                (AsList (Void)))

            (If dlqExists (Map dlqMessagesInfoWithProperIndexesSorted (lambda '(item) (block '(
                (let row '(
                    )__" QUEUE_ID_KEYS_PARAM  R"__(
                    '('Offset   (Member (Nth item '1) 'SourceOffset))))
                (return (EraseRow sourceMsgTable row))))))
                (AsList (Void)))

            (If dlqExists (Map dlqMessagesInfoWithProperIndexesSorted (lambda '(item) (block '(
                (let row '(
                    )__" QUEUE_ID_KEYS_PARAM  R"__(
                    '('SentTimestamp (Member (Nth item '1) 'SourceSentTimestamp))
                    '('Offset        (Member (Nth item '1) 'SourceOffset))))
                (return (EraseRow sourceSentTsIdx row))))))
                (AsList (Void)))

            (If dlqExists (Map dlqMessagesInfoWithProperIndexesSorted (lambda '(item) (block '(
                (let row '(
                    )__" QUEUE_ID_KEYS_PARAM  R"__(
                    '('GroupId (Member (Nth item '1) 'GroupId))))
                (let update '(
                    '('RandomId (Member (Nth item '1) 'SourceNextRandomId))
                    '('Head (Member (Nth item '1) 'SourceNextOffset))
                    '('LockTimestamp (Uint64 '0))
                    '('VisibilityDeadline (Uint64 '0))))
                (return
                    (If (Coalesce (Equal (Member (Nth item '1) 'SourceNextOffset) (Uint64 '0)) (Bool 'false))
                        (EraseRow  sourceGroupTable row)
                        (UpdateRow sourceGroupTable row update)))))))
                (AsList (Void)))

            # just return ordinary messages
            (If dlqExists (Map messagesToReturnAsStruct (lambda '(item) (block '(
                (let message (Member item 'SourceMessageFieldsRead))
                (let row '(
                    )__" QUEUE_ID_KEYS_PARAM  R"__(
                    '('Offset (Member message 'Offset))))
                (let receiveTimestamp
                    (If (Coalesce (Equal (Member message 'FirstReceiveTimestamp) (Uint64 '0)) (Bool 'false)) now (Member message 'FirstReceiveTimestamp)))
                (let update '(
                    '('FirstReceiveTimestamp receiveTimestamp)
                    '('ReceiveCount (Add (Member message 'ReceiveCount) (Uint32 '1)))))
                (return (UpdateRow sourceMsgTable row update))))))
                (AsList (Void)))
            ))
    )
)__";

const char* const WriteMessageQuery = R"__(
    (
        (let queueIdNumber       (Parameter 'QUEUE_ID_NUMBER      (DataType 'Uint64)))
        (let queueIdNumberHash   (Parameter 'QUEUE_ID_NUMBER_HASH (DataType 'Uint64)))
        (let randomId            (Parameter 'RANDOM_ID            (DataType 'Uint64)))
        (let timestamp           (Parameter 'TIMESTAMP            (DataType 'Uint64)))
        (let deduplicationPeriod (Parameter 'DEDUPLICATION_PERIOD (DataType 'Uint64)))
        (let messages  (Parameter 'MESSAGES
            (ListType (StructType
                '('Attributes (DataType 'String))
                '('Data       (DataType 'String))
                '('MessageId  (DataType 'String))
                '('SenderId   (DataType 'String))
                '('GroupId    (DataType 'String))
                '('DeduplicationId (DataType 'String))
                '('Delay      (DataType 'Uint64))
                '('Index      (DataType 'Uint64))
            ))
        ))

        (let dataTable  ')__" QUEUE_TABLES_FOLDER_PARAM R"__(/Data)
        (let dedupTable ')__" QUEUE_TABLES_FOLDER_PARAM R"__(/Deduplication)
        (let stateTable ')__" QUEUE_TABLES_FOLDER_PARAM R"__(/State)
        (let msgTable   ')__" QUEUE_TABLES_FOLDER_PARAM R"__(/Messages)
        (let groupTable ')__" QUEUE_TABLES_FOLDER_PARAM R"__(/Groups)
        (let sentTsIdx  ')__" QUEUE_TABLES_FOLDER_PARAM R"__(/SentTimestampIdx)

        (let stateRow '(
            )__" STATE_KEYS_PARAM  R"__(
        ))
        (let stateSelect '(
            'MessageCount
            'WriteOffset
            'LastModifiedTimestamp))

        (let stateRead (SelectRow stateTable stateRow stateSelect))

        (let sentTimestamp (Max timestamp (Member stateRead 'LastModifiedTimestamp)))
        (let startOffset (Add (Member stateRead 'WriteOffset) (Uint64 '1)))

        (let messagesInfo
            (MapParameter messages (lambda '(item) (block '(
                (let dedupRow '(
                    )__" QUEUE_ID_KEYS_PARAM  R"__(
                    '('DedupId (Member item 'DeduplicationId))))
                (let dedupSelect '(
                    'Deadline
                    'MessageId
                    'Offset))
                (let dedupRead (SelectRow dedupTable dedupRow dedupSelect))

                (let dedupCond (IfPresent dedupRead (lambda '(x) (Coalesce (Less (Member x 'Deadline) sentTimestamp) (Bool 'false))) (Bool 'true)))

                (let groupRow '(
                    )__" QUEUE_ID_KEYS_PARAM  R"__(
                    '('GroupId (Member item 'GroupId))))
                (let groupSelect '(
                    'Head
                    'Tail))

                (let groupRead (SelectRow groupTable groupRow groupSelect))

                (let tail (IfPresent groupRead (lambda '(x) (Coalesce (Member x 'Tail) (Uint64 '0))) (Uint64 '0)))

                (let offset (Add startOffset (Member item 'Index)))
                (return
                    (AsStruct
                        '('dedupCond dedupCond)
                        '('dedupSelect dedupRead)
                        '('groupRead groupRead)
                        '('tail tail)

                        '('Attributes (Member item 'Attributes))
                        '('Data       (Member item 'Data))
                        '('MessageId  (Member item 'MessageId))
                        '('SenderId   (Member item 'SenderId))
                        '('GroupId    (Member item 'GroupId))
                        '('DeduplicationId (Member item 'DeduplicationId))
                        '('Delay      (Member item 'Delay))
                        '('Index      (Member item 'Index))
                    ))
        )))))

        (let messagesAdded
            (Filter messagesInfo (lambda '(item) (block '(
                (return (Member item 'dedupCond))
        )))))

        (let messagesInfoFirstNotDuplicated
            (Sort messagesInfo (Bool 'true) (lambda '(item) (block '(
                (return (If (Member item 'dedupCond) (Uint64 '0) (Uint64 '1)))
            ))))
        )

        (let newMessagesCount (Add (Member stateRead 'MessageCount) (Length messagesAdded)))
        (let newWriteOffset (Add (Member stateRead 'WriteOffset) (Length messagesAdded)))

        (let messagesInfoWithProperIndexes
            (Enumerate messagesInfoFirstNotDuplicated (Coalesce startOffset (Uint64 '0))))

        (let messagesInfoWithProperIndexesSorted
            (Sort messagesInfoWithProperIndexes (Bool 'true) (lambda '(item) (block '(
                (return (Member (Nth item '1) 'Index))
            ))))
        )

        (let result
            (Map messagesInfoWithProperIndexesSorted (lambda '(item) (block '(
                (return
                    (AsStruct
                        '('dedupCond (Member (Nth item '1) 'dedupCond))
                        '('dedupSelect (Member (Nth item '1) 'dedupSelect))
                        '('offset (Nth item '0))
                    ))
        )))))

        (let stateUpdate '(
            '('LastModifiedTimestamp sentTimestamp)
            '('MessageCount newMessagesCount)
            '('WriteOffset newWriteOffset)))

        (return (Extend
            (AsList (SetResult 'result result))
            (AsList (SetResult 'newMessagesCount newMessagesCount))

            (AsList (If (Greater (Length messagesAdded) (Uint64 '0)) (UpdateRow stateTable stateRow stateUpdate) (Void)))

            (Map messagesInfoWithProperIndexesSorted (lambda '(item) (block '(
                (let dedupCond (Member (Nth item '1) 'dedupCond))
                (let dedupRow '(
                    )__" QUEUE_ID_KEYS_PARAM  R"__(
                    '('DedupId (Member (Nth item '1) 'DeduplicationId))))
                (let dedupUpdate '(
                    '('Deadline (Add sentTimestamp deduplicationPeriod))
                    '('Offset (Nth item '0))
                    '('MessageId (Member (Nth item '1) 'MessageId))))
                (return (If dedupCond (UpdateRow dedupTable dedupRow dedupUpdate) (Void)))))))

            (Map messagesInfoWithProperIndexesSorted (lambda '(item) (block '(
                (let dedupCond (Member (Nth item '1) 'dedupCond))
                (let dataRow '(
                    )__" QUEUE_ID_KEYS_PARAM  R"__(
                    '('RandomId randomId)
                    '('Offset (Nth item '0))))
                (let dataUpdate '(
                    '('Data (Member (Nth item '1) 'Data))
                    '('DedupId (Member (Nth item '1) 'DeduplicationId))
                    '('Attributes (Member (Nth item '1) 'Attributes))
                    '('SenderId (Member (Nth item '1) 'SenderId))
                    '('MessageId (Member (Nth item '1) 'MessageId))))
                (return (If dedupCond (UpdateRow dataTable dataRow dataUpdate) (Void)))))))

            (Map messagesInfoWithProperIndexesSorted (lambda '(item) (block '(
                (let dedupCond (Member (Nth item '1) 'dedupCond))
                (let msgRow '(
                    )__" QUEUE_ID_KEYS_PARAM  R"__(
                    '('Offset (Nth item '0))))
                (let messageUpdate '(
                    '('RandomId randomId)
                    '('GroupId (Member (Nth item '1) 'GroupId))
                    '('NextOffset (Uint64 '0))
                    '('NextRandomId (Uint64 '0))
                    '('ReceiveCount (Uint32 '0))
                    '('FirstReceiveTimestamp (Uint64 '0))
                    '('SentTimestamp sentTimestamp)))
                (return (If dedupCond (UpdateRow msgTable msgRow messageUpdate) (Void)))))))

            (Map messagesInfoWithProperIndexesSorted (lambda '(item) (block '(
                (let dedupCond (Member (Nth item '1) 'dedupCond))
                (let sentTsRow '(
                    )__" QUEUE_ID_KEYS_PARAM  R"__(
                    '('SentTimestamp sentTimestamp)
                    '('Offset (Nth item '0))))
                (let delay (Member (Nth item '1) 'Delay))
                (let delayDeadline (If (Equal delay (Uint64 '0)) (Uint64 '0) (Add sentTimestamp delay)))
                (let sentTsUpdate '(
                    '('RandomId randomId)
                    '('DelayDeadline delayDeadline)
                    '('GroupId (Member (Nth item '1) 'GroupId))))
                (return (If dedupCond (UpdateRow sentTsIdx sentTsRow sentTsUpdate) (Void)))))))

            (Map messagesInfoWithProperIndexesSorted (lambda '(item) (block '(
                (let dedupCond (Member (Nth item '1) 'dedupCond))
                (let groupRow '(
                    )__" QUEUE_ID_KEYS_PARAM  R"__(
                    '('GroupId (Member (Nth item '1) 'GroupId))))
                (let delay (Member (Nth item '1) 'Delay))
                (let delayDeadline (If (Equal delay (Uint64 '0)) (Uint64 '0) (Add sentTimestamp delay)))
                (let groupInsert '(
                    '('RandomId randomId)
                    '('Head (Nth item '0))
                    '('Tail (Nth item '0))
                    '('LockTimestamp (Uint64 '0))
                    '('VisibilityDeadline delayDeadline)))
                (let groupRead (Member (Nth item '1) 'groupRead))
                (let groupUpdate '(
                    '('Head (Member groupRead 'Head))
                    '('Tail (Nth item '0))))
                (let tail (Member (Nth item '1) 'tail))
                (return
                    (If dedupCond
                        (If (Equal tail (Uint64 '0))
                            (UpdateRow groupTable groupRow groupInsert)
                            (UpdateRow groupTable groupRow groupUpdate)
                        )
                        (Void)
                ))))))

            (Map messagesInfoWithProperIndexesSorted (lambda '(item) (block '(
                (let dedupCond (Member (Nth item '1) 'dedupCond))
                (let tail (Member (Nth item '1) 'tail))
                (let prevMessageRow '(
                    )__" QUEUE_ID_KEYS_PARAM  R"__(
                    '('Offset tail)))
                (let prevMessageUpdate '(
                    '('NextOffset (Nth item '0))
                    '('NextRandomId randomId)))
                (return
                    (If (And dedupCond (NotEqual tail (Uint64 '0)))
                        (UpdateRow msgTable prevMessageRow prevMessageUpdate)
                        (Void))
                )))))
        ))
    )
)__";

static const char* const DeduplicationCleanupQuery = R"__(
    (
        (let queueIdNumber      (Parameter 'QUEUE_ID_NUMBER      (DataType 'Uint64)))
        (let queueIdNumberHash  (Parameter 'QUEUE_ID_NUMBER_HASH (DataType 'Uint64)))
        (let now                (Parameter 'NOW                  (DataType 'Uint64)))
        (let batchSize          (Parameter 'BATCH_SIZE           (DataType 'Uint64)))
        (let keyRangeStart      (Parameter 'KEY_RANGE_START      (DataType 'String)))

        (let dedupTable ')__" QUEUE_TABLES_FOLDER_PARAM R"__(/Deduplication)

        (let dedupRange '(
            )__" QUEUE_ID_KEYS_RANGE_PARAM R"__(
            '('DedupId keyRangeStart (Void))))
        (let dedupSelect '(
            'DedupId
            'Deadline))
        (let dedups (SelectRange dedupTable dedupRange dedupSelect '('('"ItemsLimit" batchSize))))

        (let dedupsList (Member dedups 'List))
        (let dedupToErase (Filter dedupsList (lambda '(item) (block '(
            (return (Coalesce (Less (Member item 'Deadline) now) (Bool 'false)))
        )))))

        (let dedupsCount (Length dedupsList))
        (let lastSelectedRow (ToOptional (Skip dedupsList (Max (Sub dedupsCount (Uint64 '1)) (Uint64 '0)))))

        (return (Extend
            (AsList (SetResult 'moreData (Member dedups 'Truncated)))
            (AsList (SetResult 'lastProcessedKey (Member lastSelectedRow 'DedupId)))

            (Map dedupToErase (lambda '(item) (block '(
                (return (EraseRow dedupTable '(
                    )__" QUEUE_ID_KEYS_PARAM  R"__(
                    '('DedupId (Member item 'DedupId)))
                ))
            ))))
        ))
    )
)__";

static const char* const ReadsCleanupQuery = R"__(
    (
        (let queueIdNumber      (Parameter 'QUEUE_ID_NUMBER      (DataType 'Uint64)))
        (let queueIdNumberHash  (Parameter 'QUEUE_ID_NUMBER_HASH (DataType 'Uint64)))
        (let now                (Parameter 'NOW                  (DataType 'Uint64)))
        (let batchSize          (Parameter 'BATCH_SIZE           (DataType 'Uint64)))
        (let keyRangeStart      (Parameter 'KEY_RANGE_START      (DataType 'Utf8String)))

        (let readsTable ')__" QUEUE_TABLES_FOLDER_PARAM R"__(/Reads)

        (let readRange '(
            )__" QUEUE_ID_KEYS_RANGE_PARAM R"__(
            '('ReceiveAttemptId keyRangeStart (Void))))
        (let readSelect '(
            'ReceiveAttemptId
            'Deadline))
        (let reads (SelectRange readsTable readRange readSelect '('('"ItemsLimit" batchSize))))

        (let readsList (Member reads 'List))
        (let readsToErase (Filter readsList (lambda '(item) (block '(
            (return (Coalesce (Less (Member item 'Deadline) now) (Bool 'false)))
        )))))

        (let readsCount (Length readsList))
        (let lastSelectedRow (ToOptional (Skip readsList (Max (Sub readsCount (Uint64 '1)) (Uint64 '0)))))

        (return (Extend
            (AsList (SetResult 'moreData (Member reads 'Truncated)))
            (AsList (SetResult 'lastProcessedKey (Member lastSelectedRow 'ReceiveAttemptId)))

            (Map readsToErase (lambda '(item) (block '(
                (return (EraseRow readsTable '(
                    )__" QUEUE_ID_KEYS_PARAM  R"__(
                    '('ReceiveAttemptId (Member item 'ReceiveAttemptId))
                )))
            ))))
        ))
    )
)__";

const char* const SetRetentionQuery = R"__(
    (
        (let queueIdNumber      (Parameter 'QUEUE_ID_NUMBER      (DataType 'Uint64)))
        (let queueIdNumberHash  (Parameter 'QUEUE_ID_NUMBER_HASH (DataType 'Uint64)))
        (let now                (Parameter 'NOW                  (DataType 'Uint64)))
        (let purge              (Parameter 'PURGE                (DataType 'Bool)))

        (let attrsTable ')__" QUEUE_TABLES_FOLDER_PARAM R"__(/Attributes)
        (let stateTable ')__" QUEUE_TABLES_FOLDER_PARAM R"__(/State)

        (let attrs (block '(
            (let attrsRow '(
                )__" ATTRS_KEYS_PARAM R"__(
            ))
            (let fields '(
                'MessageRetentionPeriod))
            (return (SelectRow attrsTable attrsRow fields)))))

        (let boundary
            (If purge now (Coalesce (Sub now (Member attrs 'MessageRetentionPeriod)) (Uint64 '0))))

        (let stateRange '(
            )__" ALL_SHARDS_RANGE_PARAM R"__(
        ))
        (let fields '(
            'RetentionBoundary))
        (let records (Member (SelectRange stateTable stateRange fields '()) 'List))

        (let result
            (Map records (lambda '(item) (block '(
                (let updated
                    (Coalesce
                        (Less (Member item 'RetentionBoundary) boundary)
                        (Bool 'false)))

                (return (AsStruct
                    '('Shard ()__" SHARD_TYPE_PARAM R"__( '0))
                    '('RetentionBoundary (Max boundary (Member item 'RetentionBoundary)))
                    '('Updated updated))))))))

        (let updated (Filter result (lambda '(item) (block '(
            (return (Coalesce (Equal (Member item 'Updated) (Bool 'true)) (Bool 'false))))))))

        (return (Extend
            (AsList (SetResult 'result result))
            (AsList (SetResult 'retention (Member attrs 'MessageRetentionPeriod)))

            (Map updated (lambda '(item) (block '(
                (let row '(
                    )__" STATE_KEYS_PARAM R"__(
                ))
                (let update '(
                    '('RetentionBoundary (Member item 'RetentionBoundary))))
                (return (UpdateRow stateTable row update))))))
        ))
    )
)__";

const char* const GetMessageCountMetricsQuery = R"__(
    (
        (let queueIdNumber      (Parameter 'QUEUE_ID_NUMBER      (DataType 'Uint64)))
        (let queueIdNumberHash  (Parameter 'QUEUE_ID_NUMBER_HASH (DataType 'Uint64)))

        (let stateTable ')__" QUEUE_TABLES_FOLDER_PARAM R"__(/State)

        (let stateRow '(
            )__" STATE_KEYS_PARAM  R"__(
        ))
        (let stateSelect '(
            'MessageCount
            'InflyCount
            'CreatedTimestamp))

        (let stateRead
            (SelectRow stateTable stateRow stateSelect))

        (return (AsList
            (SetResult 'messagesCount (Member stateRead 'MessageCount))
            (SetResult 'inflyMessagesCount (Member stateRead 'InflyCount))
            (SetResult 'createdTimestamp (Member stateRead 'CreatedTimestamp))))
    )
)__";

const char* const GetOldestMessageTimestampMetricsQuery = R"__(
    (
        (let queueIdNumber      (Parameter 'QUEUE_ID_NUMBER      (DataType 'Uint64)))
        (let queueIdNumberHash  (Parameter 'QUEUE_ID_NUMBER_HASH (DataType 'Uint64)))
        (let timeFrom           (Parameter 'TIME_FROM            (DataType 'Uint64)))

        (let sentTsIdx ')__" QUEUE_TABLES_FOLDER_PARAM R"__(/SentTimestampIdx)

        (let sentIdxRange '(
            )__" QUEUE_ID_KEYS_RANGE_PARAM  R"__(
            '('SentTimestamp timeFrom (Uint64 '18446744073709551615))
            '('Offset (Uint64 '0) (Uint64 '18446744073709551615))))
        (let sentIdxSelect '(
            'SentTimestamp
            'Offset))
        (let selectResult (SelectRange sentTsIdx sentIdxRange sentIdxSelect '('('"ItemsLimit" (Uint64 '1)))))
        (let messages (Member selectResult 'List))

        (return (Extend
            (AsList (SetResult 'messages messages))
        ))
    )
)__";

const char* const GetRetentionOffsetQuery = R"__(
    (
        (let queueIdNumber      (Parameter 'QUEUE_ID_NUMBER      (DataType 'Uint64)))
        (let queueIdNumberHash  (Parameter 'QUEUE_ID_NUMBER_HASH (DataType 'Uint64)))
        (let offsetFrom         (Parameter 'OFFSET_FROM          (DataType 'Uint64)))
        (let timeFrom           (Parameter 'TIME_FROM            (DataType 'Uint64)))
        (let timeTo             (Parameter 'TIME_TO              (DataType 'Uint64)))
        (let batchSize          (Parameter 'BATCH_SIZE           (DataType 'Uint64)))

        (let sentTsIdx    ')__" QUEUE_TABLES_FOLDER_PARAM R"__(/SentTimestampIdx)

        (let sentIdxRange '(
            )__" QUEUE_ID_KEYS_RANGE_PARAM  R"__(
            '('SentTimestamp timeFrom timeTo)
            '('Offset offsetFrom (Uint64 '18446744073709551615))))
        (let sentIdxSelect '(
            'Offset))
        (let selectResult (SelectRange sentTsIdx sentIdxRange sentIdxSelect '('('"ItemsLimit" batchSize))))
        (let messages (Member selectResult 'List))
        (let truncated (Member selectResult 'Truncated))

        (return (Extend
            (AsList (SetResult 'messages messages))
            (AsList (SetResult 'truncated truncated))
        ))
    )
)__";

const char* const ListDeadLetterSourceQueuesQuery = R"__(
    (
        (let folderId (Parameter 'FOLDERID  (DataType 'Utf8String)))
        (let userName (Parameter 'USER_NAME (DataType 'Utf8String)))

        (let queuesTable ')__" ROOT_PARAM R"__(/.Queues)

        (let queuesRow '(
            '('Account userName)
            '('QueueName (Utf8String '")__" QUEUE_NAME_PARAM R"__("))))

        (let queuesRowSelect '(
            'QueueName
            'CustomQueueName))

        (let queuesRowRead (SelectRow queuesTable queuesRow queuesRowSelect))

        (let skipFolderIdFilter (Equal folderId (Utf8String '"")))

        (let dlqName
            (If skipFolderIdFilter (Member queuesRowRead 'QueueName) (Coalesce (Member queuesRowRead 'CustomQueueName) (Utf8String '""))))

        (let queuesRange '(
            '('Account userName userName)
            '('QueueName (Utf8String '"") (Void))))
        (let queuesSelect '('QueueName 'QueueState 'FolderId 'DlqName 'CustomQueueName))
        (let queues (Member (SelectRange queuesTable queuesRange queuesSelect '()) 'List))

        (let filtered (Filter queues (lambda '(item) (block '(
            (return (Coalesce
                (And
                    (And
                        (Or
                            (Equal (Member item 'QueueState) (Uint64 '1))
                            (Equal (Member item 'QueueState) (Uint64 '3)))
                        (Or
                            (Equal (Member item 'FolderId) folderId)
                            skipFolderIdFilter))
                     (Equal (Member item 'DlqName) dlqName))
                (Bool 'false)))
        )))))

        (return (AsList
            (SetResult 'queues filtered)))
    )
)__";

const char* const GetStateQuery = R"__(
    (
        (let queueIdNumber      (Parameter 'QUEUE_ID_NUMBER (DataType 'Uint64)))
        (let queueIdNumberHash  (Parameter 'QUEUE_ID_NUMBER_HASH (DataType 'Uint64)))

        (let stateTable ')__" QUEUE_TABLES_FOLDER_PARAM R"__(/State)

        (let stateRange '(
            )__" ALL_SHARDS_RANGE_PARAM R"__(
        ))
        (let stateSelect '(
            'MessageCount
            'InflyCount
            'InflyVersion
            'CreatedTimestamp
            'RetentionBoundary))

        (let stateRead (Member (SelectRange stateTable stateRange stateSelect '()) 'List))

        (return (AsList (SetResult 'state stateRead)))
    )
)__";

} // namespace

const char* GetFifoQueryById(size_t id) {
    switch (id) {
    case DELETE_MESSAGE_ID:
        return DeleteMessageQuery;
    case LOCK_GROUP_ID:
        return LockGroupsQuery;
    case READ_MESSAGE_ID:
        return ReadMessageQuery;
    case WRITE_MESSAGE_ID:
        return WriteMessageQuery;
    case PURGE_QUEUE_ID:
        return PurgeQueueQuery;
    case CHANGE_VISIBILITY_ID:
        return ChangeMessageVisibilityQuery;
    case CLEANUP_DEDUPLICATION_ID:
        return DeduplicationCleanupQuery;
    case CLEANUP_READS_ID:
        return ReadsCleanupQuery;
    case LIST_QUEUES_ID:
        return ListQueuesQuery;
    case SET_QUEUE_ATTRIBUTES_ID:
        return SetQueueAttributesQuery;
    case SET_RETENTION_ID:
        return SetRetentionQuery;
    case INTERNAL_GET_QUEUE_ATTRIBUTES_ID:
        return InternalGetQueueAttributesQuery;
    case PURGE_QUEUE_STAGE2_ID:
        return PurgeQueueStage2Query;
    case GET_MESSAGE_COUNT_METRIC_ID:
        return GetMessageCountMetricsQuery;
    case GET_OLDEST_MESSAGE_TIMESTAMP_METRIC_ID:
        return GetOldestMessageTimestampMetricsQuery;
    case GET_RETENTION_OFFSET_ID:
        return GetRetentionOffsetQuery;
    case LIST_DEAD_LETTER_SOURCE_QUEUES_ID:
        return ListDeadLetterSourceQueuesQuery;
    case READ_OR_REDRIVE_MESSAGE_ID:
        return ReadOrRedriveMessageQuery;
    case GET_STATE_ID:
        return GetStateQuery;
    case TAG_QUEUE_ID:
        return TagQueueQuery;
    }

    return nullptr;
}

} // namespace NKikimr::NSQS
