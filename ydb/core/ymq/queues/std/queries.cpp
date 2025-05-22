#include "queries.h"
#include <ydb/core/ymq/base/constants.h>
#include <ydb/core/ymq/queues/common/db_queries_defs.h>

#include <util/string/builder.h>

namespace NKikimr::NSQS {
namespace {

static const char* const AddMessagesToInflyQuery = R"__(
    (
        (let queueIdNumber              (Parameter 'QUEUE_ID_NUMBER (DataType 'Uint64)))
        (let queueIdNumberHash          (Parameter 'QUEUE_ID_NUMBER_HASH (DataType 'Uint64)))
        (let shard                      (Parameter 'SHARD  (DataType ')__" SHARD_TYPE_PARAM R"__()))
        (let queueIdNumberAndShardHash  (Parameter 'QUEUE_ID_NUMBER_AND_SHARD_HASH (DataType 'Uint64)))
        (let inflyLimit (Parameter 'INFLY_LIMIT (DataType 'Uint64)))
        (let from       (Parameter 'FROM        (DataType 'Uint64)))
        (let expectedMaxCount (Parameter 'EXPECTED_MAX_COUNT (DataType 'Uint64)))

        (let inflyTable ')__" QUEUE_TABLES_FOLDER_PER_SHARD_PARAM R"__(/Infly)
        (let msgTable   ')__" QUEUE_TABLES_FOLDER_PER_SHARD_PARAM R"__(/Messages)
        (let stateTable ')__" QUEUE_TABLES_FOLDER_PARAM R"__(/State)

        (let stateRow '(
            )__" STATE_KEYS_PARAM R"__(
        ))
        (let stateFields '(
            'InflyCount
            'MessageCount
            'ReadOffset
            'InflyVersion))
        (let state (SelectRow stateTable stateRow stateFields))

        (let inflyMaxCountToAdd
            (Convert
                (Coalesce
                    (Max
                        (Sub inflyLimit (Member state 'InflyCount))
                        (Uint64 '0)
                    )
                    (Uint64 '0)
                )
                'Uint64
            )
        )

        (let msgRange '(
            )__" QUEUE_ID_AND_SHARD_KEYS_RANGE_PARAM R"__(
            '('Offset from (Uint64 '18446744073709551615))))
        (let msgFields '(
            'Offset
            'RandomId
            'SentTimestamp
            'DelayDeadline))
        (let messages (Take (Member (SelectRange msgTable msgRange msgFields '('('"ItemsLimit" expectedMaxCount))) 'List) inflyMaxCountToAdd))

        (let inflyCount (Add (Member state 'InflyCount) (Length messages)))
        (let lastElement (ToOptional (Skip messages (Sub (Length messages) (Uint64 '1)))))
        (let nextReadOffset
            (If (HasItems messages)
                (Add
                    (Member lastElement 'Offset)
                    (Uint64 '1))
                (Member state 'ReadOffset)))
        (let currentInflyVersion
            (Coalesce
                (Member state 'InflyVersion)
                (Uint64 '0)
            )
        )
        (let newInflyVersion (If (HasItems messages) (Add currentInflyVersion (Uint64 '1)) currentInflyVersion))

        (return (Extend
            (AsList (SetResult 'messages messages))
            (AsList (SetResult 'inflyCount inflyCount))
            (AsList (SetResult 'messagesCount (Member state 'MessageCount)))
            (AsList (SetResult 'readOffset nextReadOffset))
            (AsList (SetResult 'currentInflyVersion currentInflyVersion))
            (AsList (SetResult 'newInflyVersion newInflyVersion))

            (ListIf (HasItems messages) (block '(
                (let row '(
                    )__" STATE_KEYS_PARAM R"__(
                ))
                (let update '(
                    '('ReadOffset nextReadOffset)
                    '('InflyCount inflyCount)
                    '('InflyVersion newInflyVersion)))
                (return (UpdateRow stateTable row update)))))

            (Map messages (lambda '(item) (block '(
                (let row '(
                    )__" QUEUE_ID_AND_SHARD_KEYS_PARAM R"__(
                    '('Offset (Member item 'Offset))))
                (let update '(
                    '('RandomId (Member item 'RandomId))
                    '('LoadId (Uint64 '0))
                    '('FirstReceiveTimestamp (Uint64 '0))
                    '('LockTimestamp (Uint64 '0))
                    '('ReceiveCount (Uint32 '0))
                    '('SentTimestamp (Member item 'SentTimestamp))
                    '('DelayDeadline (Member item 'DelayDeadline))
                    '('VisibilityDeadline (Uint64 '0))))
                (return (UpdateRow inflyTable row update))))))

            (Map messages (lambda '(item) (block '(
                (let row '(
                    )__" QUEUE_ID_AND_SHARD_KEYS_PARAM R"__(
                    '('Offset (Member item 'Offset))))
                (return (EraseRow msgTable row))))))))
    )
)__";

const char* const ChangeMessageVisibilityQuery = R"__(
    (
        (let queueIdNumber              (Parameter 'QUEUE_ID_NUMBER (DataType 'Uint64)))
        (let queueIdNumberHash          (Parameter 'QUEUE_ID_NUMBER_HASH (DataType 'Uint64)))
        (let shard                      (Parameter 'SHARD  (DataType ')__" SHARD_TYPE_PARAM R"__()))
        (let queueIdNumberAndShardHash  (Parameter 'QUEUE_ID_NUMBER_AND_SHARD_HASH (DataType 'Uint64)))
        (let now  (Parameter 'NOW      (DataType 'Uint64)))
        (let keys (Parameter 'KEYS
            (ListType (StructType
                '('Offset (DataType 'Uint64))
                '('LockTimestamp (DataType 'Uint64))
                '('NewVisibilityDeadline (DataType 'Uint64))))))

        (let inflyTable ')__" QUEUE_TABLES_FOLDER_PER_SHARD_PARAM R"__(/Infly)

        (let records
            (MapParameter keys (lambda '(item) (block '(
                (let messageRow '(
                    )__" QUEUE_ID_AND_SHARD_KEYS_PARAM R"__(
                    '('Offset (Member item 'Offset))))
                (let inflySelect '(
                    'VisibilityDeadline
                    'LockTimestamp))
                (let inflyRead (SelectRow inflyTable messageRow inflySelect))

                (let exists (Exists inflyRead))

                (let changeCond
                    (Coalesce
                        (And
                            (LessOrEqual now (Member inflyRead 'VisibilityDeadline))
                            (Equal (Member item 'LockTimestamp) (Member inflyRead 'LockTimestamp))
                        )
                    (Bool 'false)))

                (return (AsStruct
                    '('Offset (Member item 'Offset))
                    '('Exists exists)
                    '('ChangeCond changeCond)
                    '('CurrentVisibilityDeadline (Member inflyRead 'VisibilityDeadline))
                    '('NewVisibilityDeadline (Member item 'NewVisibilityDeadline)))))))))

        (let recordsToChange
            (Filter records (lambda '(item) (block '(
                (return (And (Member item 'Exists) (Member item 'ChangeCond)))
        )))))

        (return (Extend
            (AsList (SetResult 'result records))

            (Map recordsToChange (lambda '(item) (block '(
                (let messageRow '(
                    )__" QUEUE_ID_AND_SHARD_KEYS_PARAM R"__(
                    '('Offset (Member item 'Offset))))
                (let visibilityUpdate '(
                    '('VisibilityDeadline (Member item 'NewVisibilityDeadline))))
                (return (UpdateRow inflyTable messageRow visibilityUpdate))
            ))))
        ))
    )
)__";

const char* const PurgeQueueQuery = R"__(
    (
        (let queueIdNumber              (Parameter 'QUEUE_ID_NUMBER (DataType 'Uint64)))
        (let queueIdNumberHash          (Parameter 'QUEUE_ID_NUMBER_HASH (DataType 'Uint64)))
        (let shard                      (Parameter 'SHARD  (DataType ')__" SHARD_TYPE_PARAM R"__()))
        (let queueIdNumberAndShardHash  (Parameter 'QUEUE_ID_NUMBER_AND_SHARD_HASH (DataType 'Uint64)))

        (let offsetFrom     (Parameter 'OFFSET_FROM (DataType 'Uint64)))
        (let offsetTo       (Parameter 'OFFSET_TO   (DataType 'Uint64)))
        (let now            (Parameter 'NOW         (DataType 'Uint64)))
        (let batchSize      (Parameter 'BATCH_SIZE  (DataType 'Uint64)))

        (let msgTable   ')__" QUEUE_TABLES_FOLDER_PER_SHARD_PARAM R"__(/Messages)
        (let inflyTable ')__" QUEUE_TABLES_FOLDER_PER_SHARD_PARAM R"__(/Infly)
        (let stateTable ')__" QUEUE_TABLES_FOLDER_PARAM R"__(/State)

        (let stateRow '(
            )__" STATE_KEYS_PARAM R"__(
        ))
        (let stateSelect '(
            'CleanupVersion
            'LastModifiedTimestamp))
        (let stateRead
            (SelectRow stateTable stateRow stateSelect))

        (let modifiedTimestamp (Max now (Member stateRead 'LastModifiedTimestamp)))

        (let messageRange '(
            )__" QUEUE_ID_AND_SHARD_KEYS_RANGE_PARAM R"__(
            '('Offset offsetFrom offsetTo)
        ))
        (let inflyRange '(
            )__" QUEUE_ID_AND_SHARD_KEYS_RANGE_PARAM R"__(
            '('Offset offsetFrom offsetTo)
        ))
        (let messageSelect '(
            'SentTimestamp
            'Offset
            'RandomId))

        (let selectResult (SelectRange msgTable messageRange messageSelect '('('"ItemsLimit" batchSize))))
        (let selectInflyResult (SelectRange inflyTable inflyRange messageSelect '('('"ItemsLimit" batchSize))))

        (let messages (Member selectResult 'List))
        (let inflyMessages (Member selectInflyResult 'List))
        (let truncated (Coalesce (Member selectResult 'Truncated) (Member selectInflyResult 'Truncated) (Bool 'false)))
        (let newCleanupVersion (Add (Member stateRead 'CleanupVersion) (Uint64 '1)))

        (let stateUpdate '(
            '('LastModifiedTimestamp modifiedTimestamp)
            '('CleanupVersion newCleanupVersion)
        ))

        (return (Extend
            (AsList (SetResult 'messages messages))
            (AsList (SetResult 'inflyMessages inflyMessages))
            (AsList (SetResult 'truncated truncated))
            (AsList (SetResult 'cleanupVersion newCleanupVersion))
            (AsList (UpdateRow stateTable stateRow stateUpdate))
        ))
    )
)__";

const char* const PurgeQueueStage2Query = R"__(
    (
        (let queueIdNumber              (Parameter 'QUEUE_ID_NUMBER (DataType 'Uint64)))
        (let queueIdNumberHash          (Parameter 'QUEUE_ID_NUMBER_HASH (DataType 'Uint64)))
        (let shard                      (Parameter 'SHARD  (DataType ')__" SHARD_TYPE_PARAM R"__()))
        (let queueIdNumberAndShardHash  (Parameter 'QUEUE_ID_NUMBER_AND_SHARD_HASH (DataType 'Uint64)))

        (let cleanupVersion (Parameter 'CLEANUP_VERSION (DataType 'Uint64)))
        (let now (Parameter 'NOW (DataType 'Uint64)))
        (let messages (Parameter 'MESSAGES
            (ListType (StructType
                '('Offset (DataType 'Uint64))
                '('RandomId (DataType 'Uint64))
                '('SentTimestamp (DataType 'Uint64))
        ))))

        (let dataTable  ')__" QUEUE_TABLES_FOLDER_PER_SHARD_PARAM R"__(/MessageData)
        (let inflyTable ')__" QUEUE_TABLES_FOLDER_PER_SHARD_PARAM R"__(/Infly)
        (let msgTable   ')__" QUEUE_TABLES_FOLDER_PER_SHARD_PARAM R"__(/Messages)
        (let sentTsIdx  ')__" QUEUE_TABLES_FOLDER_PER_SHARD_PARAM R"__(/SentTimestampIdx)
        (let stateTable ')__" QUEUE_TABLES_FOLDER_PARAM R"__(/State)

        (let stateRow '(
            )__" STATE_KEYS_PARAM R"__(
        ))
        (let stateSelect '(
            'MessageCount
            'InflyCount
            'CleanupVersion
            'LastModifiedTimestamp))
        (let stateRead
            (SelectRow stateTable stateRow stateSelect))

        (let modifiedTimestamp (Max now (Member stateRead 'LastModifiedTimestamp)))

        (let inflyRecords
            (MapParameter messages (lambda '(item) (block '(
                (let row '(
                    )__" QUEUE_ID_AND_SHARD_KEYS_PARAM R"__(
                    '('Offset (Member item 'Offset))))
                (let fields '(
                    'Offset
                    'SentTimestamp))
                (return (SelectRow inflyTable row fields)))))))

        (let inflyRecordsExisted
            (Filter inflyRecords (lambda '(item) (block '(
                (return (Exists item))
        )))))

        (let messageRecords
            (MapParameter messages (lambda '(item) (block '(
                (let row '(
                    )__" QUEUE_ID_AND_SHARD_KEYS_PARAM R"__(
                    '('Offset (Member item 'Offset))))
                (let fields '(
                    'Offset
                    'SentTimestamp))
                (return (SelectRow msgTable row fields)))))))

        (let messageRecordsExisted
            (Filter messageRecords (lambda '(item) (block '(
                (return (Exists item))
        )))))

        (let messagesDeleted
            (Add (Length messageRecordsExisted) (Length inflyRecordsExisted))
        )

        (let newMessagesCount (Sub (Member stateRead 'MessageCount) messagesDeleted))

        (let stateUpdate '(
            '('LastModifiedTimestamp modifiedTimestamp)
            '('MessageCount newMessagesCount)
            '('InflyCount (Sub (Member stateRead 'InflyCount) (Length inflyRecordsExisted)))
        ))

        (let versionIsSame
            (Coalesce
                (Equal (Member stateRead 'CleanupVersion) cleanupVersion)
                (Bool 'false)
            )
        )

        (return (Extend
            (AsList (SetResult 'versionIsSame versionIsSame))
            (AsList (SetResult 'newMessagesCount (If versionIsSame newMessagesCount (Member stateRead 'MessageCount))))

            (AsList (SetResult 'messagesDeleted
                (If versionIsSame
                    messagesDeleted
                    (Uint64 '0))
            ))

            (If versionIsSame
                (AsList (UpdateRow stateTable stateRow stateUpdate))
                (AsList (Void)))

            (If versionIsSame
                (Map inflyRecordsExisted (lambda '(item) (block '(
                    (return (EraseRow inflyTable '(
                        )__" QUEUE_ID_AND_SHARD_KEYS_PARAM R"__(
                        '('Offset (Member item 'Offset))
                    )))
                ))))
                (AsList (Void)))

            (If versionIsSame
                (Map messages (lambda '(item) (block '(
                    (return (EraseRow dataTable '(
                        )__" QUEUE_ID_AND_SHARD_KEYS_PARAM R"__(
                        '('RandomId (Member item 'RandomId))
                        '('Offset   (Member item 'Offset))
                    )))
                ))))
                (AsList (Void)))

            (If versionIsSame
                (Map messageRecordsExisted (lambda '(item) (block '(
                    (return (EraseRow msgTable '(
                        )__" QUEUE_ID_AND_SHARD_KEYS_PARAM R"__(
                        '('Offset (Member item 'Offset))
                    )))
                ))))
                (AsList (Void)))

            (If versionIsSame
                (Map messages (lambda '(item) (block '(
                    (return (EraseRow sentTsIdx '(
                        )__" QUEUE_ID_AND_SHARD_KEYS_PARAM R"__(
                        '('SentTimestamp (Member item 'SentTimestamp))
                        '('Offset        (Member item 'Offset))
                    )))
                ))))
                (AsList (Void)))
        ))
    )
)__";

const char* const DeleteMessageQuery = R"__(
    (
        (let queueIdNumber              (Parameter 'QUEUE_ID_NUMBER (DataType 'Uint64)))
        (let queueIdNumberHash          (Parameter 'QUEUE_ID_NUMBER_HASH (DataType 'Uint64)))
        (let shard                      (Parameter 'SHARD  (DataType ')__" SHARD_TYPE_PARAM R"__()))
        (let queueIdNumberAndShardHash  (Parameter 'QUEUE_ID_NUMBER_AND_SHARD_HASH (DataType 'Uint64)))

        (let keys (Parameter 'KEYS
            (ListType (StructType
                '('Offset (DataType 'Uint64))
                '('LockTimestamp (DataType 'Uint64))))))
        (let now (Parameter 'NOW (DataType 'Uint64)))

        (let dataTable  ')__" QUEUE_TABLES_FOLDER_PER_SHARD_PARAM R"__(/MessageData)
        (let inflyTable ')__" QUEUE_TABLES_FOLDER_PER_SHARD_PARAM R"__(/Infly)
        (let sentTsIdx  ')__" QUEUE_TABLES_FOLDER_PER_SHARD_PARAM R"__(/SentTimestampIdx)
        (let stateTable ')__" QUEUE_TABLES_FOLDER_PARAM R"__(/State)

        (let records
            (MapParameter keys (lambda '(item) (block '(
                (let row '(
                    )__" QUEUE_ID_AND_SHARD_KEYS_PARAM R"__(
                    '('Offset (Member item 'Offset))
                ))
                (let fields '(
                    'Offset
                    'RandomId
                    'SentTimestamp))
                (return (SelectRow inflyTable row fields)))))))

        (let existed
            (Filter records (lambda '(item) (block '(
                (return (Exists item))
            )))))

        (let result
            (Map existed (lambda '(item) (block '(
                (return (AsStruct
                    '('Offset (Member item 'Offset)))))))))

        (let stateRow '(
            )__" STATE_KEYS_PARAM R"__(
        ))
        (let stateSelect '(
            'InflyCount
            'MessageCount
            'LastModifiedTimestamp))
        (let stateRead (SelectRow stateTable stateRow stateSelect))

        (let modifiedTimestamp (Max now (Member stateRead 'LastModifiedTimestamp)))

        (let newMessagesCount (Sub (Member stateRead 'MessageCount) (Length existed)))
        (let stateUpdate '(
            '('LastModifiedTimestamp modifiedTimestamp)
            '('InflyCount   (Sub (Member stateRead 'InflyCount)   (Length existed)))
            '('MessageCount newMessagesCount)))

        (let deleteCond (HasItems existed))

        (return (Extend
            (AsList (SetResult 'deleted result))
            (AsList (SetResult 'newMessagesCount newMessagesCount))
            (ListIf deleteCond (UpdateRow stateTable stateRow stateUpdate))

            (If deleteCond
                (Map existed (lambda '(item) (block '(
                    (let row '(
                        )__" QUEUE_ID_AND_SHARD_KEYS_PARAM R"__(
                        '('Offset (Member item 'Offset))
                    ))
                    (return (EraseRow inflyTable row))))))
                (AsList (Void)))

            (If deleteCond
                (Map existed (lambda '(item) (block '(
                    (let row '(
                        )__" QUEUE_ID_AND_SHARD_KEYS_PARAM R"__(
                        '('RandomId (Member item 'RandomId))
                        '('Offset   (Member item 'Offset))
                     ))
                    (return (EraseRow dataTable row))))))
                (AsList (Void)))

            (If deleteCond
                (Map existed (lambda '(item) (block '(
                    (let row '(
                        )__" QUEUE_ID_AND_SHARD_KEYS_PARAM R"__(
                        '('SentTimestamp (Member item 'SentTimestamp))
                        '('Offset        (Member item 'Offset))
                    ))
                    (return (EraseRow sentTsIdx row))))))
                (AsList (Void)))
        ))
    )
)__";

const char* const SetQueueAttributesQuery = R"__(
    (
        (let queueIdNumber              (Parameter 'QUEUE_ID_NUMBER (DataType 'Uint64)))
        (let queueIdNumberHash          (Parameter 'QUEUE_ID_NUMBER_HASH (DataType 'Uint64)))
        (let shard                      (Parameter 'SHARD  (DataType ')__" SHARD_TYPE_PARAM R"__()))
        (let queueIdNumberAndShardHash  (Parameter 'QUEUE_ID_NUMBER_AND_SHARD_HASH (DataType 'Uint64)))

        (let queueIdNumber      (Parameter 'QUEUE_ID_NUMBER (DataType 'Uint64)))
        (let queueIdNumberHash  (Parameter 'QUEUE_ID_NUMBER_HASH (DataType 'Uint64)))
        (let delay           (Parameter 'DELAY             (OptionalType (DataType 'Uint64))))
        (let retention       (Parameter 'RETENTION         (OptionalType (DataType 'Uint64))))
        (let visibility      (Parameter 'VISIBILITY        (OptionalType (DataType 'Uint64))))
        (let wait            (Parameter 'WAIT              (OptionalType (DataType 'Uint64))))
        (let maxMessageSize  (Parameter 'MAX_MESSAGE_SIZE  (OptionalType (DataType 'Uint64))))
        (let maxReceiveCount (Parameter 'MAX_RECEIVE_COUNT (OptionalType (DataType 'Uint64))))
        (let dlqArn          (Parameter 'DLQ_TARGET_ARN    (OptionalType (DataType 'Utf8String))))
        (let dlqName         (Parameter 'DLQ_TARGET_NAME   (OptionalType (DataType 'Utf8String))))
        (let userName        (Parameter 'USER_NAME         (DataType 'Utf8String)))

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
            'MaxReceiveCount))
        (let attrsRead (SelectRow attrsTable attrsRow attrsSelect))

        (let attrsUpdate '(
            '('DelaySeconds (Coalesce delay (Member attrsRead 'DelaySeconds)))
            '('MessageRetentionPeriod (Coalesce retention (Member attrsRead 'MessageRetentionPeriod)))
            '('ReceiveMessageWaitTime (Coalesce wait (Member attrsRead 'ReceiveMessageWaitTime)))
            '('VisibilityTimeout (Coalesce visibility (Member attrsRead 'VisibilityTimeout)))
            '('MaxReceiveCount (Coalesce maxReceiveCount (Member attrsRead 'MaxReceiveCount)))
            '('DlqName (Coalesce dlqName (Member attrsRead 'DlqName)))
            '('DlqArn (Coalesce dlqArn (Member attrsRead 'DlqArn)))
            '('MaximumMessageSize (Coalesce maxMessageSize (Member attrsRead 'MaximumMessageSize)))))

        (let queuesTable ')__" ROOT_PARAM R"__(/.Queues)
        (let queuesRow '(
            '('Account   userName)
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

const char* const LoadMessageQuery = R"__(
    (
        (let queueIdNumber              (Parameter 'QUEUE_ID_NUMBER (DataType 'Uint64)))
        (let shard                      (Parameter 'SHARD  (DataType ')__" SHARD_TYPE_PARAM R"__()))
        (let queueIdNumberAndShardHash  (Parameter 'QUEUE_ID_NUMBER_AND_SHARD_HASH (DataType 'Uint64)))
        (let keys (Parameter 'KEYS
            (ListType (StructType
                '('RandomId (DataType 'Uint64))
                '('Offset   (DataType 'Uint64))
                '('CurrentVisibilityDeadline (DataType 'Uint64))
                '('DlqIndex (DataType 'Uint64))
                '('IsDeadLetter (DataType 'Bool))
                '('VisibilityDeadline (DataType 'Uint64))))))
        (let now     (Parameter 'NOW     (DataType 'Uint64)))
        (let readId  (Parameter 'READ_ID (DataType 'Uint64)))

        (let dataTable  ')__" QUEUE_TABLES_FOLDER_PER_SHARD_PARAM R"__(/MessageData)
        (let inflyTable ')__" QUEUE_TABLES_FOLDER_PER_SHARD_PARAM R"__(/Infly)

        (let records
            (MapParameter keys (lambda '(item) (block '(
                (let read (block '(
                    (let row '(
                        )__" QUEUE_ID_AND_SHARD_KEYS_PARAM R"__(
                        '('Offset (Member item 'Offset))))
                    (let fields '(
                        'LoadId
                        'FirstReceiveTimestamp
                        'ReceiveCount
                        'SentTimestamp
                        'VisibilityDeadline
                        'DelayDeadline))
                    (return (SelectRow inflyTable row fields)))))

                (let data (block '(
                    (let row '(
                        )__" QUEUE_ID_AND_SHARD_KEYS_PARAM R"__(
                        '('RandomId (Member item 'RandomId))
                        '('Offset   (Member item 'Offset))))
                    (let fields '(
                        'Attributes
                        'Data
                        'SenderId
                        'MessageId))
                    (return (SelectRow dataTable row fields)))))

                (let receiveTimestamp
                    (If (Coalesce (Equal (Member read 'FirstReceiveTimestamp) (Uint64 '0)) (Bool 'false)) now (Member read 'FirstReceiveTimestamp)))

                (let visibilityDeadlineInDb
                    (Max
                        (Member read 'VisibilityDeadline)
                        (Coalesce
                            (Member read 'DelayDeadline)
                            (Uint64 '0)
                        )
                    )
                )

                (let valid
                    (Coalesce
                        (Or
                            (Equal visibilityDeadlineInDb (Member item 'CurrentVisibilityDeadline))
                            (And
                                (Equal (Member read 'LoadId) readId)
                                (Equal (Member read 'VisibilityDeadline) (Member item 'VisibilityDeadline))))
                        (Bool 'false)))

                (return
                    (AsStruct
                        '('Offset (Member item 'Offset))
                        '('RandomId (Member item 'RandomId))
                        '('LoadId readId)
                        '('Attributes (Member data 'Attributes))
                        '('Data (Member data 'Data))
                        '('SenderId (Member data 'SenderId))
                        '('MessageId (Member data 'MessageId))
                        '('FirstReceiveTimestamp receiveTimestamp)
                        '('LockTimestamp now)
                        '('IsDeadLetter (Member item 'IsDeadLetter))
                        '('DlqIndex (Member item 'DlqIndex))
                        '('ReceiveCount (Add (Member read 'ReceiveCount) (Uint32 '1)))
                        '('SentTimestamp (Member read 'SentTimestamp))
                        '('VisibilityDeadline (If valid (Member item 'VisibilityDeadline) (Member read 'VisibilityDeadline)))
                        '('Valid valid)
                        '('Exists (Exists read))))
                    )))))

        (let result
            (Filter records (lambda '(item) (block '(
                (return (Coalesce (Member item 'Valid) (Bool 'false))))))))

        (return (Extend
            (AsList (SetResult 'dlqExists (Bool 'true)))
            (AsList (SetResult 'result records))
            (AsList (SetResult 'movedMessagesCount (Uint64 '0)))

            (Map result (lambda '(item) (block '(
                (let row '(
                    )__" QUEUE_ID_AND_SHARD_KEYS_PARAM R"__(
                    '('Offset (Member item 'Offset))))
                (let update '(
                    '('LoadId                readId)
                    '('FirstReceiveTimestamp (Member item 'FirstReceiveTimestamp))
                    '('LockTimestamp         (Member item 'LockTimestamp))
                    '('ReceiveCount          (Member item 'ReceiveCount))
                    '('VisibilityDeadline    (Member item 'VisibilityDeadline))))
                (return (UpdateRow inflyTable row update))))))))
    )
)__";

const char* const LoadOrRedriveMessageQuery = R"__(
    (
        (let keys (Parameter 'KEYS
            (ListType (StructType
                '('RandomId (DataType 'Uint64))
                '('Offset   (DataType 'Uint64))
                '('CurrentVisibilityDeadline (DataType 'Uint64))
                '('DlqIndex (DataType 'Uint64))
                '('IsDeadLetter (DataType 'Bool))
                '('VisibilityDeadline (DataType 'Uint64))))))

        (let queueIdNumber              (Parameter 'QUEUE_ID_NUMBER (DataType 'Uint64)))
        (let queueIdNumberHash          (Parameter 'QUEUE_ID_NUMBER_HASH (DataType 'Uint64)))
        (let shard                      (Parameter 'SHARD  (DataType ')__" SHARD_TYPE_PARAM R"__()))
        (let queueIdNumberAndShardHash  (Parameter 'QUEUE_ID_NUMBER_AND_SHARD_HASH (DataType 'Uint64)))

        (let dlqIdNumber                (Parameter 'DLQ_ID_NUMBER (DataType 'Uint64)))
        (let dlqIdNumberHash            (Parameter 'DLQ_ID_NUMBER_HASH (DataType 'Uint64)))
        (let dlqShard                   (Parameter 'DLQ_SHARD  (DataType ')__" DLQ_SHARD_TYPE_PARAM R"__()))
        (let dlqIdNumberAndShardHash    (Parameter 'DLQ_ID_NUMBER_AND_SHARD_HASH (DataType 'Uint64)))

        (let now     (Parameter 'NOW     (DataType 'Uint64)))
        (let readId  (Parameter 'READ_ID (DataType 'Uint64)))
        (let deadLettersCount (Parameter 'DEAD_LETTERS_COUNT (DataType 'Uint64)))

        (let sourceMessageDataTable     ')__" QUEUE_TABLES_FOLDER_PER_SHARD_PARAM R"__(/MessageData)
        (let sourceSentTsIdxTable       ')__" QUEUE_TABLES_FOLDER_PER_SHARD_PARAM R"__(/SentTimestampIdx)
        (let sourceInflyTable           ')__" QUEUE_TABLES_FOLDER_PER_SHARD_PARAM R"__(/Infly)
        (let sourceStateTable           ')__" QUEUE_TABLES_FOLDER_PARAM R"__(/State)

        (let deadLetterMessageDataTable ')__" DLQ_TABLES_FOLDER_PER_SHARD_PARAM R"__(/MessageData)
        (let deadLetterMessagesTable    ')__" DLQ_TABLES_FOLDER_PER_SHARD_PARAM R"__(/Messages)
        (let deadLetterSentTsIdxTable   ')__" DLQ_TABLES_FOLDER_PER_SHARD_PARAM R"__(/SentTimestampIdx)
        (let deadLetterStateTable       ')__" DLQ_TABLES_FOLDER_PARAM R"__(/State)

        (let sourceStateRow '(
            )__" STATE_KEYS_PARAM R"__(
        ))
        (let sourceStateSelect '(
            'MessageCount
            'LastModifiedTimestamp
            'InflyCount))
        (let sourceStateRead (SelectRow sourceStateTable sourceStateRow sourceStateSelect))

        (let records
            (MapParameter keys (lambda '(item) (block '(
                (let read (block '(
                    (let row '(
                        )__" QUEUE_ID_AND_SHARD_KEYS_PARAM R"__(
                        '('Offset (Member item 'Offset))))
                    (let fields '(
                        'LoadId
                        'FirstReceiveTimestamp
                        'ReceiveCount
                        'SentTimestamp
                        'VisibilityDeadline))
                    (return (SelectRow sourceInflyTable row fields)))))

                (let data (block '(
                    (let row '(
                        )__" QUEUE_ID_AND_SHARD_KEYS_PARAM R"__(
                        '('RandomId (Member item 'RandomId))
                        '('Offset   (Member item 'Offset))))
                    (let fields '(
                        'Attributes
                        'Data
                        'SenderId
                        'MessageId))
                    (return (SelectRow sourceMessageDataTable row fields)))))

                (let receiveTimestamp
                    (If (Coalesce (Equal (Member read 'FirstReceiveTimestamp) (Uint64 '0)) (Bool 'false)) now (Member read 'FirstReceiveTimestamp)))

                (let valid
                    (Coalesce
                        (Or
                            (Equal (Member read 'VisibilityDeadline) (Member item 'CurrentVisibilityDeadline))
                            (And
                                (Equal (Member read 'LoadId) readId)
                                (Equal (Member read 'VisibilityDeadline) (Member item 'VisibilityDeadline))))
                        (Bool 'false)))

                (return
                    (AsStruct
                        '('Offset (Member item 'Offset))
                        '('RandomId (Member item 'RandomId))
                        '('IsDeadLetter (Member item 'IsDeadLetter))
                        '('DlqIndex (Member item 'DlqIndex))
                        '('LoadId readId)
                        '('Attributes (Member data 'Attributes))
                        '('Data (Member data 'Data))
                        '('SenderId (Member data 'SenderId))
                        '('MessageId (Member data 'MessageId))
                        '('FirstReceiveTimestamp receiveTimestamp)
                        '('LockTimestamp now)
                        '('ReceiveCount (Add (Member read 'ReceiveCount) (Uint32 '1)))
                        '('SentTimestamp (Member read 'SentTimestamp))
                        '('VisibilityDeadline (If valid (Member item 'VisibilityDeadline) (Member read 'VisibilityDeadline)))
                        '('Valid valid)
                        '('Exists (Exists read))))
                    )))))

        (let validMessages
            (Filter records (lambda '(item) (block '(
                (return (Coalesce (Member item 'Valid) (Bool 'false))))))))

        (let messagesToMove
            (Filter validMessages (lambda '(item) (block '(
                (return (Coalesce (Member item 'IsDeadLetter) (Bool 'false))))))))

        (let messagesToUpdate
            (Filter validMessages (lambda '(item) (block '(
                (return (Coalesce (Not (Member item 'IsDeadLetter)) (Bool 'false))))))))

        (let deadLetterStateRow '(
            )__" DLQ_STATE_KEYS_PARAM R"__(
        ))
        (let deadLetterStateSelect '(
            'MessageCount
            'WriteOffset
            'LastModifiedTimestamp))
        (let deadLetterStateRead (SelectRow deadLetterStateTable deadLetterStateRow deadLetterStateSelect))
        (let dlqExists (Exists deadLetterStateRead))

        (let newDlqMessagesCount (Add (Member deadLetterStateRead 'MessageCount) (Length messagesToMove)))
        (let newDlqWriteOffset (Add (Member deadLetterStateRead 'WriteOffset) deadLettersCount))
        (let dlqStartOffset (Add (Member deadLetterStateRead 'WriteOffset) (Uint64 '1)))

        (let dlqMostRecentTimestamp (Max (Member deadLetterStateRead 'LastModifiedTimestamp) now))

        (let deadLetterStateUpdate '(
            '('LastModifiedTimestamp dlqMostRecentTimestamp)
            '('MessageCount newDlqMessagesCount)
            '('WriteOffset newDlqWriteOffset)))

        (let newSourceMessagesCount (Sub (Member sourceStateRead 'MessageCount) (Length messagesToMove)))
        (let sourceStateUpdate '(
            '('LastModifiedTimestamp (Max (Member sourceStateRead 'LastModifiedTimestamp) now))
            '('MessageCount newSourceMessagesCount)
            '('InflyCount (Sub (Member sourceStateRead 'InflyCount) (Length messagesToMove)))))

        (return (Extend
            (AsList (SetResult 'dlqExists dlqExists))
            (AsList (SetResult 'result records))
            (AsList (SetResult 'movedMessagesCount (Length messagesToMove)))
            (AsList (SetResult 'newMessagesCount newSourceMessagesCount))
            (ListIf dlqExists (UpdateRow deadLetterStateTable deadLetterStateRow deadLetterStateUpdate))
            (ListIf dlqExists (UpdateRow sourceStateTable sourceStateRow sourceStateUpdate))

            (If dlqExists (Map messagesToUpdate (lambda '(item) (block '(
                (let row '(
                    )__" QUEUE_ID_AND_SHARD_KEYS_PARAM R"__(
                    '('Offset (Member item 'Offset))))
                (let update '(
                    '('LoadId                readId)
                    '('FirstReceiveTimestamp (Member item 'FirstReceiveTimestamp))
                    '('LockTimestamp         (Member item 'LockTimestamp))
                    '('ReceiveCount          (Member item 'ReceiveCount))
                    '('VisibilityDeadline    (Member item 'VisibilityDeadline))))
                (return (UpdateRow sourceInflyTable row update))))))
                (AsList (Void)))

            (If dlqExists (Map messagesToMove (lambda '(item) (block '(
                (let msgRow '(
                    )__" DLQ_ID_AND_SHARD_KEYS_PARAM R"__(
                    '('Offset (Add dlqStartOffset (Member item 'DlqIndex)))))
                (let delayDeadline (Uint64 '0))
                (let messageUpdate '(
                    '('RandomId readId)
                    '('SentTimestamp dlqMostRecentTimestamp)
                    '('DelayDeadline delayDeadline)))
                (return (UpdateRow deadLetterMessagesTable msgRow messageUpdate))))))
                (AsList (Void)))

            (If dlqExists (Map messagesToMove (lambda '(item) (block '(
                (let sentTsRow '(
                    )__" DLQ_ID_AND_SHARD_KEYS_PARAM R"__(
                    '('SentTimestamp dlqMostRecentTimestamp)
                    '('Offset (Add dlqStartOffset (Member item 'DlqIndex)))))
                (let delayDeadline (Uint64 '0))
                (let sentTsUpdate '(
                    '('RandomId readId)
                    '('DelayDeadline delayDeadline)))
                (return (UpdateRow deadLetterSentTsIdxTable sentTsRow sentTsUpdate))))))
                (AsList (Void)))

            (If dlqExists (Map messagesToMove (lambda '(item) (block '(
                (let dataRow '(
                    )__" DLQ_ID_AND_SHARD_KEYS_PARAM R"__(
                    '('RandomId readId)
                    '('Offset (Add dlqStartOffset (Member item 'DlqIndex)))))

                (let dataUpdate '(
                    '('Data (Member item 'Data))
                    '('Attributes (Member item 'Attributes))
                    '('SenderId (Member item 'SenderId))
                    '('MessageId (Member item 'MessageId))))
                (return (UpdateRow deadLetterMessageDataTable dataRow dataUpdate))))))
                (AsList (Void)))

            (If dlqExists (Map messagesToMove (lambda '(item) (block '(
                (let inflyRow '(
                    )__" QUEUE_ID_AND_SHARD_KEYS_PARAM R"__(
                    '('Offset (Member item 'Offset))))
                (return (EraseRow sourceInflyTable inflyRow))))))
                (AsList (Void)))

            (If dlqExists (Map messagesToMove (lambda '(item) (block '(
                (let dataRow '(
                    )__" QUEUE_ID_AND_SHARD_KEYS_PARAM R"__(
                    '('RandomId (Member item 'RandomId))
                    '('Offset (Member item 'Offset))))

                (return (EraseRow sourceMessageDataTable dataRow))))))
                (AsList (Void)))

            (If dlqExists (Map messagesToMove (lambda '(item) (block '(
                (let sentTsRow '(
                    )__" QUEUE_ID_AND_SHARD_KEYS_PARAM R"__(
                    '('SentTimestamp (Member item 'SentTimestamp))
                    '('Offset (Member item 'Offset))))
                (return (EraseRow sourceSentTsIdxTable sentTsRow))))))
                (AsList (Void)))
            ))
    )
)__";

const char* const WriteMessageQuery = R"__(
    (
        (let queueIdNumber              (Parameter 'QUEUE_ID_NUMBER (DataType 'Uint64)))
        (let queueIdNumberHash          (Parameter 'QUEUE_ID_NUMBER_HASH (DataType 'Uint64)))
        (let shard                      (Parameter 'SHARD  (DataType ')__" SHARD_TYPE_PARAM R"__()))
        (let queueIdNumberAndShardHash  (Parameter 'QUEUE_ID_NUMBER_AND_SHARD_HASH (DataType 'Uint64)))
        (let randomId  (Parameter 'RANDOM_ID  (DataType 'Uint64)))
        (let timestamp (Parameter 'TIMESTAMP  (DataType 'Uint64)))
        (let messages  (Parameter 'MESSAGES
            (ListType (StructType
                '('Attributes (DataType 'String))
                '('Data       (DataType 'String))
                '('MessageId  (DataType 'String))
                '('SenderId   (DataType 'String))
                '('Delay      (DataType 'Uint64))
                '('Index      (DataType 'Uint64))
            ))
        ))

        (let dataTable  ')__" QUEUE_TABLES_FOLDER_PER_SHARD_PARAM R"__(/MessageData)
        (let msgTable   ')__" QUEUE_TABLES_FOLDER_PER_SHARD_PARAM R"__(/Messages)
        (let sentTsIdx  ')__" QUEUE_TABLES_FOLDER_PER_SHARD_PARAM R"__(/SentTimestampIdx)
        (let stateTable ')__" QUEUE_TABLES_FOLDER_PARAM R"__(/State)

        (let stateRow '(
            )__" STATE_KEYS_PARAM R"__(
        ))
        (let stateSelect '(
            'MessageCount
            'WriteOffset
            'LastModifiedTimestamp))
        (let stateRead (SelectRow stateTable stateRow stateSelect))

        (let sentTimestamp (Max timestamp (Member stateRead 'LastModifiedTimestamp)))

        (let newMessagesCount (Add (Member stateRead 'MessageCount) (Length messages)))
        (let newWriteOffset (Add (Member stateRead 'WriteOffset) (Length messages)))
        (let startOffset (Add (Member stateRead 'WriteOffset) (Uint64 '1)))

        (let stateUpdate '(
            '('LastModifiedTimestamp sentTimestamp)
            '('MessageCount newMessagesCount)
            '('WriteOffset newWriteOffset)))

        (let result
            (MapParameter messages (lambda '(item) (block '(
                (return
                    (AsStruct
                        '('dedupCond (Bool 'true))
                    ))
        )))))

        (return (Extend
            (AsList (SetResult 'newMessagesCount newMessagesCount))
            (AsList (SetResult 'result result))

            (AsList (UpdateRow stateTable stateRow stateUpdate))

            (MapParameter messages (lambda '(item) (block '(
                (let msgRow '(
                    )__" QUEUE_ID_AND_SHARD_KEYS_PARAM R"__(
                    '('Offset (Add startOffset (Member item 'Index)))))
                (let delay (Member item 'Delay))
                (let delayDeadline (If (Equal delay (Uint64 '0)) (Uint64 '0) (Add sentTimestamp delay)))
                (let messageUpdate '(
                    '('RandomId randomId)
                    '('SentTimestamp sentTimestamp)
                    '('DelayDeadline delayDeadline)))
                (return (UpdateRow msgTable msgRow messageUpdate))))))

            (MapParameter messages (lambda '(item) (block '(
                (let sentTsRow '(
                    )__" QUEUE_ID_AND_SHARD_KEYS_PARAM R"__(
                    '('SentTimestamp sentTimestamp)
                    '('Offset (Add startOffset (Member item 'Index)))))
                (let delay (Member item 'Delay))
                (let delayDeadline (If (Equal delay (Uint64 '0)) (Uint64 '0) (Add sentTimestamp delay)))
                (let sentTsUpdate '(
                    '('RandomId randomId)
                    '('DelayDeadline delayDeadline)))
                (return (UpdateRow sentTsIdx sentTsRow sentTsUpdate))))))

            (MapParameter messages (lambda '(item) (block '(
                (let dataRow '(
                    )__" QUEUE_ID_AND_SHARD_KEYS_PARAM R"__(
                    '('RandomId randomId)
                    '('Offset (Add startOffset (Member item 'Index)))))

                (let dataUpdate '(
                    '('Data (Member item 'Data))
                    '('Attributes (Member item 'Attributes))
                    '('SenderId (Member item 'SenderId))
                    '('MessageId (Member item 'MessageId))))
                (return (UpdateRow dataTable dataRow dataUpdate))))))
        ))
    )
)__";

const char* const SetRetentionQuery = R"__(
    (
        (let queueIdNumber      (Parameter 'QUEUE_ID_NUMBER (DataType 'Uint64)))
        (let queueIdNumberHash  (Parameter 'QUEUE_ID_NUMBER_HASH (DataType 'Uint64)))
        (let now   (Parameter 'NOW   (DataType 'Uint64)))
        (let purge (Parameter 'PURGE (DataType 'Bool)))

        (let attrsTable ')__" QUEUE_TABLES_FOLDER_PARAM R"__(/Attributes)
        (let stateTable ')__" QUEUE_TABLES_FOLDER_PARAM R"__(/State)

        (let attrs (block '(
            (let row '(
                )__" ATTRS_KEYS_PARAM R"__())
            (let fields '(
                'MessageRetentionPeriod))
            (return (SelectRow attrsTable row fields)))))

        (let boundary
            (If purge now (Coalesce (Sub now (Member attrs 'MessageRetentionPeriod)) (Uint64 '0))))

        (let range '(
            )__" ALL_SHARDS_RANGE_PARAM R"__(
        ))
        (let fields '(
            ')__" SHARD_COLUMN_NAME_PARAM R"__(
            'RetentionBoundary
        ))
        (let records (Member (SelectRange stateTable range fields '()) 'List))

        (let result
            (Map records (lambda '(item) (block '(
                (let updated
                    (Coalesce
                        (Less (Member item 'RetentionBoundary) boundary)
                        (Bool 'false)))

                (return (AsStruct
                    '('Shard (
                        Member item ')__" SHARD_COLUMN_NAME_PARAM R"__(
                    ))
                    '('RetentionBoundary (Max boundary (Member item 'RetentionBoundary)))
                    '('Updated updated))))))))

        (let updated (Filter result (lambda '(item) (block '(
            (return (Coalesce (Equal (Member item 'Updated) (Bool 'true)) (Bool 'false))))))))

        (return (Extend
            (AsList (SetResult 'result result))
            (AsList (SetResult 'retention (Member attrs 'MessageRetentionPeriod)))

            (Map updated (lambda '(item) (block '(
                (let shard (Member item 'Shard))
                (let row '(
                    )__" STATE_KEYS_PARAM R"__(
                ))
                (let update '(
                    '('RetentionBoundary (Member item 'RetentionBoundary))))
                (return (UpdateRow stateTable row update))))))
        ))
    )
)__";

const char* const GetOldestMessageTimestampMetricsQuery = R"__(
    (
        (let queueIdNumber              (Parameter 'QUEUE_ID_NUMBER (DataType 'Uint64)))
        (let shard                      (Parameter 'SHARD  (DataType ')__" SHARD_TYPE_PARAM R"__()))
        (let queueIdNumberAndShardHash  (Parameter 'QUEUE_ID_NUMBER_AND_SHARD_HASH (DataType 'Uint64)))
        (let timeFrom (Parameter 'TIME_FROM (DataType 'Uint64)))

        (let sentTsIdx ')__" QUEUE_TABLES_FOLDER_PER_SHARD_PARAM R"__(/SentTimestampIdx)

        (let sentIdxRange '(
            )__" QUEUE_ID_AND_SHARD_KEYS_RANGE_PARAM R"__(
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
        (let queueIdNumber              (Parameter 'QUEUE_ID_NUMBER (DataType 'Uint64)))
        (let shard                      (Parameter 'SHARD  (DataType ')__" SHARD_TYPE_PARAM R"__()))
        (let queueIdNumberAndShardHash  (Parameter 'QUEUE_ID_NUMBER_AND_SHARD_HASH (DataType 'Uint64)))
        (let offsetFrom (Parameter 'OFFSET_FROM (DataType 'Uint64)))
        (let timeFrom   (Parameter 'TIME_FROM   (DataType 'Uint64)))
        (let timeTo     (Parameter 'TIME_TO     (DataType 'Uint64)))
        (let batchSize  (Parameter 'BATCH_SIZE  (DataType 'Uint64)))

        (let sentTsIdx    ')__" QUEUE_TABLES_FOLDER_PER_SHARD_PARAM R"__(/SentTimestampIdx)

        (let sentIdxRange '(
            )__" QUEUE_ID_AND_SHARD_KEYS_RANGE_PARAM R"__(
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

const char* const LoadInflyQuery = R"__(
    (
        (let queueIdNumber              (Parameter 'QUEUE_ID_NUMBER (DataType 'Uint64)))
        (let queueIdNumberHash          (Parameter 'QUEUE_ID_NUMBER_HASH (DataType 'Uint64)))
        (let shard                      (Parameter 'SHARD  (DataType ')__" SHARD_TYPE_PARAM R"__()))
        (let queueIdNumberAndShardHash  (Parameter 'QUEUE_ID_NUMBER_AND_SHARD_HASH (DataType 'Uint64)))

        (let inflyTable ')__" QUEUE_TABLES_FOLDER_PER_SHARD_PARAM R"__(/Infly)
        (let stateTable ')__" QUEUE_TABLES_FOLDER_PARAM R"__(/State)

        (let range '(
            )__" QUEUE_ID_AND_SHARD_KEYS_RANGE_PARAM R"__(
            '('Offset (Uint64 '0) (Uint64 '18446744073709551615))))
        (let fields '(
            'Offset
            'RandomId
            'DelayDeadline
            'ReceiveCount
            'VisibilityDeadline))
        (let infly (Member (SelectRange inflyTable range fields '()) 'List))

        (let stateRow '(
            )__" STATE_KEYS_PARAM R"__(
        ))
        (let stateFields '(
            'InflyVersion
            'InflyCount
            'MessageCount
            'ReadOffset
            'CreatedTimestamp
        ))
        (let state (SelectRow stateTable stateRow stateFields))

        (return (Extend
            (AsList (SetResult 'inflyVersion (Coalesce (Member state 'InflyVersion) (Uint64 '0))))
            (AsList (SetResult 'inflyCount (Member state 'InflyCount)))
            (AsList (SetResult 'messageCount (Member state 'MessageCount)))
            (AsList (SetResult 'readOffset (Member state 'ReadOffset)))
            (AsList (SetResult 'createdTimestamp (Member state 'CreatedTimestamp)))
            (AsList (SetResult 'infly infly))))
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
        (let queueSelect '('QueueName 'QueueState 'FolderId 'DlqName 'CustomQueueName))
        (let queues (Member (SelectRange queuesTable queuesRange queueSelect '()) 'List))

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

const char* const GetUserSettingsQuery = R"__(
    (
        (let fromUser  (Parameter 'FROM_USER  (DataType 'Utf8String)))
        (let fromName  (Parameter 'FROM_NAME  (DataType 'Utf8String)))
        (let batchSize (Parameter 'BATCH_SIZE (DataType 'Uint64)))

        (let settingsTable ')__" ROOT_PARAM R"__(/.Settings)

        (let range '(
            '('Account fromUser (Void))
            '('Name fromName (Void))))
        (let settingsSelect '(
            'Account
            'Name
            'Value))
        (let settingsResult (SelectRange settingsTable range settingsSelect '('('"ItemsLimit" batchSize))))
        (let settings (Member settingsResult 'List))
        (let truncated (Coalesce (Member settingsResult 'Truncated) (Bool 'false)))

        (return (Extend
            (AsList (SetResult 'settings settings))
            (AsList (SetResult 'truncated truncated))
        ))
    )
)__";

const char* const GetMessageCountMetricsQuery = R"__(
    (
        (let queueIdNumber      (Parameter 'QUEUE_ID_NUMBER (DataType 'Uint64)))
        (let shard              (Parameter 'SHARD  (DataType ')__" SHARD_TYPE_PARAM R"__()))
        (let queueIdNumberHash  (Parameter 'QUEUE_ID_NUMBER_HASH (DataType 'Uint64)))

        (let stateTable ')__" QUEUE_TABLES_FOLDER_PARAM R"__(/State)

        (let stateRow '(
            )__" STATE_KEYS_PARAM R"__(
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


const char* const GetStateQuery = R"__(
    (
        (let queueIdNumber      (Parameter 'QUEUE_ID_NUMBER (DataType 'Uint64)))
        (let queueIdNumberHash  (Parameter 'QUEUE_ID_NUMBER_HASH (DataType 'Uint64)))

        (let stateTable ')__" QUEUE_TABLES_FOLDER_PARAM R"__(/State)

        (let stateRange '(
            )__" ALL_SHARDS_RANGE_PARAM R"__(
        ))
        (let stateSelect '(
            ')__" SHARD_COLUMN_NAME_PARAM R"__(
            'MessageCount
            'InflyCount
            'InflyVersion
            'CreatedTimestamp
            'RetentionBoundary))

        (let stateRead (Member (SelectRange stateTable stateRange stateSelect '()) 'List))
        (return (AsList (SetResult 'state stateRead)))
    )
)__";


const char* const GetQueuesListQuery = R"__(
    (
        (let fromUser  (Parameter 'FROM_USER  (DataType 'Utf8String)))
        (let fromQueue (Parameter 'FROM_QUEUE (DataType 'Utf8String)))
        (let batchSize (Parameter 'BATCH_SIZE (DataType 'Uint64)))

        (let queuesTable ')__" ROOT_PARAM R"__(/.Queues)

        (let range '(
            '('Account fromUser (Void))
            '('QueueName fromQueue (Void))))
        (let queuesSelect '(
            'Account
            'QueueName
            'QueueState
            'FifoQueue
            'CreatedTimestamp
            'CustomQueueName
            'DlqName
            'FolderId
            'MasterTabletId
            'Version
            'Shards
            'TablesFormat))
        (let queuesResult (SelectRange queuesTable range queuesSelect '('('"ItemsLimit" batchSize))))
        (let queues (Member queuesResult 'List))
        (let truncated (Coalesce (Member queuesResult 'Truncated) (Bool 'false)))

        (return (Extend
            (AsList (SetResult 'queues queues))
            (AsList (SetResult 'truncated truncated))
        ))
    )
)__";

} // namespace

const char* GetStdQueryById(size_t id) {
    switch (id) {
    case DELETE_MESSAGE_ID:
        return DeleteMessageQuery;
    case WRITE_MESSAGE_ID:
        return WriteMessageQuery;
    case PURGE_QUEUE_ID:
        return PurgeQueueQuery;
    case CHANGE_VISIBILITY_ID:
        return ChangeMessageVisibilityQuery;
    case LIST_QUEUES_ID:
        return ListQueuesQuery;
    case SET_QUEUE_ATTRIBUTES_ID:
        return SetQueueAttributesQuery;
    case SET_RETENTION_ID:
        return SetRetentionQuery;
    case LOAD_MESSAGES_ID:
        return LoadMessageQuery;
    case INTERNAL_GET_QUEUE_ATTRIBUTES_ID:
        return InternalGetQueueAttributesQuery;
    case PURGE_QUEUE_STAGE2_ID:
        return PurgeQueueStage2Query;
    case GET_OLDEST_MESSAGE_TIMESTAMP_METRIC_ID:
        return GetOldestMessageTimestampMetricsQuery;
    case GET_RETENTION_OFFSET_ID:
        return GetRetentionOffsetQuery;
    case LOAD_INFLY_ID:
        return LoadInflyQuery;
    case ADD_MESSAGES_TO_INFLY_ID:
        return AddMessagesToInflyQuery;
    case LIST_DEAD_LETTER_SOURCE_QUEUES_ID:
        return ListDeadLetterSourceQueuesQuery;
    case LOAD_OR_REDRIVE_MESSAGE_ID:
        return LoadOrRedriveMessageQuery;
    case GET_USER_SETTINGS_ID:
        return GetUserSettingsQuery;
    case GET_QUEUES_LIST_ID:
        return GetQueuesListQuery;
    case GET_MESSAGE_COUNT_METRIC_ID:
        return GetMessageCountMetricsQuery;
    case GET_STATE_ID:
        return GetStateQuery;
    case TAG_QUEUE_ID:
        return TagQueueQuery;
    }
    return nullptr;
}

} // namespace NKikimr::NSQS
