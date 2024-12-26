#pragma once

#include <ydb/core/ymq/base/queue_path.h>

#include <ydb/core/ymq/queues/fifo/queries.h>
#include <ydb/core/ymq/queues/std/queries.h>

#include <ydb/library/yverify_stream/yverify_stream.h>

#include <util/string/builder.h>
#include <util/string/printf.h>
#include <util/datetime/base.h>

namespace NKikimr::NSQS {

const char* const QUEUE_ID_KEYS_RANGE = R"__(
            '('QueueIdNumberHash queueIdNumberHash queueIdNumberHash)
            '('QueueIdNumber queueIdNumber queueIdNumber)
        )__";

const char* const QUEUE_ID_KEYS = R"__(
            '('QueueIdNumberHash queueIdNumberHash)
            '('QueueIdNumber queueIdNumber)
        )__";

const char* const DLQ_ID_KEYS = R"__(
            '('QueueIdNumberHash dlqIdNumberHash)
            '('QueueIdNumber dlqIdNumber)
        )__";

const char* const QUEUE_ID_AND_SHARD_KEYS = R"__(
            '('QueueIdNumberAndShardHash queueIdNumberAndShardHash)
            '('QueueIdNumber queueIdNumber)
            '('Shard shard)
        )__";

const char* const DLQ_ID_AND_SHARD_KEYS = R"__(
            '('QueueIdNumberAndShardHash dlqIdNumberAndShardHash)
            '('QueueIdNumber dlqIdNumber)
            '('Shard dlqShard)
        )__";

const char* const QUEUE_ID_AND_SHARD_KEYS_RANGE = R"__(
            '('QueueIdNumberAndShardHash queueIdNumberAndShardHash queueIdNumberAndShardHash)
            '('QueueIdNumber queueIdNumber queueIdNumber)
            '('Shard shard shard)
        )__";



class TDbQueriesMaker {
public:
    TDbQueriesMaker(
        const TString& root,
        const TString& userName,
        const TString& queueName,
        ui64 queueVersion,
        bool isFifo,
        ui64 shard,
        ui32 tablesFormat,
        const TString& dlqName,
        ui64 dlqShard,
        ui64 dlqVersion,
        ui32 dlqTablesFormat
    )
        : Root_(root)
        , QueueName_(queueName)
        , TablesFormat_(tablesFormat)
        , IsFifo_(isFifo)
        , DlqTablesFormat_(dlqTablesFormat)
    {
        FillQueueVars(
            userName, queueName, queueVersion, tablesFormat, shard,
            QueueTablesFolder_, QueueTablesFolderPerShard_
        );
        FillQueueVars(
            userName, dlqName, dlqVersion, dlqTablesFormat, dlqShard,
            DlqTablesFolder_, DlqTablesFolderPerShard_
        );
    }

    TString operator() (EQueryId id) const;
    TString GetMatchQueueAttributesQuery() const;

private:
    const char* GetStateKeys() const;
    const char* GetDlqStateKeys() const;
    const char* GetAllShardsRange() const;

    const char* GetAttrKeys() const {
        return TablesFormat_ == 1 ? QUEUE_ID_KEYS : "'('State (Uint64 '0))";
    }
    const char* GetIdKeys() const {
        return TablesFormat_ == 1 ? QUEUE_ID_KEYS : "";
    }
    const char* GetDlqIdKeys() const {
        return DlqTablesFormat_ == 1 ? DLQ_ID_KEYS : "";
    }
    const char* GetIdKeysRange() const {
        return TablesFormat_ == 1 ? QUEUE_ID_KEYS_RANGE : "";
    }
    const char* GetIdAndShardKeysRange() const {
        return TablesFormat_ == 1 ? QUEUE_ID_AND_SHARD_KEYS_RANGE : "";
    }
    const char* GetIdAndShardKeys() const {
        return TablesFormat_ == 1 ? QUEUE_ID_AND_SHARD_KEYS : "";
    }
    const char* GetDlqIdAndShardKeys() const {
        return DlqTablesFormat_ == 1 ? DLQ_ID_AND_SHARD_KEYS : "";
    }
    const char* GetShardColumnName() const {
        return TablesFormat_ == 1 ? "Shard" : "State";
    }
    const char* GetShardColumnType(bool tablesFormat) const {
        return tablesFormat == 1 ? "Uint32" : "Uint64";
    }

    void FillQueueVars(
        const TString& userName,
        const TString& queueName,
        ui64 queueVersion,
        ui32 tablesFormat,
        ui64 shard,
        TString& tablesFolder,
        TString& tablesFolderPerShard
    ) const {
        if (tablesFormat == 1) {
            tablesFolder = tablesFolderPerShard = TStringBuilder() << Root_ << "/" << (IsFifo_ ? ".FIFO" : ".STD");
        } else {
            TQueuePath path(Root_, userName, queueName, queueVersion);
            tablesFolder = path.GetVersionedQueuePath();
            tablesFolderPerShard = TStringBuilder() << tablesFolder << "/" << shard;
        }
    }

    const char* GetQueryById(EQueryId id) const {
        const char* query = IsFifo_ ? GetFifoQueryById(id) : GetStdQueryById(id);
        Y_VERIFY_S(query, "unknown query id " << id << " fifo=" << IsFifo_);
        return query;
    }

    TString FillQuery(const char* query) const;

private:
    TString Root_;
    TString QueueTablesFolder_;
    TString QueueTablesFolderPerShard_;
    TString QueueName_;
    ui32 TablesFormat_;
    bool IsFifo_;
    TString DlqTablesFolder_;
    TString DlqTablesFolderPerShard_;
    ui32 DlqTablesFormat_;
};

} // namespace NKikimr::NSQS
