#include "db_queries_maker.h"

#include "queries.h"

namespace {
    const char* const STD_STATE_KEYS = R"__(
            '('QueueIdNumberHash queueIdNumberHash)
            '('QueueIdNumber queueIdNumber)
            '('Shard shard)
        )__";

    const char* const DLQ_STD_STATE_KEYS = R"__(
            '('QueueIdNumberHash dlqIdNumberHash)
            '('QueueIdNumber dlqIdNumber)
            '('Shard dlqShard)
        )__";
}



namespace NKikimr::NSQS {
    const char* TDbQueriesMaker::GetStateKeys() const {
        if (TablesFormat_ == 0) {
            return IsFifo_ ? "'('State (Uint64 '0))" : "'('State shard)";
        }
        return IsFifo_ ? GetIdKeys() : STD_STATE_KEYS;
    }

    const char* TDbQueriesMaker::GetDlqStateKeys() const {
        if (DlqTablesFormat_ == 0) {
            return IsFifo_ ? "'('State (Uint64 '0))" : "'('State dlqShard)";
        }
        return IsFifo_ ? GetDlqIdKeys() : DLQ_STD_STATE_KEYS;
    }

    const char* TDbQueriesMaker::GetAllShardsRange() const {
        if (TablesFormat_ == 1) {
            if (IsFifo_) {
                return QUEUE_ID_KEYS_RANGE;
            }
            return R"__(
                '('QueueIdNumberHash queueIdNumberHash queueIdNumberHash)
                '('QueueIdNumber queueIdNumber queueIdNumber)
                '('Shard (Uint32 '0) (Uint32 '4294967295))
            )__";
        }
        return "'('State (Uint64 '0) (Uint64 '18446744073709551615))";
    }

    TString TDbQueriesMaker::FillQuery(const char* query) const {
        return Sprintf(
            query,
            Root_.c_str(),                          // 1
            QueueTablesFolderPerShard_.c_str(),     // 2
            QueueTablesFolder_.c_str(),             // 3

            QueueName_.c_str(),                     // 4
            GetIdKeys(),                            // 5
            GetIdKeysRange(),                       // 6
            GetIdAndShardKeys(),                    // 7
            GetIdAndShardKeysRange(),               // 8
            GetShardColumnType(TablesFormat_),      // 9
            GetShardColumnName(),                   // 10
            GetStateKeys(),                         // 11
            GetAttrKeys(),                          // 12
            GetAllShardsRange(),                    // 13

            DlqTablesFolder_.c_str(),               // 14
            DlqTablesFolderPerShard_.c_str(),       // 15

            GetDlqIdKeys(),                         // 16
            GetDlqIdAndShardKeys(),                 // 17
            GetShardColumnType(DlqTablesFormat_),   // 18
            GetDlqStateKeys()                       // 19
        );
    }

    TString TDbQueriesMaker::operator() (EQueryId id) const {
        return FillQuery(GetQueryById(id));
    }

    TString TDbQueriesMaker::GetMatchQueueAttributesQuery() const {
        return FillQuery(MatchQueueAttributesQuery);
    }


} // namespace NKikimr::NSQS
