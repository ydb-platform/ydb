#include "db_queries_maker.h"

#include "queries.h"


namespace NKikimr::NSQS {

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