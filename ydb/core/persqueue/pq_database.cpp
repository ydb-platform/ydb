#include "pq_database.h"

namespace NKikimr::NPQ {

    TString GetDatabaseFromConfig(const NKikimrPQ::TPQConfig& config) {
        if (config.HasDatabase()) {
            return config.GetDatabase();
        }

        // Compatibility with configurations where database name is not specified
        // These configurations seem to always use <database>/PQ as their Root
        const TString& root = config.GetRoot();
        if (root.EndsWith("/PQ")) {
            return root.substr(0, root.size() - 3);
        }

        // Fallback: try using Root as a database
        return root;
    }

} // namespace NKikimr::NPQ
