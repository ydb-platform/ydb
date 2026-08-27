#pragma once

#include <deque>
#include <unordered_map>

#include <util/generic/singleton.h>
#include <util/string/escape.h>
#include <util/system/spinlock.h>

#include <ydb/core/data_integrity_trails/data_integrity_trails.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

namespace NKikimr {
namespace NDataIntegrity {

// Node-level cache for query text lookup in deferred lock TLI scenarios.
// Cross-node lookups use TKqpQueryTextCacheService when local cache misses.
class TNodeQueryTextCache {
public:
    static constexpr size_t MaxSpanIdEntries = 10000;

    static TNodeQueryTextCache& Instance() {
        return *Singleton<TNodeQueryTextCache>();
    }


    void Add(ui64 querySpanId, const TString& queryText) {
        if (querySpanId == 0 || queryText.empty()) {
            return;
        }
        with_lock(Lock) {
            // Only add if (querySpanId, queryText) pair is different from the previous entry
            if (!Entries.empty() &&
                Entries.back().first == querySpanId &&
                Entries.back().second == queryText) {
                return;
            }
            // Evict oldest entries if cache is full
            while (Entries.size() >= MaxSpanIdEntries) {
                Index.erase(Entries.front().first);
                Entries.pop_front();
            }
            Entries.push_back({querySpanId, queryText});
            // Point to the last element; overwrites if querySpanId already exists (keeping newest text)
            Index[querySpanId] = std::prev(Entries.end());
        }
    }

    TString Get(ui64 querySpanId) const {
        with_lock(Lock) {
            auto it = Index.find(querySpanId);
            if (it != Index.end()) {
                return it->second->second;
            }
        }
        return "";
    }

private:
    using TEntry = std::pair<ui64, TString>;
    using TDeque = std::deque<TEntry>;
    using TIterator = TDeque::iterator;

    mutable TAdaptiveLock Lock;
    TDeque Entries;
    // Auxiliary index: querySpanId -> iterator into Entries for O(1) lookup
    std::unordered_map<ui64, TIterator> Index;
};

// Structured parameters for TLI logging to improve readability
struct TTliLogParams {
    TString Component;
    TString Message;
    TString QueryText;

    struct TQueryInfo {
        TMaybe<ui64> Id;
        TString Text;
    };
    TVector<TQueryInfo> OtherQueries;

    TString TraceId;
    TMaybe<ui64> BreakerQuerySpanId;
    TMaybe<ui64> VictimQuerySpanId;
    TMaybe<ui64> CurrentQuerySpanId;
    TString VictimQueryText;
    bool IsCommitAction = false;
};

// Collects query texts and QuerySpanIds for TLI logging and victim stats attribution
class TQueryTextCollector {
public:
    // First query always stored (for victim stats); subsequent only if TLI enabled
    void AddQueryText(ui64 querySpanId, const TString& queryText) {
        if (querySpanId == 0 || queryText.empty()) {
            return;
        }

        TNodeQueryTextCache::Instance().Add(querySpanId, queryText);

        // Deduplicate consecutive identical entries
        if (QueryTexts.empty() ||
            QueryTexts.back().first != querySpanId ||
            QueryTexts.back().second != queryText) {
            QueryTexts.push_back({querySpanId, queryText});
            // Keep only the last N queries to prevent unbounded memory growth
            constexpr size_t MAX_QUERY_TEXTS = 100;
            while (QueryTexts.size() > MAX_QUERY_TEXTS) {
                QueryTexts.pop_front();
            }
        }
    }

    // Combine all query texts into a single string for logging
    TVector<NDataIntegrity::TTliLogParams::TQueryInfo> CombineQueryTexts() const {
        if (QueryTexts.empty()) {
            return {};
        }

        TVector<NDataIntegrity::TTliLogParams::TQueryInfo> result;
        for (size_t i = 0; i < QueryTexts.size(); ++i) {
            result.push_back(NDataIntegrity::TTliLogParams::TQueryInfo{QueryTexts[i].first, QueryTexts[i].second});
        }
        return result;
    }

    // Check if there are any query texts
    bool Empty() const {
        return QueryTexts.empty();
    }

    // Get the number of queries
    size_t GetQueryCount() const {
        return QueryTexts.size();
    }

    // Get all non-zero QuerySpanIds from all queries in this transaction
    TVector<ui64> GetAllQuerySpanIds() const {
        TVector<ui64> result;
        for (const auto& [spanId, _] : QueryTexts) {
            if (spanId != 0) {
                result.push_back(spanId);
            }
        }
        return result;
    }

    // Get the first query text
    TString GetFirstQueryText() const {
        if (QueryTexts.empty()) {
            return "";
        }
        return QueryTexts.front().second;
    }

    // Get query text by QuerySpanId
    TString GetQueryTextBySpanId(ui64 querySpanId) const {
        for (const auto& [spanId, queryText] : QueryTexts) {
            if (spanId == querySpanId) {
                return queryText;
            }
        }
        return "";
    }

    // Clear all query texts and QuerySpanIds
    void Clear() {
        QueryTexts.clear();
    }

private:
    std::deque<std::pair<ui64, TString>> QueryTexts;
};

inline void LogTli(const TTliLogParams& params, const NActors::TActorContext& ctx) {
    if (!IS_CTX_LOG_PRIORITY_ENABLED(ctx, NActors::NLog::PRI_INFO, NKikimrServices::TLI, 0)) {
        return;
    }

    auto message = YDB_LOG_CREATE_MESSAGE(
        {"component", params.Component},
        {"message", params.Message},
    );

    if (!params.TraceId.empty()) {
        YDB_LOG_UPDATE_MESSAGE(message, {"traceId", params.TraceId});
    }

    // Determine if this is a breaker or victim log based on which TraceId is set (and non-zero)
    const bool isBreaker = params.BreakerQuerySpanId.Defined() && *params.BreakerQuerySpanId != 0;

    if (isBreaker) {
        YDB_LOG_UPDATE_MESSAGE(message, {"breakerQuerySpanId", ToString(*params.BreakerQuerySpanId)});
    } else if (params.VictimQuerySpanId && *params.VictimQuerySpanId != 0) {
        YDB_LOG_UPDATE_MESSAGE(message, {"victimQuerySpanId", ToString(*params.VictimQuerySpanId)});
    }

    if (params.CurrentQuerySpanId && *params.CurrentQuerySpanId != 0) {
        YDB_LOG_UPDATE_MESSAGE(message, {"currentQuerySpanId", ToString(*params.CurrentQuerySpanId)});
    }

    // Use appropriate field names based on breaker vs victim
    for(auto& allQueriesItem : params.OtherQueries) {
        if (isBreaker) {
            if (allQueriesItem.Text == params.QueryText) {
                YDB_LOG_INFO_CTX_COMP(ctx, NKikimrServices::TLI, "",
                    message,
                    {"breakerQueryText", params.QueryText});
            }
            else {
                YDB_LOG_INFO_CTX_COMP(ctx, NKikimrServices::TLI, "",
                    message,
                    {"breakerQueryText", params.QueryText},
                    {"otherBreakerQueryText", allQueriesItem.Text});
            }
        }
        else {
            if (allQueriesItem.Text == params.VictimQueryText) {
                YDB_LOG_INFO_CTX_COMP(ctx, NKikimrServices::TLI, "",
                    message,
                    {"victimQueryText", params.VictimQueryText});
            }
            else {
                YDB_LOG_INFO_CTX_COMP(ctx, NKikimrServices::TLI, "",
                    message,
                    {"victimQueryText", params.VictimQueryText},
                    {"otherVictimQuerySpanId", allQueriesItem.Id},
                    {"otherVictimQueryText", allQueriesItem.Text});
            }
        }
    }
}

}
}
