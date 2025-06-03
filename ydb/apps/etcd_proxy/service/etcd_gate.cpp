#include "etcd_gate.h"
#include "etcd_shared.h"
#include "etcd_events.h"
#include "etcd_impl.h"

#include <vector>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/executor_thread.h>

namespace NEtcd {

using namespace NActors;
using namespace NYdb::NQuery;

namespace {

class TMainGate : public TActorBootstrapped<TMainGate> {
public:
    TMainGate(TIntrusivePtr<NMonitoring::TDynamicCounters> counters, TSharedStuff::TPtr stuff)
        : Counters(std::move(counters)), Stuff(std::move(stuff)), Query(Stuff->TablePrefix + NResource::Find("revision.sql"sv))
    {}

    void Bootstrap(const TActorContext&) {
        Become(&TThis::StateFunc);
        Stuff->MainGate = SelfId();
    }
private:
    using TActorsList = std::vector<TActorId>;
    std::deque<std::tuple<TKeysSet, TActorsList>> Queue;

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvRequestRevision, Handle);
            hFunc(TEvReturnRevision, Handle);

            HFunc(TEvQueryResult, Handle);
            HFunc(TEvQueryError, Handle);
        }
    }

    static bool HasIntersection(const TKeysSet& lhs, const TKeysSet& rhs) {
        for (auto i = lhs.cbegin(), j = rhs.cbegin(); lhs.cend() != i && rhs.cend() != j;) {
            if (*i < *j) {
                if (!i->second.empty() && (Endless == i->second || i->second >= j->first))
                    return true;
                else
                    i = lhs.lower_bound(*j);
            } else if (*i > *j) {
                if (!j->second.empty() && (Endless == j->second || j->second >= i->first))
                    return true;
                else
                    j = rhs.lower_bound(*i);
            } else
                return true;
        }

        return false;
    }

    void Handle(TEvRequestRevision::TPtr &ev) {
        if (Queue.empty()) {
            Queue.emplace_back(std::move(ev->Get()->KeysSet), TActorsList(1U, ev->Sender));
            RequestNextRevision();
        } else {
            if (!ev->Get()->KeysSet.empty()) {
                for (auto it = Queue.begin(); Queue.end() > it; ++it) {
                    if (auto& keys = std::get<TKeysSet>(*it); !keys.empty() && !HasIntersection(ev->Get()->KeysSet, keys)) {
                        keys.merge(std::move(ev->Get()->KeysSet));
                        std::get<TActorsList>(*it).emplace_back(ev->Sender);
                        return;
                    }
                }
            }
            Queue.emplace_back(std::move(ev->Get()->KeysSet), TActorsList(1U, ev->Sender));
        }
    }

    void Handle(TEvQueryResult::TPtr &ev, const TActorContext& ctx) {
        i64 revision = 0LL;
        if (auto parser = NYdb::TResultSetParser(ev->Get()->Results.front()); parser.TryNextRow()) {
            revision = NYdb::TValueParser(parser.GetValue(0)).GetInt64();
        }

        const auto current = std::get<TActorsList>(std::move(Queue.front()));
        Queue.pop_front();

        const auto guard = current.size() > 1U ? std::shared_ptr<TMainGate>(this, [revision, my = this->SelfId(), stuff = TSharedStuff::TWeakPtr(Stuff)](void*) {
            if (const auto lock = stuff.lock())
                lock->ActorSystem->Send(my, new TEvReturnRevision(revision));
        }) : std::shared_ptr<TMainGate>() ;

        for (const auto& actor : current)
            ctx.Send(actor, new TEvReturnRevision(revision, guard));
        RequestNextRevision();
    }

    void Handle(TEvQueryError::TPtr &ev, const TActorContext& ctx) {
        std::cout << "Request revision SQL error received " << ev->Get()->Issues.ToString() << std::endl;
        const auto current = std::get<TActorsList>(std::move(Queue.front()));
        Queue.pop_front();
        for (const auto& actor : current)
            ctx.Send(actor, new TEvQueryError(ev->Get()->Issues));
        RequestNextRevision();
    }

    void RequestNextRevision() {
        if (Queue.empty())
            return;

        TQueryClient::TQueryResultFunc callback = [query = Query](TQueryClient::TSession session) -> TAsyncExecuteQueryResult {
            return session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx());
        };

        Stuff->Client->RetryQuery(std::move(callback)).Subscribe([my = this->SelfId(), stuff = TSharedStuff::TWeakPtr(Stuff)](const auto& future) {
            if (const auto lock = stuff.lock()) {
                if (const auto res = future.GetValueSync(); res.IsSuccess())
                    lock->ActorSystem->Send(my, new TEvQueryResult(res.GetResultSets()));
                else
                    lock->ActorSystem->Send(my, new TEvQueryError(res.GetIssues()));
            }
        });
    }

    void Handle(TEvReturnRevision::TPtr &ev) {
        NYdb::TParamsBuilder params;
        const auto& revisionParamName = AddParam("Revision", params, ev->Get()->Revision);

        std::ostringstream sql;
        sql << Stuff->TablePrefix;
        sql << "insert into `commited` (`revision`,`timestamp`) values (" << revisionParamName << ",CurrentUtcTimestamp());" << std::endl;
//      std::cout << std::endl << sql.view() << std::endl;

        TQueryClient::TQueryResultFunc callback = [query = sql.str(), args = params.Build()](TQueryClient::TSession session) -> TAsyncExecuteQueryResult {
            return session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx(), args);
        };

        const auto rev = ev->Get()->Revision;
        Stuff->UpdateRevision(rev);
        Stuff->Client->RetryQuery(std::move(callback)).Subscribe([rev](const auto& future) {
            if (const auto res = future.GetValue(); res.IsSuccess())
                std::cout << "Revision " << rev << " commited succesfully." << std::endl;
            else
                std::cout << "Revision " << rev << " commit finished with errors: " << res.GetIssues().ToString() << std::endl;
        });
    }

    const TIntrusivePtr<NMonitoring::TDynamicCounters> Counters;
    const TSharedStuff::TPtr Stuff;
    const std::string Query;
};

}

NActors::IActor* BuildMainGate(TIntrusivePtr<NMonitoring::TDynamicCounters> counters, TSharedStuff::TPtr stuff) {
    return new TMainGate(std::move(counters), std::move(stuff));

}

}
