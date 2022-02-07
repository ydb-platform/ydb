#include "request_migrator.h"

#include <vector>
#include <numeric>
#include <cmath>

namespace NYdb {

namespace NMath {

TStats CalcCV(const std::vector<size_t>& in) {
    if (in.empty())
        return {0, 0.0};

    if (in.size() == 1)
        return {0, static_cast<float>(in[0])};

    const size_t sum = std::accumulate(in.begin(), in.end(), 0);
    if (!sum)
        return {0, 0.0};

    const float mean = sum / static_cast<float>(in.size());

    double tmp = 0;
    for (float t : in) {
        t -= mean;
        tmp += t * t;
    }

    auto cv = static_cast<ui64>(llround(100.0f * sqrt(tmp / static_cast<float>(in.size() - 1)) / mean));
    return {cv, mean};
}

} // namespace NMath

namespace NTable {

constexpr TDuration MIGRATION_PREPARE_CLIENT_TIMEOUT = TDuration::Seconds(5);

NThreading::TFuture<ui64> TRequestMigrator::SetHost(const TString& host, TSession targetSession) {
    std::lock_guard lock(Lock_);
    CurHost_ = host;
    TargetSession_.reset(new TSession(targetSession));
    if (!host)
        return {};

    Promise_ = NThreading::NewPromise<ui64>();
    return Promise_.GetFuture();
}

NThreading::TFuture<ui64> TRequestMigrator::SetHost(const TString& host) {
    std::lock_guard lock(Lock_);
    CurHost_ = host;
    TargetSession_.reset();
    if (!host)
        return {};

    Promise_ = NThreading::NewPromise<ui64>();
    return Promise_.GetFuture();
}

NThreading::TFuture<ui64> TRequestMigrator::PrepareQuery(size_t id) {
    const auto& query = Queries_[id];
    const auto settings = TPrepareDataQuerySettings()
        .ClientTimeout(MIGRATION_PREPARE_CLIENT_TIMEOUT);

    return TargetSession_->PrepareDataQuery(query, settings)
        .Apply([id, this](TAsyncPrepareQueryResult future) {
            auto result = future.ExtractValue();
            if (!result.IsSuccess()) {
                // In case of error SessionStatusInterception should change session state
                return NThreading::MakeFuture<ui64>(id);
            } else {
                auto nextId = id + 1;
                if (nextId == Queries_.size()) {
                    return NThreading::MakeFuture<ui64>(nextId);
                } else {
                    return PrepareQuery(nextId);
                }
            }
    });
}

std::function<void()> TRequestMigrator::CreateMigrationTask() {
    return [this]() {
        auto finishMigrationCb = [this](ui64 n) {
            // Make copy to prevent race with set new promise
            auto promise = Promise_;
            // Release target session
            TargetSession_.reset();
            promise.SetValue(n);
        };

        if (Queries_.empty() || !TargetSession_) {
            finishMigrationCb(0);
        } else {
            PrepareQuery(0).Subscribe([finishMigrationCb](NThreading::TFuture<ui64> future) {
                ui64 result = future.ExtractValue();
                finishMigrationCb(result);
            });
        }
    };
}

bool TRequestMigrator::IsOurSession(TSession::TImpl* session) const {
    if (!CurHost_)
        return false;

    if (session->GetEndpoint() != CurHost_)
        return false;

    return true;
}

bool TRequestMigrator::Reset() {
    if (Lock_.try_lock()) {
        if (CurHost_) {
            CurHost_.clear();
            TargetSession_.reset();
            Promise_.SetValue(0);
            Lock_.unlock();
            return true;
        } else {
            Lock_.unlock();
            return false;
        }
    } else {
        return false;
    }
}

bool TRequestMigrator::DoCheckAndMigrate(TSession::TImpl* session, std::shared_ptr<IMigratorClient> client) {
    if (session->GetEndpoint().empty())
        return false;

    if (Lock_.try_lock()) {
        if (IsOurSession(session)) {
            Queries_.clear();

            const auto& queryCache = session->GetQueryCacheUnsafe();
            for (auto it = queryCache.Begin(); it != queryCache.End(); ++it) {
                Queries_.emplace_back(it.Key());
            }

            // Force unlink session from ObjRegistry (if the session has been linked)
            // this allow to solve logical race between host scan task and real session dtor
            // which can cause double request migration
            session->Unlink();

            Finished_ = Promise_.GetFuture().Apply([](const NThreading::TFuture<ui64>&) {
                return NThreading::MakeFuture();
            });

            client->ScheduleTaskUnsafe(CreateMigrationTask(), TDuration());

            // Clear host to prevent multiple migrations
            CurHost_.clear();
            Lock_.unlock();
            return true;
        } else {
            Lock_.unlock();
            return false;
        }
    } else {
        return false;
    }
}

void TRequestMigrator::Wait() const {
    std::lock_guard lock(Lock_);
    if (Finished_.Initialized())
        Finished_.GetValueSync();
}

} // namespace NTable
} // namespace NYdb
