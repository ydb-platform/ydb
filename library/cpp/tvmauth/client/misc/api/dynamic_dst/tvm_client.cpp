#include "tvm_client.h"

#include <util/string/builder.h>

namespace NTvmAuth::NDynamicClient {
    TIntrusivePtr<TTvmClient> TTvmClient::Create(const NTvmApi::TClientSettings& settings, TLoggerPtr logger) {
        Y_ENSURE_EX(logger, TNonRetriableException() << "Logger is required");
        THolder<TTvmClient> p(new TTvmClient(settings, std::move(logger)));
        p->Init();
        p->StartWorker();
        return p.Release();
    }

    NThreading::TFuture<TAddResponse> TTvmClient::Add(TDsts&& dsts) {
        if (dsts.empty()) {
            LogDebug("Adding dst: got empty task");
            return NThreading::MakeFuture<TAddResponse>(TAddResponse{});
        }

        NThreading::TPromise<TAddResponse> promise = NThreading::NewPromise<TAddResponse>();

        TServiceTickets::TMapIdStr requestedTicketsFromStartUpCache = GetRequestedTicketsFromStartUpCache(dsts);

        if (requestedTicketsFromStartUpCache.size() == dsts.size() &&
            !IsInvalid(TServiceTickets::GetInvalidationTime(requestedTicketsFromStartUpCache), TInstant::Now())) {
            std::unique_lock lock(*ServiceTicketBatchUpdateMutex_);

            TPairTicketsErrors newCache;
            TServiceTicketsPtr cache = GetCachedServiceTickets();

            NTvmApi::TDstSetPtr oldDsts = GetDsts();
            std::shared_ptr<TDsts> newDsts = std::make_shared<TDsts>(oldDsts->begin(), oldDsts->end());

            for (const auto& ticket : cache->TicketsById) {
                newCache.Tickets.insert(ticket);
            }
            for (const auto& error : cache->ErrorsById) {
                newCache.Errors.insert(error);
            }
            for (const auto& ticket : requestedTicketsFromStartUpCache) {
                newCache.Tickets.insert(ticket);
                newDsts->insert(ticket.first);
            }

            UpdateServiceTicketsCache(std::move(newCache), GetStartUpCacheBornDate());
            SetDsts(std::move(newDsts));

            lock.unlock();

            TAddResponse response;

            for (const auto& dst : dsts) {
                response.emplace(dst, TDstResponse{EDstStatus::Success, TString()});
                LogDebug(TStringBuilder() << "Got ticket from disk cache"
                                          << ": dst=" << dst.Id << " got ticket");
            }

            promise.SetValue(std::move(response));
            return promise.GetFuture();
        }

        const size_t size = dsts.size();
        const ui64 id = ++TaskIds_;

        TaskQueue_.Enqueue(TTask{id, promise, std::move(dsts)});

        LogDebug(TStringBuilder() << "Adding dst: got task #" << id << " with " << size << " dsts");
        return promise.GetFuture();
    }

    std::optional<TString> TTvmClient::GetOptionalServiceTicketFor(const TTvmId dst) {
        TServiceTicketsPtr tickets = GetCachedServiceTickets();

        Y_ENSURE_EX(tickets,
                    TBrokenTvmClientSettings()
                        << "Need to enable fetching of service tickets in settings");

        auto it = tickets->TicketsById.find(dst);
        if (it != tickets->TicketsById.end()) {
            return it->second;
        }

        it = tickets->ErrorsById.find(dst);
        if (it != tickets->ErrorsById.end()) {
            ythrow TMissingServiceTicket()
                << "Failed to get ticket for '" << dst << "': "
                << it->second;
        }

        return {};
    }

    TTvmClient::TTvmClient(const NTvmApi::TClientSettings& settings, TLoggerPtr logger)
        : TBase(settings, logger)
    {
    }

    TTvmClient::~TTvmClient() {
        TBase::StopWorker();
    }

    void TTvmClient::Worker() {
        TBase::Worker();
        ProcessTasks();
    }

    void TTvmClient::ProcessTasks() {
        TaskQueue_.DequeueAll(&Tasks_);
        if (Tasks_.empty()) {
            return;
        }

        TDsts required;
        for (const TTask& task : Tasks_) {
            for (const auto& dst : task.Dsts) {
                required.insert(dst);
            }
        }

        TServiceTicketsPtr cache = UpdateMissingServiceTickets(required);
        for (TTask& task : Tasks_) {
            try {
                SetResponseForTask(task, *cache);
            } catch (const std::exception& e) {
                LogError(TStringBuilder()
                         << "Adding dst: task #" << task.Id << ": exception: " << e.what());
            } catch (...) {
                LogError(TStringBuilder()
                         << "Adding dst: task #" << task.Id << ": exception: " << CurrentExceptionMessage());
            }
        }

        Tasks_.clear();
    }

    static const TString UNKNOWN = "Unknown reason";
    void TTvmClient::SetResponseForTask(TTvmClient::TTask& task, const TServiceTickets& cache) {
        if (task.Promise.HasValue()) {
            LogWarning(TStringBuilder() << "Adding dst: task #" << task.Id << " already has value");
            return;
        }

        TAddResponse response;

        for (const auto& dst : task.Dsts) {
            if (cache.TicketsById.contains(dst.Id)) {
                response.emplace(dst, TDstResponse{EDstStatus::Success, TString()});

                LogDebug(TStringBuilder() << "Adding dst: task #" << task.Id
                                          << ": dst=" << dst.Id << " got ticket");
                continue;
            }

            auto it = cache.ErrorsById.find(dst.Id);
            const TString& error = it == cache.ErrorsById.end() ? UNKNOWN : it->second;
            response.emplace(dst, TDstResponse{EDstStatus::Fail, error});

            LogWarning(TStringBuilder() << "Adding dst: task #" << task.Id
                                        << ": dst=" << dst.Id
                                        << " failed to get ticket: " << error);
        }

        LogDebug(TStringBuilder() << "Adding dst: task #" << task.Id << ": set value");
        task.Promise.SetValue(std::move(response));
    }
}
