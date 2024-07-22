#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/statistics/events.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/core/kqp/gateway/actors/kqp_ic_gateway_actors.h>


namespace NKikimr::NKqp {

class TAnalyzeActor : public NActors::TActorBootstrapped<TAnalyzeActor> { 
public:
    TAnalyzeActor(const TString& tablePath, NThreading::TPromise<NYql::IKikimrGateway::TGenericResult> promise)
        : TablePath_(tablePath) 
        , Promise_(promise)
    {}

    void Bootstrap() {
        using TNavigate = NSchemeCache::TSchemeCacheNavigate;
        auto navigate = std::make_unique<TNavigate>();
        auto& entry = navigate->ResultSet.emplace_back();
        entry.Path = SplitPath(TablePath_);
        entry.Operation = TNavigate::EOp::OpTable;
        entry.RequestType = TNavigate::TEntry::ERequestType::ByPath;
        navigate->Cookie = FirstRoundCookie;

        Send(NKikimr::MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(navigate.release()));

        Become(&TAnalyzeActor::StateWork);
    }

    STFUNC(StateWork) {
        switch(ev->GetTypeRewrite()) {
            HFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            HFunc(NStat::TEvStatistics::TEvScanTableResponse, Handle);
            IgnoreFunc(NStat::TEvStatistics::TEvScanTableAccepted);
            default:
                ALOG_CRIT(
                    NKikimrServices::KQP_GATEWAY, 
                    "TAnalyzeActor, unexpected event, request type: " << ev->GetTypeRewrite();
                );
                    
                Promise_.SetValue(
                    NYql::NCommon::ResultFromError<NYql::IKikimrGateway::TGenericResult>(
                        YqlIssue(
                            {}, NYql::TIssuesIds::UNEXPECTED, 
                            TStringBuilder() << "Unexpected event: " << ev->GetTypeRewrite()
                        )
                    )
                );

                this->PassAway();
        }
    }

private:
    void Handle(NStat::TEvStatistics::TEvScanTableResponse::TPtr& ev, const TActorContext& ctx) {
        NYql::IKikimrGateway::TGenericResult result;
        result.SetSuccess();
        Promise_.SetValue(std::move(result));
        
        this->Die(ctx);
    }

private:
    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx) {
        using TNavigate = NSchemeCache::TSchemeCacheNavigate;
        std::unique_ptr<TNavigate> navigate(ev->Get()->Request.Release());
        Y_ABORT_UNLESS(navigate->ResultSet.size() == 1);
        auto& entry = navigate->ResultSet.front();

        if (entry.Status != TNavigate::EStatus::Ok) {
            Promise_.SetValue(
                NYql::NCommon::ResultFromIssues<NYql::IKikimrGateway::TGenericResult>(
                    NYql::TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE,
                    TStringBuilder() << "Can't get statistics aggregator ID. " << entry.Status, 
                    {}
                )
            );
            this->Die(ctx);
            return;
        }

        if (navigate->Cookie == SecondRoundCookie) {
            if (entry.DomainInfo->Params.HasStatisticsAggregator()) {
                SendStatisticsAggregatorAnalyze(entry.DomainInfo->Params.GetStatisticsAggregator());
            } else {
                Promise_.SetValue(
                    NYql::NCommon::ResultFromIssues<NYql::IKikimrGateway::TGenericResult>(
                        NYql::TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE,
                        TStringBuilder() << "Can't get statistics aggregator ID.", {}
                    )
                );
            }

            this->Die(ctx);
            return;
        }
        
        PathId_ = entry.TableId.PathId;

        auto& domainInfo = entry.DomainInfo;

        auto navigateDomainKey = [this] (TPathId domainKey) {
            using TNavigate = NSchemeCache::TSchemeCacheNavigate;
            auto navigate = std::make_unique<TNavigate>();
            auto& entry = navigate->ResultSet.emplace_back();
            entry.TableId = TTableId(domainKey.OwnerId, domainKey.LocalPathId);
            entry.Operation = TNavigate::EOp::OpPath;
            entry.RequestType = TNavigate::TEntry::ERequestType::ByTableId;
            entry.RedirectRequired = false;
            navigate->Cookie = SecondRoundCookie;

            Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(navigate.release()));
        };

        if (!domainInfo->IsServerless()) {
            if (domainInfo->Params.HasStatisticsAggregator()) {
                SendStatisticsAggregatorAnalyze(domainInfo->Params.GetStatisticsAggregator());
                return;
            }
                
            navigateDomainKey(domainInfo->DomainKey);  
        } else {
            navigateDomainKey(domainInfo->ResourcesDomainKey);
        }
    }

    void SendStatisticsAggregatorAnalyze(ui64 statisticsAggregatorId) {
        auto scanTable = std::make_unique<NStat::TEvStatistics::TEvScanTable>();
        auto& record = scanTable->Record;
        PathIdFromPathId(PathId_, record.MutablePathId());

        
        Send(
            MakePipePerNodeCacheID(false),
            new TEvPipeCache::TEvForward(scanTable.release(), statisticsAggregatorId, true),
            IEventHandle::FlagTrackDelivery
        );
    }

private:
    enum _ : uint32_t {
        FirstRoundCookie,
        SecondRoundCookie,
    };

private:
    TString TablePath_;
    
    // For Statistics Aggregator
    TPathId PathId_;

    NThreading::TPromise<NYql::IKikimrGateway::TGenericResult> Promise_;
};

} // end of NKikimr::NKqp
