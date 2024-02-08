#pragma once
#include <ydb/core/client/server/msgbus_server_pq_metacache.h>

#include <ydb/library/actors/core/actor.h>
#include <library/cpp/testing/gmock_in_unittest/gmock.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/ptr.h>

namespace NKikimr {

using TEvPqMetaCache = NMsgBusProxy::NPqMetaCacheV2::TEvPqNewMetaCache;
using TPQGroupInfoPtr = TIntrusiveConstPtr<NSchemeCache::TSchemeCacheNavigate::TPQGroupInfo>;
using ESchemeStatus = NSchemeCache::TSchemeCacheNavigate::EStatus;
// The functionality of this class is not full.
// So anyone is welcome to improve it.
class TMockPQMetaCache: public TActor<TMockPQMetaCache> {
public:
    TMockPQMetaCache()
        : TActor<TMockPQMetaCache>(&TMockPQMetaCache::StateFunc)
    {
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPqMetaCache::TEvDescribeTopicsRequest, HandleDescribeTopics);
            HFunc(TEvPqMetaCache::TEvDescribeAllTopicsRequest, HandleDescribeAllTopics);
        default:
            UNIT_FAIL_NONFATAL("Unexpected event to PQ metacache: " << ev->GetTypeRewrite());
        }
    }

    MOCK_METHOD(void, HandleDescribeTopics, (TEvPqMetaCache::TEvDescribeTopicsRequest::TPtr& ev, const TActorContext& ctx), ());
    MOCK_METHOD(void, HandleDescribeAllTopics, (TEvPqMetaCache::TEvDescribeAllTopicsRequest::TPtr& ev, const TActorContext& ctx), ());
    //MOCK_METHOD4(HandleDescribeAllTopics, void(const TString& topic, ui64 balancerTabletId, NMsgBusProxy::TEvPqMetaCache::TEvGetBalancerDescribe::TPtr& ev, const TActorContext& ctx));

    //
    // Helpers
    //

    void SetDescribeCustomTopicsAnswer(const NSchemeCache::TSchemeCacheNavigate::TResultSet& resultSet = {}) {
        // ToDo - !!!
        using namespace testing;
//        auto handle = [success, description](NMsgBusProxy::TEvPqMetaCache::TEvGetNode::TPtr& ev, const TActorContext& ctx) {
//            auto& req = ev->Get()->Request;
//            req->Description = description;
//            req->Succeded = success;
//            auto result = MakeHolder<NMsgBusProxy::TEvPqMetaCache::TEvGetNodeResult>(std::move(req));
//
//            ctx.Send(ev->Sender, std::move(result));
//        };
        auto handle = [=](TEvPqMetaCache::TEvDescribeTopicsRequest::TPtr& ev, const TActorContext& ctx) {
            auto result = std::make_shared<NSchemeCache::TSchemeCacheNavigate>();
            result->ResultSet = resultSet;
            TVector<TString> topics;
            for (auto& res : resultSet) {
                topics.push_back(res.Path.back());
            }
            auto* response = new TEvPqMetaCache::TEvDescribeTopicsResponse(std::move(ev->Get()->Topics), result);
            ctx.Send(ev->Sender, response);
        };

        EXPECT_CALL(*this, HandleDescribeTopics(_, _))
            .WillOnce(Invoke(handle));
    }

    void SetAllTopicsAnswer(
            bool success = true, const NSchemeCache::TSchemeCacheNavigate::TResultSet& resultSet = {}
    ) {
        using namespace testing;
        auto handle = [=](TEvPqMetaCache::TEvDescribeAllTopicsRequest::TPtr& ev, const TActorContext& ctx) {
            auto* response = new TEvPqMetaCache::TEvDescribeAllTopicsResponse();
            response->Success = success;
            response->Path = "/Root/PQ/";
            auto* result = new NSchemeCache::TSchemeCacheNavigate();
            result->ResultSet = resultSet;
            response->Result.reset(result);

            auto factory = NPersQueue::TTopicNamesConverterFactory(AppData(ctx)->PQConfig, {});

            for (const auto & entry : resultSet) {
                auto converter = entry.PQGroupInfo ? factory.MakeTopicConverter(
                                   entry.PQGroupInfo->Description.GetPQTabletConfig()
                            ) : nullptr;
                response->Topics.push_back(converter);
            }

            ctx.Send(ev->Sender, std::move(response));
        };

        EXPECT_CALL(*this, HandleDescribeAllTopics(_, _))
            .WillOnce(Invoke(handle));
    }
};

} // namespace NKikimr
