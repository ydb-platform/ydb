#include "utils.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/grpc_services/base/iface.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>
#include <ydb/core/persqueue/public/metrics/metrics.h>
#include <ydb/core/persqueue/public/mlp/mlp.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/services/sqs_topic/queue_url/utils.h>

#include <library/cpp/digest/md5/md5.h>

#include <util/system/hostname.h>
#include <util/system/unaligned_mem.h>

#include <format>

namespace NKikimr::NSqsTopic {

    TQueueNameWithConsumer SplitExtendedQueueName(TStringBuf queueNameExt) {
        TQueueNameWithConsumer result;
        if (!queueNameExt.TryRSplit('@', result.QueueName, result.Consumer)) {
            result.QueueName = queueNameExt;
        }
        return result;
    }

    const NKikimrConfig::TSqsConfig& Cfg() {
        return AppData()->SqsConfig;
    }

    TString MakeQueueUrl(const TRichQueueUrl& queueUrl, const NGRpcService::IRequestCtxBaseMtSafe* request) {
        TString requestEndpoint;
        if (request) {
            if (const auto value = request->GetPeerMetaValues(TString{REQUEST_ENDPOINT_METADATA_KEY})) {
                requestEndpoint = *value;
            }
        }
        const auto& httpProxyConfig = AppData()->HttpProxyConfig;
        return MakeQueueUrl(
            queueUrl,
            requestEndpoint,
            FQDNHostName(),
            static_cast<ui16>(httpProxyConfig.GetPort()),
            httpProxyConfig.GetSecure());
    }

    TString GenerateMessageId(const TString& database, const TString& topicPath, const NPQ::NMLP::TMessageId& pos) {
        MD5 md5;
        md5.Init();
        md5.Update(database);
        md5.Update("/");
        md5.Update(topicPath);
        md5.Update("/");
        md5.Update(ToString(pos.PartitionId));
        md5.Update("/");
        md5.Update(ToString(pos.Offset));
        ui8 digest[16];
        md5.Final(digest);
        // make guid v3 like
        digest[8] &= 0b10111111;
        digest[8] |= 0b10000000;
        digest[6] &= 0b01011111;
        digest[6] |= 0b01010000;

        TStringBuilder res;
        for (int i = 0; i < std::ssize(digest); ++i) {
            res << (EqualToOneOf(i, 4, 6, 8, 10) ? "-" : "") << Hex(digest[i], HF_FULL);
        }
        return res;
    }

    TVector<std::pair<TString, TString>> GetMetricsLabels(
        const TString& databasePath,
        const TString& topicPath,
        const TString& consumerName,
        const TString& method,
        TVector<std::pair<TString, TString>>&& labels,
        const TString& databaseId,
        const TString& cloudId,
        const TString& folderId,
        const NKikimrPQ::TPQConfig* pqConfig
    ) {
        TString fullDatabasePath = databasePath + "/";
        TString adjustedTopicPath;
        if (topicPath.StartsWith(fullDatabasePath)) {
            adjustedTopicPath = topicPath.substr(fullDatabasePath.size());
        } else {
            adjustedTopicPath = topicPath;
        }

        TVector<std::pair<TString, TString>> common{
            {"database", databasePath},
            {"database_id", databaseId},
        };
        if (!cloudId.empty() || !folderId.empty()) {
            common.emplace_back("cloud_id", cloudId);
            common.emplace_back("folder_id", folderId);
        }
        common.emplace_back("method", method);
        common.emplace_back("topic", adjustedTopicPath);
        // QueueUrl stores the path form. MetricsConsumerName converts the config name,
        // so restore that name before applying the label function.
        const auto& resolvedPqConfig = pqConfig ? *pqConfig : AppData()->PQConfig;
        common.emplace_back("consumer", NPQ::MetricsConsumerName(
            NPersQueue::ConvertNewConsumerName(consumerName, resolvedPqConfig), resolvedPqConfig));
        std::move(labels.begin(), labels.end(), std::back_inserter(common));
        return common;
    }

    TVector<std::pair<TString, TString>> GetRequestMessageCountMetricsLabels(
        const TString& databasePath,
        const TString& topicPath,
        const TString& consumer,
        const TString& method,
        const NKikimrPQ::TPQConfig* pqConfig
    ) {
        return GetMetricsLabels(
            databasePath,
            topicPath,
            consumer,
            method,
            {
                {"name", "api.sqs.request.message_count"}
            },
            {},
            {},
            {},
            pqConfig
        );
    }

    TVector<std::pair<TString, TString>> GetResponseMessageCountMetricsLabels(
        const TString& databasePath,
        const TString& topicPath,
        const TString& consumer,
        const TString& method,
        const TString& status,
        const NKikimrPQ::TPQConfig* pqConfig
    ) {
        return GetMetricsLabels(
            databasePath,
            topicPath,
            consumer,
            method,
            {
                {"name", "api.sqs.response.message_count"},
                {"status", status}
            },
            {},
            {},
            {},
            pqConfig
        );
    }

    TVector<std::pair<TString, TString>> GetResponseEmptyCountMetricsLabels(
        const TString& databasePath,
        const TString& topicPath,
        const TString& consumer,
        const TString& method,
        const NKikimrPQ::TPQConfig* pqConfig
    ) {
        return GetMetricsLabels(
            databasePath,
            topicPath,
            consumer,
            method,
            {
                {"name", "api.sqs.response.empty_count"}
            },
            {},
            {},
            {},
            pqConfig
        );
    }

    TVector<std::pair<TString, TString>> GetRequestSizeMetricsLabels(
        const TString& databasePath,
        const TString& topicPath,
        const TString& consumer,
        const TString& method,
        const NKikimrPQ::TPQConfig* pqConfig
    ) {
        return GetMetricsLabels(
            databasePath,
            topicPath,
            consumer,
            method,
            {
                {"name", "api.sqs.request.bytes"}
            },
            {},
            {},
            {},
            pqConfig
        );
    }


    ui64 SampleIdFromRequestId(const TStringBuf requestId) {
        if (sizeof(ui64) <= requestId.size()) [[likely]] {
            return ReadUnaligned<ui64>(requestId.data());
        }
        return 0;
    }
} // namespace NKikimr::NSqsTopic
