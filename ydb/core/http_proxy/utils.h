#pragma once

#include "auth_factory.h"
#include "events.h"
#include "http_req.h"
#include "json_proto_conversion.h"
#include "custom_metrics.h"
#include "exceptions_mapping.h"

#include <ydb/library/actors/http/http_proxy.h>
#include <library/cpp/cgiparam/cgiparam.h>
#include <library/cpp/digest/old_crc/crc.h>
#include <library/cpp/http/misc/parsed_request.h>
#include <library/cpp/http/server/response.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/protobuf/json/json_output_create.h>
#include <library/cpp/protobuf/json/proto2json.h>
#include <library/cpp/protobuf/json/proto2json_printer.h>
#include <library/cpp/uri/uri.h>

#include <ydb/core/security/ticket_parser_impl.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/grpc_caching/cached_grpc_request_actor.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/core/protos/serverless_proxy_config.pb.h>
#include <ydb/core/viewer/json/json.h>
#include <ydb/core/base/path.h>

#include <ydb/library/http_proxy/authorization/auth_helpers.h>
#include <ydb/library/http_proxy/error/error.h>
#include <ydb/services/sqs_topic/utils.h>
#include <yql/essentials/public/issue/yql_issue_message.h>
#include <ydb/library/ycloud/api/access_service.h>
#include <ydb/library/ycloud/api/iam_token_service.h>
#include <ydb/library/grpc/actor_client/grpc_service_cache.h>
#include <ydb/library/ycloud/impl/access_service.h>
#include <ydb/library/ycloud/impl/iam_token_service.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/datastreams/datastreams.h>
#include <ydb/public/sdk/cpp/src/client/topic/impl/common.h>

#include <ydb/services/datastreams/datastreams_proxy.h>
#include <ydb/services/datastreams/next_token.h>
#include <ydb/services/datastreams/shard_iterator.h>
#include <ydb/services/lib/sharding/sharding.h>

#include <ydb/services/ymq/grpc_service.h>
#include <ydb/services/ymq/ymq_proxy.h>

#include <ydb/public/api/grpc/draft/ydb_sqs_topic_v1.grpc.pb.h>
#include <ydb/services/sqs_topic/sqs_topic_proxy.h>
#include <ydb/services/sqs_topic/queue_url/utils.h>

#include <util/generic/guid.h>
#include <util/stream/file.h>
#include <util/string/ascii.h>
#include <util/string/cast.h>
#include <util/string/join.h>
#include <util/string/vector.h>

#include <nlohmann/json.hpp>

#include <ydb/library/folder_service/folder_service.h>
#include <ydb/library/folder_service/events.h>

#include <ydb/core/ymq/actor/auth_multi_factory.h>
#include <ydb/core/ymq/actor/serviceid.h>

#include <ydb/library/http_proxy/error/error.h>

#include <ydb/public/sdk/cpp/adapters/issue/issue.h>

#include <ydb/services/ymq/rpc_params.h>
#include <ydb/services/ymq/utils.h>

namespace NKikimr::NHttpProxy {

TException MapToException(NYdb::EStatus status, const TString& method, size_t issueCode = ISSUE_CODE_ERROR);

TString LogHttpRequestResponseCommonInfoString(const THttpRequestContext& httpContext, TInstant startTime, TStringBuf api, TStringBuf topicPath, TStringBuf method, TStringBuf userSid, int httpCode, TStringBuf httpResponseMessage);

} // NKikimr::NHttpProxy