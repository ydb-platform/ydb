/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once

/* Generic header includes */
#include <aws/sqs/SQSErrors.h>
#include <aws/core/client/GenericClientConfiguration.h>
#include <aws/core/client/AWSError.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/client/AsyncCallerContext.h>
#include <aws/core/http/HttpTypes.h>
#include <aws/sqs/SQSEndpointProvider.h>
#include <future>
#include <functional>
/* End of generic header includes */

/* Service model headers required in SQSClient header */
#include <aws/sqs/model/ChangeMessageVisibilityBatchResult.h>
#include <aws/sqs/model/CreateQueueResult.h>
#include <aws/sqs/model/DeleteMessageBatchResult.h>
#include <aws/sqs/model/GetQueueAttributesResult.h>
#include <aws/sqs/model/GetQueueUrlResult.h>
#include <aws/sqs/model/ListDeadLetterSourceQueuesResult.h>
#include <aws/sqs/model/ListQueueTagsResult.h>
#include <aws/sqs/model/ListQueuesResult.h>
#include <aws/sqs/model/ReceiveMessageResult.h>
#include <aws/sqs/model/SendMessageResult.h>
#include <aws/sqs/model/SendMessageBatchResult.h>
#include <aws/core/NoResult.h>
/* End of service model headers required in SQSClient header */

namespace Aws
{
  namespace Http
  {
    class HttpClient;
    class HttpClientFactory;
  } // namespace Http

  namespace Utils
  {
    template< typename R, typename E> class Outcome;

    namespace Threading
    {
      class Executor;
    } // namespace Threading
  } // namespace Utils

  namespace Auth
  {
    class AWSCredentials;
    class AWSCredentialsProvider;
  } // namespace Auth

  namespace Client
  {
    class RetryStrategy;
  } // namespace Client

  namespace SQS
  {
    using SQSClientConfiguration = Aws::Client::GenericClientConfiguration<false>;
    using SQSEndpointProviderBase = Aws::SQS::Endpoint::SQSEndpointProviderBase;
    using SQSEndpointProvider = Aws::SQS::Endpoint::SQSEndpointProvider;

    namespace Model
    {
      /* Service model forward declarations required in SQSClient header */
      class AddPermissionRequest;
      class ChangeMessageVisibilityRequest;
      class ChangeMessageVisibilityBatchRequest;
      class CreateQueueRequest;
      class DeleteMessageRequest;
      class DeleteMessageBatchRequest;
      class DeleteQueueRequest;
      class GetQueueAttributesRequest;
      class GetQueueUrlRequest;
      class ListDeadLetterSourceQueuesRequest;
      class ListQueueTagsRequest;
      class ListQueuesRequest;
      class PurgeQueueRequest;
      class ReceiveMessageRequest;
      class RemovePermissionRequest;
      class SendMessageRequest;
      class SendMessageBatchRequest;
      class SetQueueAttributesRequest;
      class TagQueueRequest;
      class UntagQueueRequest;
      /* End of service model forward declarations required in SQSClient header */

      /* Service model Outcome class definitions */
      typedef Aws::Utils::Outcome<Aws::NoResult, SQSError> AddPermissionOutcome;
      typedef Aws::Utils::Outcome<Aws::NoResult, SQSError> ChangeMessageVisibilityOutcome;
      typedef Aws::Utils::Outcome<ChangeMessageVisibilityBatchResult, SQSError> ChangeMessageVisibilityBatchOutcome;
      typedef Aws::Utils::Outcome<CreateQueueResult, SQSError> CreateQueueOutcome;
      typedef Aws::Utils::Outcome<Aws::NoResult, SQSError> DeleteMessageOutcome;
      typedef Aws::Utils::Outcome<DeleteMessageBatchResult, SQSError> DeleteMessageBatchOutcome;
      typedef Aws::Utils::Outcome<Aws::NoResult, SQSError> DeleteQueueOutcome;
      typedef Aws::Utils::Outcome<GetQueueAttributesResult, SQSError> GetQueueAttributesOutcome;
      typedef Aws::Utils::Outcome<GetQueueUrlResult, SQSError> GetQueueUrlOutcome;
      typedef Aws::Utils::Outcome<ListDeadLetterSourceQueuesResult, SQSError> ListDeadLetterSourceQueuesOutcome;
      typedef Aws::Utils::Outcome<ListQueueTagsResult, SQSError> ListQueueTagsOutcome;
      typedef Aws::Utils::Outcome<ListQueuesResult, SQSError> ListQueuesOutcome;
      typedef Aws::Utils::Outcome<Aws::NoResult, SQSError> PurgeQueueOutcome;
      typedef Aws::Utils::Outcome<ReceiveMessageResult, SQSError> ReceiveMessageOutcome;
      typedef Aws::Utils::Outcome<Aws::NoResult, SQSError> RemovePermissionOutcome;
      typedef Aws::Utils::Outcome<SendMessageResult, SQSError> SendMessageOutcome;
      typedef Aws::Utils::Outcome<SendMessageBatchResult, SQSError> SendMessageBatchOutcome;
      typedef Aws::Utils::Outcome<Aws::NoResult, SQSError> SetQueueAttributesOutcome;
      typedef Aws::Utils::Outcome<Aws::NoResult, SQSError> TagQueueOutcome;
      typedef Aws::Utils::Outcome<Aws::NoResult, SQSError> UntagQueueOutcome;
      /* End of service model Outcome class definitions */

      /* Service model Outcome callable definitions */
      typedef std::future<AddPermissionOutcome> AddPermissionOutcomeCallable;
      typedef std::future<ChangeMessageVisibilityOutcome> ChangeMessageVisibilityOutcomeCallable;
      typedef std::future<ChangeMessageVisibilityBatchOutcome> ChangeMessageVisibilityBatchOutcomeCallable;
      typedef std::future<CreateQueueOutcome> CreateQueueOutcomeCallable;
      typedef std::future<DeleteMessageOutcome> DeleteMessageOutcomeCallable;
      typedef std::future<DeleteMessageBatchOutcome> DeleteMessageBatchOutcomeCallable;
      typedef std::future<DeleteQueueOutcome> DeleteQueueOutcomeCallable;
      typedef std::future<GetQueueAttributesOutcome> GetQueueAttributesOutcomeCallable;
      typedef std::future<GetQueueUrlOutcome> GetQueueUrlOutcomeCallable;
      typedef std::future<ListDeadLetterSourceQueuesOutcome> ListDeadLetterSourceQueuesOutcomeCallable;
      typedef std::future<ListQueueTagsOutcome> ListQueueTagsOutcomeCallable;
      typedef std::future<ListQueuesOutcome> ListQueuesOutcomeCallable;
      typedef std::future<PurgeQueueOutcome> PurgeQueueOutcomeCallable;
      typedef std::future<ReceiveMessageOutcome> ReceiveMessageOutcomeCallable;
      typedef std::future<RemovePermissionOutcome> RemovePermissionOutcomeCallable;
      typedef std::future<SendMessageOutcome> SendMessageOutcomeCallable;
      typedef std::future<SendMessageBatchOutcome> SendMessageBatchOutcomeCallable;
      typedef std::future<SetQueueAttributesOutcome> SetQueueAttributesOutcomeCallable;
      typedef std::future<TagQueueOutcome> TagQueueOutcomeCallable;
      typedef std::future<UntagQueueOutcome> UntagQueueOutcomeCallable;
      /* End of service model Outcome callable definitions */
    } // namespace Model

    class SQSClient;

    /* Service model async handlers definitions */
    typedef std::function<void(const SQSClient*, const Model::AddPermissionRequest&, const Model::AddPermissionOutcome&, const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) > AddPermissionResponseReceivedHandler;
    typedef std::function<void(const SQSClient*, const Model::ChangeMessageVisibilityRequest&, const Model::ChangeMessageVisibilityOutcome&, const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) > ChangeMessageVisibilityResponseReceivedHandler;
    typedef std::function<void(const SQSClient*, const Model::ChangeMessageVisibilityBatchRequest&, const Model::ChangeMessageVisibilityBatchOutcome&, const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) > ChangeMessageVisibilityBatchResponseReceivedHandler;
    typedef std::function<void(const SQSClient*, const Model::CreateQueueRequest&, const Model::CreateQueueOutcome&, const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) > CreateQueueResponseReceivedHandler;
    typedef std::function<void(const SQSClient*, const Model::DeleteMessageRequest&, const Model::DeleteMessageOutcome&, const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) > DeleteMessageResponseReceivedHandler;
    typedef std::function<void(const SQSClient*, const Model::DeleteMessageBatchRequest&, const Model::DeleteMessageBatchOutcome&, const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) > DeleteMessageBatchResponseReceivedHandler;
    typedef std::function<void(const SQSClient*, const Model::DeleteQueueRequest&, const Model::DeleteQueueOutcome&, const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) > DeleteQueueResponseReceivedHandler;
    typedef std::function<void(const SQSClient*, const Model::GetQueueAttributesRequest&, const Model::GetQueueAttributesOutcome&, const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) > GetQueueAttributesResponseReceivedHandler;
    typedef std::function<void(const SQSClient*, const Model::GetQueueUrlRequest&, const Model::GetQueueUrlOutcome&, const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) > GetQueueUrlResponseReceivedHandler;
    typedef std::function<void(const SQSClient*, const Model::ListDeadLetterSourceQueuesRequest&, const Model::ListDeadLetterSourceQueuesOutcome&, const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) > ListDeadLetterSourceQueuesResponseReceivedHandler;
    typedef std::function<void(const SQSClient*, const Model::ListQueueTagsRequest&, const Model::ListQueueTagsOutcome&, const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) > ListQueueTagsResponseReceivedHandler;
    typedef std::function<void(const SQSClient*, const Model::ListQueuesRequest&, const Model::ListQueuesOutcome&, const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) > ListQueuesResponseReceivedHandler;
    typedef std::function<void(const SQSClient*, const Model::PurgeQueueRequest&, const Model::PurgeQueueOutcome&, const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) > PurgeQueueResponseReceivedHandler;
    typedef std::function<void(const SQSClient*, const Model::ReceiveMessageRequest&, const Model::ReceiveMessageOutcome&, const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) > ReceiveMessageResponseReceivedHandler;
    typedef std::function<void(const SQSClient*, const Model::RemovePermissionRequest&, const Model::RemovePermissionOutcome&, const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) > RemovePermissionResponseReceivedHandler;
    typedef std::function<void(const SQSClient*, const Model::SendMessageRequest&, const Model::SendMessageOutcome&, const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) > SendMessageResponseReceivedHandler;
    typedef std::function<void(const SQSClient*, const Model::SendMessageBatchRequest&, const Model::SendMessageBatchOutcome&, const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) > SendMessageBatchResponseReceivedHandler;
    typedef std::function<void(const SQSClient*, const Model::SetQueueAttributesRequest&, const Model::SetQueueAttributesOutcome&, const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) > SetQueueAttributesResponseReceivedHandler;
    typedef std::function<void(const SQSClient*, const Model::TagQueueRequest&, const Model::TagQueueOutcome&, const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) > TagQueueResponseReceivedHandler;
    typedef std::function<void(const SQSClient*, const Model::UntagQueueRequest&, const Model::UntagQueueOutcome&, const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) > UntagQueueResponseReceivedHandler;
    /* End of service model async handlers definitions */
  } // namespace SQS
} // namespace Aws
