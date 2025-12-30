/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/sqs/SQS_EXPORTS.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/AmazonSerializableWebServiceRequest.h>
#include <aws/core/client/AWSClient.h>
#include <aws/core/client/AWSClientAsyncCRTP.h>
#include <aws/core/utils/xml/XmlSerializer.h>
#include <aws/sqs/SQSServiceClientModel.h>

namespace Aws
{
namespace SQS
{
  /**
   * <p>Welcome to the <i>Amazon SQS API Reference</i>.</p> <p>Amazon SQS is a
   * reliable, highly-scalable hosted queue for storing messages as they travel
   * between applications or microservices. Amazon SQS moves data between distributed
   * application components and helps you decouple these components.</p> <p>For
   * information on the permissions you need to use this API, see <a
   * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-authentication-and-access-control.html">Identity
   * and access management</a> in the <i>Amazon SQS Developer Guide.</i> </p> <p>You
   * can use <a href="http://aws.amazon.com/tools/#sdk">Amazon Web Services SDKs</a>
   * to access Amazon SQS using your favorite programming language. The SDKs perform
   * tasks such as the following automatically:</p> <ul> <li> <p>Cryptographically
   * sign your service requests</p> </li> <li> <p>Retry requests</p> </li> <li>
   * <p>Handle error responses</p> </li> </ul> <p> <b>Additional information</b> </p>
   * <ul> <li> <p> <a href="http://aws.amazon.com/sqs/">Amazon SQS Product Page</a>
   * </p> </li> <li> <p> <i>Amazon SQS Developer Guide</i> </p> <ul> <li> <p> <a
   * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-making-api-requests.html">Making
   * API Requests</a> </p> </li> <li> <p> <a
   * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-message-metadata.html#sqs-message-attributes">Amazon
   * SQS Message Attributes</a> </p> </li> <li> <p> <a
   * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html">Amazon
   * SQS Dead-Letter Queues</a> </p> </li> </ul> </li> <li> <p> <a
   * href="http://docs.aws.amazon.com/cli/latest/reference/sqs/index.html">Amazon SQS
   * in the <i>Command Line Interface</i> </a> </p> </li> <li> <p> <i>Amazon Web
   * Services General Reference</i> </p> <ul> <li> <p> <a
   * href="https://docs.aws.amazon.com/general/latest/gr/rande.html#sqs_region">Regions
   * and Endpoints</a> </p> </li> </ul> </li> </ul>
   */
  class AWS_SQS_API SQSClient : public Aws::Client::AWSXMLClient, public Aws::Client::ClientWithAsyncTemplateMethods<SQSClient>
  {
    public:
      typedef Aws::Client::AWSXMLClient BASECLASS;
      static const char* SERVICE_NAME;
      static const char* ALLOCATION_TAG;

       /**
        * Initializes client to use DefaultCredentialProviderChain, with default http client factory, and optional client config. If client config
        * is not specified, it will be initialized to default values.
        */
        SQSClient(const Aws::SQS::SQSClientConfiguration& clientConfiguration = Aws::SQS::SQSClientConfiguration(),
                  std::shared_ptr<SQSEndpointProviderBase> endpointProvider = Aws::MakeShared<SQSEndpointProvider>(ALLOCATION_TAG));

       /**
        * Initializes client to use SimpleAWSCredentialsProvider, with default http client factory, and optional client config. If client config
        * is not specified, it will be initialized to default values.
        */
        SQSClient(const Aws::Auth::AWSCredentials& credentials,
                  std::shared_ptr<SQSEndpointProviderBase> endpointProvider = Aws::MakeShared<SQSEndpointProvider>(ALLOCATION_TAG),
                  const Aws::SQS::SQSClientConfiguration& clientConfiguration = Aws::SQS::SQSClientConfiguration());

       /**
        * Initializes client to use specified credentials provider with specified client config. If http client factory is not supplied,
        * the default http client factory will be used
        */
        SQSClient(const std::shared_ptr<Aws::Auth::AWSCredentialsProvider>& credentialsProvider,
                  std::shared_ptr<SQSEndpointProviderBase> endpointProvider = Aws::MakeShared<SQSEndpointProvider>(ALLOCATION_TAG),
                  const Aws::SQS::SQSClientConfiguration& clientConfiguration = Aws::SQS::SQSClientConfiguration());


        /* Legacy constructors due deprecation */
       /**
        * Initializes client to use DefaultCredentialProviderChain, with default http client factory, and optional client config. If client config
        * is not specified, it will be initialized to default values.
        */
        SQSClient(const Aws::Client::ClientConfiguration& clientConfiguration);

       /**
        * Initializes client to use SimpleAWSCredentialsProvider, with default http client factory, and optional client config. If client config
        * is not specified, it will be initialized to default values.
        */
        SQSClient(const Aws::Auth::AWSCredentials& credentials,
                  const Aws::Client::ClientConfiguration& clientConfiguration);

       /**
        * Initializes client to use specified credentials provider with specified client config. If http client factory is not supplied,
        * the default http client factory will be used
        */
        SQSClient(const std::shared_ptr<Aws::Auth::AWSCredentialsProvider>& credentialsProvider,
                  const Aws::Client::ClientConfiguration& clientConfiguration);

        /* End of legacy constructors due deprecation */
        virtual ~SQSClient();


       /**
        * Converts any request object to a presigned URL with the GET method, using region for the signer and a timeout of 15 minutes.
        */
        Aws::String ConvertRequestToPresignedUrl(const Aws::AmazonSerializableWebServiceRequest& requestToConvert, const char* region) const;


        /**
         * <p>Adds a permission to a queue for a specific <a
         * href="https://docs.aws.amazon.com/general/latest/gr/glos-chap.html#P">principal</a>.
         * This allows sharing access to the queue.</p> <p>When you create a queue, you
         * have full control access rights for the queue. Only you, the owner of the queue,
         * can grant or deny permissions to the queue. For more information about these
         * permissions, see <a
         * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-writing-an-sqs-policy.html#write-messages-to-shared-queue">Allow
         * Developers to Write Messages to a Shared Queue</a> in the <i>Amazon SQS
         * Developer Guide</i>.</p>  <ul> <li> <p> <code>AddPermission</code>
         * generates a policy for you. You can use <code> <a>SetQueueAttributes</a> </code>
         * to upload your policy. For more information, see <a
         * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-creating-custom-policies.html">Using
         * Custom Policies with the Amazon SQS Access Policy Language</a> in the <i>Amazon
         * SQS Developer Guide</i>.</p> </li> <li> <p>An Amazon SQS policy can have a
         * maximum of 7 actions.</p> </li> <li> <p>To remove the ability to change queue
         * permissions, you must deny permission to the <code>AddPermission</code>,
         * <code>RemovePermission</code>, and <code>SetQueueAttributes</code> actions in
         * your IAM policy.</p> </li> </ul>  <p>Some actions take lists of
         * parameters. These lists are specified using the <code>param.n</code> notation.
         * Values of <code>n</code> are integers starting from 1. For example, a parameter
         * list with two elements looks like this:</p> <p>
         * <code>&amp;AttributeName.1=first</code> </p> <p>
         * <code>&amp;AttributeName.2=second</code> </p>  <p>Cross-account
         * permissions don't apply to this action. For more information, see <a
         * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-customer-managed-policy-examples.html#grant-cross-account-permissions-to-role-and-user-name">Grant
         * cross-account permissions to a role and a user name</a> in the <i>Amazon SQS
         * Developer Guide</i>.</p> <p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/sqs-2012-11-05/AddPermission">AWS
         * API Reference</a></p>
         */
        virtual Model::AddPermissionOutcome AddPermission(const Model::AddPermissionRequest& request) const;

        /**
         * A Callable wrapper for AddPermission that returns a future to the operation so that it can be executed in parallel to other requests.
         */
        template<typename AddPermissionRequestT = Model::AddPermissionRequest>
        Model::AddPermissionOutcomeCallable AddPermissionCallable(const AddPermissionRequestT& request) const
        {
            return SubmitCallable(&SQSClient::AddPermission, request);
        }

        /**
         * An Async wrapper for AddPermission that queues the request into a thread executor and triggers associated callback when operation has finished.
         */
        template<typename AddPermissionRequestT = Model::AddPermissionRequest>
        void AddPermissionAsync(const AddPermissionRequestT& request, const AddPermissionResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context = nullptr) const
        {
            return SubmitAsync(&SQSClient::AddPermission, request, handler, context);
        }

        /**
         * <p>Changes the visibility timeout of a specified message in a queue to a new
         * value. The default visibility timeout for a message is 30 seconds. The minimum
         * is 0 seconds. The maximum is 12 hours. For more information, see <a
         * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html">Visibility
         * Timeout</a> in the <i>Amazon SQS Developer Guide</i>.</p> <p>For example, you
         * have a message with a visibility timeout of 5 minutes. After 3 minutes, you call
         * <code>ChangeMessageVisibility</code> with a timeout of 10 minutes. You can
         * continue to call <code>ChangeMessageVisibility</code> to extend the visibility
         * timeout to the maximum allowed time. If you try to extend the visibility timeout
         * beyond the maximum, your request is rejected.</p> <p>An Amazon SQS message has
         * three basic states:</p> <ol> <li> <p>Sent to a queue by a producer.</p> </li>
         * <li> <p>Received from the queue by a consumer.</p> </li> <li> <p>Deleted from
         * the queue.</p> </li> </ol> <p>A message is considered to be <i>stored</i> after
         * it is sent to a queue by a producer, but not yet received from the queue by a
         * consumer (that is, between states 1 and 2). There is no limit to the number of
         * stored messages. A message is considered to be <i>in flight</i> after it is
         * received from a queue by a consumer, but not yet deleted from the queue (that
         * is, between states 2 and 3). There is a limit to the number of inflight
         * messages.</p> <p>Limits that apply to inflight messages are unrelated to the
         * <i>unlimited</i> number of stored messages.</p> <p>For most standard queues
         * (depending on queue traffic and message backlog), there can be a maximum of
         * approximately 120,000 inflight messages (received from a queue by a consumer,
         * but not yet deleted from the queue). If you reach this limit, Amazon SQS returns
         * the <code>OverLimit</code> error message. To avoid reaching the limit, you
         * should delete messages from the queue after they're processed. You can also
         * increase the number of queues you use to process your messages. To request a
         * limit increase, <a
         * href="https://console.aws.amazon.com/support/home#/case/create?issueType=service-limit-increase&amp;limitType=service-code-sqs">file
         * a support request</a>.</p> <p>For FIFO queues, there can be a maximum of 20,000
         * inflight messages (received from a queue by a consumer, but not yet deleted from
         * the queue). If you reach this limit, Amazon SQS returns no error messages.</p>
         *  <p>If you attempt to set the <code>VisibilityTimeout</code> to a
         * value greater than the maximum time left, Amazon SQS returns an error. Amazon
         * SQS doesn't automatically recalculate and increase the timeout to the maximum
         * remaining time.</p> <p>Unlike with a queue, when you change the visibility
         * timeout for a specific message the timeout value is applied immediately but
         * isn't saved in memory for that message. If you don't delete a message after it
         * is received, the visibility timeout for the message reverts to the original
         * timeout value (not to the value you set using the
         * <code>ChangeMessageVisibility</code> action) the next time the message is
         * received.</p> <p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/sqs-2012-11-05/ChangeMessageVisibility">AWS
         * API Reference</a></p>
         */
        virtual Model::ChangeMessageVisibilityOutcome ChangeMessageVisibility(const Model::ChangeMessageVisibilityRequest& request) const;

        /**
         * A Callable wrapper for ChangeMessageVisibility that returns a future to the operation so that it can be executed in parallel to other requests.
         */
        template<typename ChangeMessageVisibilityRequestT = Model::ChangeMessageVisibilityRequest>
        Model::ChangeMessageVisibilityOutcomeCallable ChangeMessageVisibilityCallable(const ChangeMessageVisibilityRequestT& request) const
        {
            return SubmitCallable(&SQSClient::ChangeMessageVisibility, request);
        }

        /**
         * An Async wrapper for ChangeMessageVisibility that queues the request into a thread executor and triggers associated callback when operation has finished.
         */
        template<typename ChangeMessageVisibilityRequestT = Model::ChangeMessageVisibilityRequest>
        void ChangeMessageVisibilityAsync(const ChangeMessageVisibilityRequestT& request, const ChangeMessageVisibilityResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context = nullptr) const
        {
            return SubmitAsync(&SQSClient::ChangeMessageVisibility, request, handler, context);
        }

        /**
         * <p>Changes the visibility timeout of multiple messages. This is a batch version
         * of <code> <a>ChangeMessageVisibility</a>.</code> The result of the action on
         * each message is reported individually in the response. You can send up to 10
         * <code> <a>ChangeMessageVisibility</a> </code> requests with each
         * <code>ChangeMessageVisibilityBatch</code> action.</p>  <p>Because the
         * batch request can result in a combination of successful and unsuccessful
         * actions, you should check for batch errors even when the call returns an HTTP
         * status code of <code>200</code>.</p>  <p>Some actions take lists of
         * parameters. These lists are specified using the <code>param.n</code> notation.
         * Values of <code>n</code> are integers starting from 1. For example, a parameter
         * list with two elements looks like this:</p> <p>
         * <code>&amp;AttributeName.1=first</code> </p> <p>
         * <code>&amp;AttributeName.2=second</code> </p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/sqs-2012-11-05/ChangeMessageVisibilityBatch">AWS
         * API Reference</a></p>
         */
        virtual Model::ChangeMessageVisibilityBatchOutcome ChangeMessageVisibilityBatch(const Model::ChangeMessageVisibilityBatchRequest& request) const;

        /**
         * A Callable wrapper for ChangeMessageVisibilityBatch that returns a future to the operation so that it can be executed in parallel to other requests.
         */
        template<typename ChangeMessageVisibilityBatchRequestT = Model::ChangeMessageVisibilityBatchRequest>
        Model::ChangeMessageVisibilityBatchOutcomeCallable ChangeMessageVisibilityBatchCallable(const ChangeMessageVisibilityBatchRequestT& request) const
        {
            return SubmitCallable(&SQSClient::ChangeMessageVisibilityBatch, request);
        }

        /**
         * An Async wrapper for ChangeMessageVisibilityBatch that queues the request into a thread executor and triggers associated callback when operation has finished.
         */
        template<typename ChangeMessageVisibilityBatchRequestT = Model::ChangeMessageVisibilityBatchRequest>
        void ChangeMessageVisibilityBatchAsync(const ChangeMessageVisibilityBatchRequestT& request, const ChangeMessageVisibilityBatchResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context = nullptr) const
        {
            return SubmitAsync(&SQSClient::ChangeMessageVisibilityBatch, request, handler, context);
        }

        /**
         * <p>Creates a new standard or FIFO queue. You can pass one or more attributes in
         * the request. Keep the following in mind:</p> <ul> <li> <p>If you don't specify
         * the <code>FifoQueue</code> attribute, Amazon SQS creates a standard queue.</p>
         *  <p>You can't change the queue type after you create it and you can't
         * convert an existing standard queue into a FIFO queue. You must either create a
         * new FIFO queue for your application or delete your existing standard queue and
         * recreate it as a FIFO queue. For more information, see <a
         * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues.html#FIFO-queues-moving">Moving
         * From a Standard Queue to a FIFO Queue</a> in the <i>Amazon SQS Developer
         * Guide</i>. </p>  </li> <li> <p>If you don't provide a value for an
         * attribute, the queue is created with the default value for the attribute.</p>
         * </li> <li> <p>If you delete a queue, you must wait at least 60 seconds before
         * creating a queue with the same name.</p> </li> </ul> <p>To successfully create a
         * new queue, you must provide a queue name that adheres to the <a
         * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/limits-queues.html">limits
         * related to queues</a> and is unique within the scope of your queues.</p> 
         * <p>After you create a queue, you must wait at least one second after the queue
         * is created to be able to use the queue.</p>  <p>To get the queue URL, use
         * the <code> <a>GetQueueUrl</a> </code> action. <code> <a>GetQueueUrl</a> </code>
         * requires only the <code>QueueName</code> parameter. be aware of existing queue
         * names:</p> <ul> <li> <p>If you provide the name of an existing queue along with
         * the exact names and values of all the queue's attributes,
         * <code>CreateQueue</code> returns the queue URL for the existing queue.</p> </li>
         * <li> <p>If the queue name, attribute names, or attribute values don't match an
         * existing queue, <code>CreateQueue</code> returns an error.</p> </li> </ul>
         * <p>Some actions take lists of parameters. These lists are specified using the
         * <code>param.n</code> notation. Values of <code>n</code> are integers starting
         * from 1. For example, a parameter list with two elements looks like this:</p> <p>
         * <code>&amp;AttributeName.1=first</code> </p> <p>
         * <code>&amp;AttributeName.2=second</code> </p>  <p>Cross-account
         * permissions don't apply to this action. For more information, see <a
         * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-customer-managed-policy-examples.html#grant-cross-account-permissions-to-role-and-user-name">Grant
         * cross-account permissions to a role and a user name</a> in the <i>Amazon SQS
         * Developer Guide</i>.</p> <p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/sqs-2012-11-05/CreateQueue">AWS API
         * Reference</a></p>
         */
        virtual Model::CreateQueueOutcome CreateQueue(const Model::CreateQueueRequest& request) const;

        /**
         * A Callable wrapper for CreateQueue that returns a future to the operation so that it can be executed in parallel to other requests.
         */
        template<typename CreateQueueRequestT = Model::CreateQueueRequest>
        Model::CreateQueueOutcomeCallable CreateQueueCallable(const CreateQueueRequestT& request) const
        {
            return SubmitCallable(&SQSClient::CreateQueue, request);
        }

        /**
         * An Async wrapper for CreateQueue that queues the request into a thread executor and triggers associated callback when operation has finished.
         */
        template<typename CreateQueueRequestT = Model::CreateQueueRequest>
        void CreateQueueAsync(const CreateQueueRequestT& request, const CreateQueueResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context = nullptr) const
        {
            return SubmitAsync(&SQSClient::CreateQueue, request, handler, context);
        }

        /**
         * <p>Deletes the specified message from the specified queue. To select the message
         * to delete, use the <code>ReceiptHandle</code> of the message (<i>not</i> the
         * <code>MessageId</code> which you receive when you send the message). Amazon SQS
         * can delete a message from a queue even if a visibility timeout setting causes
         * the message to be locked by another consumer. Amazon SQS automatically deletes
         * messages left in a queue longer than the retention period configured for the
         * queue. </p>  <p>The <code>ReceiptHandle</code> is associated with a
         * <i>specific instance</i> of receiving a message. If you receive a message more
         * than once, the <code>ReceiptHandle</code> is different each time you receive a
         * message. When you use the <code>DeleteMessage</code> action, you must provide
         * the most recently received <code>ReceiptHandle</code> for the message
         * (otherwise, the request succeeds, but the message might not be deleted).</p>
         * <p>For standard queues, it is possible to receive a message even after you
         * delete it. This might happen on rare occasions if one of the servers which
         * stores a copy of the message is unavailable when you send the request to delete
         * the message. The copy remains on the server and might be returned to you during
         * a subsequent receive request. You should ensure that your application is
         * idempotent, so that receiving a message more than once does not cause
         * issues.</p> <p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/sqs-2012-11-05/DeleteMessage">AWS
         * API Reference</a></p>
         */
        virtual Model::DeleteMessageOutcome DeleteMessage(const Model::DeleteMessageRequest& request) const;

        /**
         * A Callable wrapper for DeleteMessage that returns a future to the operation so that it can be executed in parallel to other requests.
         */
        template<typename DeleteMessageRequestT = Model::DeleteMessageRequest>
        Model::DeleteMessageOutcomeCallable DeleteMessageCallable(const DeleteMessageRequestT& request) const
        {
            return SubmitCallable(&SQSClient::DeleteMessage, request);
        }

        /**
         * An Async wrapper for DeleteMessage that queues the request into a thread executor and triggers associated callback when operation has finished.
         */
        template<typename DeleteMessageRequestT = Model::DeleteMessageRequest>
        void DeleteMessageAsync(const DeleteMessageRequestT& request, const DeleteMessageResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context = nullptr) const
        {
            return SubmitAsync(&SQSClient::DeleteMessage, request, handler, context);
        }

        /**
         * <p>Deletes up to ten messages from the specified queue. This is a batch version
         * of <code> <a>DeleteMessage</a>.</code> The result of the action on each message
         * is reported individually in the response.</p>  <p>Because the batch
         * request can result in a combination of successful and unsuccessful actions, you
         * should check for batch errors even when the call returns an HTTP status code of
         * <code>200</code>.</p>  <p>Some actions take lists of parameters.
         * These lists are specified using the <code>param.n</code> notation. Values of
         * <code>n</code> are integers starting from 1. For example, a parameter list with
         * two elements looks like this:</p> <p> <code>&amp;AttributeName.1=first</code>
         * </p> <p> <code>&amp;AttributeName.2=second</code> </p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/sqs-2012-11-05/DeleteMessageBatch">AWS
         * API Reference</a></p>
         */
        virtual Model::DeleteMessageBatchOutcome DeleteMessageBatch(const Model::DeleteMessageBatchRequest& request) const;

        /**
         * A Callable wrapper for DeleteMessageBatch that returns a future to the operation so that it can be executed in parallel to other requests.
         */
        template<typename DeleteMessageBatchRequestT = Model::DeleteMessageBatchRequest>
        Model::DeleteMessageBatchOutcomeCallable DeleteMessageBatchCallable(const DeleteMessageBatchRequestT& request) const
        {
            return SubmitCallable(&SQSClient::DeleteMessageBatch, request);
        }

        /**
         * An Async wrapper for DeleteMessageBatch that queues the request into a thread executor and triggers associated callback when operation has finished.
         */
        template<typename DeleteMessageBatchRequestT = Model::DeleteMessageBatchRequest>
        void DeleteMessageBatchAsync(const DeleteMessageBatchRequestT& request, const DeleteMessageBatchResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context = nullptr) const
        {
            return SubmitAsync(&SQSClient::DeleteMessageBatch, request, handler, context);
        }

        /**
         * <p>Deletes the queue specified by the <code>QueueUrl</code>, regardless of the
         * queue's contents.</p>  <p>Be careful with the
         * <code>DeleteQueue</code> action: When you delete a queue, any messages in the
         * queue are no longer available. </p>  <p>When you delete a queue, the
         * deletion process takes up to 60 seconds. Requests you send involving that queue
         * during the 60 seconds might succeed. For example, a <code> <a>SendMessage</a>
         * </code> request might succeed, but after 60 seconds the queue and the message
         * you sent no longer exist.</p> <p>When you delete a queue, you must wait at least
         * 60 seconds before creating a queue with the same name.</p> 
         * <p>Cross-account permissions don't apply to this action. For more information,
         * see <a
         * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-customer-managed-policy-examples.html#grant-cross-account-permissions-to-role-and-user-name">Grant
         * cross-account permissions to a role and a user name</a> in the <i>Amazon SQS
         * Developer Guide</i>.</p> <p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/sqs-2012-11-05/DeleteQueue">AWS API
         * Reference</a></p>
         */
        virtual Model::DeleteQueueOutcome DeleteQueue(const Model::DeleteQueueRequest& request) const;

        /**
         * A Callable wrapper for DeleteQueue that returns a future to the operation so that it can be executed in parallel to other requests.
         */
        template<typename DeleteQueueRequestT = Model::DeleteQueueRequest>
        Model::DeleteQueueOutcomeCallable DeleteQueueCallable(const DeleteQueueRequestT& request) const
        {
            return SubmitCallable(&SQSClient::DeleteQueue, request);
        }

        /**
         * An Async wrapper for DeleteQueue that queues the request into a thread executor and triggers associated callback when operation has finished.
         */
        template<typename DeleteQueueRequestT = Model::DeleteQueueRequest>
        void DeleteQueueAsync(const DeleteQueueRequestT& request, const DeleteQueueResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context = nullptr) const
        {
            return SubmitAsync(&SQSClient::DeleteQueue, request, handler, context);
        }

        /**
         * <p>Gets attributes for the specified queue.</p>  <p>To determine whether a
         * queue is <a
         * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues.html">FIFO</a>,
         * you can check whether <code>QueueName</code> ends with the <code>.fifo</code>
         * suffix.</p> <p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/sqs-2012-11-05/GetQueueAttributes">AWS
         * API Reference</a></p>
         */
        virtual Model::GetQueueAttributesOutcome GetQueueAttributes(const Model::GetQueueAttributesRequest& request) const;

        /**
         * A Callable wrapper for GetQueueAttributes that returns a future to the operation so that it can be executed in parallel to other requests.
         */
        template<typename GetQueueAttributesRequestT = Model::GetQueueAttributesRequest>
        Model::GetQueueAttributesOutcomeCallable GetQueueAttributesCallable(const GetQueueAttributesRequestT& request) const
        {
            return SubmitCallable(&SQSClient::GetQueueAttributes, request);
        }

        /**
         * An Async wrapper for GetQueueAttributes that queues the request into a thread executor and triggers associated callback when operation has finished.
         */
        template<typename GetQueueAttributesRequestT = Model::GetQueueAttributesRequest>
        void GetQueueAttributesAsync(const GetQueueAttributesRequestT& request, const GetQueueAttributesResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context = nullptr) const
        {
            return SubmitAsync(&SQSClient::GetQueueAttributes, request, handler, context);
        }

        /**
         * <p>Returns the URL of an existing Amazon SQS queue.</p> <p>To access a queue
         * that belongs to another AWS account, use the <code>QueueOwnerAWSAccountId</code>
         * parameter to specify the account ID of the queue's owner. The queue's owner must
         * grant you permission to access the queue. For more information about shared
         * queue access, see <code> <a>AddPermission</a> </code> or see <a
         * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-writing-an-sqs-policy.html#write-messages-to-shared-queue">Allow
         * Developers to Write Messages to a Shared Queue</a> in the <i>Amazon SQS
         * Developer Guide</i>. </p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/sqs-2012-11-05/GetQueueUrl">AWS API
         * Reference</a></p>
         */
        virtual Model::GetQueueUrlOutcome GetQueueUrl(const Model::GetQueueUrlRequest& request) const;

        /**
         * A Callable wrapper for GetQueueUrl that returns a future to the operation so that it can be executed in parallel to other requests.
         */
        template<typename GetQueueUrlRequestT = Model::GetQueueUrlRequest>
        Model::GetQueueUrlOutcomeCallable GetQueueUrlCallable(const GetQueueUrlRequestT& request) const
        {
            return SubmitCallable(&SQSClient::GetQueueUrl, request);
        }

        /**
         * An Async wrapper for GetQueueUrl that queues the request into a thread executor and triggers associated callback when operation has finished.
         */
        template<typename GetQueueUrlRequestT = Model::GetQueueUrlRequest>
        void GetQueueUrlAsync(const GetQueueUrlRequestT& request, const GetQueueUrlResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context = nullptr) const
        {
            return SubmitAsync(&SQSClient::GetQueueUrl, request, handler, context);
        }

        /**
         * <p>Returns a list of your queues that have the <code>RedrivePolicy</code> queue
         * attribute configured with a dead-letter queue.</p> <p> The
         * <code>ListDeadLetterSourceQueues</code> methods supports pagination. Set
         * parameter <code>MaxResults</code> in the request to specify the maximum number
         * of results to be returned in the response. If you do not set
         * <code>MaxResults</code>, the response includes a maximum of 1,000 results. If
         * you set <code>MaxResults</code> and there are additional results to display, the
         * response includes a value for <code>NextToken</code>. Use <code>NextToken</code>
         * as a parameter in your next request to <code>ListDeadLetterSourceQueues</code>
         * to receive the next page of results. </p> <p>For more information about using
         * dead-letter queues, see <a
         * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html">Using
         * Amazon SQS Dead-Letter Queues</a> in the <i>Amazon SQS Developer
         * Guide</i>.</p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/sqs-2012-11-05/ListDeadLetterSourceQueues">AWS
         * API Reference</a></p>
         */
        virtual Model::ListDeadLetterSourceQueuesOutcome ListDeadLetterSourceQueues(const Model::ListDeadLetterSourceQueuesRequest& request) const;

        /**
         * A Callable wrapper for ListDeadLetterSourceQueues that returns a future to the operation so that it can be executed in parallel to other requests.
         */
        template<typename ListDeadLetterSourceQueuesRequestT = Model::ListDeadLetterSourceQueuesRequest>
        Model::ListDeadLetterSourceQueuesOutcomeCallable ListDeadLetterSourceQueuesCallable(const ListDeadLetterSourceQueuesRequestT& request) const
        {
            return SubmitCallable(&SQSClient::ListDeadLetterSourceQueues, request);
        }

        /**
         * An Async wrapper for ListDeadLetterSourceQueues that queues the request into a thread executor and triggers associated callback when operation has finished.
         */
        template<typename ListDeadLetterSourceQueuesRequestT = Model::ListDeadLetterSourceQueuesRequest>
        void ListDeadLetterSourceQueuesAsync(const ListDeadLetterSourceQueuesRequestT& request, const ListDeadLetterSourceQueuesResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context = nullptr) const
        {
            return SubmitAsync(&SQSClient::ListDeadLetterSourceQueues, request, handler, context);
        }

        /**
         * <p>List all cost allocation tags added to the specified Amazon SQS queue. For an
         * overview, see <a
         * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-queue-tags.html">Tagging
         * Your Amazon SQS Queues</a> in the <i>Amazon SQS Developer Guide</i>.</p> 
         * <p>Cross-account permissions don't apply to this action. For more information,
         * see <a
         * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-customer-managed-policy-examples.html#grant-cross-account-permissions-to-role-and-user-name">Grant
         * cross-account permissions to a role and a user name</a> in the <i>Amazon SQS
         * Developer Guide</i>.</p> <p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/sqs-2012-11-05/ListQueueTags">AWS
         * API Reference</a></p>
         */
        virtual Model::ListQueueTagsOutcome ListQueueTags(const Model::ListQueueTagsRequest& request) const;

        /**
         * A Callable wrapper for ListQueueTags that returns a future to the operation so that it can be executed in parallel to other requests.
         */
        template<typename ListQueueTagsRequestT = Model::ListQueueTagsRequest>
        Model::ListQueueTagsOutcomeCallable ListQueueTagsCallable(const ListQueueTagsRequestT& request) const
        {
            return SubmitCallable(&SQSClient::ListQueueTags, request);
        }

        /**
         * An Async wrapper for ListQueueTags that queues the request into a thread executor and triggers associated callback when operation has finished.
         */
        template<typename ListQueueTagsRequestT = Model::ListQueueTagsRequest>
        void ListQueueTagsAsync(const ListQueueTagsRequestT& request, const ListQueueTagsResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context = nullptr) const
        {
            return SubmitAsync(&SQSClient::ListQueueTags, request, handler, context);
        }

        /**
         * <p>Returns a list of your queues in the current region. The response includes a
         * maximum of 1,000 results. If you specify a value for the optional
         * <code>QueueNamePrefix</code> parameter, only queues with a name that begins with
         * the specified value are returned.</p> <p> The <code>listQueues</code> methods
         * supports pagination. Set parameter <code>MaxResults</code> in the request to
         * specify the maximum number of results to be returned in the response. If you do
         * not set <code>MaxResults</code>, the response includes a maximum of 1,000
         * results. If you set <code>MaxResults</code> and there are additional results to
         * display, the response includes a value for <code>NextToken</code>. Use
         * <code>NextToken</code> as a parameter in your next request to
         * <code>listQueues</code> to receive the next page of results. </p> 
         * <p>Cross-account permissions don't apply to this action. For more information,
         * see <a
         * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-customer-managed-policy-examples.html#grant-cross-account-permissions-to-role-and-user-name">Grant
         * cross-account permissions to a role and a user name</a> in the <i>Amazon SQS
         * Developer Guide</i>.</p> <p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/sqs-2012-11-05/ListQueues">AWS API
         * Reference</a></p>
         */
        virtual Model::ListQueuesOutcome ListQueues(const Model::ListQueuesRequest& request) const;

        /**
         * A Callable wrapper for ListQueues that returns a future to the operation so that it can be executed in parallel to other requests.
         */
        template<typename ListQueuesRequestT = Model::ListQueuesRequest>
        Model::ListQueuesOutcomeCallable ListQueuesCallable(const ListQueuesRequestT& request) const
        {
            return SubmitCallable(&SQSClient::ListQueues, request);
        }

        /**
         * An Async wrapper for ListQueues that queues the request into a thread executor and triggers associated callback when operation has finished.
         */
        template<typename ListQueuesRequestT = Model::ListQueuesRequest>
        void ListQueuesAsync(const ListQueuesRequestT& request, const ListQueuesResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context = nullptr) const
        {
            return SubmitAsync(&SQSClient::ListQueues, request, handler, context);
        }

        /**
         * <p>Deletes the messages in a queue specified by the <code>QueueURL</code>
         * parameter.</p>  <p>When you use the <code>PurgeQueue</code> action,
         * you can't retrieve any messages deleted from a queue.</p> <p>The message
         * deletion process takes up to 60 seconds. We recommend waiting for 60 seconds
         * regardless of your queue's size. </p>  <p>Messages sent to the queue
         * <i>before</i> you call <code>PurgeQueue</code> might be received but are deleted
         * within the next minute.</p> <p>Messages sent to the queue <i>after</i> you call
         * <code>PurgeQueue</code> might be deleted while the queue is being
         * purged.</p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/sqs-2012-11-05/PurgeQueue">AWS API
         * Reference</a></p>
         */
        virtual Model::PurgeQueueOutcome PurgeQueue(const Model::PurgeQueueRequest& request) const;

        /**
         * A Callable wrapper for PurgeQueue that returns a future to the operation so that it can be executed in parallel to other requests.
         */
        template<typename PurgeQueueRequestT = Model::PurgeQueueRequest>
        Model::PurgeQueueOutcomeCallable PurgeQueueCallable(const PurgeQueueRequestT& request) const
        {
            return SubmitCallable(&SQSClient::PurgeQueue, request);
        }

        /**
         * An Async wrapper for PurgeQueue that queues the request into a thread executor and triggers associated callback when operation has finished.
         */
        template<typename PurgeQueueRequestT = Model::PurgeQueueRequest>
        void PurgeQueueAsync(const PurgeQueueRequestT& request, const PurgeQueueResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context = nullptr) const
        {
            return SubmitAsync(&SQSClient::PurgeQueue, request, handler, context);
        }

        /**
         * <p>Retrieves one or more messages (up to 10), from the specified queue. Using
         * the <code>WaitTimeSeconds</code> parameter enables long-poll support. For more
         * information, see <a
         * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-long-polling.html">Amazon
         * SQS Long Polling</a> in the <i>Amazon SQS Developer Guide</i>. </p> <p>Short
         * poll is the default behavior where a weighted random set of machines is sampled
         * on a <code>ReceiveMessage</code> call. Thus, only the messages on the sampled
         * machines are returned. If the number of messages in the queue is small (fewer
         * than 1,000), you most likely get fewer messages than you requested per
         * <code>ReceiveMessage</code> call. If the number of messages in the queue is
         * extremely small, you might not receive any messages in a particular
         * <code>ReceiveMessage</code> response. If this happens, repeat the request. </p>
         * <p>For each message returned, the response includes the following:</p> <ul> <li>
         * <p>The message body.</p> </li> <li> <p>An MD5 digest of the message body. For
         * information about MD5, see <a
         * href="https://www.ietf.org/rfc/rfc1321.txt">RFC1321</a>.</p> </li> <li> <p>The
         * <code>MessageId</code> you received when you sent the message to the queue.</p>
         * </li> <li> <p>The receipt handle.</p> </li> <li> <p>The message attributes.</p>
         * </li> <li> <p>An MD5 digest of the message attributes.</p> </li> </ul> <p>The
         * receipt handle is the identifier you must provide when deleting the message. For
         * more information, see <a
         * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-queue-message-identifiers.html">Queue
         * and Message Identifiers</a> in the <i>Amazon SQS Developer Guide</i>.</p> <p>You
         * can provide the <code>VisibilityTimeout</code> parameter in your request. The
         * parameter is applied to the messages that Amazon SQS returns in the response. If
         * you don't include the parameter, the overall visibility timeout for the queue is
         * used for the returned messages. For more information, see <a
         * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html">Visibility
         * Timeout</a> in the <i>Amazon SQS Developer Guide</i>.</p> <p>A message that
         * isn't deleted or a message whose visibility isn't extended before the visibility
         * timeout expires counts as a failed receive. Depending on the configuration of
         * the queue, the message might be sent to the dead-letter queue.</p>  <p>In
         * the future, new attributes might be added. If you write code that calls this
         * action, we recommend that you structure your code so that it can handle new
         * attributes gracefully.</p> <p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/sqs-2012-11-05/ReceiveMessage">AWS
         * API Reference</a></p>
         */
        virtual Model::ReceiveMessageOutcome ReceiveMessage(const Model::ReceiveMessageRequest& request) const;

        /**
         * A Callable wrapper for ReceiveMessage that returns a future to the operation so that it can be executed in parallel to other requests.
         */
        template<typename ReceiveMessageRequestT = Model::ReceiveMessageRequest>
        Model::ReceiveMessageOutcomeCallable ReceiveMessageCallable(const ReceiveMessageRequestT& request) const
        {
            return SubmitCallable(&SQSClient::ReceiveMessage, request);
        }

        /**
         * An Async wrapper for ReceiveMessage that queues the request into a thread executor and triggers associated callback when operation has finished.
         */
        template<typename ReceiveMessageRequestT = Model::ReceiveMessageRequest>
        void ReceiveMessageAsync(const ReceiveMessageRequestT& request, const ReceiveMessageResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context = nullptr) const
        {
            return SubmitAsync(&SQSClient::ReceiveMessage, request, handler, context);
        }

        /**
         * <p>Revokes any permissions in the queue policy that matches the specified
         * <code>Label</code> parameter.</p>  <ul> <li> <p>Only the owner of a queue
         * can remove permissions from it.</p> </li> <li> <p>Cross-account permissions
         * don't apply to this action. For more information, see <a
         * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-customer-managed-policy-examples.html#grant-cross-account-permissions-to-role-and-user-name">Grant
         * cross-account permissions to a role and a user name</a> in the <i>Amazon SQS
         * Developer Guide</i>.</p> </li> <li> <p>To remove the ability to change queue
         * permissions, you must deny permission to the <code>AddPermission</code>,
         * <code>RemovePermission</code>, and <code>SetQueueAttributes</code> actions in
         * your IAM policy.</p> </li> </ul> <p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/sqs-2012-11-05/RemovePermission">AWS
         * API Reference</a></p>
         */
        virtual Model::RemovePermissionOutcome RemovePermission(const Model::RemovePermissionRequest& request) const;

        /**
         * A Callable wrapper for RemovePermission that returns a future to the operation so that it can be executed in parallel to other requests.
         */
        template<typename RemovePermissionRequestT = Model::RemovePermissionRequest>
        Model::RemovePermissionOutcomeCallable RemovePermissionCallable(const RemovePermissionRequestT& request) const
        {
            return SubmitCallable(&SQSClient::RemovePermission, request);
        }

        /**
         * An Async wrapper for RemovePermission that queues the request into a thread executor and triggers associated callback when operation has finished.
         */
        template<typename RemovePermissionRequestT = Model::RemovePermissionRequest>
        void RemovePermissionAsync(const RemovePermissionRequestT& request, const RemovePermissionResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context = nullptr) const
        {
            return SubmitAsync(&SQSClient::RemovePermission, request, handler, context);
        }

        /**
         * <p>Delivers a message to the specified queue.</p>  <p>A message can
         * include only XML, JSON, and unformatted text. The following Unicode characters
         * are allowed:</p> <p> <code>#x9</code> | <code>#xA</code> | <code>#xD</code> |
         * <code>#x20</code> to <code>#xD7FF</code> | <code>#xE000</code> to
         * <code>#xFFFD</code> | <code>#x10000</code> to <code>#x10FFFF</code> </p> <p>Any
         * characters not included in this list will be rejected. For more information, see
         * the <a href="http://www.w3.org/TR/REC-xml/#charsets">W3C specification for
         * characters</a>.</p> <p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/sqs-2012-11-05/SendMessage">AWS API
         * Reference</a></p>
         */
        virtual Model::SendMessageOutcome SendMessage(const Model::SendMessageRequest& request) const;

        /**
         * A Callable wrapper for SendMessage that returns a future to the operation so that it can be executed in parallel to other requests.
         */
        template<typename SendMessageRequestT = Model::SendMessageRequest>
        Model::SendMessageOutcomeCallable SendMessageCallable(const SendMessageRequestT& request) const
        {
            return SubmitCallable(&SQSClient::SendMessage, request);
        }

        /**
         * An Async wrapper for SendMessage that queues the request into a thread executor and triggers associated callback when operation has finished.
         */
        template<typename SendMessageRequestT = Model::SendMessageRequest>
        void SendMessageAsync(const SendMessageRequestT& request, const SendMessageResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context = nullptr) const
        {
            return SubmitAsync(&SQSClient::SendMessage, request, handler, context);
        }

        /**
         * <p>Delivers up to ten messages to the specified queue. This is a batch version
         * of <code> <a>SendMessage</a>.</code> For a FIFO queue, multiple messages within
         * a single batch are enqueued in the order they are sent.</p> <p>The result of
         * sending each message is reported individually in the response. Because the batch
         * request can result in a combination of successful and unsuccessful actions, you
         * should check for batch errors even when the call returns an HTTP status code of
         * <code>200</code>.</p> <p>The maximum allowed individual message size and the
         * maximum total payload size (the sum of the individual lengths of all of the
         * batched messages) are both 256 KB (262,144 bytes).</p>  <p>A message
         * can include only XML, JSON, and unformatted text. The following Unicode
         * characters are allowed:</p> <p> <code>#x9</code> | <code>#xA</code> |
         * <code>#xD</code> | <code>#x20</code> to <code>#xD7FF</code> |
         * <code>#xE000</code> to <code>#xFFFD</code> | <code>#x10000</code> to
         * <code>#x10FFFF</code> </p> <p>Any characters not included in this list will be
         * rejected. For more information, see the <a
         * href="http://www.w3.org/TR/REC-xml/#charsets">W3C specification for
         * characters</a>.</p>  <p>If you don't specify the
         * <code>DelaySeconds</code> parameter for an entry, Amazon SQS uses the default
         * value for the queue.</p> <p>Some actions take lists of parameters. These lists
         * are specified using the <code>param.n</code> notation. Values of <code>n</code>
         * are integers starting from 1. For example, a parameter list with two elements
         * looks like this:</p> <p> <code>&amp;AttributeName.1=first</code> </p> <p>
         * <code>&amp;AttributeName.2=second</code> </p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/sqs-2012-11-05/SendMessageBatch">AWS
         * API Reference</a></p>
         */
        virtual Model::SendMessageBatchOutcome SendMessageBatch(const Model::SendMessageBatchRequest& request) const;

        /**
         * A Callable wrapper for SendMessageBatch that returns a future to the operation so that it can be executed in parallel to other requests.
         */
        template<typename SendMessageBatchRequestT = Model::SendMessageBatchRequest>
        Model::SendMessageBatchOutcomeCallable SendMessageBatchCallable(const SendMessageBatchRequestT& request) const
        {
            return SubmitCallable(&SQSClient::SendMessageBatch, request);
        }

        /**
         * An Async wrapper for SendMessageBatch that queues the request into a thread executor and triggers associated callback when operation has finished.
         */
        template<typename SendMessageBatchRequestT = Model::SendMessageBatchRequest>
        void SendMessageBatchAsync(const SendMessageBatchRequestT& request, const SendMessageBatchResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context = nullptr) const
        {
            return SubmitAsync(&SQSClient::SendMessageBatch, request, handler, context);
        }

        /**
         * <p>Sets the value of one or more queue attributes. When you change a queue's
         * attributes, the change can take up to 60 seconds for most of the attributes to
         * propagate throughout the Amazon SQS system. Changes made to the
         * <code>MessageRetentionPeriod</code> attribute can take up to 15 minutes.</p>
         *  <ul> <li> <p>In the future, new attributes might be added. If you write
         * code that calls this action, we recommend that you structure your code so that
         * it can handle new attributes gracefully.</p> </li> <li> <p>Cross-account
         * permissions don't apply to this action. For more information, see <a
         * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-customer-managed-policy-examples.html#grant-cross-account-permissions-to-role-and-user-name">Grant
         * cross-account permissions to a role and a user name</a> in the <i>Amazon SQS
         * Developer Guide</i>.</p> </li> <li> <p>To remove the ability to change queue
         * permissions, you must deny permission to the <code>AddPermission</code>,
         * <code>RemovePermission</code>, and <code>SetQueueAttributes</code> actions in
         * your IAM policy.</p> </li> </ul> <p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/sqs-2012-11-05/SetQueueAttributes">AWS
         * API Reference</a></p>
         */
        virtual Model::SetQueueAttributesOutcome SetQueueAttributes(const Model::SetQueueAttributesRequest& request) const;

        /**
         * A Callable wrapper for SetQueueAttributes that returns a future to the operation so that it can be executed in parallel to other requests.
         */
        template<typename SetQueueAttributesRequestT = Model::SetQueueAttributesRequest>
        Model::SetQueueAttributesOutcomeCallable SetQueueAttributesCallable(const SetQueueAttributesRequestT& request) const
        {
            return SubmitCallable(&SQSClient::SetQueueAttributes, request);
        }

        /**
         * An Async wrapper for SetQueueAttributes that queues the request into a thread executor and triggers associated callback when operation has finished.
         */
        template<typename SetQueueAttributesRequestT = Model::SetQueueAttributesRequest>
        void SetQueueAttributesAsync(const SetQueueAttributesRequestT& request, const SetQueueAttributesResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context = nullptr) const
        {
            return SubmitAsync(&SQSClient::SetQueueAttributes, request, handler, context);
        }

        /**
         * <p>Add cost allocation tags to the specified Amazon SQS queue. For an overview,
         * see <a
         * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-queue-tags.html">Tagging
         * Your Amazon SQS Queues</a> in the <i>Amazon SQS Developer Guide</i>.</p> <p>When
         * you use queue tags, keep the following guidelines in mind:</p> <ul> <li>
         * <p>Adding more than 50 tags to a queue isn't recommended.</p> </li> <li> <p>Tags
         * don't have any semantic meaning. Amazon SQS interprets tags as character
         * strings.</p> </li> <li> <p>Tags are case-sensitive.</p> </li> <li> <p>A new tag
         * with a key identical to that of an existing tag overwrites the existing tag.</p>
         * </li> </ul> <p>For a full list of tag restrictions, see <a
         * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-limits.html#limits-queues">Quotas
         * related to queues</a> in the <i>Amazon SQS Developer Guide</i>.</p> 
         * <p>Cross-account permissions don't apply to this action. For more information,
         * see <a
         * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-customer-managed-policy-examples.html#grant-cross-account-permissions-to-role-and-user-name">Grant
         * cross-account permissions to a role and a user name</a> in the <i>Amazon SQS
         * Developer Guide</i>.</p> <p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/sqs-2012-11-05/TagQueue">AWS API
         * Reference</a></p>
         */
        virtual Model::TagQueueOutcome TagQueue(const Model::TagQueueRequest& request) const;

        /**
         * A Callable wrapper for TagQueue that returns a future to the operation so that it can be executed in parallel to other requests.
         */
        template<typename TagQueueRequestT = Model::TagQueueRequest>
        Model::TagQueueOutcomeCallable TagQueueCallable(const TagQueueRequestT& request) const
        {
            return SubmitCallable(&SQSClient::TagQueue, request);
        }

        /**
         * An Async wrapper for TagQueue that queues the request into a thread executor and triggers associated callback when operation has finished.
         */
        template<typename TagQueueRequestT = Model::TagQueueRequest>
        void TagQueueAsync(const TagQueueRequestT& request, const TagQueueResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context = nullptr) const
        {
            return SubmitAsync(&SQSClient::TagQueue, request, handler, context);
        }

        /**
         * <p>Remove cost allocation tags from the specified Amazon SQS queue. For an
         * overview, see <a
         * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-queue-tags.html">Tagging
         * Your Amazon SQS Queues</a> in the <i>Amazon SQS Developer Guide</i>.</p> 
         * <p>Cross-account permissions don't apply to this action. For more information,
         * see <a
         * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-customer-managed-policy-examples.html#grant-cross-account-permissions-to-role-and-user-name">Grant
         * cross-account permissions to a role and a user name</a> in the <i>Amazon SQS
         * Developer Guide</i>.</p> <p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/sqs-2012-11-05/UntagQueue">AWS API
         * Reference</a></p>
         */
        virtual Model::UntagQueueOutcome UntagQueue(const Model::UntagQueueRequest& request) const;

        /**
         * A Callable wrapper for UntagQueue that returns a future to the operation so that it can be executed in parallel to other requests.
         */
        template<typename UntagQueueRequestT = Model::UntagQueueRequest>
        Model::UntagQueueOutcomeCallable UntagQueueCallable(const UntagQueueRequestT& request) const
        {
            return SubmitCallable(&SQSClient::UntagQueue, request);
        }

        /**
         * An Async wrapper for UntagQueue that queues the request into a thread executor and triggers associated callback when operation has finished.
         */
        template<typename UntagQueueRequestT = Model::UntagQueueRequest>
        void UntagQueueAsync(const UntagQueueRequestT& request, const UntagQueueResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context = nullptr) const
        {
            return SubmitAsync(&SQSClient::UntagQueue, request, handler, context);
        }


        void OverrideEndpoint(const Aws::String& endpoint);
        std::shared_ptr<SQSEndpointProviderBase>& accessEndpointProvider();
  private:
        friend class Aws::Client::ClientWithAsyncTemplateMethods<SQSClient>;
        void init(const SQSClientConfiguration& clientConfiguration);

        SQSClientConfiguration m_clientConfiguration;
        std::shared_ptr<Aws::Utils::Threading::Executor> m_executor;
        std::shared_ptr<SQSEndpointProviderBase> m_endpointProvider;
  };

} // namespace SQS
} // namespace Aws
