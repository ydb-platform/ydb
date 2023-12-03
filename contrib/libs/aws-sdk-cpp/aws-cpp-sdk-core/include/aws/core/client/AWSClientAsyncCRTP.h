/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once

#include <aws/core/client/AWSAsyncOperationTemplate.h>

namespace Aws
{
namespace Client
{
    class AsyncCallerContext;

    /**
     * A helper to determine if AWS Operation is EventStream-enabled or not (based on const-ness of the request)
    */
    template<typename T>
    struct AWS_CORE_LOCAL IsEventStreamOperation : IsEventStreamOperation<decltype(&T::operator())> {};

    template<typename ReturnT, typename ClassT, typename RequestT>
    struct AWS_CORE_LOCAL IsEventStreamOperation<ReturnT(ClassT::*)(RequestT) const>
    {
        static const bool value = !std::is_const<typename std::remove_reference<RequestT>::type>::value;
    };

    template<typename ReturnT, typename ClassT>
    struct AWS_CORE_LOCAL IsEventStreamOperation<ReturnT(ClassT::*)() const>
    {
        static const bool value = false;
    };


    /**
     * A CRTP-base class template that is used to add template methods to call AWS Operations in parallel using ThreadExecutor
     * An Aws<Service>Client is going to inherit from this class and will get methods below available.
    */
    template <typename AwsServiceClientT>
    class ClientWithAsyncTemplateMethods
    {
    public:
        /**
         * A template to submit a AwsServiceClient regular operation method for async execution.
         * This template method copies and queues the request into a thread executor and triggers associated callback when operation has finished.
        */
        template<typename RequestT, typename HandlerT, typename OperationFuncT, typename std::enable_if<!IsEventStreamOperation<OperationFuncT>::value, int>::type = 0>
        void SubmitAsync(OperationFuncT operationFunc,
                         const RequestT& request,
                         const HandlerT& handler,
                         const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context = nullptr) const
        {
            const AwsServiceClientT* clientThis = static_cast<const AwsServiceClientT*>(this);
            Aws::Client::MakeAsyncOperation(operationFunc, clientThis, request, handler, context, clientThis->m_executor.get());
        }

        /**
         * A template to submit a AwsServiceClient event stream enabled operation method for async execution.
         * This template method queues the original request object into a thread executor and triggers associated callback when operation has finished.
         * It is caller's responsibility to ensure the lifetime of the original request object for a duration of the async execution.
        */
        template<typename RequestT, typename HandlerT, typename OperationFuncT, typename std::enable_if<IsEventStreamOperation<OperationFuncT>::value, int>::type = 0>
        void SubmitAsync(OperationFuncT operationFunc,
                         RequestT& request, // note non-const ref
                         const HandlerT& handler,
                         const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context = nullptr) const
        {
            const AwsServiceClientT* clientThis = static_cast<const AwsServiceClientT*>(this);
            Aws::Client::MakeAsyncStreamingOperation(operationFunc, clientThis, request, handler, context, clientThis->m_executor.get());
        }

        /**
         * A template to submit a AwsServiceClient regular operation method without arguments for async execution.
         * This template method submits a task into a thread executor and triggers associated callback when operation has finished.
        */
        template<typename HandlerT, typename OperationFuncT>
        void SubmitAsync(OperationFuncT operationFunc,
                         const HandlerT& handler,
                         const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context = nullptr) const
        {
            const AwsServiceClientT* clientThis = static_cast<const AwsServiceClientT*>(this);
            Aws::Client::MakeAsyncOperation(operationFunc, clientThis, handler, context, clientThis->m_executor.get());
        }

        /**
         * A template to submit a AwsServiceClient regular operation method for async execution that returns a future<OperationOutcome> object.
         * This template method copies and queues the request into a thread executor and returns a future<OperationOutcome> object when operation has finished.
         */
        template<typename RequestT, typename OperationFuncT, typename std::enable_if<!IsEventStreamOperation<OperationFuncT>::value, int>::type = 0>
        auto SubmitCallable(OperationFuncT operationFunc,
                            const RequestT& request) const
            -> std::future<decltype((static_cast<const AwsServiceClientT*>(nullptr)->*operationFunc)(request))>
        {
            const AwsServiceClientT* clientThis = static_cast<const AwsServiceClientT*>(this);
            return Aws::Client::MakeCallableOperation(AwsServiceClientT::ALLOCATION_TAG, operationFunc, clientThis, request, clientThis->m_executor.get());
        }

        /**
         * A template to submit a AwsServiceClient event stream enabled operation method for async execution that returns a future<OperationOutcome> object.
         * This template method queues the original request into a thread executor and returns a future<OperationOutcome> object when operation has finished.
         * It is caller's responsibility to ensure the lifetime of the original request object for a duration of the async execution.
         */
        template<typename RequestT, typename OperationFuncT, typename std::enable_if<IsEventStreamOperation<OperationFuncT>::value, int>::type = 0>
        auto SubmitCallable(OperationFuncT operationFunc, /*note non-const ref*/ RequestT& request) const
            -> std::future<decltype((static_cast<const AwsServiceClientT*>(nullptr)->*operationFunc)(request))>
        {
            const AwsServiceClientT* clientThis = static_cast<const AwsServiceClientT*>(this);
            return Aws::Client::MakeCallableStreamingOperation(AwsServiceClientT::ALLOCATION_TAG, operationFunc, clientThis, request, clientThis->m_executor.get());
        }

        /**
         * A template to submit a AwsServiceClient regular operation without request argument for
         *   an async execution that returns a future<OperationOutcome> object.
         * This template method copies and queues the request into a thread executor and returns a future<OperationOutcome> object when operation has finished.
         */
        template<typename OperationFuncT>
        auto SubmitCallable(OperationFuncT operationFunc) const
            -> std::future<decltype((static_cast<const AwsServiceClientT*>(nullptr)->*operationFunc)())>
        {
            const AwsServiceClientT* clientThis = static_cast<const AwsServiceClientT*>(this);
            return Aws::Client::MakeCallableOperation(AwsServiceClientT::ALLOCATION_TAG, operationFunc, clientThis, clientThis->m_executor.get());
        }
    };
} // namespace Client
} // namespace Aws
