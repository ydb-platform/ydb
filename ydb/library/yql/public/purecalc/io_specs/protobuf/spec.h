#pragma once

#include "proto_variant.h"

#include <ydb/library/yql/public/purecalc/io_specs/protobuf_raw/spec.h>

namespace NYql {
    namespace NPureCalc {
        /**
         * Processing mode for working with non-raw protobuf messages.
         *
         * @tparam T message type.
         */
        template <typename T>
        class TProtobufInputSpec: public TProtobufRawInputSpec {
            static_assert(std::is_base_of<google::protobuf::Message, T>::value,
                          "should be derived from google::protobuf::Message");
        public:
            TProtobufInputSpec(
                const TMaybe<TString>& timestampColumn = Nothing(),
                const TProtoSchemaOptions& options = {}
            )
                : TProtobufRawInputSpec(*T::descriptor(), timestampColumn, options)
            {
            }
        };

        /**
         * Processing mode for working with non-raw protobuf messages.
         *
         * @tparam T message type.
         */
        template <typename T>
        class TProtobufOutputSpec: public TProtobufRawOutputSpec {
            static_assert(std::is_base_of<google::protobuf::Message, T>::value,
                          "should be derived from google::protobuf::Message");
        public:
            TProtobufOutputSpec(
                const TProtoSchemaOptions& options = {},
                google::protobuf::Arena* arena = nullptr
            )
                : TProtobufRawOutputSpec(*T::descriptor(), nullptr, options, arena)
            {
            }
        };

        /**
         * Processing mode for working with non-raw protobuf messages and several outputs.
         */
        template <typename... T>
        class TProtobufMultiOutputSpec: public TProtobufRawMultiOutputSpec {
            static_assert(
                std::conjunction_v<std::is_base_of<google::protobuf::Message, T>...>,
                "all types should be derived from google::protobuf::Message");
        public:
            TProtobufMultiOutputSpec(
                const TProtoSchemaOptions& options = {},
                TMaybe<TVector<google::protobuf::Arena*>> arenas = {}
            )
                : TProtobufRawMultiOutputSpec({T::descriptor()...}, Nothing(), options, std::move(arenas))
            {
            }
        };

        template <typename T>
        struct TInputSpecTraits<TProtobufInputSpec<T>> {
            static const constexpr bool IsPartial = false;

            static const constexpr bool SupportPullStreamMode = true;
            static const constexpr bool SupportPullListMode = true;
            static const constexpr bool SupportPushStreamMode = true;

            using TConsumerType = THolder<IConsumer<T*>>;

            static void PreparePullStreamWorker(const TProtobufInputSpec<T>& inputSpec, IPullStreamWorker* worker, THolder<IStream<T*>> stream) {
                auto raw = ConvertStream<google::protobuf::Message*>(std::move(stream));
                TInputSpecTraits<TProtobufRawInputSpec>::PreparePullStreamWorker(inputSpec, worker, std::move(raw));
            }

            static void PreparePullListWorker(const TProtobufInputSpec<T>& inputSpec, IPullListWorker* worker, THolder<IStream<T*>> stream) {
                auto raw = ConvertStream<google::protobuf::Message*>(std::move(stream));
                TInputSpecTraits<TProtobufRawInputSpec>::PreparePullListWorker(inputSpec, worker, std::move(raw));
            }

            static TConsumerType MakeConsumer(const TProtobufInputSpec<T>& inputSpec, TWorkerHolder<IPushStreamWorker> worker) {
                auto raw = TInputSpecTraits<TProtobufRawInputSpec>::MakeConsumer(inputSpec, std::move(worker));
                return ConvertConsumer<T*>(std::move(raw));
            }
        };

        template <typename T>
        struct TOutputSpecTraits<TProtobufOutputSpec<T>> {
            static const constexpr bool IsPartial = false;

            static const constexpr bool SupportPullStreamMode = true;
            static const constexpr bool SupportPullListMode = true;
            static const constexpr bool SupportPushStreamMode = true;

            using TOutputItemType = T*;
            using TPullStreamReturnType = THolder<IStream<TOutputItemType>>;
            using TPullListReturnType = THolder<IStream<TOutputItemType>>;

            static TPullStreamReturnType ConvertPullStreamWorkerToOutputType(const TProtobufOutputSpec<T>& outputSpec, TWorkerHolder<IPullStreamWorker> worker) {
                auto raw = TOutputSpecTraits<TProtobufRawOutputSpec>::ConvertPullStreamWorkerToOutputType(outputSpec, std::move(worker));
                return ConvertStreamUnsafe<TOutputItemType>(std::move(raw));
            }

            static TPullListReturnType ConvertPullListWorkerToOutputType(const TProtobufOutputSpec<T>& outputSpec, TWorkerHolder<IPullListWorker> worker) {
                auto raw = TOutputSpecTraits<TProtobufRawOutputSpec>::ConvertPullListWorkerToOutputType(outputSpec, std::move(worker));
                return ConvertStreamUnsafe<TOutputItemType>(std::move(raw));
            }

            static void SetConsumerToWorker(const TProtobufOutputSpec<T>& outputSpec, IPushStreamWorker* worker, THolder<IConsumer<T*>> consumer) {
                auto raw = ConvertConsumerUnsafe<google::protobuf::Message*>(std::move(consumer));
                TOutputSpecTraits<TProtobufRawOutputSpec>::SetConsumerToWorker(outputSpec, worker, std::move(raw));
            }
        };

        template <typename... T>
        struct TOutputSpecTraits<TProtobufMultiOutputSpec<T...>> {
            static const constexpr bool IsPartial = false;

            static const constexpr bool SupportPullStreamMode = true;
            static const constexpr bool SupportPullListMode = true;
            static const constexpr bool SupportPushStreamMode = true;

            using TOutputItemType = std::variant<T*...>;
            using TPullStreamReturnType = THolder<IStream<TOutputItemType>>;
            using TPullListReturnType = THolder<IStream<TOutputItemType>>;

            static TPullStreamReturnType ConvertPullStreamWorkerToOutputType(const TProtobufMultiOutputSpec<T...>& outputSpec, TWorkerHolder<IPullStreamWorker> worker) {
                auto raw = TOutputSpecTraits<TProtobufRawMultiOutputSpec>::ConvertPullStreamWorkerToOutputType(outputSpec, std::move(worker));
                return THolder(new NPrivate::TProtobufsMappingStream<T...>(std::move(raw)));
            }

            static TPullListReturnType ConvertPullListWorkerToOutputType(const TProtobufMultiOutputSpec<T...>& outputSpec, TWorkerHolder<IPullListWorker> worker) {
                auto raw = TOutputSpecTraits<TProtobufRawMultiOutputSpec>::ConvertPullListWorkerToOutputType(outputSpec, std::move(worker));
                return THolder(new NPrivate::TProtobufsMappingStream<T...>(std::move(raw)));
            }

            static void SetConsumerToWorker(const TProtobufMultiOutputSpec<T...>& outputSpec, IPushStreamWorker* worker, THolder<IConsumer<TOutputItemType>> consumer) {
                auto wrapper = MakeHolder<NPrivate::TProtobufsMappingConsumer<T...>>(std::move(consumer));
                TOutputSpecTraits<TProtobufRawMultiOutputSpec>::SetConsumerToWorker(outputSpec, worker, std::move(wrapper));
            }
        };
    }
}
