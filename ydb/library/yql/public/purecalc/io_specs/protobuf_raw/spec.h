#pragma once

#include <ydb/library/yql/public/purecalc/common/interface.h>
#include <ydb/library/yql/public/purecalc/helpers/protobuf/schema_from_proto.h>

#include <google/protobuf/message.h>

#include <util/generic/maybe.h>

namespace NYql {
    namespace NPureCalc {
        /**
         * Processing mode for working with raw protobuf message inputs.
         *
         * In this mode purecalc accept pointers to abstract protobuf messages and processes them using the reflection
         * mechanism. All passed messages should have the same descriptor (the one you pass to the constructor
         * of the input spec).
         *
         * All working modes are implemented. In pull stream and pull list modes a program would accept a single object
         * stream of const protobuf messages. In push mode, a program will return a consumer of const protobuf messages.
         *
         * The program synopsis follows:
         *
         * @code
         * ... TPullStreamProgram::Apply(IStream<google::protobuf::Message*>);
         * ... TPullListProgram::Apply(IStream<google::protobuf::Message*>);
         * TConsumer<google::protobuf::Message*> TPushStreamProgram::Apply(...);
         * @endcode
         */
        class TProtobufRawInputSpec: public TInputSpecBase {
        private:
            const google::protobuf::Descriptor& Descriptor_;
            const TMaybe<TString> TimestampColumn_;
            const TProtoSchemaOptions SchemaOptions_;
            mutable TVector<NYT::TNode> SavedSchemas_;

        public:
            /**
             * Build input spec and associate the given message descriptor.
             */
            explicit TProtobufRawInputSpec(
                const google::protobuf::Descriptor& descriptor,
                const TMaybe<TString>& timestampColumn = Nothing(),
                const TProtoSchemaOptions& options = {}
            );

        public:
            const TVector<NYT::TNode>& GetSchemas() const override;

            /**
             * Get the descriptor associated with this spec.
             */
            const google::protobuf::Descriptor& GetDescriptor() const;

            const TMaybe<TString>& GetTimestampColumn() const;

            /*
             * Get options that customize input struct type building.
             */
            const TProtoSchemaOptions& GetSchemaOptions() const;
        };

        /**
         * Processing mode for working with raw protobuf message outputs.
         *
         * In this mode purecalc yields pointers to abstract protobuf messages. All generated messages share the same
         * descriptor so they can be safely converted into an appropriate message type.
         *
         * Note that one should not expect that the returned pointer will be valid forever; in can (and will) become
         * outdated once a new output is requested/pushed.
         *
         * All working modes are implemented. In pull stream and pull list modes a program will return an object
         * stream of non-const protobuf messages. In push mode, it will accept a single consumer of non-const
         * messages.
         *
         * The program synopsis follows:
         *
         * @code
         * IStream<google::protobuf::Message*> TPullStreamProgram::Apply(...);
         * IStream<google::protobuf::Message*> TPullListProgram::Apply(...);
         * ... TPushStreamProgram::Apply(TConsumer<google::protobuf::Message*>);
         * @endcode
         */
        class TProtobufRawOutputSpec: public TOutputSpecBase {
        private:
            const google::protobuf::Descriptor& Descriptor_;
            google::protobuf::MessageFactory* Factory_;
            TProtoSchemaOptions SchemaOptions_;
            google::protobuf::Arena* Arena_;
            mutable TMaybe<NYT::TNode> SavedSchema_;

        public:
            /**
             * Build output spec and associate the given message descriptor and maybe the given message factory.
             */
            explicit TProtobufRawOutputSpec(
                const google::protobuf::Descriptor& descriptor,
                google::protobuf::MessageFactory* = nullptr,
                const TProtoSchemaOptions& options = {},
                google::protobuf::Arena* arena = nullptr
            );

        public:
            const NYT::TNode& GetSchema() const override;

            /**
             * Get the descriptor associated with this spec.
             */
            const google::protobuf::Descriptor& GetDescriptor() const;

            /**
             * Set a new message factory which will be used to generate messages. Pass a null pointer to use the
             * default factory.
             */
            void SetFactory(google::protobuf::MessageFactory*);

            /**
             * Get the message factory which is currently associated with this spec.
             */
            google::protobuf::MessageFactory* GetFactory() const;

            /**
             * Set a new arena which will be used to generate messages. Pass a null pointer to create on the heap.
             */
            void SetArena(google::protobuf::Arena*);

            /**
             * Get the arena  which is currently associated with this spec.
             */
            google::protobuf::Arena* GetArena() const;

            /**
             * Get options that customize output struct type building.
             */
            const TProtoSchemaOptions& GetSchemaOptions() const;
        };

        /**
         * Processing mode for working with raw protobuf messages and several outputs.
         *
         * The program synopsis follows:
         *
         * @code
         * IStream<std::pair<ui32, google::protobuf::Message*>> TPullStreamProgram::Apply(...);
         * IStream<std::pair<ui32, google::protobuf::Message*>> TPullListProgram::Apply(...);
         * ... TPushStreamProgram::Apply(TConsumer<std::pair<ui32, google::protobuf::Message*>>);
         * @endcode
         */
        class TProtobufRawMultiOutputSpec: public TOutputSpecBase {
        private:
            TVector<const google::protobuf::Descriptor*> Descriptors_;
            TVector<google::protobuf::MessageFactory*> Factories_;
            const TProtoSchemaOptions SchemaOptions_;
            TVector<google::protobuf::Arena*> Arenas_;
            mutable NYT::TNode SavedSchema_;

        public:
            TProtobufRawMultiOutputSpec(
                TVector<const google::protobuf::Descriptor*>,
                TMaybe<TVector<google::protobuf::MessageFactory*>> = {},
                const TProtoSchemaOptions& options = {},
                TMaybe<TVector<google::protobuf::Arena*>> arenas = {}
            );

        public:
            const NYT::TNode& GetSchema() const override;

            /**
             * Get the descriptor associated with given output.
             */
            const google::protobuf::Descriptor& GetDescriptor(ui32) const;

            /**
             * Set a new message factory for given output. It will be used to generate messages for this output.
             */
            void SetFactory(ui32, google::protobuf::MessageFactory*);

            /**
             * Get the message factory which is currently associated with given output.
             */
            google::protobuf::MessageFactory* GetFactory(ui32) const;

            /**
             * Set a new arena for given output. It will be used to generate messages for this output.
             */
            void SetArena(ui32, google::protobuf::Arena*);

            /**
             * Get the arena which is currently associated with given output.
             */
            google::protobuf::Arena* GetArena(ui32) const;

            /**
             * Get number of outputs for this spec.
             */
            ui32 GetOutputsNumber() const;

            /**
             * Get options that customize output struct type building.
             */
            const TProtoSchemaOptions& GetSchemaOptions() const;
        };

        template <>
        struct TInputSpecTraits<TProtobufRawInputSpec> {
            static const constexpr bool IsPartial = false;

            static const constexpr bool SupportPullStreamMode = true;
            static const constexpr bool SupportPullListMode = true;
            static const constexpr bool SupportPushStreamMode = true;

            using TConsumerType = THolder<IConsumer<google::protobuf::Message*>>;

            static void PreparePullStreamWorker(const TProtobufRawInputSpec&, IPullStreamWorker*, THolder<IStream<google::protobuf::Message*>>);
            static void PreparePullListWorker(const TProtobufRawInputSpec&, IPullListWorker*, THolder<IStream<google::protobuf::Message*>>);
            static TConsumerType MakeConsumer(const TProtobufRawInputSpec&, TWorkerHolder<IPushStreamWorker>);
        };

        template <>
        struct TOutputSpecTraits<TProtobufRawOutputSpec> {
            static const constexpr bool IsPartial = false;

            static const constexpr bool SupportPullStreamMode = true;
            static const constexpr bool SupportPullListMode = true;
            static const constexpr bool SupportPushStreamMode = true;

            using TOutputItemType = google::protobuf::Message*;
            using TPullStreamReturnType = THolder<IStream<TOutputItemType>>;
            using TPullListReturnType = THolder<IStream<TOutputItemType>>;

            static const constexpr TOutputItemType StreamSentinel = nullptr;

            static TPullStreamReturnType ConvertPullStreamWorkerToOutputType(const TProtobufRawOutputSpec&, TWorkerHolder<IPullStreamWorker>);
            static TPullListReturnType ConvertPullListWorkerToOutputType(const TProtobufRawOutputSpec&, TWorkerHolder<IPullListWorker>);
            static void SetConsumerToWorker(const TProtobufRawOutputSpec&, IPushStreamWorker*, THolder<IConsumer<TOutputItemType>>);
        };

        template <>
        struct TOutputSpecTraits<TProtobufRawMultiOutputSpec> {
            static const constexpr bool IsPartial = false;

            static const constexpr bool SupportPullStreamMode = true;
            static const constexpr bool SupportPullListMode = true;
            static const constexpr bool SupportPushStreamMode = true;

            using TOutputItemType = std::pair<ui32, google::protobuf::Message*>;
            using TPullStreamReturnType = THolder<IStream<TOutputItemType>>;
            using TPullListReturnType = THolder<IStream<TOutputItemType>>;

            static const constexpr TOutputItemType StreamSentinel = {0, nullptr};

            static TPullStreamReturnType ConvertPullStreamWorkerToOutputType(const TProtobufRawMultiOutputSpec&, TWorkerHolder<IPullStreamWorker>);
            static TPullListReturnType ConvertPullListWorkerToOutputType(const TProtobufRawMultiOutputSpec&, TWorkerHolder<IPullListWorker>);
            static void SetConsumerToWorker(const TProtobufRawMultiOutputSpec&, IPushStreamWorker*, THolder<IConsumer<TOutputItemType>>);
        };
    }
}
