#pragma once

#include "fwd.h"
#include "wrappers.h"

#include <ydb/library/yql/core/user_data/yql_user_data.h>

#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/public/udf/udf_counter.h>
#include <ydb/library/yql/public/udf/udf_registrator.h>

#include <ydb/library/yql/public/issue/yql_issue.h>
#include <library/cpp/yson/node/node.h>

#include <library/cpp/logger/priority.h>

#include <util/generic/ptr.h>
#include <util/generic/maybe.h>
#include <util/generic/hash_set.h>
#include <util/generic/string.h>
#include <util/stream/output.h>

class ITimeProvider;

namespace NKikimr {
    namespace NMiniKQL {
        class TScopedAlloc;
        class IComputationGraph;
        class IFunctionRegistry;
        class TTypeEnvironment;
        class TType;
        class TStructType;
    }
}

namespace NYql {
    namespace NPureCalc {
        /**
         * SQL or s-expression translation error.
         */
        class TCompileError: public yexception {
        private:
            TString Yql_;
            TString Issues_;

        public:
            // TODO: maybe accept an actual list of issues here?
            // See https://a.yandex-team.ru/arc/review/439403/details#comment-778237
            TCompileError(TString yql, TString issues)
                : Yql_(std::move(yql))
                , Issues_(std::move(issues))
            {
            }

        public:
            /**
             * Get the sql query which caused the error (if there is one available).
             */
            const TString& GetYql() const {
                return Yql_;
            }

            /**
             * Get detailed description for all errors and warnings that happened during sql translation.
             */
            const TString& GetIssues() const {
                return Issues_;
            }
        };

        ////////////////////////////////////////////////////////////////////////////////////////////////////

        /**
         * A generic input stream of objects.
         */
        template <typename T>
        class IStream {
        public:
            virtual ~IStream() = default;

        public:
            /**
             * Pops and returns a next value in the stream. If the stream is finished, should return some sentinel object.
             *
             * Depending on return type, this function may not transfer object ownership to a user.
             * Thus, the stream may manage the returned object * itself.
             * That is, the returned object's lifetime may be bound to the input stream lifetime; it may be destroyed
             * upon calling Fetch() or upon destroying the stream, whichever happens first.
             */
            virtual T Fetch() = 0;
        };

        /**
         * Create a new stream which applies the given functor to the elements of the original stream.
         */
        template <typename TOld, typename TNew, typename TFunctor>
        inline THolder<IStream<TNew>> MapStream(THolder<IStream<TOld>> stream, TFunctor functor) {
            return THolder(new NPrivate::TMappingStream<TNew, TOld, TFunctor>(std::move(stream), std::move(functor)));
        };

        /**
         * Convert stream of objects into a stream of potentially incompatible objects.
         *
         * This conversion applies static cast to the output of the original stream. Use with caution!
         */
        /// @{
        template <
            typename TNew, typename TOld,
            std::enable_if_t<!std::is_same<TNew, TOld>::value>* = nullptr>
        inline THolder<IStream<TNew>> ConvertStreamUnsafe(THolder<IStream<TOld>> stream) {
            return MapStream<TOld, TNew>(std::move(stream), [](TOld x) -> TNew { return static_cast<TNew>(x); });
        }
        template <typename T>
        inline THolder<IStream<T>> ConvertStreamUnsafe(THolder<IStream<T>> stream) {
            return stream;
        }
        /// @}

        /**
         * Convert stream of objects into a stream of compatible objects.
         *
         * Note: each conversion adds one level of indirection so avoid them if possible.
         */
        template <typename TNew, typename TOld, std::enable_if_t<std::is_convertible<TOld, TNew>::value>* = nullptr>
        inline THolder<IStream<TNew>> ConvertStream(THolder<IStream<TOld>> stream) {
            return ConvertStreamUnsafe<TNew, TOld>(std::move(stream));
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////

        /**
         * A generic push consumer.
         */
        template <typename T>
        class IConsumer {
        public:
            virtual ~IConsumer() = default;

        public:
            /**
             * Feed an object to consumer.
             *
             * Depending on argument type, the consumer may not take ownership of the passed object;
             * in that case it is the caller responsibility to manage the object lifetime after passing it to this method.
             *
             * The passed object can be destroyed after the consumer returns from this function; the consumer should
             * not store pointer to the passed object or the passed object itself without taking all necessary precautions
             * to ensure that the pointer or the object stays valid after returning.
             */
            virtual void OnObject(T) = 0;

            /**
             * Close the consumer and run finalization logic. Calling OnObject after calling this function is an error.
             */
            virtual void OnFinish() = 0;
        };

        /**
         * Create a new consumer which applies the given functor to objects before .
         */
        template <typename TOld, typename TNew, typename TFunctor>
        inline THolder<IConsumer<TNew>> MapConsumer(THolder<IConsumer<TOld>> stream, TFunctor functor) {
            return THolder(new NPrivate::TMappingConsumer<TNew, TOld, TFunctor>(std::move(stream), std::move(functor)));
        };


        /**
         * Convert consumer of objects into a consumer of potentially incompatible objects.
         *
         * This conversion applies static cast to the input value. Use with caution.
         */
        /// @{
        template <
            typename TNew, typename TOld,
            std::enable_if_t<!std::is_same<TNew, TOld>::value>* = nullptr>
        inline THolder<IConsumer<TNew>> ConvertConsumerUnsafe(THolder<IConsumer<TOld>> consumer) {
            return MapConsumer<TOld, TNew>(std::move(consumer), [](TNew x) -> TOld { return static_cast<TOld>(x); });
        }
        template <typename T>
        inline THolder<IConsumer<T>> ConvertConsumerUnsafe(THolder<IConsumer<T>> consumer) {
            return consumer;
        }
        /// @}

        /**
         * Convert consumer of objects into a consumer of compatible objects.
         *
         * Note: each conversion adds one level of indirection so avoid them if possible.
         */
        template <typename TNew, typename TOld, std::enable_if_t<std::is_convertible<TNew, TOld>::value>* = nullptr>
        inline THolder<IConsumer<TNew>> ConvertConsumer(THolder<IConsumer<TOld>> consumer) {
            return ConvertConsumerUnsafe<TNew, TOld>(std::move(consumer));
        }

        /**
         * Create a consumer which holds a non-owning pointer to the given consumer
         * and passes all messages to the latter.
         */
        template <typename T, typename C>
        THolder<NPrivate::TNonOwningConsumer<T, C>> MakeNonOwningConsumer(C consumer) {
            return MakeHolder<NPrivate::TNonOwningConsumer<T, C>>(consumer);
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////

        /**
         * Logging options.
         */
        struct TLoggingOptions final {
        public:
            /// Logging level for messages generated during compilation.
            ELogPriority LogLevel_;  // TODO: rename to LogLevel

            /// Where to write log messages.
            IOutputStream* LogDestination;

        public:
            TLoggingOptions();
            /**
             * Set a new logging level.
             *
             * @return reference to self, to allow method chaining.
             */
            TLoggingOptions& SetLogLevel(ELogPriority);

            /**
             * Set a new logging destination.
             *
             * @return reference to self, to allow method chaining.
             */
            TLoggingOptions& SetLogDestination(IOutputStream*);
        };

        /**
         * General options for program factory.
         */
        struct TProgramFactoryOptions final {
        public:
            /// Path to a directory with compiled UDFs. Leave empty to disable loading external UDFs.
            TString UdfsDir_;  // TODO: rename to UDFDir

            /// List of available external resources, e.g. files, UDFs, libraries.
            TVector<NUserData::TUserData> UserData_;   // TODO: rename to UserData

            /// LLVM settings. Assign "OFF" to disable LLVM, empty string for default settings.
            TString LLVMSettings;

            /// Block engine settings. Assign "force" to unconditionally enable
            /// it, "disable" for turn it off and "auto" to left the final
            /// decision to the platform heuristics.
            TString BlockEngineSettings;

            /// Output stream to dump the compiled and optimized expressions.
            IOutputStream* ExprOutputStream;

            /// Provider for generic counters which can be used to export statistics from UDFs.
            NKikimr::NUdf::ICountersProvider* CountersProvider;

            /// YT Type V3 flags for Skiff/Yson serialization.
            ui64 NativeYtTypeFlags;

            /// Seed for deterministic time provider
            TMaybe<ui64> DeterministicTimeProviderSeed;

            /// Use special system columns to support tables naming (supports non empty ``TablePath()``/``TableName()``)
            bool UseSystemColumns;

            /// Reuse allocated workers
            bool UseWorkerPool;

        public:
            TProgramFactoryOptions();

        public:
            /**
             * Set a new path to a directory with UDFs.
             *
             * @return reference to self, to allow method chaining.
             */
            TProgramFactoryOptions& SetUDFsDir(TStringBuf);

            /**
             * Add a new library to the UserData list.
             *
             * @param disposition where the resource resides, e.g. on filesystem, in memory, etc.
             *                    NB: URL disposition is not supported.
             * @param name name of the resource.
             * @param content depending on disposition, either path to the resource or its content.
             * @return reference to self, to allow method chaining.
             */
            TProgramFactoryOptions& AddLibrary(NUserData::EDisposition disposition, TStringBuf name, TStringBuf content);

            /**
             * Add a new file to the UserData list.
             *
             * @param disposition where the resource resides, e.g. on filesystem, in memory, etc.
             *                    NB: URL disposition is not supported.
             * @param name name of the resource.
             * @param content depending on disposition, either path to the resource or its content.
             * @return reference to self, to allow method chaining.
             */
            TProgramFactoryOptions& AddFile(NUserData::EDisposition disposition, TStringBuf name, TStringBuf content);

            /**
             * Add a new UDF to the UserData list.
             *
             * @param disposition where the resource resides, e.g. on filesystem, in memory, etc.
             *                    NB: URL disposition is not supported.
             * @param name name of the resource.
             * @param content depending on disposition, either path to the resource or its content.
             * @return reference to self, to allow method chaining.
             */
            TProgramFactoryOptions& AddUDF(NUserData::EDisposition disposition, TStringBuf name, TStringBuf content);

            /**
             * Set new LLVM settings.
             *
             * @return reference to self, to allow method chaining.
             */
            TProgramFactoryOptions& SetLLVMSettings(TStringBuf llvm_settings);

            /**
             * Set new block engine settings.
             *
             * @return reference to self, to allow method chaining.
             */
            TProgramFactoryOptions& SetBlockEngineSettings(TStringBuf blockEngineSettings);

            /**
             * Set the stream to dump the compiled and optimized expressions.
             *
             * @return reference to self, to allow method chaining.
             */
            TProgramFactoryOptions& SetExprOutputStream(IOutputStream* exprOutputStream);

            /**
             * Set new counters provider. Passed pointer should stay alive for as long as the processor factory
             * stays alive.
             *
             * @return reference to self, to allow method chaining.
             */
            TProgramFactoryOptions& SetCountersProvider(NKikimr::NUdf::ICountersProvider* countersProvider);

            /**
             * Set new YT Type V3 mode. Deprecated method. Use SetNativeYtTypeFlags instead
             *
             * @return reference to self, to allow method chaining.
             */
            TProgramFactoryOptions& SetUseNativeYtTypes(bool useNativeTypes);

            /**
             * Set YT Type V3 flags.
             *
             * @return reference to self, to allow method chaining.
             */
            TProgramFactoryOptions& SetNativeYtTypeFlags(ui64 nativeTypeFlags);

            /**
             * Set seed for deterministic time provider.
             *
             * @return reference to self, to allow method chaining.
             */
            TProgramFactoryOptions& SetDeterministicTimeProviderSeed(TMaybe<ui64> seed);

            /**
             * Set new flag whether to allow using system columns or not.
             *
             * @return reference to self, to allow method chaining.
             */
            TProgramFactoryOptions& SetUseSystemColumns(bool useSystemColumns);

            /**
             * Set new flag whether to allow reusing workers or not.
             *
             * @return reference to self, to allow method chaining.
             */
            TProgramFactoryOptions& SetUseWorkerPool(bool useWorkerPool);
        };

        ////////////////////////////////////////////////////////////////////////////////////////////////////

        /**
         * What exactly are we parsing: SQL or an s-expression.
         */
        enum class ETranslationMode {
            SQL   /* "SQL" */,
            SExpr /* "s-expression" */,
            Mkql  /* "mkql" */,
            PG    /* PostgreSQL */
        };

        /**
         * A facility for compiling sql and s-expressions and making programs from them.
         */
        class IProgramFactory: public TThrRefBase {
        protected:
            virtual IPullStreamWorkerFactoryPtr MakePullStreamWorkerFactory(const TInputSpecBase&, const TOutputSpecBase&, TString, ETranslationMode, ui16) = 0;
            virtual IPullListWorkerFactoryPtr MakePullListWorkerFactory(const TInputSpecBase&, const TOutputSpecBase&, TString, ETranslationMode, ui16) = 0;
            virtual IPushStreamWorkerFactoryPtr MakePushStreamWorkerFactory(const TInputSpecBase&, const TOutputSpecBase&, TString, ETranslationMode, ui16) = 0;

        public:
            /**
             * Add new udf module. It's not specified whether adding new modules will affect existing programs
             * (theoretical answer is 'no').
             */
            virtual void AddUdfModule(const TStringBuf&, NKikimr::NUdf::TUniquePtr<NKikimr::NUdf::IUdfModule>&&) = 0;
            // TODO: support setting udf modules via factory options.

            /**
             * Set new counters provider, override one that was specified via factory options. Note that existing
             * programs will still reference the previous provider.
             */
            virtual void SetCountersProvider(NKikimr::NUdf::ICountersProvider*) = 0;
            // TODO: support setting providers via factory options.

            template <typename TInputSpec, typename TOutputSpec>
            THolder<TPullStreamProgram<TInputSpec, TOutputSpec>> MakePullStreamProgram(
                TInputSpec inputSpec, TOutputSpec outputSpec, TString query, ETranslationMode mode = ETranslationMode::SQL, ui16 syntaxVersion = 1
            ) {
                auto workerFactory = MakePullStreamWorkerFactory(inputSpec, outputSpec, std::move(query), mode, syntaxVersion);
                return MakeHolder<TPullStreamProgram<TInputSpec, TOutputSpec>>(std::move(inputSpec), std::move(outputSpec), workerFactory);
            }

            template <typename TInputSpec, typename TOutputSpec>
            THolder<TPullListProgram<TInputSpec, TOutputSpec>> MakePullListProgram(
                TInputSpec inputSpec, TOutputSpec outputSpec, TString query, ETranslationMode mode = ETranslationMode::SQL, ui16 syntaxVersion = 1
            ) {
                auto workerFactory = MakePullListWorkerFactory(inputSpec, outputSpec, std::move(query), mode, syntaxVersion);
                return MakeHolder<TPullListProgram<TInputSpec, TOutputSpec>>(std::move(inputSpec), std::move(outputSpec), workerFactory);
            }

            template <typename TInputSpec, typename TOutputSpec>
            THolder<TPushStreamProgram<TInputSpec, TOutputSpec>> MakePushStreamProgram(
                TInputSpec inputSpec, TOutputSpec outputSpec, TString query, ETranslationMode mode = ETranslationMode::SQL, ui16 syntaxVersion = 1
            ) {
                auto workerFactory = MakePushStreamWorkerFactory(inputSpec, outputSpec, std::move(query), mode, syntaxVersion);
                return MakeHolder<TPushStreamProgram<TInputSpec, TOutputSpec>>(std::move(inputSpec), std::move(outputSpec), workerFactory);
            }
        };

        ////////////////////////////////////////////////////////////////////////////////////////////////////

        /**
         * A facility for creating workers. Despite being a part of a public API, worker factory is not used directly.
         */
        class IWorkerFactory: public std::enable_shared_from_this<IWorkerFactory> {
        public:
            virtual ~IWorkerFactory() = default;
            /**
             * Get input column names for specified input that are actually used in the query.
             */
            virtual const THashSet<TString>& GetUsedColumns(ui32) const = 0;
            /**
             * Overload for single-input programs.
             */
            virtual const THashSet<TString>& GetUsedColumns() const = 0;

            /**
             * Make input type schema for specified input as deduced by program optimizer. This schema is equivalent
             * to one provided by input spec up to the order of the fields in structures.
             */
            virtual NYT::TNode MakeInputSchema(ui32) const = 0;
            /**
             * Overload for single-input programs.
             */
            virtual NYT::TNode MakeInputSchema() const = 0;

            /**
             * Make output type schema as deduced by program optimizer. If output spec provides its own schema, than
             * this schema is equivalent to one provided by output spec up to the order of the fields in structures.
             */
            /// @{
            /**
             * Overload for single-table output programs (i.e. output type is struct).
             */
            virtual NYT::TNode MakeOutputSchema() const = 0;
            /**
             * Overload for multi-table output programs (i.e. output type is variant over tuple).
             */
            virtual NYT::TNode MakeOutputSchema(ui32) const = 0;
            /**
             * Overload for multi-table output programs (i.e. output type is variant over struct).
             */
            virtual NYT::TNode MakeOutputSchema(TStringBuf) const = 0;
            /// @}

            /**
             * Make full output schema. For single-output programs returns struct type, for multi-output programs
             * returns variant type.
             *
             * Warning: calling this function may result in extended memory usage for large number of output tables.
             */
            virtual NYT::TNode MakeFullOutputSchema() const = 0;

            /**
             * Get compilation issues
             */
            virtual TIssues GetIssues() const = 0;

            /**
             * Get precompiled mkql program
             */
            virtual TString GetCompiledProgram() = 0;

            /**
             * Return a worker to the factory for possible reuse
             */
            virtual void ReturnWorker(IWorker* worker) = 0;
        };

        class TReleaseWorker {
        public:
            template <class T>
            static inline void Destroy(T* t) noexcept {
                t->Release();
            }
        };

        template <class T>
        using TWorkerHolder = THolder<T, TReleaseWorker>;

        /**
         * Factory for generating pull stream workers.
         */
        class IPullStreamWorkerFactory: public IWorkerFactory {
        public:
            /**
             * Create a new pull stream worker.
             */
            virtual TWorkerHolder<IPullStreamWorker> MakeWorker() = 0;
        };

        /**
         * Factory for generating pull list workers.
         */
        class IPullListWorkerFactory: public IWorkerFactory {
        public:
            /**
             * Create a new pull list worker.
             */
            virtual TWorkerHolder<IPullListWorker> MakeWorker() = 0;
        };

        /**
         * Factory for generating push stream workers.
         */
        class IPushStreamWorkerFactory: public IWorkerFactory {
        public:
            /**
             * Create a new push stream worker.
             */
            virtual TWorkerHolder<IPushStreamWorker> MakeWorker() = 0;
        };

        ////////////////////////////////////////////////////////////////////////////////////////////////////

        /**
         * Worker is a central part of any program instance. It contains current computation state
         * (called computation graph) and objects required to work with it, including an allocator for unboxed values.
         *
         * Usually, users do not interact with workers directly. They use program instance entry points such as streams
         * and consumers instead. The only case when one would have to to interact with workers is when implementing
         * custom io-specification.
         */
        class IWorker {
        protected:
            friend class TReleaseWorker;
            /**
             * Cleanup the worker and return to a worker factory for reuse
             */
            virtual void Release() = 0;

        public:
            virtual ~IWorker() = default;

        public:
            /**
             * Number of inputs for this program.
             */
            virtual ui32 GetInputsCount() const = 0;

            /**
             * MiniKQL input struct type of specified input for this program. Type is equivalent to the deduced input
             * schema (see IWorker::MakeInputSchema())
             *
             * If ``original`` is set to ``true``, returns type without virtual system columns.
             */
            virtual const NKikimr::NMiniKQL::TStructType* GetInputType(ui32, bool original = false) const = 0;
            /**
             * Overload for single-input programs.
             */
            virtual const NKikimr::NMiniKQL::TStructType* GetInputType(bool original = false) const = 0;

            /**
             * MiniKQL output struct type for this program. The returned type is equivalent to the deduced output
             * schema (see IWorker::MakeFullOutputSchema()).
             */
            virtual const NKikimr::NMiniKQL::TType* GetOutputType() const = 0;

            /**
             * Make input type schema for specified input as deduced by program optimizer. This schema is equivalent
             * to one provided by input spec up to the order of the fields in structures.
             */
            virtual NYT::TNode MakeInputSchema(ui32) const = 0;
            /**
             * Overload for single-input programs.
             */
            virtual NYT::TNode MakeInputSchema() const = 0;

            /**
             * Make output type schema as deduced by program optimizer. If output spec provides its own schema, than
             * this schema is equivalent to one provided by output spec up to the order of the fields in structures.
             */
            /// @{
            /**
             * Overload for single-table output programs (i.e. output type is struct).
             */
            virtual NYT::TNode MakeOutputSchema() const = 0;
            /**
             * Overload for multi-table output programs (i.e. output type is variant over tuple).
             */
            virtual NYT::TNode MakeOutputSchema(ui32) const = 0;
            /**
             * Overload for multi-table output programs (i.e. output type is variant over struct).
             */
            virtual NYT::TNode MakeOutputSchema(TStringBuf) const = 0;
            /// @}

            /**
             * Generates full output schema. For single-output programs returns struct type, for multi-output programs
             * returns variant type.
             *
             * Warning: calling this function may result in extended memory usage for large number of output tables.
             */
            virtual NYT::TNode MakeFullOutputSchema() const = 0;

            /**
             * Get scoped alloc used in this worker.
             */
            virtual NKikimr::NMiniKQL::TScopedAlloc& GetScopedAlloc() = 0;

            /**
             * Get computation graph.
             */
            virtual NKikimr::NMiniKQL::IComputationGraph& GetGraph() = 0;

            /**
             * Get function registry for this worker.
             */
            virtual const NKikimr::NMiniKQL::IFunctionRegistry& GetFunctionRegistry() const = 0;

            /**
             * Get type environment for this worker.
             */
            virtual NKikimr::NMiniKQL::TTypeEnvironment& GetTypeEnvironment() = 0;

            /**
             * Get llvm settings for this worker.
             */
            virtual const TString& GetLLVMSettings() const = 0;

            /**
             * Get YT Type V3 flags
             */
            virtual ui64 GetNativeYtTypeFlags() const = 0;

            /**
             * Get time provider
             */
            virtual ITimeProvider* GetTimeProvider() const = 0;
        };

        /**
         * Worker which operates in pull stream mode.
         */
        class IPullStreamWorker: public IWorker {
        public:
            /**
             * Set input computation graph node for specified input. The passed unboxed value should be a stream of
             * structs. It should be created via the allocator associated with this very worker.
             * This function can only be called once for each input.
             */
            virtual void SetInput(NKikimr::NUdf::TUnboxedValue&&, ui32) = 0;

            /**
             * Get the output computation graph node. The returned node will be a stream of structs or variants.
             * This function cannot be called before setting an input value.
             */
            virtual NKikimr::NUdf::TUnboxedValue& GetOutput() = 0;
        };

        /**
         * Worker which operates in pull list mode.
         */
        class IPullListWorker: public IWorker {
        public:
            /**
             * Set input computation graph node for specified input. The passed unboxed value should be a list of
             * structs. It should be created via the allocator associated with this very worker.
             * This function can only be called once for each index.
             */
            virtual void SetInput(NKikimr::NUdf::TUnboxedValue&&, ui32) = 0;

            /**
             * Get the output computation graph node. The returned node will be a list of structs or variants.
             * This function cannot be called before setting an input value.
             */
            virtual NKikimr::NUdf::TUnboxedValue& GetOutput() = 0;

            /**
             * Get iterator over the output list.
             */
            virtual NKikimr::NUdf::TUnboxedValue& GetOutputIterator() = 0;

            /**
             * Reset iterator to the beginning of the output list. After calling this function, GetOutputIterator()
             * will return a fresh iterator; all previously returned iterators will become invalid.
             */
            virtual void ResetOutputIterator() = 0;
        };

        /**
         * Worker which operates in push stream mode.
         */
        class IPushStreamWorker: public IWorker {
        public:
            /**
             * Set a consumer where the worker will relay its output. This function can only be called once.
             */
            virtual void SetConsumer(THolder<IConsumer<const NKikimr::NUdf::TUnboxedValue*>>) = 0;

            /**
             * Push new value to the graph, than feed all new output to the consumer. Values cannot be pushed before
             * assigning a consumer.
             */
            virtual void Push(NKikimr::NUdf::TUnboxedValue&&) = 0;

            /**
             * Send finish event and clear the computation graph. No new values will be accepted.
             */
            virtual void OnFinish() = 0;
        };

        ////////////////////////////////////////////////////////////////////////////////////////////////////

        /**
         * Input specifications describe format for program input. They carry information about input data schema
         * as well as the knowledge about how to convert input structures into unboxed values (data format which can be
         * processed by the YQL runtime).
         *
         * Input spec defines the arguments of the program's Apply method. For example, a program
         * with the protobuf input spec will accept a stream of protobuf messages while a program with the
         * yson spec will accept an input stream (binary or text one).
         *
         * See documentation for input and output spec traits for hints on how to implement a custom specs.
         */
        class TInputSpecBase {
        protected:
            mutable TVector<THashMap<TString, NYT::TNode>> AllVirtualColumns_;

        public:
            virtual ~TInputSpecBase() = default;

        public:
            /**
             * Get input data schemas in YQL format (NB: not a YT format). Each item of the returned vector must
             * describe a structure.
             *
             * Format of each item is approximately this one:
             *
             * @code
             * [
             *   'StructType',
             *   [
             *     ["Field1Name", ["DataType", "Int32"]],
             *     ["Field2Name", ["DataType", "String"]],
             *     ...
             *   ]
             * ]
             * @endcode
             */
            virtual const TVector<NYT::TNode>& GetSchemas() const = 0;
            // TODO: make a neat schema builder

            /**
             * Get virtual columns for each input.
             *
             * Key of each mapping is column name, value is data schema in YQL format.
             */
            const TVector<THashMap<TString, NYT::TNode>>& GetAllVirtualColumns() const {
                if (AllVirtualColumns_.empty()) {
                    AllVirtualColumns_ = TVector<THashMap<TString, NYT::TNode>>(GetSchemas().size());
                }

                return AllVirtualColumns_;
            }

            static constexpr bool ProvidesBlocks = false;
        };

        /**
         * Output specifications describe format for program output. Like input specifications, they cary knowledge
         * about program output type and how to convert unboxed values into that type.
         */
        class TOutputSpecBase {
        private:
            TMaybe<THashSet<TString>> OutputColumnsFilter_;

        public:
            virtual ~TOutputSpecBase() = default;

        public:
            /**
             * Get output data schema in YQL format (NB: not a YT format). The returned value must describe a structure
             * or a variant made of structures for fulti-table outputs (note: not all specs support multi-table output).
             *
             * See docs for the input spec's GetSchemas().
             *
             * Also TNode entity could be returned (NYT::TNode::CreateEntity()),
             * in which case output schema would be inferred from query and could be
             * obtained by Program::GetOutputSchema() call.
             */
            virtual const NYT::TNode& GetSchema() const = 0;

            /**
             * Get an output columns filter.
             *
             * Output columns filter is a set of column names that should be left in the output. All columns that are
             * not in this set will not be calculated. Depending on the output schema, they will be either removed
             * completely (for optional columns) or filled with defaults (for required columns).
             */
            const TMaybe<THashSet<TString>>& GetOutputColumnsFilter() const {
                return OutputColumnsFilter_;
            }

            /**
             * Set new output columns filter.
             */
            void SetOutputColumnsFilter(const TMaybe<THashSet<TString>>& outputColumnsFilter) {
                OutputColumnsFilter_ = outputColumnsFilter;
            }

            static constexpr bool AcceptsBlocks = false;
        };

        ////////////////////////////////////////////////////////////////////////////////////////////////////

        /**
         * Input spec traits provide information on how to process program input.
         *
         * Each input spec should create a template specialization for this class, in which it should provide several
         * static variables and functions.
         *
         * For example, a hypothetical example of implementing a JSON input spec would look like this:
         *
         * @code
         * class TJsonInputSpec: public TInputSpecBase {
         *     // whatever magic you require for this spec
         * };
         *
         * template <>
         * class TInputSpecTraits<TJsonInputSpec> {
         *     // write here four constants, one typedef and three static functions described below
         * };
         * @endcode
         *
         * @tparam T input spec type.
         */
        template <typename T>
        struct TInputSpecTraits {
            /// Safety flag which should be set to false in all template specializations of this class. Attempt to
            /// build a program using a spec with `IsPartial=true` will result in compilation error.
            static const constexpr bool IsPartial = true;

            /// Indicates whether this spec supports pull stream mode.
            static const constexpr bool SupportPullStreamMode = false;
            /// Indicates whether this spec supports pull list mode.
            static const constexpr bool SupportPullListMode = false;
            /// Indicates whether this spec supports push stream mode.
            static const constexpr bool SupportPushStreamMode = false;

            /// For push mode, indicates the return type of the builder's Process function.
            using TConsumerType = void;

            /// For pull stream mode, should take an input spec, a pull stream worker and whatever the user passed
            /// to the program's Apply function, create an unboxed values with a custom stream implementations
            /// and pass it to the worker's SetInput function for each input.
            template <typename ...A>
            static void PreparePullStreamWorker(const T&, IPullStreamWorker*, A&&...) {
                Y_UNREACHABLE();
            }

            /// For pull list mode, should take an input spec, a pull list worker and whatever the user passed
            /// to the program's Apply function, create an unboxed values with a custom list implementations
            /// and pass it to the worker's SetInput function for each input.
            template <typename ...A>
            static void PreparePullListWorker(const T&, IPullListWorker*, A&&...) {
                Y_UNREACHABLE();
            }

            /// For push stream mode, should take an input spec and a worker and create a consumer which will
            /// be returned to the user. The consumer should keep the worker alive until its own destruction.
            /// The return type of this function should exactly match the one defined in ConsumerType typedef.
            static TConsumerType MakeConsumer(const T&, TWorkerHolder<IPushStreamWorker>) {
                Y_UNREACHABLE();
            }
        };

        /**
         * Output spec traits provide information on how to process program output. Like with input specs, each output
         * spec requires an appropriate template specialization of this class.
         *
         * @tparam T output spec type.
         */
        template <typename T>
        struct TOutputSpecTraits {
            /// Safety flag which should be set to false in all template specializations of this class. Attempt to
            /// build a program using a spec with `IsPartial=false` will result in compilation error.
            static const constexpr bool IsPartial = true;

            /// Indicates whether this spec supports pull stream mode.
            static const constexpr bool SupportPullStreamMode = false;
            /// Indicates whether this spec supports pull list mode.
            static const constexpr bool SupportPullListMode = false;
            /// Indicates whether this spec supports push stream mode.
            static const constexpr bool SupportPushStreamMode = false;

            /// For pull stream mode, indicates the return type of the program's Apply function.
            using TPullStreamReturnType = void;

            /// For pull list mode, indicates the return type of the program's Apply function.
            using TPullListReturnType = void;

            /// For pull stream mode, should take an output spec and a worker and build a stream which will be returned
            /// to the user. The return type of this function must match the one specified in the PullStreamReturnType.
            static TPullStreamReturnType ConvertPullStreamWorkerToOutputType(const T&, TWorkerHolder<IPullStreamWorker>) {
                Y_UNREACHABLE();
            }

            /// For pull list mode, should take an output spec and a worker and build a list which will be returned
            /// to the user. The return type of this function must match the one specified in the PullListReturnType.
            static TPullListReturnType ConvertPullListWorkerToOutputType(const T&, TWorkerHolder<IPullListWorker>) {
                Y_UNREACHABLE();
            }

            /// For push stream mode, should take an output spec, a worker and whatever arguments the user passed
            /// to the program's Apply function, create a consumer for unboxed values and pass it to the worker's
            /// SetConsumer function.
            template <typename ...A>
            static void SetConsumerToWorker(const T&, IPushStreamWorker*, A&&...) {
                Y_UNREACHABLE();
            }
        };

        ////////////////////////////////////////////////////////////////////////////////////////////////////

#define NOT_SPEC_MSG(spec_type) "passed class should be derived from " spec_type " spec base"
#define PARTIAL_SPEC_MSG(spec_type) "this " spec_type " spec does not define its traits. Make sure you've passed "     \
                                    "an " spec_type " spec and not some other object; also make sure you've included " \
                                    "all  necessary headers. If you're developing a spec, make sure you have "         \
                                    "a spec traits template specialization"
#define UNSUPPORTED_MODE_MSG(spec_type, mode) "this " spec_type " spec does not support " mode " mode"

        class IProgram {
        public:
            virtual ~IProgram() = default;

        public:
            virtual const TInputSpecBase& GetInputSpecBase() const = 0;
            virtual const TOutputSpecBase& GetOutputSpecBase() const = 0;
            virtual const THashSet<TString>& GetUsedColumns(ui32) const = 0;
            virtual const THashSet<TString>& GetUsedColumns() const = 0;
            virtual NYT::TNode MakeInputSchema(ui32) const = 0;
            virtual NYT::TNode MakeInputSchema() const = 0;
            virtual NYT::TNode MakeOutputSchema() const = 0;
            virtual NYT::TNode MakeOutputSchema(ui32) const = 0;
            virtual NYT::TNode MakeOutputSchema(TStringBuf) const = 0;
            virtual NYT::TNode MakeFullOutputSchema() const = 0;
            virtual TIssues GetIssues() const = 0;
            virtual TString GetCompiledProgram() = 0;

            inline void MergeUsedColumns(THashSet<TString>& columns, ui32 inputIndex) {
                const auto& usedColumns = GetUsedColumns(inputIndex);
                columns.insert(usedColumns.begin(), usedColumns.end());
            }

            inline void MergeUsedColumns(THashSet<TString>& columns) {
                const auto& usedColumns = GetUsedColumns();
                columns.insert(usedColumns.begin(), usedColumns.end());
            }
        };

        template <typename TInputSpec, typename TOutputSpec, typename WorkerFactory>
        class TProgramCommon: public IProgram {
            static_assert(std::is_base_of<TInputSpecBase, TInputSpec>::value, NOT_SPEC_MSG("input"));
            static_assert(std::is_base_of<TOutputSpecBase, TOutputSpec>::value, NOT_SPEC_MSG("output"));

        protected:
            TInputSpec InputSpec_;
            TOutputSpec OutputSpec_;
            std::shared_ptr<WorkerFactory> WorkerFactory_;

        public:
            explicit TProgramCommon(
                TInputSpec inputSpec,
                TOutputSpec outputSpec,
                std::shared_ptr<WorkerFactory> workerFactory
            )
                : InputSpec_(inputSpec)
                , OutputSpec_(outputSpec)
                , WorkerFactory_(std::move(workerFactory))
            {
            }

        public:
            const TInputSpec& GetInputSpec() const {
                return InputSpec_;
            }

            const TOutputSpec& GetOutputSpec() const {
                return OutputSpec_;
            }

            const TInputSpecBase& GetInputSpecBase() const override {
                return InputSpec_;
            }

            const TOutputSpecBase& GetOutputSpecBase() const override {
                return OutputSpec_;
            }

            const THashSet<TString>& GetUsedColumns(ui32 inputIndex) const override {
                return WorkerFactory_->GetUsedColumns(inputIndex);
            }

            const THashSet<TString>& GetUsedColumns() const override {
                return WorkerFactory_->GetUsedColumns();
            }

            NYT::TNode MakeInputSchema(ui32 inputIndex) const override {
                return WorkerFactory_->MakeInputSchema(inputIndex);
            }

            NYT::TNode MakeInputSchema() const override {
                return WorkerFactory_->MakeInputSchema();
            }

            NYT::TNode MakeOutputSchema() const override {
                return WorkerFactory_->MakeOutputSchema();
            }

            NYT::TNode MakeOutputSchema(ui32 outputIndex) const override {
                return WorkerFactory_->MakeOutputSchema(outputIndex);
            }

            NYT::TNode MakeOutputSchema(TStringBuf outputName) const override {
                return WorkerFactory_->MakeOutputSchema(outputName);
            }

            NYT::TNode MakeFullOutputSchema() const override {
                return WorkerFactory_->MakeFullOutputSchema();
            }

            TIssues GetIssues() const override {
                return WorkerFactory_->GetIssues();
            }

            TString GetCompiledProgram() override {
                return WorkerFactory_->GetCompiledProgram();
            }
        };

        template <typename TInputSpec, typename TOutputSpec>
        class TPullStreamProgram final: public TProgramCommon<TInputSpec, TOutputSpec, IPullStreamWorkerFactory> {
            using TProgramCommon<TInputSpec, TOutputSpec, IPullStreamWorkerFactory>::WorkerFactory_;
            using TProgramCommon<TInputSpec, TOutputSpec, IPullStreamWorkerFactory>::InputSpec_;
            using TProgramCommon<TInputSpec, TOutputSpec, IPullStreamWorkerFactory>::OutputSpec_;

        public:
            using TProgramCommon<TInputSpec, TOutputSpec, IPullStreamWorkerFactory>::TProgramCommon;

        public:
            template <typename ...T>
            typename TOutputSpecTraits<TOutputSpec>::TPullStreamReturnType Apply(T&& ... t) {
                static_assert(!TInputSpecTraits<TInputSpec>::IsPartial, PARTIAL_SPEC_MSG("input"));
                static_assert(!TOutputSpecTraits<TOutputSpec>::IsPartial, PARTIAL_SPEC_MSG("output"));
                static_assert(TInputSpecTraits<TInputSpec>::SupportPullStreamMode, UNSUPPORTED_MODE_MSG("input", "pull stream"));
                static_assert(TOutputSpecTraits<TOutputSpec>::SupportPullStreamMode, UNSUPPORTED_MODE_MSG("output", "pull stream"));

                auto worker = WorkerFactory_->MakeWorker();
                TInputSpecTraits<TInputSpec>::PreparePullStreamWorker(InputSpec_, worker.Get(), std::forward<T>(t)...);
                return TOutputSpecTraits<TOutputSpec>::ConvertPullStreamWorkerToOutputType(OutputSpec_, std::move(worker));
            }
        };

        template <typename TInputSpec, typename TOutputSpec>
        class TPullListProgram final: public TProgramCommon<TInputSpec, TOutputSpec, IPullListWorkerFactory> {
            using TProgramCommon<TInputSpec, TOutputSpec, IPullListWorkerFactory>::WorkerFactory_;
            using TProgramCommon<TInputSpec, TOutputSpec, IPullListWorkerFactory>::InputSpec_;
            using TProgramCommon<TInputSpec, TOutputSpec, IPullListWorkerFactory>::OutputSpec_;

        public:
            using TProgramCommon<TInputSpec, TOutputSpec, IPullListWorkerFactory>::TProgramCommon;

        public:
            template <typename ...T>
            typename TOutputSpecTraits<TOutputSpec>::TPullListReturnType Apply(T&& ... t) {
                static_assert(!TInputSpecTraits<TInputSpec>::IsPartial, PARTIAL_SPEC_MSG("input"));
                static_assert(!TOutputSpecTraits<TOutputSpec>::IsPartial, PARTIAL_SPEC_MSG("output"));
                static_assert(TInputSpecTraits<TInputSpec>::SupportPullListMode, UNSUPPORTED_MODE_MSG("input", "pull list"));
                static_assert(TOutputSpecTraits<TOutputSpec>::SupportPullListMode, UNSUPPORTED_MODE_MSG("output", "pull list"));

                auto worker = WorkerFactory_->MakeWorker();
                TInputSpecTraits<TInputSpec>::PreparePullListWorker(InputSpec_, worker.Get(), std::forward<T>(t)...);
                return TOutputSpecTraits<TOutputSpec>::ConvertPullListWorkerToOutputType(OutputSpec_, std::move(worker));
            }
        };

        template <typename TInputSpec, typename TOutputSpec>
        class TPushStreamProgram final: public TProgramCommon<TInputSpec, TOutputSpec, IPushStreamWorkerFactory> {
            using TProgramCommon<TInputSpec, TOutputSpec, IPushStreamWorkerFactory>::WorkerFactory_;
            using TProgramCommon<TInputSpec, TOutputSpec, IPushStreamWorkerFactory>::InputSpec_;
            using TProgramCommon<TInputSpec, TOutputSpec, IPushStreamWorkerFactory>::OutputSpec_;

        public:
            using TProgramCommon<TInputSpec, TOutputSpec, IPushStreamWorkerFactory>::TProgramCommon;

        public:
            template <typename ...T>
            typename TInputSpecTraits<TInputSpec>::TConsumerType Apply(T&& ... t) {
                static_assert(!TInputSpecTraits<TInputSpec>::IsPartial, PARTIAL_SPEC_MSG("input"));
                static_assert(!TOutputSpecTraits<TOutputSpec>::IsPartial, PARTIAL_SPEC_MSG("output"));
                static_assert(TInputSpecTraits<TInputSpec>::SupportPushStreamMode, UNSUPPORTED_MODE_MSG("input", "push stream"));
                static_assert(TOutputSpecTraits<TOutputSpec>::SupportPushStreamMode, UNSUPPORTED_MODE_MSG("output", "push stream"));

                auto worker = WorkerFactory_->MakeWorker();
                TOutputSpecTraits<TOutputSpec>::SetConsumerToWorker(OutputSpec_, worker.Get(), std::forward<T>(t)...);
                return TInputSpecTraits<TInputSpec>::MakeConsumer(InputSpec_, std::move(worker));
            }
        };

#undef NOT_SPEC_MSG
#undef PARTIAL_SPEC_MSG
#undef UNSUPPORTED_MODE_MSG

        ////////////////////////////////////////////////////////////////////////////////////////////////////

        /**
         * Configure global logging facilities. Affects all YQL modules.
         */
        void ConfigureLogging(const TLoggingOptions& = {});

        /**
         * Create a new program factory.
         * Custom logging initialization could be preformed by a call to the ConfigureLogging method beforehand.
         * If the ConfigureLogging method has not been called the default logging initialization will be performed.
         */
        IProgramFactoryPtr MakeProgramFactory(const TProgramFactoryOptions& = {});
    }
}

Y_DECLARE_OUT_SPEC(inline, NYql::NPureCalc::TCompileError, stream, value) {
        stream << value.AsStrBuf() << Endl << "Issues:" << Endl << value.GetIssues() << Endl << Endl << "Yql:" << Endl <<value.GetYql();
}
