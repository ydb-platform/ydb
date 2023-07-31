#pragma once

#include <ydb/library/yql/public/purecalc/common/interface.h>

#include <util/generic/noncopyable.h>

namespace NYT {
    class TRawTableReader;
}

namespace NYql {
    namespace NPureCalc {
        /**
         * Processing mode for working with Skiff/YSON IO.
         *
         * In this mode purecalc accepts vector of pointers to `IInputStream` as an inputs and returns a handle
         * which can be used to invoke program writing all output to a stream.
         *
         * For example:
         *
         * @code
         * auto handle = program.Apply(&Cin);
         * handle->Run(&Cout);  // run the program, read from Cin and write to Cout
         * @endcode
         *
         * All working modes except PushStream are supported.
         */
        template <bool UseSkiff, typename TBase>
        class TMkqlSpec: public TBase {
            static_assert(
                std::is_same<TBase, TInputSpecBase>::value ||
                std::is_same<TBase, TOutputSpecBase>::value,
                "Class is used in unintended way!"
            );
        };

        /**
         * Skiff/YSON input spec. In this mode purecalc takes a non-owning pointers to a text input streams and parses
         * them using Skiff or YSON codec.
         *
         * The program synopsis follows:
         *
         * @code
         * ... TPullStreamProgram::Apply(TVector<IInputStream*>);
         * ... TPullStreamProgram::Apply(TVector<NYT::TRawTableReader*>);
         * ... TPullListProgram::Apply(TVector<IInputStream*>);
         * ... TPullListProgram::Apply(TVector<NYT::TRawTableReader*>);
         * @endcode
         *
         * @tparam UseSkiff expect Skiff format if true, YSON otherwise.
         */
        template <bool UseSkiff>
        class TMkqlInputSpec: public TMkqlSpec<UseSkiff, TInputSpecBase> {
        public:
            using TBase = TInputSpecBase;
            static constexpr bool UseSkiffValue = UseSkiff;

        private:
            TVector<NYT::TNode> Schemas_;
            bool StrictSchema_ = true;
            bool IgnoreStreamTableIndex_ = false;
            TVector<TMaybe<TVector<TString>>> AllTableNames_;
            // Allows to read structure columns with custom members order.
            // Instead of chain TNode => TTypeAnnotationNode => TType => TNode (which looses members order) use
            // original schema as row spec.
            bool UseOriginalRowSpec_ = false;

        public:
            explicit TMkqlInputSpec(TVector<NYT::TNode>);
            explicit TMkqlInputSpec(NYT::TNode, bool ignoreStreamTableIndex = false);

            const TVector<NYT::TNode>& GetSchemas() const override;

            bool IgnoreStreamTableIndex() const;

            bool IsStrictSchema() const;
            TMkqlInputSpec& SetStrictSchema(bool strictSchema);

            const TMaybe<TVector<TString>>& GetTableNames() const;
            const TMaybe<TVector<TString>>& GetTableNames(ui32) const;
            bool UseOriginalRowSpec() const;

            TMkqlInputSpec& SetTableNames(TVector<TString>);
            TMkqlInputSpec& SetTableNames(TVector<TString>, ui32);
            TMkqlInputSpec& SetUseOriginalRowSpec(bool value);
        };

        /**
         * Skiff/YSON output. In this mode purecalc returns a handle which can be used to invoke an underlying program.
         *
         * So far this is the only spec that supports multi-table output.
         *
         * The program synopsis follows:
         *
         * @code
         * THolder<THandle> TPullStreamProgram::Apply(...);
         * THolder<THandle> TPullListProgram::Apply(...);
         * @endcode
         *
         * @tparam UseSkiff write output in Skiff format if true, use YSON otherwise.
         */
        template <bool UseSkiff>
        class TMkqlOutputSpec: public TMkqlSpec<UseSkiff, TOutputSpecBase> {
        public:
            using TMkqlSpec<UseSkiff, TOutputSpecBase>::TMkqlSpec;

            using TBase = TOutputSpecBase;
            static constexpr bool UseSkiffValue = UseSkiff;

        private:
            NYT::TNode Schema_;

        public:
            explicit TMkqlOutputSpec(NYT::TNode);

            const NYT::TNode& GetSchema() const override;
        };

        /**
         * A class which can invoke a purecalc program and store its output in the given output stream.
         */
        class THandle: private TMoveOnly {
        public:
            /**
             * Run the program. Read a chunk from the program's assigned input, parse it and pass it to the program.
             * Than serialize the program's output and write it to the given output stream. Repeat until the input
             * stream is empty.
             */
            /// @{
            /**
             * Overload for single-table output programs (i.e. output type is struct).
             */
            virtual void Run(IOutputStream*) = 0;
            /**
             * Overload for multi-table output programs (i.e. output type is variant over tuple).
             * Size of vector should match number of variant alternatives.
             */
            virtual void Run(const TVector<IOutputStream*>&) = 0;
            /**
             * Overload for multi-table output programs (i.e. output type is variant over struct).
             * Size of map should match number of variant alternatives. For every alternative there should be a stream
             * in the map.
             */
            virtual void Run(const TMap<TString, IOutputStream*>&) = 0;
            /// @}

            virtual ~THandle() = default;
        };

        template <bool UseSkiff>
        struct TInputSpecTraits<TMkqlInputSpec<UseSkiff>> {
            static const constexpr bool IsPartial = false;

            static const constexpr bool SupportPullStreamMode = true;
            static const constexpr bool SupportPullListMode = true;
            static const constexpr bool SupportPushStreamMode = false;

            static void PreparePullStreamWorker(
                const TMkqlInputSpec<UseSkiff>& spec, IPullStreamWorker* worker, const TVector<IInputStream*>& streams);

            static void PreparePullStreamWorker(
                const TMkqlInputSpec<UseSkiff>& spec, IPullStreamWorker* worker, TVector<THolder<IInputStream>>&& streams);

            static void PreparePullStreamWorker(
                const TMkqlInputSpec<UseSkiff>& spec, IPullStreamWorker* worker, const TVector<NYT::TRawTableReader*>& streams);

            static void PreparePullStreamWorker(
                const TMkqlInputSpec<UseSkiff>& spec, IPullStreamWorker* worker, TVector<THolder<NYT::TRawTableReader>>&& streams);

            static void PreparePullListWorker(
                const TMkqlInputSpec<UseSkiff>& spec, IPullListWorker* worker, const TVector<IInputStream*>& streams);

            static void PreparePullListWorker(
                const TMkqlInputSpec<UseSkiff>& spec, IPullListWorker* worker, TVector<THolder<IInputStream>>&& streams);

            static void PreparePullListWorker(
                const TMkqlInputSpec<UseSkiff>& spec, IPullListWorker* worker, const TVector<NYT::TRawTableReader*>& streams);

            static void PreparePullListWorker(
                const TMkqlInputSpec<UseSkiff>& spec, IPullListWorker* worker, TVector<THolder<NYT::TRawTableReader>>&& streams);

            // Members for single-input programs

            static void PreparePullStreamWorker(
                const TMkqlInputSpec<UseSkiff>& spec, IPullStreamWorker* worker, IInputStream* stream);

            static void PreparePullStreamWorker(
                const TMkqlInputSpec<UseSkiff>& spec, IPullStreamWorker* worker, THolder<IInputStream> stream);

            static void PreparePullStreamWorker(
                const TMkqlInputSpec<UseSkiff>& spec, IPullStreamWorker* worker, NYT::TRawTableReader* stream);

            static void PreparePullStreamWorker(
                const TMkqlInputSpec<UseSkiff>& spec, IPullStreamWorker* worker, THolder<NYT::TRawTableReader> stream);

            static void PreparePullListWorker(
                const TMkqlInputSpec<UseSkiff>& spec, IPullListWorker* worker, IInputStream* stream);

            static void PreparePullListWorker(
                const TMkqlInputSpec<UseSkiff>& spec, IPullListWorker* worker, THolder<IInputStream> stream);

            static void PreparePullListWorker(
                const TMkqlInputSpec<UseSkiff>& spec, IPullListWorker* worker, NYT::TRawTableReader* stream);

            static void PreparePullListWorker(
                const TMkqlInputSpec<UseSkiff>& spec, IPullListWorker* worker, THolder<NYT::TRawTableReader> stream);
        };

        template <bool UseSkiff>
        struct TOutputSpecTraits<TMkqlOutputSpec<UseSkiff>> {
            static const constexpr bool IsPartial = false;

            static const constexpr bool SupportPullStreamMode = true;
            static const constexpr bool SupportPullListMode = true;
            static const constexpr bool SupportPushStreamMode = false;

            using TPullStreamReturnType = THolder<THandle>;
            using TPullListReturnType = THolder<THandle>;

            static TPullStreamReturnType ConvertPullStreamWorkerToOutputType(const TMkqlOutputSpec<UseSkiff>&, TWorkerHolder<IPullStreamWorker>);

            static TPullListReturnType ConvertPullListWorkerToOutputType(const TMkqlOutputSpec<UseSkiff>&, TWorkerHolder<IPullListWorker>);
        };

        using TSkiffInputSpec = TMkqlInputSpec<true>;
        using TSkiffOutputSpec = TMkqlOutputSpec<true>;

        using TYsonInputSpec = TMkqlInputSpec<false>;
        using TYsonOutputSpec = TMkqlOutputSpec<false>;
    }
}
