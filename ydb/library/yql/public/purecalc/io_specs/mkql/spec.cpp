#include "spec.h"

#include <ydb/library/yql/public/purecalc/common/names.h>
#include <ydb/library/yql/minikql/computation/mkql_custom_list.h>
#include <ydb/library/yql/providers/yt/codec/yt_codec_io.h>
#include <ydb/library/yql/providers/yt/lib/mkql_helpers/mkql_helpers.h>
#include <ydb/library/yql/providers/yt/common/yql_names.h>
#include <ydb/library/yql/providers/common/codec/yql_codec_type_flags.h>
#include <ydb/library/yql/providers/common/schema/mkql/yql_mkql_schema.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>

#include <library/cpp/yson/node/node_io.h>

#include <util/generic/noncopyable.h>
#include <util/generic/ptr.h>


namespace {
    const TStringBuf PathColumnShortName = "path";

    template <typename T>
    inline TVector<THolder<T>> VectorFromHolder(THolder<T> holder) {
        TVector<THolder<T>> result;
        result.push_back(std::move(holder));
        return result;
    }

    template <typename TRowType>
    NYT::TNode ComposeRowSpec(const TRowType* rowType, ui64 nativeYtTypeFlags, bool strictSchema) {
        constexpr bool isNodeType = std::is_same_v<TRowType, NYT::TNode>;

        static_assert(isNodeType || std::is_same_v<TRowType, NKikimr::NMiniKQL::TType>);

        auto typeNode = NYT::TNode::CreateMap();
        if constexpr (isNodeType) {
            typeNode[NYql::RowSpecAttrType] = *rowType;
        } else {
            typeNode[NYql::RowSpecAttrType] = NYql::NCommon::TypeToYsonNode(rowType);
        }
        typeNode[NYql::RowSpecAttrNativeYtTypeFlags] = nativeYtTypeFlags;
        typeNode[NYql::RowSpecAttrStrictSchema] = strictSchema;

        auto attrNode = NYT::TNode::CreateMap();
        attrNode[NYql::YqlRowSpecAttribute] = std::move(typeNode);

        return attrNode;
    }

    struct TInputDescription {
    public:
        ui32 InputIndex;
        const TMaybe<TVector<TString>>& TableNames;
        const NYT::TNode& InputSchema;
        const bool UseOriginalRowSpec;

    public:
        template <bool UseSkiff>
        TInputDescription(const NYql::NPureCalc::TMkqlInputSpec<UseSkiff>& spec, ui32 inputIndex)
            : InputIndex(inputIndex)
            , TableNames(spec.GetTableNames(InputIndex))
            , InputSchema(spec.GetSchemas().at(inputIndex))
            , UseOriginalRowSpec(spec.UseOriginalRowSpec())
        {
        }

        bool UseSystemColumns() const {
            return TableNames.Defined();
        }

        size_t GetTablesNumber() const {
            if (TableNames.Defined()) {
                return TableNames->size();
            }

            return 1;
        }
    };

    NYT::TNode ComposeYqlAttributesFromSchema(
        const NKikimr::NMiniKQL::TType* type,
        ui64 nativeYtTypeFlags,
        bool strictSchema,
        const TInputDescription* inputDescription = nullptr)
    {
        auto attrs = NYT::TNode::CreateMap();
        NYT::TNode& tables = attrs[NYql::YqlIOSpecTables];

        switch (type->GetKind()) {
            case NKikimr::NMiniKQL::TType::EKind::Variant:
            {
                YQL_ENSURE(!inputDescription);

                const auto* vtype = AS_TYPE(NKikimr::NMiniKQL::TVariantType, type);

                NYT::TNode& registryNode = attrs[NYql::YqlIOSpecRegistry];
                THashMap<TString, TString> uniqSpecs;

                for (ui32 i = 0; i < vtype->GetAlternativesCount(); i++) {
                    TString refName = TStringBuilder() << "$table" << uniqSpecs.size();

                    auto rowSpec = ComposeRowSpec(vtype->GetAlternativeType(i), nativeYtTypeFlags, strictSchema);

                    auto res = uniqSpecs.emplace(NYT::NodeToCanonicalYsonString(rowSpec), refName);
                    if (res.second) {
                        registryNode[refName] = rowSpec;
                    } else {
                        refName = res.first->second;
                    }
                    tables.Add(refName);
                }
                break;
            }
            case NKikimr::NMiniKQL::TType::EKind::Struct:
            {
                auto rowSpec = NYT::TNode();

                if (inputDescription && inputDescription->UseOriginalRowSpec) {
                    rowSpec = ComposeRowSpec(&inputDescription->InputSchema, nativeYtTypeFlags, strictSchema);
                } else {
                    rowSpec = ComposeRowSpec(type, nativeYtTypeFlags, strictSchema);
                }

                if (inputDescription && inputDescription->UseSystemColumns()) {
                    rowSpec[NYql::YqlSysColumnPrefix] = NYT::TNode().Add(PathColumnShortName);
                }

                if (inputDescription && inputDescription->GetTablesNumber() > 1) {
                    TStringBuf refName = "$table0";
                    attrs[NYql::YqlIOSpecRegistry][refName] = std::move(rowSpec);
                    for (ui32 i = 0; i < inputDescription->GetTablesNumber(); ++i) {
                        tables.Add(refName);
                    }
                } else {
                    tables.Add(std::move(rowSpec));
                }
                break;
            }
            default:
                Y_UNREACHABLE();
        }

        return attrs;
    }

    NYql::NCommon::TCodecContext MakeCodecCtx(NYql::NPureCalc::IWorker* worker) {
        return NYql::NCommon::TCodecContext(
            worker->GetTypeEnvironment(),
            worker->GetFunctionRegistry(),
            &worker->GetGraph().GetHolderFactory()
        );
    }

    NYql::TMkqlIOSpecs GetIOSpecs(
        NYql::NPureCalc::IWorker* worker,
        NYql::NCommon::TCodecContext& codecCtx,
        bool useSkiff,
        const TInputDescription* inputDescription = nullptr,
        bool strictSchema = true
    ) {
        NYql::TMkqlIOSpecs specs;
        if (useSkiff) {
            specs.SetUseSkiff(worker->GetLLVMSettings());
        }

        if (inputDescription) {
            const auto* type = worker->GetInputType(inputDescription->InputIndex, true);
            const auto* fullType = worker->GetInputType(inputDescription->InputIndex, false);

            YQL_ENSURE(!type->FindMemberIndex(NYql::YqlSysColumnPath));

            size_t extraColumnsCount = 0;
            if (inputDescription->UseSystemColumns()) {
                YQL_ENSURE(fullType->FindMemberIndex(NYql::YqlSysColumnPath));
                ++extraColumnsCount;
            }
            if (!strictSchema) {
                YQL_ENSURE(fullType->FindMemberIndex(NYql::YqlOthersColumnName));
                ++extraColumnsCount;
            }

            if (extraColumnsCount != 0) {
                YQL_ENSURE(fullType->GetMembersCount() == type->GetMembersCount() + extraColumnsCount);
            } else {
                YQL_ENSURE(type == fullType);
            }

            auto attrs = ComposeYqlAttributesFromSchema(type, worker->GetNativeYtTypeFlags(), strictSchema, inputDescription);
            if (inputDescription->TableNames) {
                specs.Init(codecCtx, attrs, inputDescription->TableNames.GetRef(), {});
            } else {
                specs.Init(codecCtx, attrs, {}, {});
            }
        } else {
            auto attrs = ComposeYqlAttributesFromSchema(worker->GetOutputType(), worker->GetNativeYtTypeFlags(), strictSchema);
            specs.Init(codecCtx, attrs);
        }

        return specs;
    }

    class TRawTableReaderImpl final: public NYT::TRawTableReader {
    private:
        // If we own Underlying_, than Owned_ == Underlying_, otherwise Owned_ is nullptr.
        THolder<IInputStream> Owned_;
        IInputStream* Underlying_;
        NKikimr::NMiniKQL::TScopedAlloc& ScopedAlloc_;

    private:
        TRawTableReaderImpl(
            IInputStream* underlying,
            THolder<IInputStream> owned,
            NKikimr::NMiniKQL::TScopedAlloc& scopedAlloc
        )
            : Owned_(std::move(owned))
            , Underlying_(underlying)
            , ScopedAlloc_(scopedAlloc)
        {
        }

    public:
        TRawTableReaderImpl(THolder<IInputStream> stream, NKikimr::NMiniKQL::TScopedAlloc& scopedAlloc)
            : TRawTableReaderImpl(stream.Get(), nullptr, scopedAlloc)
        {
            Owned_ = std::move(stream);
        }

        TRawTableReaderImpl(IInputStream* stream, NKikimr::NMiniKQL::TScopedAlloc& scopedAlloc)
            : TRawTableReaderImpl(stream, nullptr, scopedAlloc)
        {
        }

        bool Retry(const TMaybe<ui32>&, const TMaybe<ui64>&, const std::exception_ptr&) override {
            return false;
        }

        void ResetRetries() override {
        }

        bool HasRangeIndices() const override {
            return false;
        }

    protected:
        size_t DoRead(void* buf, size_t len) override {
            auto unguard = Unguard(ScopedAlloc_);
            return Underlying_->Read(buf, len);
        }
    };


    class TMkqlListValue: public NKikimr::NMiniKQL::TCustomListValue {
    private:
        mutable bool HasIterator_ = false;
        NYql::NPureCalc::IWorker* Worker_;
        // Keeps struct members reorders
        NYql::NCommon::TCodecContext CodecCtx_;
        NYql::TMkqlIOSpecs IOSpecs_;
        // If we own Underlying_, than Owned_ == Underlying_, otherwise Owned_ is nullptr.
        THolder<NYT::TRawTableReader> Owned_;
        NYT::TRawTableReader* Underlying_;
        NYql::TMkqlReaderImpl Reader_;

    private:
        TMkqlListValue(
            NKikimr::NMiniKQL::TMemoryUsageInfo* memInfo,
            bool useSkiff,
            NYT::TRawTableReader* underlying,
            THolder<NYT::TRawTableReader> owned,
            NYql::NPureCalc::IWorker* worker,
            const TInputDescription& inputDescription,
            bool ignoreStreamTableIndex = false,
            bool strictSchema = true
        ) : TCustomListValue(memInfo)
            , Worker_(worker)
            , CodecCtx_(MakeCodecCtx(Worker_))
            , IOSpecs_(GetIOSpecs(Worker_, CodecCtx_, useSkiff, &inputDescription, strictSchema))
            , Owned_(std::move(owned))
            , Underlying_(underlying)
            , Reader_(*Underlying_, 0, 1ul << 20, 0, ignoreStreamTableIndex)
        {
            Reader_.SetSpecs(IOSpecs_, Worker_->GetGraph().GetHolderFactory());
            Reader_.Next();
        }

    public:
        TMkqlListValue(
            NKikimr::NMiniKQL::TMemoryUsageInfo* memInfo,
            bool useSkiff,
            THolder<NYT::TRawTableReader> stream,
            NYql::NPureCalc::IWorker* worker,
            const TInputDescription& inputDescription,
            bool ignoreStreamTableIndex = false,
            bool strictSchema = true
        )
            : TMkqlListValue(
                memInfo, useSkiff, stream.Get(), nullptr, worker, inputDescription, ignoreStreamTableIndex, strictSchema)
        {
            Owned_ = std::move(stream);
        }

        TMkqlListValue(
            NKikimr::NMiniKQL::TMemoryUsageInfo* memInfo,
            bool useSkiff,
            NYT::TRawTableReader* stream,
            NYql::NPureCalc::IWorker* worker,
            const TInputDescription& inputDescription,
            bool ignoreStreamTableIndex,
            bool strictSchema = true
        )
            : TMkqlListValue(memInfo, useSkiff, stream, nullptr, worker, inputDescription, ignoreStreamTableIndex, strictSchema)
        {
        }

        NKikimr::NUdf::TUnboxedValue GetListIterator() const override {
            YQL_ENSURE(!HasIterator_, "Only one pass over input is supported");
            HasIterator_ = true;
            return NKikimr::NUdf::TUnboxedValuePod(const_cast<TMkqlListValue*>(this));
        }

        bool Next(NKikimr::NUdf::TUnboxedValue& result) override {
            if (!Reader_.IsValid()) {
                return false;
            }

            result = Reader_.GetRow();
            Reader_.Next();

            return true;
        }

        NKikimr::NUdf::EFetchStatus Fetch(
            NKikimr::NUdf::TUnboxedValue& result
        ) override {
            if (Next(result)) {
                return NKikimr::NUdf::EFetchStatus::Ok;
            }

            return NKikimr::NUdf::EFetchStatus::Finish;
        }
    };

    class TMkqlWriter: public NYql::NPureCalc::THandle {
    protected:
        virtual const NYql::NPureCalc::IWorker* GetWorker() const = 0;
        virtual void DoRun(const TVector<IOutputStream*>& stream) = 0;

    public:
        void Run(IOutputStream* stream) final {
            Y_ENSURE(
                GetWorker()->GetOutputType()->IsStruct(),
                "NYql::NPureCalc::THandle::Run(IOutputStream*) cannot be used with multi-output programs; "
                "use other overloads of Run() instead.");

            DoRun({stream});
        }

        void Run(const TVector<IOutputStream*>& streams) final {
            Y_ENSURE(
                GetWorker()->GetOutputType()->IsVariant(),
                "NYql::NPureCalc::THandle::Run(TVector<IOutputStream*>) cannot be used with single-output programs; "
                "use NYql::NPureCalc::THandle::Run(IOutputStream*) instead.");

            const auto* variantType = AS_TYPE(NKikimr::NMiniKQL::TVariantType, GetWorker()->GetOutputType());

            Y_ENSURE(
                variantType->GetUnderlyingType()->IsTuple(),
                "NYql::NPureCalc::THandle::Run(TVector<IOutputStream*>) cannot be used to process variants over struct; "
                "use NYql::NPureCalc::THandle::Run(TMap<TString, IOutputStream*>) instead.");

            const auto* tupleType = AS_TYPE(NKikimr::NMiniKQL::TTupleType, variantType->GetUnderlyingType());

            Y_ENSURE(
                tupleType->GetElementsCount() == streams.size(),
                "Number of variant alternatives should match number of streams.");

            DoRun(streams);
        }

        void Run(const TMap<TString, IOutputStream*>& streams) final {
            Y_ENSURE(
                GetWorker()->GetOutputType()->IsVariant(),
                "NYql::NPureCalc::THandle::Run(TMap<TString, IOutputStream*>) cannot be used with single-output programs; "
                "use NYql::NPureCalc::THandle::Run(IOutputStream*) instead.");

            const auto* variantType = AS_TYPE(NKikimr::NMiniKQL::TVariantType, GetWorker()->GetOutputType());

            Y_ENSURE(
                variantType->GetUnderlyingType()->IsStruct(),
                "NYql::NPureCalc::THandle::Run(TMap<TString, IOutputStream*>) cannot be used to process variants over tuple; "
                "use NYql::NPureCalc::THandle::Run(TVector<IOutputStream*>) instead.");

            const auto* structType = AS_TYPE(NKikimr::NMiniKQL::TStructType, variantType->GetUnderlyingType());

            Y_ENSURE(
                structType->GetMembersCount() == streams.size(),
                "Number of variant alternatives should match number of streams.");

            TVector<IOutputStream*> sortedStreams;
            sortedStreams.reserve(structType->GetMembersCount());

            for (ui32 i = 0; i < structType->GetMembersCount(); i++) {
                auto name = TString{structType->GetMemberName(i)};
                Y_ENSURE(streams.contains(name), "Cannot find stream for alternative " << name.Quote());
                sortedStreams.push_back(streams.at(name));
            }

            DoRun(sortedStreams);
        }
    };

    class TPullListMkqlWriter: public TMkqlWriter {
    private:
        NYql::NPureCalc::TWorkerHolder<NYql::NPureCalc::IPullListWorker> Worker_;
        NYql::NCommon::TCodecContext CodecCtx_;
        NYql::TMkqlIOSpecs IOSpecs_;

    public:
        TPullListMkqlWriter(
            NYql::NPureCalc::TWorkerHolder<NYql::NPureCalc::IPullListWorker> worker,
            bool useSkiff
        )
            : Worker_(std::move(worker))
            , CodecCtx_(MakeCodecCtx(Worker_.Get()))
            , IOSpecs_(GetIOSpecs(Worker_.Get(), CodecCtx_, useSkiff))
        {
        }

    protected:
        const NYql::NPureCalc::IWorker* GetWorker() const override {
            return Worker_.Get();
        }

        void DoRun(const TVector<IOutputStream*>& outputs) override {
            NKikimr::NMiniKQL::TBindTerminator bind(Worker_->GetGraph().GetTerminator());

            with_lock(Worker_->GetScopedAlloc()) {
                NYql::TMkqlWriterImpl writer{outputs, 0, 1ul << 20};
                writer.SetSpecs(IOSpecs_);

                const auto outputIterator = Worker_->GetOutputIterator();

                for (NKikimr::NUdf::TUnboxedValue value; outputIterator.Next(value); writer.AddRow(value))
                    continue;

                writer.Finish();
            }
        }
    };

    class TPullStreamMkqlWriter: public TMkqlWriter {
    private:
        NYql::NPureCalc::TWorkerHolder<NYql::NPureCalc::IPullStreamWorker> Worker_;
        NYql::NCommon::TCodecContext CodecCtx_;
        NYql::TMkqlIOSpecs IOSpecs_;

    public:
        TPullStreamMkqlWriter(
            NYql::NPureCalc::TWorkerHolder<NYql::NPureCalc::IPullStreamWorker> worker,
            bool useSkiff
        )
            : Worker_(std::move(worker))
            , CodecCtx_(MakeCodecCtx(Worker_.Get()))
            , IOSpecs_(GetIOSpecs(Worker_.Get(), CodecCtx_, useSkiff))
        {
        }

    protected:
        const NYql::NPureCalc::IWorker* GetWorker() const override {
            return Worker_.Get();
        }

        void DoRun(const TVector<IOutputStream*>& outputs) override {
            NKikimr::NMiniKQL::TBindTerminator bind(Worker_->GetGraph().GetTerminator());

            with_lock(Worker_->GetScopedAlloc()) {
                NYql::TMkqlWriterImpl writer{outputs, 0, 1ul << 20};
                writer.SetSpecs(IOSpecs_);

                const auto output = Worker_->GetOutput();

                for (NKikimr::NUdf::TUnboxedValue value;;) {
                    const auto status = output.Fetch(value);

                    if (status == NKikimr::NUdf::EFetchStatus::Ok) {
                        writer.AddRow(value);
                    } else if (status == NKikimr::NUdf::EFetchStatus::Finish) {
                        break;
                    } else {
                        YQL_ENSURE(false, "Yield is not supported in pull mode");
                    }
                }

                writer.Finish();
            }
        }
    };
}

namespace NYql {
    namespace NPureCalc {
        template <bool UseSkiff>
        TMkqlInputSpec<UseSkiff>::TMkqlInputSpec(TVector<NYT::TNode> schemas)
            : Schemas_(std::move(schemas))
        {
            AllTableNames_ = TVector<TMaybe<TVector<TString>>>(Schemas_.size(), Nothing());
            this->AllVirtualColumns_ = TVector<THashMap<TString, NYT::TNode>>(Schemas_.size());
        }

        template <bool UseSkiff>
        TMkqlInputSpec<UseSkiff>::TMkqlInputSpec(NYT::TNode schema, bool ignoreStreamTableIndex)
        {
            Schemas_.push_back(std::move(schema));
            IgnoreStreamTableIndex_ = ignoreStreamTableIndex;
            AllTableNames_.push_back(Nothing());
            this->AllVirtualColumns_.push_back({});
        }

        template <bool UseSkiff>
        const TVector<NYT::TNode>& TMkqlInputSpec<UseSkiff>::GetSchemas() const {
            return Schemas_;
        }

        template <bool UseSkiff>
        bool TMkqlInputSpec<UseSkiff>::IgnoreStreamTableIndex() const {
            return IgnoreStreamTableIndex_;
        }

        template <bool UseSkiff>
        bool TMkqlInputSpec<UseSkiff>::IsStrictSchema() const {
            return StrictSchema_;
        }

        template <bool UseSkiff>
        TMkqlInputSpec<UseSkiff>& TMkqlInputSpec<UseSkiff>::SetStrictSchema(bool strictSchema) {
            static const NYT::TNode stringType = NYT::TNode::CreateList().Add("DataType").Add("String");
            static const NYT::TNode othersColumntype = NYT::TNode::CreateList().Add("DictType").Add(stringType).Add(stringType);

            StrictSchema_ = strictSchema;

            for (size_t index = 0; index < Schemas_.size(); ++index) {
                auto& schemaVirtualColumns = this->AllVirtualColumns_.at(index);
                if (StrictSchema_) {
                    schemaVirtualColumns.erase(NYql::YqlOthersColumnName);
                } else {
                    schemaVirtualColumns.emplace(NYql::YqlOthersColumnName, othersColumntype);
                }
            }

            return *this;
        }

        template <bool UseSkiff>
        bool TMkqlInputSpec<UseSkiff>::UseOriginalRowSpec() const {
            return UseOriginalRowSpec_;
        }

        template <bool UseSkiff>
        TMkqlInputSpec<UseSkiff>& TMkqlInputSpec<UseSkiff>::SetUseOriginalRowSpec(bool value) {
            UseOriginalRowSpec_ = value;

            return *this;
        }

        template <bool UseSkiff>
        const TMaybe<TVector<TString>>& TMkqlInputSpec<UseSkiff>::GetTableNames() const {
            Y_ENSURE(AllTableNames_.size() == 1, "expected single-input spec");

            return AllTableNames_[0];
        }

        template <bool UseSkiff>
        const TMaybe<TVector<TString>>& TMkqlInputSpec<UseSkiff>::GetTableNames(ui32 index) const {
            Y_ENSURE(index < AllTableNames_.size(), "invalid input index");

            return AllTableNames_[index];
        }

        template <bool UseSkiff>
        TMkqlInputSpec<UseSkiff>& TMkqlInputSpec<UseSkiff>::SetTableNames(TVector<TString> tableNames) {
            Y_ENSURE(AllTableNames_.size() == 1, "expected single-input spec");

            return SetTableNames(std::move(tableNames), 0);
        }

        template <bool UseSkiff>
        TMkqlInputSpec<UseSkiff>& TMkqlInputSpec<UseSkiff>::SetTableNames(TVector<TString> tableNames, ui32 index) {
            Y_ENSURE(index < AllTableNames_.size(), "invalid input index");

            auto& value = AllTableNames_[index];

            if (!value.Defined()) {
                YQL_ENSURE(NYql::YqlSysColumnPath == NYql::NPureCalc::PurecalcSysColumnTablePath);
                YQL_ENSURE(NYql::GetSysColumnTypeId(PathColumnShortName) == NYql::NUdf::TDataType<char*>::Id);
                this->AllVirtualColumns_.at(index).emplace(
                    NYql::YqlSysColumnPath, NYT::TNode::CreateList().Add("DataType").Add("String")
                );
            }

            value = std::move(tableNames);

            return *this;
        }

        template <bool UseSkiff>
        TMkqlOutputSpec<UseSkiff>::TMkqlOutputSpec(NYT::TNode schema)
            : Schema_(std::move(schema))
        {
        }

        template <bool UseSkiff>
        const NYT::TNode& TMkqlOutputSpec<UseSkiff>::GetSchema() const {
            return Schema_;
        }

        template <bool UseSkiff>
        void TInputSpecTraits<TMkqlInputSpec<UseSkiff>>::PreparePullStreamWorker(
            const TMkqlInputSpec<UseSkiff>& spec,
            IPullStreamWorker* worker,
            const TVector<IInputStream*>& streams
        ) {
            YQL_ENSURE(
                worker->GetInputsCount() == streams.size(),
                "number of input streams should match number of inputs provided by spec");

            TVector<THolder<NYT::TRawTableReader>> wrappers;
            auto& scopedAlloc = worker->GetScopedAlloc();
            for (ui32 i = 0; i < streams.size(); ++i) {
                wrappers.push_back(MakeHolder<TRawTableReaderImpl>(streams[i], scopedAlloc));
            }

            NYql::NPureCalc::TInputSpecTraits<NYql::NPureCalc::TMkqlInputSpec<UseSkiff>>::PreparePullStreamWorker(
                spec,
                worker,
                std::move(wrappers)
            );
        }

        template <bool UseSkiff>
        void TInputSpecTraits<TMkqlInputSpec<UseSkiff>>::PreparePullStreamWorker(
            const TMkqlInputSpec<UseSkiff>& spec,
            IPullStreamWorker* worker,
            IInputStream* stream
        ) {
            TInputSpecTraits<TMkqlInputSpec<UseSkiff>>::PreparePullStreamWorker(
                spec,
                worker,
                TVector<IInputStream*>({stream})
            );
        }

        template <bool UseSkiff>
        void TInputSpecTraits<TMkqlInputSpec<UseSkiff>>::PreparePullStreamWorker(
            const TMkqlInputSpec<UseSkiff>& spec,
            IPullStreamWorker* worker,
            TVector<THolder<IInputStream>>&& streams
        ) {
            YQL_ENSURE(
                worker->GetInputsCount() == streams.size(),
                "number of input streams should match number of inputs provided by spec");

            TVector<THolder<NYT::TRawTableReader>> wrappers;
            auto& scopedAlloc = worker->GetScopedAlloc();
            for (ui32 i = 0; i < streams.size(); ++i) {
                wrappers.push_back(MakeHolder<TRawTableReaderImpl>(std::move(streams[i]), scopedAlloc));
            }

            TInputSpecTraits<NYql::NPureCalc::TMkqlInputSpec<UseSkiff>>::PreparePullStreamWorker(
                spec,
                worker,
                std::move(wrappers)
            );
        }

        template <bool UseSkiff>
        void TInputSpecTraits<TMkqlInputSpec<UseSkiff>>::PreparePullStreamWorker(
            const TMkqlInputSpec<UseSkiff>& spec,
            IPullStreamWorker* worker,
            THolder<IInputStream> stream
        ) {
            TInputSpecTraits<TMkqlInputSpec<UseSkiff>>::PreparePullStreamWorker(
                spec,
                worker,
                VectorFromHolder<IInputStream>(std::move(stream))
            );
        }

        template <bool UseSkiff>
        void TInputSpecTraits<TMkqlInputSpec<UseSkiff>>::PreparePullStreamWorker(
            const TMkqlInputSpec<UseSkiff>& spec,
            IPullStreamWorker* worker,
            const TVector<NYT::TRawTableReader*>& streams
        ) {
            YQL_ENSURE(
                worker->GetInputsCount() == streams.size(),
                "number of input streams should match number of inputs provided by spec");

            with_lock(worker->GetScopedAlloc()) {
                auto& holderFactory = worker->GetGraph().GetHolderFactory();
                for (ui32 i = 0; i < streams.size(); ++i) {
                    TInputDescription inputDescription(spec, i);
                    auto input = holderFactory.Create<TMkqlListValue>(
                        UseSkiff, streams[i], worker, inputDescription, spec.IgnoreStreamTableIndex(), spec.IsStrictSchema()
                    );
                    worker->SetInput(std::move(input), i);
                }
            }
        }

        template <bool UseSkiff>
        void TInputSpecTraits<TMkqlInputSpec<UseSkiff>>::PreparePullStreamWorker(
            const TMkqlInputSpec<UseSkiff>& spec,
            IPullStreamWorker* worker,
            NYT::TRawTableReader* stream
        ) {
            TInputSpecTraits<TMkqlInputSpec<UseSkiff>>::PreparePullStreamWorker(
                spec,
                worker,
                TVector<NYT::TRawTableReader*>({stream})
            );
        }

        template <bool UseSkiff>
        void TInputSpecTraits<TMkqlInputSpec<UseSkiff>>::PreparePullStreamWorker(
            const TMkqlInputSpec<UseSkiff>& spec,
            IPullStreamWorker* worker,
            TVector<THolder<NYT::TRawTableReader>>&& streams
        ) {
            YQL_ENSURE(
                worker->GetInputsCount() == streams.size(),
                "number of input streams should match number of inputs provided by spec");

            with_lock(worker->GetScopedAlloc()) {
                auto& holderFactory = worker->GetGraph().GetHolderFactory();
                for (ui32 i = 0; i < streams.size(); ++i) {
                    TInputDescription inputDescription(spec, i);
                    auto input = holderFactory.Create<TMkqlListValue>(
                        UseSkiff, std::move(streams[i]), worker, inputDescription, spec.IgnoreStreamTableIndex(), spec.IsStrictSchema()
                    );
                    worker->SetInput(std::move(input), i);
                }
            }
        }

        template <bool UseSkiff>
        void TInputSpecTraits<TMkqlInputSpec<UseSkiff>>::PreparePullStreamWorker(
            const TMkqlInputSpec<UseSkiff>& spec,
            IPullStreamWorker* worker,
            THolder<NYT::TRawTableReader> stream
        ) {
            TInputSpecTraits<TMkqlInputSpec<UseSkiff>>::PreparePullStreamWorker(
                spec,
                worker,
                VectorFromHolder<NYT::TRawTableReader>(std::move(stream))
            );
        }

        template <bool UseSkiff>
        void TInputSpecTraits<TMkqlInputSpec<UseSkiff>>::PreparePullListWorker(
            const TMkqlInputSpec<UseSkiff>& spec,
            IPullListWorker* worker,
            const TVector<IInputStream*>& streams
        ) {
            YQL_ENSURE(
                worker->GetInputsCount() == streams.size(),
                "number of input streams should match number of inputs provided by spec");

            TVector<THolder<NYT::TRawTableReader>> wrappers;
            auto& scopedAlloc = worker->GetScopedAlloc();
            for (ui32 i = 0; i < streams.size(); ++i) {
                wrappers.push_back(MakeHolder<TRawTableReaderImpl>(streams[i], scopedAlloc));
            }

            NYql::NPureCalc::TInputSpecTraits<NYql::NPureCalc::TMkqlInputSpec<UseSkiff>>::PreparePullListWorker(
                spec,
                worker,
                std::move(wrappers)
            );
        }

        template <bool UseSkiff>
        void TInputSpecTraits<TMkqlInputSpec<UseSkiff>>::PreparePullListWorker(
            const TMkqlInputSpec<UseSkiff>& spec,
            IPullListWorker* worker,
            IInputStream* stream
        ) {
            TInputSpecTraits<TMkqlInputSpec<UseSkiff>>::PreparePullListWorker(
                spec,
                worker,
                TVector<IInputStream*>({stream})
            );
        }

        template <bool UseSkiff>
        void TInputSpecTraits<TMkqlInputSpec<UseSkiff>>::PreparePullListWorker(
            const TMkqlInputSpec<UseSkiff>& spec,
            IPullListWorker* worker,
            TVector<THolder<IInputStream>>&& streams
        ) {
            YQL_ENSURE(
                worker->GetInputsCount() == streams.size(),
                "number of input streams should match number of inputs provided by spec");

            TVector<THolder<NYT::TRawTableReader>> wrappers;
            auto& scopedAlloc = worker->GetScopedAlloc();
            for (ui32 i = 0; i < streams.size(); ++i) {
                wrappers.push_back(MakeHolder<TRawTableReaderImpl>(std::move(streams[i]), scopedAlloc));
            }

            NYql::NPureCalc::TInputSpecTraits<NYql::NPureCalc::TMkqlInputSpec<UseSkiff>>::PreparePullListWorker(
                spec,
                worker,
                std::move(wrappers)
            );
        }

        template <bool UseSkiff>
        void TInputSpecTraits<TMkqlInputSpec<UseSkiff>>::PreparePullListWorker(
            const TMkqlInputSpec<UseSkiff>& spec,
            IPullListWorker* worker,
            THolder<IInputStream> stream
        ) {
            TInputSpecTraits<TMkqlInputSpec<UseSkiff>>::PreparePullListWorker(
                spec,
                worker,
                VectorFromHolder<IInputStream>(std::move(stream))
            );
        }

        template <bool UseSkiff>
        void TInputSpecTraits<TMkqlInputSpec<UseSkiff>>::PreparePullListWorker(
            const TMkqlInputSpec<UseSkiff>& spec,
            IPullListWorker* worker,
            const TVector<NYT::TRawTableReader*>& streams
        ) {
            YQL_ENSURE(
                worker->GetInputsCount() == streams.size(),
                "number of input streams should match number of inputs provided by spec");

            with_lock(worker->GetScopedAlloc()) {
                auto& holderFactory = worker->GetGraph().GetHolderFactory();
                for (ui32 i = 0; i < streams.size(); ++i) {
                    TInputDescription inputDescription(spec, i);
                    auto input = holderFactory.Create<TMkqlListValue>(
                        UseSkiff, streams[i], worker, inputDescription, spec.IgnoreStreamTableIndex(), spec.IsStrictSchema()
                    );
                    worker->SetInput(std::move(input), i);
                }
            }
        }

        template <bool UseSkiff>
        void TInputSpecTraits<TMkqlInputSpec<UseSkiff>>::PreparePullListWorker(
            const TMkqlInputSpec<UseSkiff>& spec,
            IPullListWorker* worker,
            NYT::TRawTableReader* stream
        ) {
            TInputSpecTraits<TMkqlInputSpec<UseSkiff>>::PreparePullListWorker(
                spec,
                worker,
                TVector<NYT::TRawTableReader*>({stream})
            );
        }

        template <bool UseSkiff>
        void TInputSpecTraits<TMkqlInputSpec<UseSkiff>>::PreparePullListWorker(
            const TMkqlInputSpec<UseSkiff>& spec,
            IPullListWorker* worker,
            TVector<THolder<NYT::TRawTableReader>>&& streams
        ) {
            YQL_ENSURE(
                worker->GetInputsCount() == streams.size(),
                "number of input streams should match number of inputs provided by spec");

            with_lock(worker->GetScopedAlloc()) {
                auto& holderFactory = worker->GetGraph().GetHolderFactory();
                for (ui32 i = 0; i < streams.size(); ++i) {
                    TInputDescription inputDescription(spec, i);
                    auto input = holderFactory.Create<TMkqlListValue>(
                        UseSkiff, std::move(streams[i]), worker, inputDescription, spec.IgnoreStreamTableIndex(), spec.IsStrictSchema()
                    );
                    worker->SetInput(std::move(input), i);
                }
            }
        }

        template <bool UseSkiff>
        void TInputSpecTraits<TMkqlInputSpec<UseSkiff>>::PreparePullListWorker(
            const TMkqlInputSpec<UseSkiff>& spec,
            IPullListWorker* worker,
            THolder<NYT::TRawTableReader> stream
        ) {
            TInputSpecTraits<TMkqlInputSpec<UseSkiff>>::PreparePullListWorker(
                spec,
                worker,
                VectorFromHolder<NYT::TRawTableReader>(std::move(stream))
            );
        }

        template <bool UseSkiff>
        THolder<THandle> TOutputSpecTraits<TMkqlOutputSpec<UseSkiff>>::ConvertPullListWorkerToOutputType(
            const NYql::NPureCalc::TMkqlOutputSpec<UseSkiff>&,
            NYql::NPureCalc::TWorkerHolder<NYql::NPureCalc::IPullListWorker> worker
        ) {
            with_lock(worker->GetScopedAlloc()) {
                return MakeHolder<TPullListMkqlWriter>(std::move(worker), UseSkiff);
            }
        }

        template <bool UseSkiff>
        THolder<THandle> TOutputSpecTraits<TMkqlOutputSpec<UseSkiff>>::ConvertPullStreamWorkerToOutputType(
            const NYql::NPureCalc::TMkqlOutputSpec<UseSkiff>&,
            NYql::NPureCalc::TWorkerHolder<NYql::NPureCalc::IPullStreamWorker> worker
        ) {
            with_lock(worker->GetScopedAlloc()) {
                return MakeHolder<TPullStreamMkqlWriter>(std::move(worker), UseSkiff);
            }
        }

        template class TMkqlSpec<true, TInputSpecBase>;
        template class TMkqlSpec<false, TInputSpecBase>;
        template class TMkqlSpec<true, TOutputSpecBase>;
        template class TMkqlSpec<false, TOutputSpecBase>;

        template class TMkqlInputSpec<true>;
        template class TMkqlInputSpec<false>;
        template class TMkqlOutputSpec<true>;
        template class TMkqlOutputSpec<false>;

        template struct TInputSpecTraits<TMkqlInputSpec<true>>;
        template struct TInputSpecTraits<TMkqlInputSpec<false>>;
        template struct TOutputSpecTraits<TMkqlOutputSpec<true>>;
        template struct TOutputSpecTraits<TMkqlOutputSpec<false>>;
    }
}
