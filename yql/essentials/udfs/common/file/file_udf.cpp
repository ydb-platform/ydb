#include <yql/essentials/public/udf/udf_helpers.h>
#include <yql/essentials/utils/line_split.h>

#include <util/generic/yexception.h>
#include <util/stream/buffered.h>
#include <util/stream/file.h>
#include <util/string/cast.h>
#include <util/ysaveload.h>

#include <functional>

using namespace NKikimr;
using namespace NUdf;

extern const char ByLineFuncName[];
const char ByLineFuncName[] = "ByLines";

namespace {
    namespace Helper {
        template <class TUserType>
        inline bool ConvertToUnboxed(const IValueBuilder& valueBuilder, const TString& curLine, TUnboxedValue& result) {
            Y_UNUSED(valueBuilder);
            TUserType userType;
            if (!TryFromString<TUserType>(curLine, userType)) {
                return false;
            }
            result = TUnboxedValuePod(userType);
            return true;
        }

        template <>
        inline bool ConvertToUnboxed<const char*>(const IValueBuilder& valueBuilder, const TString& curLine, TUnboxedValue& result) {
            result = valueBuilder.NewString(curLine);
            return true;
        }

        template <>
        inline bool ConvertToUnboxed<TUtf8>(const IValueBuilder& valueBuilder, const TString& curLine, TUnboxedValue& result) {
            result = valueBuilder.NewString(curLine);
            return true;
        }

        template <>
        inline bool ConvertToUnboxed<TYson>(const IValueBuilder& valueBuilder, const TString& curLine, TUnboxedValue& result) {
            result = valueBuilder.NewString(curLine);
            return true;
        }

        template <>
        inline bool ConvertToUnboxed<TJson>(const IValueBuilder& valueBuilder, const TString& curLine, TUnboxedValue& result) {
            result = valueBuilder.NewString(curLine);
            return true;
        }

        template <typename T>
        struct TypeToTypeName {
            static const char* Name() {
                return "Unknown";
            }
        };
        template <>
        struct TypeToTypeName<bool> {
            static constexpr const char* Name() {
                return "Bool";
            }
        };
        template <>
        struct TypeToTypeName<i8> {
            static constexpr const char* Name() {
                return "Int8";
            }
        };
        template <>
        struct TypeToTypeName<ui8> {
            static constexpr const char* Name() {
                return "Uint8";
            }
        };
        template <>
        struct TypeToTypeName<i16> {
            static constexpr const char* Name() {
                return "Int16";
            }
        };
        template <>
        struct TypeToTypeName<ui16> {
            static constexpr const char* Name() {
                return "Uint16";
            }
        };
        template <>
        struct TypeToTypeName<ui32> {
            static constexpr const char* Name() {
                return "Uint32";
            }
        };
        template <>
        struct TypeToTypeName<ui64> {
            static constexpr const char* Name() {
                return "Uint64";
            }
        };
        template <>
        struct TypeToTypeName<i32> {
            static constexpr const char* Name() {
                return "Int32";
            }
        };
        template <>
        struct TypeToTypeName<i64> {
            static constexpr const char* Name() {
                return "Int64";
            }
        };
        template <>
        struct TypeToTypeName<float> {
            static constexpr const char* Name() {
                return "Float";
            }
        };
        template <>
        struct TypeToTypeName<double> {
            static constexpr const char* Name() {
                return "Double";
            }
        };
        template <>
        struct TypeToTypeName<const char*> {
            static constexpr const char* Name() {
                return "String";
            }
        };
        template <>
        struct TypeToTypeName<TUtf8> {
            static constexpr const char* Name() {
                return "Utf8";
            }
        };
        template <>
        struct TypeToTypeName<TYson> {
            static constexpr const char* Name() {
                return "Yson";
            }
        };
        template <>
        struct TypeToTypeName<TJson> {
            static constexpr const char* Name() {
                return "Json";
            }
        };
    }

    static const ui64 TAKE_UNLIM = -1;

    bool SkipElements(IBoxedValue& iter, ui64 skip) {
        for (; skip > 0; --skip) {
            if (!TBoxedValueAccessor::Skip(iter)) {
                return false;
            }
        }
        return true;
    }

    typedef std::function<void(const TString& message)> TTerminateFunc;

    class TStreamMeta: public TThrRefBase {
    public:
        typedef TBuffered<TUnbufferedFileInput> TStream;
        typedef TIntrusivePtr<TStreamMeta> TPtr;

        TStreamMeta(TString filePath)
            : FilePath_(filePath)
        {
            // work in greedy mode to catch error on creation
            Cached_ = DoCreateStream();
        }

        std::unique_ptr<TStream> CreateStream(TTerminateFunc terminateFunc) {
            if (Cached_) {
                return std::move(Cached_);
            }

            terminateFunc("The file iterator was already created. To scan file data multiple times please use ListCollect either over ParseFile or over some lazy function over it, e.g. ListMap");
            Y_ABORT("Terminate unstoppable!");
        }

        bool GetLinesCount(ui64& count) const {
            if (LinesCount_ == Unknown)
                return false;
            count = LinesCount_;
            return true;
        }
        void SetLinesCount(ui64 count) {
            Y_DEBUG_ABORT_UNLESS(LinesCount_ == Unknown || count == LinesCount_, "Set another value of count lines");
            if (LinesCount_ == Unknown) {
                LinesCount_ = count;
            }
        }

        const TString& GetFilePath() const {
            return FilePath_;
        }

    private:
        std::unique_ptr<TStream> DoCreateStream() {
            static const auto bufferSize = 1 << 12;
            TFile file(FilePath_, OpenExisting | RdOnly | Seq);
            if (FileSize_ == Unknown) {
                FileSize_ = file.GetLength();
            }
            return std::make_unique<TBuffered<TUnbufferedFileInput>>(bufferSize, file);
        }

        TString FilePath_;
        static const ui64 Unknown = -1;
        ui64 FileSize_ = Unknown;
        ui64 LinesCount_ = Unknown;
        std::unique_ptr<TStream> Cached_;
    };

    class TEmptyIter: public TBoxedValue {
    private:
        bool Skip() override {
            return false;
        }
        bool Next(TUnboxedValue&) override {
            return false;
        }

    public:
        TEmptyIter(TTerminateFunc terminateFunc)
            : TerminateFunc_(terminateFunc)
        {
        }

    private:
        const TTerminateFunc TerminateFunc_;
    };

    template <class TUserType>
    class TLineByLineBoxedValueIterator: public TBoxedValue {
    public:
        TLineByLineBoxedValueIterator(TStreamMeta::TPtr metaPtr, std::unique_ptr<TStreamMeta::TStream>&& stream, const IValueBuilder& valueBuilder, TTerminateFunc terminateFunc)
            : MetaPtr_(metaPtr)
            , ValueBuilder_(valueBuilder)
            , Stream_(std::move(stream))
            , Splitter_(*Stream_)
            , TerminateFunc_(terminateFunc)
        {
        }

        void SetLimit(ui64 limit = TAKE_UNLIM) {
            Limit_ = limit;
        }

    private:
        bool SkipLimit() {
            if (Limit_ != TAKE_UNLIM) {
                if (Limit_ == 0) {
                    return false;
                }
                --Limit_;
            }
            return true;
        }

        bool Skip() final {
            ++CurLineNum_;
            return Splitter_.Next(CurLine_) && SkipLimit();
        }

        bool Next(TUnboxedValue& value) override {
            if (!Skip()) {
                return false;
            }
            if (!Helper::ConvertToUnboxed<TUserType>(ValueBuilder_, CurLine_, value)) {
                TStringBuilder sb;
                sb << "File::ByLines failed to cast string '" << CurLine_ << "' to " << Helper::TypeToTypeName<TUserType>::Name() << Endl;
                sb << "- path: " << MetaPtr_->GetFilePath() << Endl;
                sb << "- line: " << CurLineNum_ << Endl;
                TerminateFunc_(sb);
                Y_ABORT("Terminate unstoppable!");
            }
            return true;
        }

        TStreamMeta::TPtr MetaPtr_;
        const IValueBuilder& ValueBuilder_;

        std::unique_ptr<TStreamMeta::TStream> Stream_;
        TLineSplitter Splitter_;
        TTerminateFunc TerminateFunc_;
        TString CurLine_;
        ui64 CurLineNum_ = 0;
        ui64 Limit_ = TAKE_UNLIM;
        TUnboxedValue Result_;
    };

    template <class TUserType>
    class TListByLineBoxedValue: public TBoxedValue {
    public:
        TListByLineBoxedValue(TStreamMeta::TPtr metaPtr, const IValueBuilder& valueBuilder, TTerminateFunc terminateFunc, ui64 skip = 0ULL, ui64 take = TAKE_UNLIM)
            : MetaPtr_(metaPtr)
            , ValueBuilder_(valueBuilder)
            , TerminateFunc_(terminateFunc)
            , Skip_(skip)
            , Take_(take)
        {}
    private:
        bool HasFastListLength() const override {
            ui64 tmp;
            return MetaPtr_->GetLinesCount(tmp);
        }
        ui64 GetListLength() const override {
            ui64 length;
            if (!MetaPtr_->GetLinesCount(length)) {
                length = Skip_;
                for (const auto iter = GetListIterator(); iter.Skip(); ++length)
                    continue;
                if (Take_ == TAKE_UNLIM) {
                    MetaPtr_->SetLinesCount(length);
                }
            }
            if (length <= Skip_) {
                return 0;
            }
            return Min(length - Skip_, Take_);
        }
        ui64 GetEstimatedListLength() const override {
            /// \todo some optimisation?
            return GetListLength();
        }

        TUnboxedValue GetListIterator() const override {
            try {
                auto stream = MetaPtr_->CreateStream(TerminateFunc_);
                IBoxedValuePtr iter(new TLineByLineBoxedValueIterator<TUserType>(MetaPtr_, std::move(stream), ValueBuilder_, TerminateFunc_));
                if (!Take_ || !SkipElements(*iter, Skip_)) {
                    return TUnboxedValuePod(new TEmptyIter(TerminateFunc_));
                }
                static_cast<TLineByLineBoxedValueIterator<TUserType>*>(iter.Get())->SetLimit(Take_);
                return TUnboxedValuePod(std::move(iter));
            } catch (const std::exception& e) {
                TerminateFunc_(CurrentExceptionMessage());
                Y_ABORT("Terminate unstoppable!");
            }
        }

        IBoxedValuePtr SkipListImpl(const IValueBuilder& builder, ui64 count) const override {
            return new TListByLineBoxedValue(MetaPtr_, builder, TerminateFunc_, Skip_ + count, Take_ == TAKE_UNLIM ? TAKE_UNLIM : Take_ - std::min(Take_, count));
        }
        IBoxedValuePtr TakeListImpl(const IValueBuilder& builder, ui64 count) const override {
            return new TListByLineBoxedValue(MetaPtr_, builder, TerminateFunc_, Skip_, std::min(Take_, count));
        }

        bool HasListItems() const override {
            return true;
        }

        TStreamMeta::TPtr MetaPtr_;
        const IValueBuilder& ValueBuilder_;
        TTerminateFunc TerminateFunc_;
        ui64 Skip_ = 0ULL;
        ui64 Take_ = TAKE_UNLIM;
    };

    template <class TUserType>
    class TByLinesFunc: public TBoxedValue {
    private:
        TSourcePosition Pos_;

        TByLinesFunc(TSourcePosition pos)
            : Pos_(pos)
        {}

        TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const override {
            try {
                TString filePath(args[0].AsStringRef());
                TStreamMeta::TPtr metaPtr(new TStreamMeta(filePath));
                auto pos = Pos_;
                auto terminateFunc = [pos](const TString& message) {
                    UdfTerminate((TStringBuilder() << pos << " " << message).c_str());
                };
                return TUnboxedValuePod(new TListByLineBoxedValue<TUserType>(metaPtr, *valueBuilder, terminateFunc));
            } catch (const std::exception& e) {
                UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).c_str());
            }
        }

    public:
        static void DeclareSignature(
            TStringRef name,
            TType* userType,
            IFunctionTypeInfoBuilder& builder,
            bool typesOnly)
        {
            Y_UNUSED(name);
            builder.UserType(userType);
            builder.SimpleSignature<TListType<TUserType>(char*)>();
            if (!typesOnly) {
                builder.Implementation(new TByLinesFunc<TUserType>(builder.GetSourcePosition()));
            }
        }
    };

    class TFolderListFromFile: public TBoxedValue {
    private:
        class TIterator : public TBoxedValue {
        public:
            TIterator(ui32 indexP, ui32 indexT, ui32 indexA, const IValueBuilder& valueBuilder, const TSourcePosition& pos, TString filePath)
                : IndexP_(indexP)
                , IndexT_(indexT)
                , IndexA_(indexA)
                , ValueBuilder_(valueBuilder)
                , Pos_(pos)
                , Input_(filePath)
            {
            }

        private:
            bool Next(NUdf::TUnboxedValue& value) override {
                try {
                    TString type;
                    TString path;
                    TString attrs;
                    ::Load(&Input_, type);
                    if (!type) {
                        return false;
                    }
                    ::Load(&Input_, path);
                    ::Load(&Input_, attrs);

                    NUdf::TUnboxedValue* items = nullptr;
                    value = ValueBuilder_.NewArray(3, items);
                    items[IndexT_] = ValueBuilder_.NewString(type);
                    items[IndexP_] = ValueBuilder_.NewString(path);
                    items[IndexA_] = ValueBuilder_.NewString(attrs);
                }
                catch (const std::exception& e) {
                    UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).c_str());
                }
                return true;
            }

        private:
            const ui32 IndexP_;
            const ui32 IndexT_;
            const ui32 IndexA_;
            const IValueBuilder& ValueBuilder_;
            const TSourcePosition Pos_;
            TIFStream Input_;
        };

        class TList: public TBoxedValue {
        public:
            TList(ui32 indexP, ui32 indexT, ui32 indexA, const IValueBuilder& valueBuilder, const TSourcePosition& pos, TString filePath)
                : IndexP_(indexP)
                , IndexT_(indexT)
                , IndexA_(indexA)
                , ValueBuilder_(valueBuilder)
                , Pos_(pos)
                , FilePath_(std::move(filePath))
            {
            }

        protected:
            NUdf::TUnboxedValue GetListIterator() const override {
                return NUdf::TUnboxedValuePod(new TIterator(IndexP_, IndexT_, IndexA_, ValueBuilder_, Pos_, FilePath_));
            }

            bool HasFastListLength() const override {
                return bool(Length_);
            }

            ui64 GetListLength() const override {
                if (!Length_) {
                    ui64 length = 0ULL;
                    for (const auto it = GetListIterator(); it.Skip();) {
                        ++length;
                    }

                    Length_ = length;
                }

                return *Length_;
            }

            ui64 GetEstimatedListLength() const override {
                return GetListLength();
            }

            bool HasListItems() const override {
                if (HasItems_) {
                    return *HasItems_;
                }

                if (Length_) {
                    HasItems_ = (*Length_ != 0);
                    return *HasItems_;
                }

                auto iter = GetListIterator();
                HasItems_ = iter.Skip();
                return *HasItems_;
            }

        protected:
            const ui32 IndexP_;
            const ui32 IndexT_;
            const ui32 IndexA_;
            const IValueBuilder& ValueBuilder_;
            const TSourcePosition Pos_;
            const TString FilePath_;
            mutable TMaybe<ui64> Length_;
            mutable TMaybe<bool> HasItems_;
        };

    public:
        TFolderListFromFile(ui32 indexP, ui32 indexT, ui32 indexA, const TSourcePosition& pos)
            : IndexP_(indexP)
            , IndexT_(indexT)
            , IndexA_(indexA)
            , Pos_(pos)
        {
        }

        static const ::NYql::NUdf::TStringRef& Name() {
            static auto name = ::NYql::NUdf::TStringRef::Of("FolderListFromFile");
            return name;
        }

        TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const override {
            try {
                TString filePath(args[0].AsStringRef());
                return TUnboxedValuePod(new TList(IndexP_, IndexT_, IndexA_, *valueBuilder, Pos_, filePath));
            } catch (const std::exception& e) {
                UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).c_str());
            }
        }

        static bool DeclareSignature(const TStringRef& name, TType* userType, IFunctionTypeInfoBuilder& builder, bool typesOnly) {
            if (Name() != name) {
                // the only case when we return false
                return false;
            }

            builder.UserType(userType);

            ui32 indexP, indexT, indexA;
            auto itemType = builder.Struct()
                ->AddField<const char*>("Path", &indexP)
                .AddField<const char*>("Type", &indexT)
                .AddField<TYson>("Attributes", &indexA)
                .Build();
            auto resultType = builder.List()->Item(itemType).Build();

            builder.Args()->Add<const char*>().Done().Returns(resultType);
            if (!typesOnly) {
                builder.Implementation(new TFolderListFromFile(indexP, indexT, indexA, builder.GetSourcePosition()));
            }
            return true;
        }

    private:
        const ui32 IndexP_;
        const ui32 IndexT_;
        const ui32 IndexA_;
        const TSourcePosition Pos_;
    };

    SIMPLE_MODULE(TFileModule,
        TUserDataTypeFuncFactory<false, false, ByLineFuncName, TByLinesFunc, const char*, TUtf8, TYson, TJson, i8, ui8, i16, ui16, ui32, ui64, i32, i64, float, double, bool>,
        TFolderListFromFile
    )

}

REGISTER_MODULES(TFileModule)
