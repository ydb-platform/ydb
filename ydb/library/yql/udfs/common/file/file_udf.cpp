#include <ydb/library/yql/public/udf/udf_helpers.h>

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
            : FilePath(filePath)
        {
            // work in greedy mode to catch error on creation
            Cached = DoCreateStream();
        }

        std::unique_ptr<TStream> CreateStream(TTerminateFunc terminateFunc) {
            if (Cached) {
                return std::move(Cached);
            }

            terminateFunc("The file iterator was already created. To scan file data multiple times please use ListCollect either over ParseFile or over some lazy function over it, e.g. ListMap");
            Y_ABORT("Terminate unstoppable!");
        }

        bool GetLinesCount(ui64& count) const {
            if (LinesCount == Unknown)
                return false;
            count = LinesCount;
            return true;
        }
        void SetLinesCount(ui64 count) {
            Y_DEBUG_ABORT_UNLESS(LinesCount == Unknown || count == LinesCount, "Set another value of count lines");
            if (LinesCount == Unknown) {
                LinesCount = count;
            }
        }

        const TString& GetFilePath() const {
            return FilePath;
        }

    private:
        std::unique_ptr<TStream> DoCreateStream() {
            static const auto bufferSize = 1 << 12;
            TFile file(FilePath, OpenExisting | RdOnly | Seq);
            if (FileSize == Unknown) {
                FileSize = file.GetLength();
            }
            return std::make_unique<TBuffered<TUnbufferedFileInput>>(bufferSize, file);
        }

        TString FilePath;
        static const ui64 Unknown = -1;
        ui64 FileSize = Unknown;
        ui64 LinesCount = Unknown;
        std::unique_ptr<TStream> Cached;
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
            : TerminateFunc(terminateFunc)
        {
        }

    private:
        const TTerminateFunc TerminateFunc;
    };

    class TLineSplitter {
    public:
        TLineSplitter(IInputStream& stream)
            : Stream_(stream)
        {
        }

        size_t Next(TString& st) {
            st.clear();
            char c;
            size_t ret = 0;
            if (HasPendingLineChar_) {
                st.push_back(PendingLineChar_);
                HasPendingLineChar_ = false;
                ++ret;
            }

            while (Stream_.ReadChar(c)) {
                ++ret;
                if (c == '\n') {
                    break;
                } else if (c == '\r') {
                    if (Stream_.ReadChar(c)) {
                        ++ret;
                        if (c != '\n') {
                            --ret;
                            PendingLineChar_ = c;
                            HasPendingLineChar_ = true;
                        }
                    }

                    break;
                } else {
                    st.push_back(c);
                }
            }

            return ret;
        }

    private:
        IInputStream& Stream_;
        bool HasPendingLineChar_ = false;
        char PendingLineChar_ = 0;
    };

    template <class TUserType>
    class TLineByLineBoxedValueIterator: public TBoxedValue {
    public:
        TLineByLineBoxedValueIterator(TStreamMeta::TPtr metaPtr, std::unique_ptr<TStreamMeta::TStream>&& stream, const IValueBuilder& valueBuilder, TTerminateFunc terminateFunc)
            : MetaPtr(metaPtr)
            , ValueBuilder(valueBuilder)
            , Stream(std::move(stream))
            , Splitter(*Stream)
            , TerminateFunc(terminateFunc)
        {
        }

        void SetLimit(ui64 limit = TAKE_UNLIM) {
            Limit = limit;
        }

    private:
        bool SkipLimit() {
            if (Limit != TAKE_UNLIM) {
                if (Limit == 0) {
                    return false;
                }
                --Limit;
            }
            return true;
        }

        bool Skip() final {
            ++CurLineNum;
            return Splitter.Next(CurLine) && SkipLimit();
        }

        bool Next(TUnboxedValue& value) override {
            if (!Skip()) {
                return false;
            }
            if (!Helper::ConvertToUnboxed<TUserType>(ValueBuilder, CurLine, value)) {
                TStringBuilder sb;
                sb << "File::ByLines failed to cast string '" << CurLine << "' to " << Helper::TypeToTypeName<TUserType>::Name() << Endl;
                sb << "- path: " << MetaPtr->GetFilePath() << Endl;
                sb << "- line: " << CurLineNum << Endl;
                TerminateFunc(sb);
                Y_ABORT("Terminate unstoppable!");
            }
            return true;
        }

        TStreamMeta::TPtr MetaPtr;
        const IValueBuilder& ValueBuilder;

        std::unique_ptr<TStreamMeta::TStream> Stream;
        TLineSplitter Splitter;
        TTerminateFunc TerminateFunc;
        TString CurLine;
        ui64 CurLineNum = 0;
        ui64 Limit = TAKE_UNLIM;
        TUnboxedValue Result;
    };

    template <class TUserType>
    class TListByLineBoxedValue: public TBoxedValue {
    public:
        TListByLineBoxedValue(TStreamMeta::TPtr metaPtr, const IValueBuilder& valueBuilder, TTerminateFunc terminateFunc, ui64 skip = 0ULL, ui64 take = TAKE_UNLIM)
            : MetaPtr(metaPtr)
            , ValueBuilder(valueBuilder)
            , TerminateFunc(terminateFunc)
            , Skip(skip)
            , Take(take)
        {}
    private:
        bool HasFastListLength() const override {
            ui64 tmp;
            return MetaPtr->GetLinesCount(tmp);
        }
        ui64 GetListLength() const override {
            ui64 length;
            if (!MetaPtr->GetLinesCount(length)) {
                length = Skip;
                for (const auto iter = GetListIterator(); iter.Skip(); ++length)
                    continue;
                if (Take == TAKE_UNLIM) {
                    MetaPtr->SetLinesCount(length);
                }
            }
            if (length <= Skip) {
                return 0;
            }
            return Min(length - Skip, Take);
        }
        ui64 GetEstimatedListLength() const override {
            /// \todo some optimisation?
            return GetListLength();
        }

        TUnboxedValue GetListIterator() const override {
            try {
                auto stream = MetaPtr->CreateStream(TerminateFunc);
                IBoxedValuePtr iter(new TLineByLineBoxedValueIterator<TUserType>(MetaPtr, std::move(stream), ValueBuilder, TerminateFunc));
                if (!Take || !SkipElements(*iter, Skip)) {
                    return TUnboxedValuePod(new TEmptyIter(TerminateFunc));
                }
                static_cast<TLineByLineBoxedValueIterator<TUserType>*>(iter.Get())->SetLimit(Take);
                return TUnboxedValuePod(std::move(iter));
            } catch (const std::exception& e) {
                TerminateFunc(CurrentExceptionMessage());
                Y_ABORT("Terminate unstoppable!");
            }
        }

        IBoxedValuePtr SkipListImpl(const IValueBuilder& builder, ui64 count) const override {
            return new TListByLineBoxedValue(MetaPtr, builder, TerminateFunc, Skip + count, Take == TAKE_UNLIM ? TAKE_UNLIM : Take - std::min(Take, count));
        }
        IBoxedValuePtr TakeListImpl(const IValueBuilder& builder, ui64 count) const override {
            return new TListByLineBoxedValue(MetaPtr, builder, TerminateFunc, Skip, std::min(Take, count));
        }

        bool HasListItems() const override {
            return true;
        }

        TStreamMeta::TPtr MetaPtr;
        const IValueBuilder& ValueBuilder;
        TTerminateFunc TerminateFunc;
        ui64 Skip = 0ULL;
        ui64 Take = TAKE_UNLIM;
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
                    UdfTerminate((TStringBuilder() << pos << " " << message).data());
                };
                return TUnboxedValuePod(new TListByLineBoxedValue<TUserType>(metaPtr, *valueBuilder, terminateFunc));
            } catch (const std::exception& e) {
                UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
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
                    UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
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
                return bool(Length);
            }

            ui64 GetListLength() const override {
                if (!Length) {
                    ui64 length = 0ULL;
                    for (const auto it = GetListIterator(); it.Skip();) {
                        ++length;
                    }

                    Length = length;
                }

                return *Length;
            }

            ui64 GetEstimatedListLength() const override {
                return GetListLength();
            }

            bool HasListItems() const override {
                if (HasItems) {
                    return *HasItems;
                }

                if (Length) {
                    HasItems = (*Length != 0);
                    return *HasItems;
                }

                auto iter = GetListIterator();
                HasItems = iter.Skip();
                return *HasItems;
            }

        protected:
            const ui32 IndexP_;
            const ui32 IndexT_;
            const ui32 IndexA_;
            const IValueBuilder& ValueBuilder_;
            const TSourcePosition Pos_;
            const TString FilePath_;
            mutable TMaybe<ui64> Length;
            mutable TMaybe<bool> HasItems;
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
                UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
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
