#include <yql/essentials/public/udf/udf_value.h>
#include <yql/essentials/public/udf/udf_registrator.h>
#include <yql/essentials/public/udf/udf_type_builder.h>
#include <yql/essentials/public/udf/udf_value_builder.h>
#include <yql/essentials/public/udf/udf_terminator.h>

#include <util/generic/buffer.h>
#include <util/generic/mem_copy.h>
#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/string/builder.h>
#include <util/stream/mem.h>
#include <library/cpp/deprecated/kmp/kmp.h>
#include <util/string/strip.h>
#include <util/system/condvar.h>
#include <util/system/shellcommand.h>
#include <util/system/tempfile.h>
#include <util/system/sysstat.h>

#include <functional>

using namespace NKikimr;
using namespace NUdf;

namespace {
    // Cyclic Read-Write buffer.
    // Not thread safe, synchronization between reader and writer threads
    // should be managed externally.
    class TCyclicRWBuffer {
    public:
        TCyclicRWBuffer(size_t capacity)
            : Buffer_(capacity)
            , Finished_(false)
            , DataStart_(0)
            , DataSize_(0)
        {
            Buffer_.Resize(capacity);
        }

        bool IsFinished() const {
            return Finished_;
        }

        void Finish() {
            Finished_ = true;
        }

        bool HasData() const {
            return DataSize_ > 0;
        }

        size_t GetDataSize() const {
            return DataSize_;
        }

        void GetData(const char*& ptr, size_t& len) const {
            size_t readSize = GetDataRegionSize(DataStart_, DataSize_);
            ptr = Buffer_.Data() + DataStart_;
            len = readSize;
        }

        void CommitRead(size_t len) {
            Y_DEBUG_ABORT_UNLESS(len <= GetDataRegionSize(DataStart_, DataSize_));

            DataStart_ = GetBufferPosition(DataStart_ + len);
            DataSize_ -= len;
        }

        bool CanWrite() const {
            return WriteSize() > 0;
        }

        size_t WriteSize() const {
            return Buffer_.Size() - DataSize_;
        }

        size_t Write(const char*& ptr, size_t& len) {
            if (!CanWrite()) {
                return 0;
            }

            size_t bytesWritten = 0;
            size_t bytesToWrite = std::min(len, WriteSize());
            while (bytesToWrite > 0) {
                size_t writeStart = GetWriteStart();
                size_t writeSize = GetDataRegionSize(writeStart, bytesToWrite);

                MemCopy(Data(writeStart), ptr, writeSize);

                DataSize_ += writeSize;
                bytesWritten += writeSize;
                bytesToWrite -= writeSize;

                ptr += writeSize;
                len -= writeSize;
            }

            return bytesWritten;
        }

        size_t Write(IZeroCopyInput& input) {
            const void* ptr;
            size_t dataLen = input.Next(&ptr, WriteSize());
            const char* dataPtr = reinterpret_cast<const char*>(ptr);
            return Write(dataPtr, dataLen);
        }

    private:
        size_t GetBufferPosition(size_t pos) const {
            return pos % Buffer_.Size();
        }

        size_t GetDataRegionSize(size_t start, size_t size) const {
            Y_DEBUG_ABORT_UNLESS(start < Buffer_.Size());

            return std::min(size, Buffer_.Size() - start);
        }

        size_t GetWriteStart() const {
            return GetBufferPosition(DataStart_ + DataSize_);
        }

        char* Data(size_t pos) {
            Y_DEBUG_ABORT_UNLESS(pos < Buffer_.Size());

            return (Buffer_.Data() + pos);
        }

    private:
        TBuffer Buffer_;

        bool Finished_;

        size_t DataStart_;
        size_t DataSize_;
    };

    struct TStreamingParams {
    public:
        const size_t DefaultProcessPollLatencyMs = 5 * 1000;          // 5 seconds
        const size_t DefaultInputBufferSizeBytes = 4 * 1024 * 1024;   // 4MB
        const size_t DefaultOutputBufferSizeBytes = 16 * 1024 * 1024; // 16MB
        const char* DefaultInputDelimiter = "\n";
        const char* DefaultOutputDelimiter = "\n";

    public:
        TUnboxedValue InputStreamObj;
        TString CommandLine;
        TUnboxedValue ArgumentsList;
        TString InputDelimiter;
        TString OutputDelimiter;
        size_t InputBufferSizeBytes;
        size_t OutputBufferSizeBytes;
        size_t ProcessPollLatencyMs;

        TStreamingParams()
            : InputDelimiter(DefaultInputDelimiter)
            , OutputDelimiter(DefaultOutputDelimiter)
            , InputBufferSizeBytes(DefaultInputBufferSizeBytes)
            , OutputBufferSizeBytes(DefaultOutputBufferSizeBytes)
            , ProcessPollLatencyMs(DefaultProcessPollLatencyMs)
        {
        }
    };

    struct TThreadSyncData {
        TMutex BuffersMutex;
        TCondVar InputBufferCanReadCond;
        TCondVar MainThreadHasWorkCond;
        TCondVar OutputBufferCanWriteCond;
    };

    class TStringListBufferedInputStream: public IInputStream {
    public:
        TStringListBufferedInputStream(TUnboxedValue rowsStream, const TString& delimiter, size_t bufferSizeBytes,
                                       TThreadSyncData& syncData, TSourcePosition pos)
            : RowsStream_(rowsStream)
            , Delimiter_(delimiter)
            , SyncData_(syncData)
            , Pos_(pos)
            , DelimiterMatcher_(delimiter)
            , DelimiterInput_(delimiter)
            , Buffer_(bufferSizeBytes)
            , CurReadMode_(ReadMode::Start)
        {
        }

        TStringListBufferedInputStream(const TStringListBufferedInputStream&) = delete;
        TStringListBufferedInputStream& operator=(const TStringListBufferedInputStream&) = delete;

        TCyclicRWBuffer& GetBuffer() {
            return Buffer_;
        }

        // Fetch input from upstream list iterator to the buffer.
        // Called from Main thread.
        EFetchStatus FetchInput() {
            with_lock (SyncData_.BuffersMutex) {
                Y_DEBUG_ABORT_UNLESS(!Buffer_.HasData());
                Y_DEBUG_ABORT_UNLESS(Buffer_.CanWrite());

                bool receivedYield = false;

                while (Buffer_.CanWrite() && CurReadMode_ != ReadMode::Done && !receivedYield) {
                    switch (CurReadMode_) {
                        case ReadMode::Start: {
                            auto status = ReadNextString();
                            if (status == EFetchStatus::Yield) {
                                receivedYield = true;
                                break;
                            }

                            CurReadMode_ = (status == EFetchStatus::Ok)
                                              ? ReadMode::String
                                              : ReadMode::Done;

                            break;
                        }

                        case ReadMode::String:
                            if (CurStringInput_.Exhausted()) {
                                DelimiterInput_.Reset(Delimiter_.data(), Delimiter_.size());
                                CurReadMode_ = ReadMode::Delimiter;
                                break;
                            }

                            Buffer_.Write(CurStringInput_);
                            break;

                        case ReadMode::Delimiter:
                            if (DelimiterInput_.Exhausted()) {
                                CurReadMode_ = ReadMode::Start;
                                break;
                            }

                            Buffer_.Write(DelimiterInput_);
                            break;

                        default:
                            break;
                    }
                }

                if (CurReadMode_ == ReadMode::Done) {
                    Buffer_.Finish();
                }

                SyncData_.InputBufferCanReadCond.Signal();
                return receivedYield ? EFetchStatus::Yield : EFetchStatus::Ok;
            }
        }

    private:
        // Read data to pass into the child process input pipe.
        // Called from Communicate thread.
        size_t DoRead(void* buf, size_t len) override {
            try {
                with_lock (SyncData_.BuffersMutex) {
                    while (!Buffer_.HasData() && !Buffer_.IsFinished()) {
                        SyncData_.MainThreadHasWorkCond.Signal();
                        SyncData_.InputBufferCanReadCond.WaitI(SyncData_.BuffersMutex);
                    }

                    if (!Buffer_.HasData()) {
                        Y_DEBUG_ABORT_UNLESS(Buffer_.IsFinished());
                        return 0;
                    }

                    const char* dataPtr;
                    size_t dataLen;
                    Buffer_.GetData(dataPtr, dataLen);

                    size_t bytesRead = std::min(dataLen, len);
                    Y_DEBUG_ABORT_UNLESS(bytesRead > 0);
                    memcpy(buf, dataPtr, bytesRead);
                    Buffer_.CommitRead(bytesRead);
                    return bytesRead;
                }

                ythrow yexception();
            } catch (const std::exception& e) {
                UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
            }
        }

        EFetchStatus ReadNextString() {
            TUnboxedValue item;
            EFetchStatus status = RowsStream_.Fetch(item);
            switch (status) {
                case EFetchStatus::Yield:
                case EFetchStatus::Finish:
                    return status;
                default:
                    break;
            }

            CurString_ = item.GetElement(0);
            CurStringInput_.Reset(CurString_.AsStringRef().Data(), CurString_.AsStringRef().Size());

            // Check that input string doesn't contain delimiters
            const char* match;
            Y_UNUSED(match);
            if (DelimiterMatcher_.SubStr(
                    CurString_.AsStringRef().Data(),
                    CurString_.AsStringRef().Data() + CurString_.AsStringRef().Size(),
                    match))
            {
                ythrow yexception() << "Delimiter found in input string.";
            }

            return EFetchStatus::Ok;
        }

    private:
        enum class ReadMode {
            Start,
            String,
            Delimiter,
            Done
        };

        TUnboxedValue RowsStream_;
        TString Delimiter_;
        TThreadSyncData& SyncData_;
        TSourcePosition Pos_;

        TKMPMatcher DelimiterMatcher_;
        TUnboxedValue CurString_;
        TMemoryInput CurStringInput_;
        TMemoryInput DelimiterInput_;

        TCyclicRWBuffer Buffer_;

        ReadMode CurReadMode_;
    };

    class TStringListBufferedOutputStream: public IOutputStream {
    public:
        TStringListBufferedOutputStream(const TString& delimiter, size_t stringBufferSizeBytes,
                                        TStringListBufferedInputStream& inputStream, TThreadSyncData& syncData)
            : Delimiter_(delimiter)
            , InputStream_(inputStream)
            , SyncData_(syncData)
            , HasDelimiterMatch_(false)
            , DelimiterMatcherCallback_(HasDelimiterMatch_)
            , DelimiterMatcher_(delimiter.data(), delimiter.data() + delimiter.size(), &DelimiterMatcherCallback_)
            , Buffer_(stringBufferSizeBytes)
        {
        }

        TStringListBufferedOutputStream(const TStringListBufferedOutputStream&) = delete;
        TStringListBufferedOutputStream& operator=(const TStringListBufferedOutputStream&) = delete;

        // Get string record from buffer.
        // Called from Main thread.
        EFetchStatus FetchNextString(TString& str) {
            while (!HasDelimiterMatch_) {
                with_lock (SyncData_.BuffersMutex) {
                    bool inputHasData;
                    bool bufferNeedsData;

                    do {
                        inputHasData = InputStream_.GetBuffer().HasData() || InputStream_.GetBuffer().IsFinished();
                        bufferNeedsData = !Buffer_.HasData() && !Buffer_.IsFinished();

                        if (inputHasData && bufferNeedsData) {
                            SyncData_.MainThreadHasWorkCond.WaitI(SyncData_.BuffersMutex);
                        }
                    } while (inputHasData && bufferNeedsData);

                    if (!inputHasData) {
                        auto status = InputStream_.FetchInput();
                        if (status == EFetchStatus::Yield) {
                            return EFetchStatus::Yield;
                        }
                    }

                    if (bufferNeedsData) {
                        continue;
                    }

                    if (!Buffer_.HasData()) {
                        Y_DEBUG_ABORT_UNLESS(Buffer_.IsFinished());
                        str = TString(TStringBuf(CurrentString_.Data(), CurrentString_.Size()));
                        CurrentString_.Clear();
                        return str.empty() ? EFetchStatus::Finish : EFetchStatus::Ok;
                    }

                    const char* data;
                    size_t size;
                    Buffer_.GetData(data, size);

                    size_t read = 0;
                    while (!HasDelimiterMatch_ && read < size) {
                        DelimiterMatcher_.Push(data[read]);
                        ++read;
                    }

                    Y_DEBUG_ABORT_UNLESS(read > 0);
                    CurrentString_.Append(data, read);
                    bool signalCanWrite = !Buffer_.CanWrite();
                    Buffer_.CommitRead(read);

                    if (signalCanWrite) {
                        SyncData_.OutputBufferCanWriteCond.Signal();
                    }
                }
            }

            Y_DEBUG_ABORT_UNLESS(CurrentString_.Size() >= Delimiter_.size());
            str = TString(TStringBuf(CurrentString_.Data(), CurrentString_.Size() - Delimiter_.size()));
            CurrentString_.Clear();
            HasDelimiterMatch_ = false;

            return EFetchStatus::Ok;
        }

        TCyclicRWBuffer& GetBuffer() {
            return Buffer_;
        }

    private:
        // Write data from child process output to buffer.
        // Called from Communicate thread.
        void DoWrite(const void* buf, size_t len) override {
            const char* curStrPos = reinterpret_cast<const char*>(buf);
            size_t curStrLen = len;

            while (curStrLen > 0) {
                with_lock (SyncData_.BuffersMutex) {
                    while (!Buffer_.CanWrite() && !Buffer_.IsFinished()) {
                        SyncData_.OutputBufferCanWriteCond.WaitI(SyncData_.BuffersMutex);
                    }

                    if (Buffer_.IsFinished()) {
                        return;
                    }

                    bool signalCanRead = !Buffer_.HasData();
                    Buffer_.Write(curStrPos, curStrLen);

                    if (signalCanRead) {
                        SyncData_.MainThreadHasWorkCond.Signal();
                    }
                }
            }
        }

        void DoFinish() override {
            IOutputStream::DoFinish();

            with_lock (SyncData_.BuffersMutex) {
                Buffer_.Finish();
                SyncData_.MainThreadHasWorkCond.Signal();
            }
        }

    private:
        class MatcherCallback: public TKMPStreamMatcher<char>::ICallback {
        public:
            MatcherCallback(bool& hasMatch)
                : HasMatch_(hasMatch)
            {
            }

            void OnMatch(const char* begin, const char* end) override {
                Y_UNUSED(begin);
                Y_UNUSED(end);

                HasMatch_ = true;
            }

        private:
            bool& HasMatch_;
        };

    private:
        TString Delimiter_;
        TStringListBufferedInputStream& InputStream_;
        TThreadSyncData& SyncData_;

        bool HasDelimiterMatch_;
        MatcherCallback DelimiterMatcherCallback_;
        TKMPStreamMatcher<char> DelimiterMatcher_;

        TBuffer CurrentString_;

        TCyclicRWBuffer Buffer_;
    };

    class TStreamingOutputListIterator {
    public:
        TStreamingOutputListIterator(const TStreamingParams& params, const IValueBuilder* valueBuilder, TSourcePosition pos)
            : StreamingParams_(params)
            , ValueBuilder_(valueBuilder)
            , Pos_(pos)
        {
        }

        TStreamingOutputListIterator(const TStreamingOutputListIterator&) = delete;
        TStreamingOutputListIterator& operator=(const TStreamingOutputListIterator&) = delete;

        ~TStreamingOutputListIterator() {
            if (ShellCommand_) {
                Y_DEBUG_ABORT_UNLESS(InputStream_ && OutputStream_);

                try {
                    ShellCommand_->Terminate();
                } catch (const std::exception& e) {
                    Cerr << CurrentExceptionMessage();
                }

                // Let Communicate thread finish.
                with_lock (ThreadSyncData_.BuffersMutex) {
                    InputStream_->GetBuffer().Finish();
                    OutputStream_->GetBuffer().Finish();
                    ThreadSyncData_.InputBufferCanReadCond.Signal();
                    ThreadSyncData_.OutputBufferCanWriteCond.Signal();
                }

                ShellCommand_->Wait();
            }
        }

        EFetchStatus Fetch(TUnboxedValue& result) {
            try {
                EFetchStatus status = EFetchStatus::Ok;

                if (!ProcessStarted()) {
                    StartProcess();

                    // Don't try to fetch data if there was a problem starting the process,
                    // this causes infinite wait on Windows system due to incorrect ShellCommand behavior.
                    if (ShellCommand_->GetStatus() != TShellCommand::SHELL_RUNNING && ShellCommand_->GetStatus() != TShellCommand::SHELL_FINISHED) {
                        status = EFetchStatus::Finish;
                    }
                }

                if (status == EFetchStatus::Ok) {
                    status = OutputStream_->FetchNextString(CurrentRecord_);
                }

                if (status == EFetchStatus::Finish) {
                    switch (ShellCommand_->GetStatus()) {
                        case TShellCommand::SHELL_FINISHED:
                            break;
                        case TShellCommand::SHELL_INTERNAL_ERROR:
                            ythrow yexception() << "Internal error running process: " << ShellCommand_->GetInternalError();
                            break;
                        case TShellCommand::SHELL_ERROR:
                            ythrow yexception() << "Error running user process: " << ShellCommand_->GetError();
                            break;
                        default:
                            ythrow yexception() << "Unexpected shell command status: " << (int)ShellCommand_->GetStatus();
                    }
                    return EFetchStatus::Finish;
                }

                if (status == EFetchStatus::Ok) {
                    TUnboxedValue* items = nullptr;
                    result = ValueBuilder_->NewArray(1, items);
                    *items = ValueBuilder_->NewString(TStringRef(CurrentRecord_.data(), CurrentRecord_.size()));
                }

                return status;
            } catch (const std::exception& e) {
                UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
            }
        }

    private:
        void StartProcess() {
            InputStream_.Reset(new TStringListBufferedInputStream(
                StreamingParams_.InputStreamObj, StreamingParams_.InputDelimiter,
                StreamingParams_.InputBufferSizeBytes, ThreadSyncData_, Pos_));

            OutputStream_.Reset(new TStringListBufferedOutputStream(
                StreamingParams_.OutputDelimiter, StreamingParams_.OutputBufferSizeBytes, *InputStream_,
                ThreadSyncData_));

            TShellCommandOptions opt;
            opt.SetAsync(true).SetUseShell(false).SetLatency(StreamingParams_.ProcessPollLatencyMs).SetInputStream(InputStream_.Get()).SetOutputStream(OutputStream_.Get()).SetCloseStreams(true).SetCloseAllFdsOnExec(true);

            TList<TString> commandArguments;
            auto argumetsIterator = StreamingParams_.ArgumentsList.GetListIterator();
            for (TUnboxedValue item; argumetsIterator.Next(item);) {
                commandArguments.emplace_back(TStringBuf(item.AsStringRef()));
            }

            ShellCommand_.Reset(new TShellCommand(StreamingParams_.CommandLine, commandArguments, opt));
            ShellCommand_->Run();
        }

        bool ProcessStarted() const {
            return !!ShellCommand_;
        }

    private:
        TStreamingParams StreamingParams_;
        const IValueBuilder* ValueBuilder_;
        TSourcePosition Pos_;

        TThreadSyncData ThreadSyncData_;

        THolder<TShellCommand> ShellCommand_;
        THolder<TStringListBufferedInputStream> InputStream_;
        THolder<TStringListBufferedOutputStream> OutputStream_;

        TString CurrentRecord_;
    };

    class TStreamingOutput: public TBoxedValue {
    public:
        TStreamingOutput(const TStreamingParams& params, const IValueBuilder* valueBuilder, TSourcePosition pos)
            : StreamingParams_(params)
            , ValueBuilder_(valueBuilder)
            , Pos_(pos)
        {
        }

        TStreamingOutput(const TStreamingOutput&) = delete;
        TStreamingOutput& operator=(const TStreamingOutput&) = delete;

    private:
        EFetchStatus Fetch(TUnboxedValue& result) override {
            if (IsFinished_) {
                return EFetchStatus::Finish;
            }

            if (!Iterator_) {
                Iterator_.Reset(new TStreamingOutputListIterator(StreamingParams_, ValueBuilder_, Pos_));
            }

            auto ret = Iterator_->Fetch(result);

            if (ret == EFetchStatus::Finish) {
                IsFinished_ = true;
                Iterator_.Reset();
            }

            return ret;
        }

        TStreamingParams StreamingParams_;
        const IValueBuilder* ValueBuilder_;
        TSourcePosition Pos_;
        bool IsFinished_ = false;
        THolder<TStreamingOutputListIterator> Iterator_;
    };

    class TStreamingScriptOutput: public TStreamingOutput {
    public:
        TStreamingScriptOutput(const TStreamingParams& params, const IValueBuilder* valueBuilder,
                               TSourcePosition pos, const TString& script, const TString& scriptFilename)
            : TStreamingOutput(params, valueBuilder, pos)
            , ScriptFileHandle_(scriptFilename)
        {
            auto scriptStripped = StripBeforeShebang(script);
            ScriptFileHandle_.Write(scriptStripped.data(), scriptStripped.size());
            ScriptFileHandle_.Close();

            if (Chmod(ScriptFileHandle_.Name().c_str(), MODE0755) != 0) {
                ythrow yexception() << "Chmod failed for script file:" << ScriptFileHandle_.Name()
                                    << " with error: " << LastSystemErrorText();
            }
        }

    private:
        static TString StripBeforeShebang(const TString& script) {
            auto shebangIndex = script.find("#!");
            if (shebangIndex != TString::npos) {
                auto scriptStripped = StripStringLeft(script);

                if (scriptStripped.size() == script.size() - shebangIndex) {
                    return scriptStripped;
                }
            }

            return script;
        }

        TTempFileHandle ScriptFileHandle_;
    };

    class TStreamingProcess: public TBoxedValue {
    public:
        TStreamingProcess(TSourcePosition pos)
            : Pos_(pos)
        {}

    private:
        TUnboxedValue Run(const IValueBuilder* valueBuilder,
                          const TUnboxedValuePod* args) const override {
            auto inputListArg = args[0];
            auto commandLineArg = args[1].AsStringRef();
            auto argumentsArg = args[2];
            auto inputDelimiterArg = args[3];
            auto outputDelimiterArg = args[4];

            Y_DEBUG_ABORT_UNLESS(inputListArg.IsBoxed());

            TStreamingParams params;
            params.InputStreamObj = TUnboxedValuePod(inputListArg);
            params.CommandLine = TString(TStringBuf(commandLineArg));
            params.ArgumentsList = !argumentsArg
                                       ? valueBuilder->NewEmptyList()
                                       : TUnboxedValue(argumentsArg.GetOptionalValue());

            if (inputDelimiterArg) {
                params.InputDelimiter = TString(TStringBuf(inputDelimiterArg.AsStringRef()));
            }
            if (outputDelimiterArg) {
                params.OutputDelimiter = TString(TStringBuf(outputDelimiterArg.AsStringRef()));
            }

            return TUnboxedValuePod(new TStreamingOutput(params, valueBuilder, Pos_));
        }

    public:
        static TStringRef Name() {
            static auto name = TStringRef::Of("Process");
            return name;
        }

    private:
        TSourcePosition Pos_;
    };

    class TStreamingProcessInline: public TBoxedValue {
    public:
        TStreamingProcessInline(TSourcePosition pos)
            : Pos_(pos)
        {}

    private:
        TUnboxedValue Run(const IValueBuilder* valueBuilder,
                          const TUnboxedValuePod* args) const override {
            auto inputListArg = args[0];
            auto scriptArg = args[1].AsStringRef();
            auto argumentsArg = args[2];
            auto inputDelimiterArg = args[3];
            auto outputDelimiterArg = args[4];

            TString script(scriptArg);
            TString scriptFilename = MakeTempName(".");

            TStreamingParams params;
            params.InputStreamObj = TUnboxedValuePod(inputListArg);
            params.CommandLine = scriptFilename;
            params.ArgumentsList = !argumentsArg
                                       ? valueBuilder->NewEmptyList()
                                       : TUnboxedValue(argumentsArg.GetOptionalValue());

            if (inputDelimiterArg) {
                params.InputDelimiter = TString(TStringBuf(inputDelimiterArg.AsStringRef()));
            }
            if (outputDelimiterArg) {
                params.OutputDelimiter = TString(TStringBuf(outputDelimiterArg.AsStringRef()));
            }

            return TUnboxedValuePod(new TStreamingScriptOutput(params, valueBuilder, Pos_, script, scriptFilename));
        }

    public:
        static TStringRef Name() {
            static auto name = TStringRef::Of("ProcessInline");
            return name;
        }

    private:
        TSourcePosition Pos_;
    };

    class TStreamingModule: public IUdfModule {
    public:
        TStringRef Name() const {
            return TStringRef::Of("Streaming");
        }

        void CleanupOnTerminate() const final {
        }

        void GetAllFunctions(IFunctionsSink& sink) const final {
            sink.Add(TStreamingProcess::Name());
            sink.Add(TStreamingProcessInline::Name());
        }

        void BuildFunctionTypeInfo(
            const TStringRef& name,
            NUdf::TType* userType,
            const TStringRef& typeConfig,
            ui32 flags,
            IFunctionTypeInfoBuilder& builder) const override {
            try {
                Y_UNUSED(userType);
                Y_UNUSED(typeConfig);

                bool typesOnly = (flags & TFlags::TypesOnly);

                auto optionalStringType = builder.Optional()->Item<char*>().Build();
                auto rowType = builder.Struct(1)->AddField("Data", TDataType<char*>::Id, nullptr).Build();
                auto rowsType = builder.Stream()->Item(rowType).Build();
                auto stringListType = builder.List()->Item(TDataType<char*>::Id).Build();
                auto optionalStringListType = builder.Optional()->Item(stringListType).Build();

                if (TStreamingProcess::Name() == name) {
                    builder.Returns(rowsType).Args()->Add(rowsType).Add<char*>().Add(optionalStringListType).Add(optionalStringType).Add(optionalStringType).Done().OptionalArgs(3);

                    if (!typesOnly) {
                        builder.Implementation(new TStreamingProcess(builder.GetSourcePosition()));
                    }
                }

                if (TStreamingProcessInline::Name() == name) {
                    builder.Returns(rowsType).Args()->Add(rowsType).Add<char*>().Add(optionalStringListType).Add(optionalStringType).Add(optionalStringType).Done().OptionalArgs(3);

                    if (!typesOnly) {
                        builder.Implementation(new TStreamingProcessInline(builder.GetSourcePosition()));
                    }
                }
            } catch (const std::exception& e) {
                builder.SetError(CurrentExceptionMessage());
            }
        }
    };

}

REGISTER_MODULES(TStreamingModule)
