#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/public/udf/udf_registrator.h>
#include <ydb/library/yql/public/udf/udf_type_builder.h>
#include <ydb/library/yql/public/udf/udf_value_builder.h>
#include <ydb/library/yql/public/udf/udf_terminator.h>

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
            : Buffer(capacity)
            , Finished(false)
            , DataStart(0)
            , DataSize(0)
        {
            Buffer.Resize(capacity);
        }

        bool IsFinished() const {
            return Finished;
        }

        void Finish() {
            Finished = true;
        }

        bool HasData() const {
            return DataSize > 0;
        }

        size_t GetDataSize() const {
            return DataSize;
        }

        void GetData(const char*& ptr, size_t& len) const {
            size_t readSize = GetDataRegionSize(DataStart, DataSize);
            ptr = Buffer.Data() + DataStart;
            len = readSize;
        }

        void CommitRead(size_t len) {
            Y_DEBUG_ABORT_UNLESS(len <= GetDataRegionSize(DataStart, DataSize));

            DataStart = GetBufferPosition(DataStart + len);
            DataSize -= len;
        }

        bool CanWrite() const {
            return WriteSize() > 0;
        }

        size_t WriteSize() const {
            return Buffer.Size() - DataSize;
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

                DataSize += writeSize;
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
            return pos % Buffer.Size();
        }

        size_t GetDataRegionSize(size_t start, size_t size) const {
            Y_DEBUG_ABORT_UNLESS(start < Buffer.Size());

            return std::min(size, Buffer.Size() - start);
        }

        size_t GetWriteStart() const {
            return GetBufferPosition(DataStart + DataSize);
        }

        char* Data(size_t pos) {
            Y_DEBUG_ABORT_UNLESS(pos < Buffer.Size());

            return (Buffer.Data() + pos);
        }

    private:
        TBuffer Buffer;

        bool Finished;

        size_t DataStart;
        size_t DataSize;
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
            : RowsStream(rowsStream)
            , Delimiter(delimiter)
            , SyncData(syncData)
            , Pos_(pos)
            , DelimiterMatcher(delimiter)
            , DelimiterInput(delimiter)
            , Buffer(bufferSizeBytes)
            , CurReadMode(ReadMode::Start)
        {
        }

        TStringListBufferedInputStream(const TStringListBufferedInputStream&) = delete;
        TStringListBufferedInputStream& operator=(const TStringListBufferedInputStream&) = delete;

        TCyclicRWBuffer& GetBuffer() {
            return Buffer;
        }

        // Fetch input from upstream list iterator to the buffer.
        // Called from Main thread.
        EFetchStatus FetchInput() {
            with_lock (SyncData.BuffersMutex) {
                Y_DEBUG_ABORT_UNLESS(!Buffer.HasData());
                Y_DEBUG_ABORT_UNLESS(Buffer.CanWrite());

                bool receivedYield = false;

                while (Buffer.CanWrite() && CurReadMode != ReadMode::Done && !receivedYield) {
                    switch (CurReadMode) {
                        case ReadMode::Start: {
                            auto status = ReadNextString();
                            if (status == EFetchStatus::Yield) {
                                receivedYield = true;
                                break;
                            }

                            CurReadMode = (status == EFetchStatus::Ok)
                                              ? ReadMode::String
                                              : ReadMode::Done;

                            break;
                        }

                        case ReadMode::String:
                            if (CurStringInput.Exhausted()) {
                                DelimiterInput.Reset(Delimiter.data(), Delimiter.size());
                                CurReadMode = ReadMode::Delimiter;
                                break;
                            }

                            Buffer.Write(CurStringInput);
                            break;

                        case ReadMode::Delimiter:
                            if (DelimiterInput.Exhausted()) {
                                CurReadMode = ReadMode::Start;
                                break;
                            }

                            Buffer.Write(DelimiterInput);
                            break;

                        default:
                            break;
                    }
                }

                if (CurReadMode == ReadMode::Done) {
                    Buffer.Finish();
                }

                SyncData.InputBufferCanReadCond.Signal();
                return receivedYield ? EFetchStatus::Yield : EFetchStatus::Ok;
            }
        }

    private:
        // Read data to pass into the child process input pipe.
        // Called from Communicate thread.
        size_t DoRead(void* buf, size_t len) override {
            try {
                with_lock (SyncData.BuffersMutex) {
                    while (!Buffer.HasData() && !Buffer.IsFinished()) {
                        SyncData.MainThreadHasWorkCond.Signal();
                        SyncData.InputBufferCanReadCond.WaitI(SyncData.BuffersMutex);
                    }

                    if (!Buffer.HasData()) {
                        Y_DEBUG_ABORT_UNLESS(Buffer.IsFinished());
                        return 0;
                    }

                    const char* dataPtr;
                    size_t dataLen;
                    Buffer.GetData(dataPtr, dataLen);

                    size_t bytesRead = std::min(dataLen, len);
                    Y_DEBUG_ABORT_UNLESS(bytesRead > 0);
                    memcpy(buf, dataPtr, bytesRead);
                    Buffer.CommitRead(bytesRead);
                    return bytesRead;
                }

                ythrow yexception();
            } catch (const std::exception& e) {
                UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
            }
        }

        EFetchStatus ReadNextString() {
            TUnboxedValue item;
            EFetchStatus status = RowsStream.Fetch(item);
            switch (status) {
                case EFetchStatus::Yield:
                case EFetchStatus::Finish:
                    return status;
                default:
                    break;
            }

            CurString = item.GetElement(0);
            CurStringInput.Reset(CurString.AsStringRef().Data(), CurString.AsStringRef().Size());

            // Check that input string doesn't contain delimiters
            const char* match;
            Y_UNUSED(match);
            if (DelimiterMatcher.SubStr(
                    CurString.AsStringRef().Data(),
                    CurString.AsStringRef().Data() + CurString.AsStringRef().Size(),
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

        TUnboxedValue RowsStream;
        TString Delimiter;
        TThreadSyncData& SyncData;
        TSourcePosition Pos_;

        TKMPMatcher DelimiterMatcher;
        TUnboxedValue CurString;
        TMemoryInput CurStringInput;
        TMemoryInput DelimiterInput;

        TCyclicRWBuffer Buffer;

        ReadMode CurReadMode;
    };

    class TStringListBufferedOutputStream: public IOutputStream {
    public:
        TStringListBufferedOutputStream(const TString& delimiter, size_t stringBufferSizeBytes,
                                        TStringListBufferedInputStream& inputStream, TThreadSyncData& syncData)
            : Delimiter(delimiter)
            , InputStream(inputStream)
            , SyncData(syncData)
            , HasDelimiterMatch(false)
            , DelimiterMatcherCallback(HasDelimiterMatch)
            , DelimiterMatcher(delimiter.data(), delimiter.data() + delimiter.size(), &DelimiterMatcherCallback)
            , Buffer(stringBufferSizeBytes)
        {
        }

        TStringListBufferedOutputStream(const TStringListBufferedOutputStream&) = delete;
        TStringListBufferedOutputStream& operator=(const TStringListBufferedOutputStream&) = delete;

        // Get string record from buffer.
        // Called from Main thread.
        EFetchStatus FetchNextString(TString& str) {
            while (!HasDelimiterMatch) {
                with_lock (SyncData.BuffersMutex) {
                    bool inputHasData;
                    bool bufferNeedsData;

                    do {
                        inputHasData = InputStream.GetBuffer().HasData() || InputStream.GetBuffer().IsFinished();
                        bufferNeedsData = !Buffer.HasData() && !Buffer.IsFinished();

                        if (inputHasData && bufferNeedsData) {
                            SyncData.MainThreadHasWorkCond.WaitI(SyncData.BuffersMutex);
                        }
                    } while (inputHasData && bufferNeedsData);

                    if (!inputHasData) {
                        auto status = InputStream.FetchInput();
                        if (status == EFetchStatus::Yield) {
                            return EFetchStatus::Yield;
                        }
                    }

                    if (bufferNeedsData) {
                        continue;
                    }

                    if (!Buffer.HasData()) {
                        Y_DEBUG_ABORT_UNLESS(Buffer.IsFinished());
                        str = TString(TStringBuf(CurrentString.Data(), CurrentString.Size()));
                        CurrentString.Clear();
                        return str.empty() ? EFetchStatus::Finish : EFetchStatus::Ok;
                    }

                    const char* data;
                    size_t size;
                    Buffer.GetData(data, size);

                    size_t read = 0;
                    while (!HasDelimiterMatch && read < size) {
                        DelimiterMatcher.Push(data[read]);
                        ++read;
                    }

                    Y_DEBUG_ABORT_UNLESS(read > 0);
                    CurrentString.Append(data, read);
                    bool signalCanWrite = !Buffer.CanWrite();
                    Buffer.CommitRead(read);

                    if (signalCanWrite) {
                        SyncData.OutputBufferCanWriteCond.Signal();
                    }
                }
            }

            Y_DEBUG_ABORT_UNLESS(CurrentString.Size() >= Delimiter.size());
            str = TString(TStringBuf(CurrentString.Data(), CurrentString.Size() - Delimiter.size()));
            CurrentString.Clear();
            HasDelimiterMatch = false;

            return EFetchStatus::Ok;
        }

        TCyclicRWBuffer& GetBuffer() {
            return Buffer;
        }

    private:
        // Write data from child process output to buffer.
        // Called from Communicate thread.
        void DoWrite(const void* buf, size_t len) override {
            const char* curStrPos = reinterpret_cast<const char*>(buf);
            size_t curStrLen = len;

            while (curStrLen > 0) {
                with_lock (SyncData.BuffersMutex) {
                    while (!Buffer.CanWrite() && !Buffer.IsFinished()) {
                        SyncData.OutputBufferCanWriteCond.WaitI(SyncData.BuffersMutex);
                    }

                    if (Buffer.IsFinished()) {
                        return;
                    }

                    bool signalCanRead = !Buffer.HasData();
                    Buffer.Write(curStrPos, curStrLen);

                    if (signalCanRead) {
                        SyncData.MainThreadHasWorkCond.Signal();
                    }
                }
            }
        }

        void DoFinish() override {
            IOutputStream::DoFinish();

            with_lock (SyncData.BuffersMutex) {
                Buffer.Finish();
                SyncData.MainThreadHasWorkCond.Signal();
            }
        }

    private:
        class MatcherCallback: public TKMPStreamMatcher<char>::ICallback {
        public:
            MatcherCallback(bool& hasMatch)
                : HasMatch(hasMatch)
            {
            }

            void OnMatch(const char* begin, const char* end) override {
                Y_UNUSED(begin);
                Y_UNUSED(end);

                HasMatch = true;
            }

        private:
            bool& HasMatch;
        };

    private:
        TString Delimiter;
        TStringListBufferedInputStream& InputStream;
        TThreadSyncData& SyncData;

        bool HasDelimiterMatch;
        MatcherCallback DelimiterMatcherCallback;
        TKMPStreamMatcher<char> DelimiterMatcher;

        TBuffer CurrentString;

        TCyclicRWBuffer Buffer;
    };

    class TStreamingOutputListIterator {
    public:
        TStreamingOutputListIterator(const TStreamingParams& params, const IValueBuilder* valueBuilder, TSourcePosition pos)
            : StreamingParams(params)
            , ValueBuilder(valueBuilder)
            , Pos_(pos)
        {
        }

        TStreamingOutputListIterator(const TStreamingOutputListIterator&) = delete;
        TStreamingOutputListIterator& operator=(const TStreamingOutputListIterator&) = delete;

        ~TStreamingOutputListIterator() {
            if (ShellCommand) {
                Y_DEBUG_ABORT_UNLESS(InputStream && OutputStream);

                try {
                    ShellCommand->Terminate();
                } catch (const std::exception& e) {
                    Cerr << CurrentExceptionMessage();
                }

                // Let Communicate thread finish.
                with_lock (ThreadSyncData.BuffersMutex) {
                    InputStream->GetBuffer().Finish();
                    OutputStream->GetBuffer().Finish();
                    ThreadSyncData.InputBufferCanReadCond.Signal();
                    ThreadSyncData.OutputBufferCanWriteCond.Signal();
                }

                ShellCommand->Wait();
            }
        }

        EFetchStatus Fetch(TUnboxedValue& result) {
            try {
                EFetchStatus status = EFetchStatus::Ok;

                if (!ProcessStarted()) {
                    StartProcess();

                    // Don't try to fetch data if there was a problem starting the process,
                    // this causes infinite wait on Windows system due to incorrect ShellCommand behavior.
                    if (ShellCommand->GetStatus() != TShellCommand::SHELL_RUNNING && ShellCommand->GetStatus() != TShellCommand::SHELL_FINISHED) {
                        status = EFetchStatus::Finish;
                    }
                }

                if (status == EFetchStatus::Ok) {
                    status = OutputStream->FetchNextString(CurrentRecord);
                }

                if (status == EFetchStatus::Finish) {
                    switch (ShellCommand->GetStatus()) {
                        case TShellCommand::SHELL_FINISHED:
                            break;
                        case TShellCommand::SHELL_INTERNAL_ERROR:
                            ythrow yexception() << "Internal error running process: " << ShellCommand->GetInternalError();
                            break;
                        case TShellCommand::SHELL_ERROR:
                            ythrow yexception() << "Error running user process: " << ShellCommand->GetError();
                            break;
                        default:
                            ythrow yexception() << "Unexpected shell command status: " << (int)ShellCommand->GetStatus();
                    }
                    return EFetchStatus::Finish;
                }

                if (status == EFetchStatus::Ok) {
                    TUnboxedValue* items = nullptr;
                    result = ValueBuilder->NewArray(1, items);
                    *items = ValueBuilder->NewString(TStringRef(CurrentRecord.data(), CurrentRecord.size()));
                }

                return status;
            } catch (const std::exception& e) {
                UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
            }
        }

    private:
        void StartProcess() {
            InputStream.Reset(new TStringListBufferedInputStream(
                StreamingParams.InputStreamObj, StreamingParams.InputDelimiter,
                StreamingParams.InputBufferSizeBytes, ThreadSyncData, Pos_));

            OutputStream.Reset(new TStringListBufferedOutputStream(
                StreamingParams.OutputDelimiter, StreamingParams.OutputBufferSizeBytes, *InputStream,
                ThreadSyncData));

            TShellCommandOptions opt;
            opt.SetAsync(true).SetUseShell(false).SetLatency(StreamingParams.ProcessPollLatencyMs).SetInputStream(InputStream.Get()).SetOutputStream(OutputStream.Get()).SetCloseStreams(true).SetCloseAllFdsOnExec(true);

            TList<TString> commandArguments;
            auto argumetsIterator = StreamingParams.ArgumentsList.GetListIterator();
            for (TUnboxedValue item; argumetsIterator.Next(item);) {
                commandArguments.emplace_back(TStringBuf(item.AsStringRef()));
            }

            ShellCommand.Reset(new TShellCommand(StreamingParams.CommandLine, commandArguments, opt));
            ShellCommand->Run();
        }

        bool ProcessStarted() const {
            return !!ShellCommand;
        }

    private:
        TStreamingParams StreamingParams;
        const IValueBuilder* ValueBuilder;
        TSourcePosition Pos_;

        TThreadSyncData ThreadSyncData;

        THolder<TShellCommand> ShellCommand;
        THolder<TStringListBufferedInputStream> InputStream;
        THolder<TStringListBufferedOutputStream> OutputStream;

        TString CurrentRecord;
    };

    class TStreamingOutput: public TBoxedValue {
    public:
        TStreamingOutput(const TStreamingParams& params, const IValueBuilder* valueBuilder, TSourcePosition pos)
            : StreamingParams(params)
            , ValueBuilder(valueBuilder)
            , Pos_(pos)
        {
        }

        TStreamingOutput(const TStreamingOutput&) = delete;
        TStreamingOutput& operator=(const TStreamingOutput&) = delete;

    private:
        EFetchStatus Fetch(TUnboxedValue& result) override {
            if (IsFinished) {
                return EFetchStatus::Finish;
            }

            if (!Iterator) {
                Iterator.Reset(new TStreamingOutputListIterator(StreamingParams, ValueBuilder, Pos_));
            }

            auto ret = Iterator->Fetch(result);

            if (ret == EFetchStatus::Finish) {
                IsFinished = true;
                Iterator.Reset();
            }

            return ret;
        }

        TStreamingParams StreamingParams;
        const IValueBuilder* ValueBuilder;
        TSourcePosition Pos_;
        bool IsFinished = false;
        THolder<TStreamingOutputListIterator> Iterator;
    };

    class TStreamingScriptOutput: public TStreamingOutput {
    public:
        TStreamingScriptOutput(const TStreamingParams& params, const IValueBuilder* valueBuilder,
                               TSourcePosition pos, const TString& script, const TString& scriptFilename)
            : TStreamingOutput(params, valueBuilder, pos)
            , ScriptFileHandle(scriptFilename)
        {
            auto scriptStripped = StripBeforeShebang(script);
            ScriptFileHandle.Write(scriptStripped.data(), scriptStripped.size());
            ScriptFileHandle.Close();

            if (Chmod(ScriptFileHandle.Name().c_str(), MODE0755) != 0) {
                ythrow yexception() << "Chmod failed for script file:" << ScriptFileHandle.Name()
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

        TTempFileHandle ScriptFileHandle;
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
