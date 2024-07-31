#include "tasks_runner_pipe.h"

#include <ydb/library/yql/dq/runtime/dq_input_channel.h>
#include <ydb/library/yql/dq/runtime/dq_output_channel.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/minikql/mkql_node_serialization.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/aligned_page_pool.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/utils/backtrace/backtrace.h>
#include <ydb/library/yql/utils/yql_panic.h>
#include <ydb/library/yql/utils/rope_over_buffer.h>
#include <ydb/library/yql/utils/failure_injector/failure_injector.h>

#include <ydb/library/yql/providers/dq/common/yql_dq_settings.h>

#include <library/cpp/yson/node/node.h>
#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/svnversion/svnversion.h>

#include <library/cpp/threading/task_scheduler/task_scheduler.h>

#include <util/system/shellcommand.h>
#include <util/system/thread.h>
#include <util/system/fs.h>
#include <util/stream/file.h>
#include <util/stream/pipe.h>
#include <util/stream/length.h>
#include <util/generic/size_literals.h>
#include <util/generic/maybe.h>
#include <util/string/cast.h>
#include <util/string/strip.h>
#include <util/string/builder.h>

namespace NYql::NTaskRunnerProxy {

const TString WorkingDirectoryParamName = "TaskRunnerProxy.WorkingDirectory";
const TString WorkingDirectoryDontInitParamName = "TaskRunnerProxy.WorkingDirectoryDontInit";
const TString UseMetaParamName = "TaskRunnerProxy.UseMeta"; // COMPAT(aozeritsky)


using namespace NKikimr::NMiniKQL;
using namespace NKikimr;
using namespace NDq;

#ifndef _win_
extern "C" int fork(void);
extern "C" int dup2(int oldfd, int newfd);
extern "C" int close(int);
extern "C" int pipe(int pipefd[2]);
extern "C" int kill(int pid, int sig);
extern "C" int waitpid(int pid, int* status, int options);
#endif

namespace {

void Load(IInputStream& input, void* buf, size_t size) {
    char* p = (char*)buf;
    while (size) {
        auto len = input.Read(p, size);
        YQL_ENSURE(len != 0);
        p += len;
        size -= len;
    }
}

} // namespace {

i64 SaveRopeToPipe(IOutputStream& output, const TRope& rope) {
    i64 total = 0;
    for (const auto& [data, size] : rope) {
        output.Write(&size, sizeof(size_t));
        YQL_ENSURE(size != 0);
        output.Write(data, size);
        total += size;
    }
    size_t zero = 0;
    output.Write(&zero, sizeof(size_t));
    return total;
}

void LoadRopeFromPipe(IInputStream& input, TRope& rope) {
    size_t size;
    do {
        Load(input, &size, sizeof(size_t));
        if (size) {
            auto buffer = std::shared_ptr<char[]>(new char[size]);
            Load(input, buffer.get(), size);            
            rope.Insert(rope.End(), NYql::MakeReadOnlyRope(buffer, buffer.get(), size));
        }
    } while (size != 0);
}

class TFilesHolder {
public:
    using TPtr = std::unique_ptr<TFilesHolder>;

    TFilesHolder(const IFileCache::TPtr& fileCache)
        : FileCache(fileCache)
    { }

    ~TFilesHolder() {
        for (const auto& file : Files) {
            FileCache->ReleaseFile(file);
        }
    }

    void Add(const TString& objectId) {
        Files.emplace(objectId);
    }

private:
    std::unordered_set<TString> Files;
    const IFileCache::TPtr FileCache;
};

class TChildProcess: private TNonCopyable {
public:
    TChildProcess(const TString& exeName, const TVector<TString>& args, const THashMap<TString, TString>& env, const TString& workDir)
        : ExeName(exeName)
        , Args(args)
        , Env(env)
        , WorkDir(workDir)
    {
        NFs::MakeDirectory(WorkDir);
    }

    virtual ~TChildProcess() {
        try {
            NFs::RemoveRecursive(WorkDir);
        } catch (...) {
            YQL_CLOG(DEBUG, ProviderDq) << "Error on WorkDir cleanup: " << CurrentExceptionMessage();
        }
    }

    virtual TString ExternalWorkDir() {
        return WorkDir;
    }

    virtual TString InternalWorkDir() {
        return WorkDir;
    }

    void Run()
    {
#ifdef _win_
        Y_ABORT_UNLESS(false);
#else
        int input[2];
        int output[2];
        int error[2];

        Y_ABORT_UNLESS(pipe(input) == 0);
        Y_ABORT_UNLESS(pipe(output) == 0);
        Y_ABORT_UNLESS(pipe(error) == 0);

        PrepareForExec();
        Pid = fork();
        Y_ABORT_UNLESS(Pid >= 0);

        if (Pid == 0) {
            try {
                close(input[1]);
                close(output[0]);
                close(error[0]);

                if (dup2(input[0], 0) == -1) {
                    ythrow TSystemError() << "Cannot dup2 input";
                } // stdin
                if (dup2(output[1], 1) == -1) {
                    ythrow TSystemError() << "Cannot dup2 output";
                } // stdout
                if  (dup2(error[1], 2) == -1) {
                    ythrow TSystemError() << "Cannot dup2 error";
                } // stderr

                Exec();
            } catch (const yexception& ex) {
                Cerr << ex.what() << Endl;
            }

            _exit(127);
        }

        close(input[0]);
        close(output[1]);
        close(error[1]);

        Stdin = MakeHolder<TPipedOutput>(input[1]);
        Stdout = MakeHolder<TPipedInput>(output[0]);
        Stderr = MakeHolder<TPipedInput>(error[0]);
        YQL_CLOG(DEBUG, ProviderDq) << "Forked child, pid: " << Pid;
#endif
    }

    virtual void Kill() {
#ifndef _win_
        // todo: investigate why ain't killed sometimes
        YQL_CLOG(DEBUG, ProviderDq) << "Kill child, pid: " << Pid;
        kill(Pid, 9);
#endif
    }

    bool IsAlive() {
#ifdef _win_
        return true;
#else
        int status;
        YQL_CLOG(TRACE, ProviderDq) << "Check Pid " << Pid;
        return waitpid(Pid, &status, WNOHANG) <= 0;
#endif
    }

    int Wait(TDuration timeout = TDuration::Seconds(5)) {
        int status;
        int ret;
#ifndef _win_
        TInstant start = TInstant::Now();
        while ((ret = waitpid(Pid, &status, WNOHANG)) == 0 && TInstant::Now() - start < timeout) {
            Sleep(TDuration::MilliSeconds(10));
        }
        if (ret <= 0) {
            kill(Pid, 9);
            waitpid(Pid, &status, 0);
        }
#endif
        return status;
    }

    IOutputStream& GetStdin() {
        return *Stdin;
    }

    IInputStream& GetStdout() {
        return *Stdout;
    }

    IInputStream& GetStderr() {
        return *Stderr;
    }

protected:
    TString ExeName;
    const TVector<TString> Args;
    const THashMap<TString, TString> Env;
    const TString WorkDir;

    THolder<TPipedOutput> Stdin;
    THolder<TPipedInput> Stdout;
    THolder<TPipedInput> Stderr;
    TVector<TString> EnvElems;
    TVector<char*> ExecArgs;
    TVector<char*> ExecEnv;

    int Pid = -1;

    virtual void PrepareForExec() {
        ExecArgs.resize(Args.size() + 1, nullptr);
        for (size_t i = 0; i < Args.size(); ++i) {
            ExecArgs[i] = const_cast<char*>(Args[i].c_str());
        }

        ExecEnv.resize(Env.size() + 1, nullptr);
        EnvElems.reserve(Env.size());
        size_t i = 0;
        for (const auto& [k, v] : Env) {
            EnvElems.push_back(k + "=" + v);
            ExecEnv[i++] = const_cast<char*>(EnvElems.back().c_str());
        }
    }

    virtual void Exec() {
        for (int i = 3; i < 32768; ++i) {
            close(i);
        }

        if (!WorkDir.empty()) {
            NFs::SetCurrentWorkingDirectory(WorkDir);
        }

        if (execve(ExeName.c_str(), ExecArgs.data(), ExecEnv.data()) == -1) {
            ythrow TSystemError() << "Cannot execl";
        }
    }
};

/*______________________________________________________________________________________________*/

struct TProcessHolder {
    TProcessHolder()
        : Watcher(MakeHolder<TThread>([this] () { Watch(); }))
    {
        Running.test_and_set();
        Watcher->Start();
    }

    ~TProcessHolder()
    {
        Running.clear();
        Watcher->Join();
    }

    i64 Size() {
        TGuard<TMutex> lock(Mutex);
        return Processes.size();
    }

    void Put(const TString& key, THolder<TChildProcess>process) {
        TGuard<TMutex> lock(Mutex);
        Processes.emplace_back(key, std::move(process));
    }

    THolder<TChildProcess> Acquire(const TString& key, TList<THolder<TChildProcess>>* stopList) {
        TGuard<TMutex> lock(Mutex);
        THolder<TChildProcess> result;
        while (!Processes.empty()) {
            auto first = std::move(Processes.front());
            Processes.pop_front();
            if (first.first == key) {
                result = std::move(first.second);
                break;
            }
            stopList->push_back(std::move(first.second));
        }
        return result;
    }

    void Watch() {
        while (Running.test()) {
            TList<THolder<TChildProcess>> stopList;
            {
                TGuard<TMutex> lock(Mutex);
                auto it = Processes.begin();
                while (it != Processes.end()) {
                    if (!it->second->IsAlive()) {
                        YQL_CLOG(DEBUG, ProviderDq) << "Remove dead process";
                        stopList.emplace_back(std::move(it->second));
                        it = Processes.erase(it);
                    } else {
                        ++it;
                    }
                }
            }

            for (const auto& job : stopList) {
                job->Kill();
                job->Wait(TDuration::Seconds(1));
            }
            Sleep(TDuration::MilliSeconds(1000));
        }
    }

    THolder<TThread> Watcher;
    std::atomic_flag Running;

    TMutex Mutex;
    TList<std::pair<TString, THolder<TChildProcess>>> Processes;
};

struct TPortoSettings {
    bool Enable;
    TMaybe<ui64> MemoryLimit;
    TString Layer;
    TString ContainerNamePrefix;

    bool operator == (const TPortoSettings& o) const {
        return Enable == o.Enable && MemoryLimit == o.MemoryLimit && Layer == o.Layer;
    }

    bool operator != (const TPortoSettings& o) const {
        return ! operator == (o);
    }

    TString ToString() const {
        return TStringBuilder() << Enable << "," << MemoryLimit << "," << Layer;
    }
};

class TPortoProcess: public TChildProcess
{
public:
    TPortoProcess(const TString& portoCtl, const TString& exeName, const TVector<TString>& args, const THashMap<TString, TString>& env, const TString& workDir, const TPortoSettings& portoSettings)
        : TChildProcess(exeName, args, env, workDir)
        , PortoCtl(portoCtl)
        , PortoLayer(portoSettings.Layer)
        , MemoryLimit(portoSettings.MemoryLimit)
        , ContainerName(WorkDir.substr(WorkDir.rfind("/") + 1))
        , InternalWorkDir_("mnt/work")
        , InternalExeDir("usr/local/bin")
        , TmpDir("TmpDir" + ContainerName)
    {
        NFs::MakeDirectory(TmpDir);
        auto pos = ExeName.rfind("/");
        TString name = ExeName.substr(pos + 1);

        if (portoSettings.ContainerNamePrefix) {
            ContainerName = portoSettings.ContainerNamePrefix + "/" + ContainerName;
        }

        YQL_CLOG(DEBUG, ProviderDq) << "HardLink " << ExeName << " -> '" << WorkDir << "/" << name << "'";
        if (NFs::HardLink(ExeName, WorkDir + "/" + name)) {
            ExeName = WorkDir + "/" + name;
        } else {
            YQL_CLOG(DEBUG, ProviderDq) << "HardLink Failed " << ExeName << "'" << WorkDir << "/" << name << "'";
        }
    }

    TString InternalWorkDir() override {
        return InternalWorkDir_;
    }

    ~TPortoProcess() {
        try {
            NFs::RemoveRecursive(TmpDir);
        } catch (...) {
            YQL_CLOG(DEBUG, ProviderDq) << "Error on TmpDir cleanup: " << CurrentExceptionMessage();
        }
    }

private:

    TString GetPortoSetting(const TString& name) const {
        TShellCommand cmd(PortoCtl, {"get", ContainerName, name});
        cmd.Run().Wait();
        return Strip(cmd.GetOutput());
    }

    void SetPortoSetting(const TString& name, const TString& value) const {
        TShellCommand cmd(PortoCtl, {"set", ContainerName, name, value});
        cmd.Run().Wait();
    }

    void Kill() override {
        if (MemoryLimit) {
            try {
                // see YQL-13760
                i64 anonLimit = -1;
                i64 anonUsage = -1;
                if (auto val = GetPortoSetting("anon_limit")) {
                    if (!TryFromString(val, anonLimit)) {
                        anonLimit = -1;
                    }
                }
                if (auto val = GetPortoSetting("anon_usage")) {
                    if (!TryFromString(val, anonUsage)) {
                        anonUsage = -1;
                    }
                }
                if (anonLimit != -1 && anonUsage != -1 &&  anonUsage >= anonLimit) {
                    SetPortoSetting("anon_limit", ToString(anonUsage + 1_MB));
                }
            } catch (...) {
                YQL_CLOG(DEBUG, ProviderDq) << "Cannot set anon_limit: " << CurrentExceptionMessage();
            }
        }

        try {
            TShellCommand cmd(PortoCtl, {"destroy", ContainerName});
            cmd.Run().Wait();
        } catch (...) {
            YQL_CLOG(DEBUG, ProviderDq) << "Cannot destroy: " << CurrentExceptionMessage();
        }
        TChildProcess::Kill();
    }

    void PrepareForExec() override {
        auto pos = ExeName.rfind("/");
        TString exeDir = ExeName.substr(0, pos);
        TString exeName = ExeName.substr(pos + 1);

        TStringBuilder command;
        command << InternalExeDir << '/' << exeName << ' ';
        for (size_t i = 1; i < Args.size(); ++i) {
            command << Args[i] << ' ';
        }

        TString caps = "CHOWN;DAC_OVERRIDE;FOWNER;KILL;SETPCAP;IPC_LOCK;SYS_CHROOT;SYS_PTRACE;MKNOD;AUDIT_WRITE;SETFCAP";
        TStringBuilder env;
        for (const auto& [k, v] : Env) {
            env << k << '=' << v << ';';
        }
        env << "TMPDIR=/tmp;";

        TStringBuilder bind;
        bind << WorkDir << ' ' << InternalWorkDir() << " rw;"; // see YQL-11392
        bind << exeDir << ' ' << InternalExeDir << " ro;";
        bind << TmpDir << " /tmp rw;";

        ArgsElems = TVector<TString>{
            "portoctl",
            "exec",
            "-L",
            PortoLayer.empty() ? "/" : PortoLayer,
            ContainerName,
            "command=" + command,
            "capabilities=" + caps,
            "env=" + env,
            "bind=" + bind,
            "root_readonly=true",
            "weak=false",
            "cpu_policy=idle"
        };
        if (MemoryLimit) {
            ArgsElems.push_back("anon_limit=" + ToString(*MemoryLimit));
        }
        ExecArgs.resize(ArgsElems.size() + 1, nullptr);
        for (size_t i = 0; i < ArgsElems.size(); ++i) {
            ExecArgs[i] = const_cast<char*>(ArgsElems[i].c_str());
        }
    }

    void Exec() override {
        for (int i = 3; i < 32768; ++i) {
            close(i);
        }

        if (execvp(PortoCtl.c_str(), ExecArgs.data()) == -1) {
            ythrow TSystemError() << "Cannot execl";
        }
    }

    const TString PortoCtl;
    const TString PortoLayer;
    const TMaybe<ui64> MemoryLimit;
    TString ContainerName;

    const TString InternalWorkDir_;
    const TString InternalExeDir;
    const TString TmpDir;
    TVector<TString> ArgsElems;
};

void LoadFromProto(TDqAsyncStats& stats, const NYql::NDqProto::TDqAsyncBufferStats& f)
{
    stats.Bytes = f.GetBytes();
    stats.DecompressedBytes = f.GetDecompressedBytes();
    stats.Rows = f.GetRows();
    stats.Chunks = f.GetChunks();
    stats.Splits = f.GetSplits();

    stats.FirstMessageTs = TInstant::MilliSeconds(f.GetFirstMessageMs());
    stats.PauseMessageTs = TInstant::MilliSeconds(f.GetPauseMessageMs());
    stats.ResumeMessageTs = TInstant::MilliSeconds(f.GetResumeMessageMs());
    stats.LastMessageTs = TInstant::MilliSeconds(f.GetLastMessageMs());
    stats.WaitTime = TDuration::MicroSeconds(f.GetWaitTimeUs());
    stats.WaitPeriods = f.GetWaitPeriods();
}

/*______________________________________________________________________________________________*/


class IPipeTaskRunner: public ITaskRunner {
public:
    virtual IOutputStream& GetOutput() = 0;
    virtual IInputStream& GetInput() = 0;
    [[noreturn]]
    virtual void RaiseException() = 0;
};

/*______________________________________________________________________________________________*/

class TInputChannel : public IInputChannel {
public:
    TInputChannel(ITaskRunner* taskRunner, ui64 taskId, ui64 channelId, IInputStream& input, IOutputStream& output, i64 channelBufferSize)
        : TaskId(taskId)
        , ChannelId(channelId)
        , Input(input)
        , Output(output)
        , TaskRunner(taskRunner)
        , FreeSpace(channelBufferSize)
    { }

    i64 GetFreeSpace() override {
        int protocolVersion = TaskRunner->GetProtocolVersion();
        if (protocolVersion <= 1) {
            return std::numeric_limits<i64>::max();
        }
        
        if (protocolVersion < 6) {
            NDqProto::TCommandHeader header;
            header.SetVersion(2);
            header.SetCommand(NDqProto::TCommandHeader::GET_FREE_SPACE);
            header.SetTaskId(TaskId);
            header.SetChannelId(ChannelId);
            header.Save(&Output);

            NDqProto::TGetFreeSpaceResponse response;
            response.Load(&Input);
            return response.GetFreeSpace();
        }

        return FreeSpace;
    }

    void SetFreeSpace(i64 space) {
        FreeSpace = space;
    }

    void Push(TDqSerializedBatch&& data) override {
        NDqProto::TCommandHeader header;
        header.SetVersion(1);
        header.SetCommand(NDqProto::TCommandHeader::PUSH);
        header.SetTaskId(TaskId);
        header.SetChannelId(ChannelId);
        header.Save(&Output);

        i64 written = 0; 
        TCountingOutput countingOutput(&Output);
        data.Proto.Save(&countingOutput);
        if (data.IsOOB()) {
            written = SaveRopeToPipe(Output, data.Payload);
        } else {
            written = countingOutput.Counter();
        }

        if (TaskRunner->GetProtocolVersion() >= 6) {
            // estimate free space
            FreeSpace -= written;
        }
    }

    void Finish() override {
        NDqProto::TCommandHeader header;
        header.SetVersion(1);
        header.SetCommand(NDqProto::TCommandHeader::FINISH);
        header.SetTaskId(TaskId);
        header.SetChannelId(ChannelId);
        header.Save(&Output);
    }

private:
    ui64 TaskId;
    ui64 ChannelId;

    IInputStream& Input;
    IOutputStream& Output;

    ITaskRunner* TaskRunner; // this channel is owned by TaskRunner

    i64 FreeSpace;
};

class TDqInputChannel: public IDqInputChannel {
public:
    TDqInputChannel(const IInputChannel::TPtr& channel, ui64 taskId, ui64 channelId, ui32 srcStageId, IPipeTaskRunner* taskRunner)
        : Delegate(channel)
        , TaskId(taskId)
        , ChannelId(channelId)
        , TaskRunner(taskRunner)
        , Input(TaskRunner->GetInput())
        , Output(TaskRunner->GetOutput())
    {
        PushStats.ChannelId = channelId;
        PushStats.SrcStageId = srcStageId;
    }

    ui64 GetChannelId() const override {
        return ChannelId;
    }

    const TDqInputChannelStats& GetPushStats() const override {
        return PushStats;
    }

    const TDqInputStats& GetPopStats() const override {
        return PopStats;
    }

    i64 GetFreeSpace() const override {
        try {
            return Delegate->GetFreeSpace();
        } catch (...) {
            TaskRunner->RaiseException();
        }
    }

    ui64 GetStoredBytes() const override {
        try {
            NDqProto::TCommandHeader header;
            header.SetVersion(3);
            header.SetCommand(NDqProto::TCommandHeader::GET_STORED_BYTES);
            header.SetTaskId(TaskId);
            header.SetChannelId(ChannelId);
            header.Save(&Output);

            NDqProto::TGetStoredBytesResponse response;
            response.Load(&Input);

            return response.GetResult();
        } catch (...) {
            TaskRunner->RaiseException();
        }
    }

    bool Empty() const override {
        ythrow yexception() << "unimplemented";
    }

    void Push(TDqSerializedBatch&& data) override {
        try {
            return Delegate->Push(std::move(data));
        } catch (...) {
            TaskRunner->RaiseException();
        }
    }

    [[nodiscard]]
    bool Pop(NKikimr::NMiniKQL::TUnboxedValueBatch& batch) override {
        Y_UNUSED(batch);
        ythrow yexception() << "unimplemented";
    }

    void Finish() override {
        try {
            Delegate->Finish();
        } catch (...) {
            TaskRunner->RaiseException();
        }
    }

    bool IsFinished() const override {
        ythrow yexception() << "unimplemented";
    }

    NKikimr::NMiniKQL::TType* GetInputType() const override {
        ythrow yexception() << "unimplemented";
    }

    void Pause() override {
        Y_ABORT("Checkpoints are not supported");
    }

    void Resume() override {
        Y_ABORT("Checkpoints are not supported");
    }

    bool IsPaused() const override {
        return false;
    }

    template<typename T>
    void FromProto(const T& f)
    {
        YQL_ENSURE(PushStats.ChannelId == f.GetChannelId());
        LoadFromProto(PushStats, f.GetPush());
        PushStats.MaxMemoryUsage = f.GetMaxMemoryUsage();
        PushStats.DeserializationTime = TDuration::MicroSeconds(f.GetDeserializationTimeUs());
        LoadFromProto(PopStats, f.GetPop());
    }

private:
    IInputChannel::TPtr Delegate;
    ui64 TaskId;
    ui64 ChannelId;

    IPipeTaskRunner* TaskRunner;
    IInputStream& Input;
    IOutputStream& Output;
    TDqInputChannelStats PushStats;
    TDqInputStats PopStats;
};

class TDqSource: public IDqAsyncInputBuffer {
public:
    TDqSource(ui64 taskId, ui64 inputIndex, TType* inputType, i64 channelBufferSize, IPipeTaskRunner* taskRunner)
        : TaskId(taskId)
        , TaskRunner(taskRunner)
        , Input(TaskRunner->GetInput())
        , Output(TaskRunner->GetOutput())
        , InputType(inputType)
        , BufferSize(channelBufferSize)
        , FreeSpace(channelBufferSize)
    {
        PushStats.InputIndex = inputIndex;
    }

    i64 GetFreeSpace() const override {
        if (TaskRunner->GetProtocolVersion() < 6) {
            NDqProto::TCommandHeader header;
            header.SetVersion(4);
            header.SetCommand(NDqProto::TCommandHeader::GET_FREE_SPACE_SOURCE);
            header.SetTaskId(TaskId);
            header.SetChannelId(PushStats.InputIndex);
            header.Save(&Output);

            NDqProto::TGetFreeSpaceResponse response;
            response.Load(&Input);
            return response.GetFreeSpace();
        }

        return FreeSpace;
    }

    void SetFreeSpace(i64 space) {
        FreeSpace = space;
    }

    ui64 GetStoredBytes() const override {
        if (TaskRunner->GetProtocolVersion() < 6) {
            NDqProto::TCommandHeader header;
            header.SetVersion(4);
            header.SetCommand(NDqProto::TCommandHeader::GET_STORED_BYTES_SOURCE);
            header.SetTaskId(TaskId);
            header.SetChannelId(PushStats.InputIndex);
            header.Save(&Output);

            NDqProto::TGetStoredBytesResponse response;
            response.Load(&Input);

            return response.GetResult();
        }

        return BufferSize - FreeSpace;
    }

    const TDqAsyncInputBufferStats& GetPushStats() const override {
        return PushStats;
    }

    const TDqInputStats& GetPopStats() const override {
        return PopStats;
    }

    bool Empty() const override {
        ythrow yexception() << "unimplemented";
    }

    void Push(NDq::TDqSerializedBatch&& serialized, i64 space) override {
        NDqProto::TSourcePushRequest data;
        bool isOOB = serialized.IsOOB();
        *data.MutableData() = std::move(serialized.Proto);
        data.SetSpace(space);

        NDqProto::TCommandHeader header;
        header.SetVersion(4);
        header.SetCommand(NDqProto::TCommandHeader::PUSH_SOURCE);
        header.SetTaskId(TaskId);
        header.SetChannelId(PushStats.InputIndex);
        header.Save(&Output);
        data.Save(&Output);
        if (isOOB) {
            SaveRopeToPipe(Output, serialized.Payload);
        }

        if (TaskRunner->GetProtocolVersion() >= 6) {
            FreeSpace -= space;
        }
    }

    void Push(NKikimr::NMiniKQL::TUnboxedValueBatch&& batch, i64 space) override {
        auto inputType = GetInputType();
        TDqDataSerializer dataSerializer(TaskRunner->GetTypeEnv(), TaskRunner->GetHolderFactory(), NDqProto::DATA_TRANSPORT_UV_PICKLE_1_0);
        TDqSerializedBatch serialized = dataSerializer.Serialize(batch, inputType);
        Push(std::move(serialized), space);
    }

    [[nodiscard]]
    bool Pop(NKikimr::NMiniKQL::TUnboxedValueBatch& batch) override {
        Y_UNUSED(batch);
        ythrow yexception() << "unimplemented";
    }

    void Finish() override {
        NDqProto::TCommandHeader header;
        header.SetVersion(4);
        header.SetCommand(NDqProto::TCommandHeader::FINISH_SOURCE);
        header.SetTaskId(TaskId);
        header.SetChannelId(PushStats.InputIndex);
        header.Save(&Output);
    }

    bool IsFinished() const override {
        ythrow yexception() << "unimplemented";
    }

    ui64 GetInputIndex() const override {
        return PushStats.InputIndex;
    }

    NKikimr::NMiniKQL::TType* GetInputType() const override {
        return InputType;
    }

    void Pause() override {
        Y_ABORT("Checkpoints are not supported");
    }

    void Resume() override {
        Y_ABORT("Checkpoints are not supported");
    }

    bool IsPaused() const override {
        return false;
    }

    template<typename T>
    void FromProto(const T& f)
    {
        YQL_ENSURE(PushStats.InputIndex == f.GetInputIndex());
        LoadFromProto(PushStats, f.GetPush());
        PushStats.RowsInMemory = f.GetRowsInMemory();
        PushStats.MaxMemoryUsage = f.GetMaxMemoryUsage();
        LoadFromProto(PopStats, f.GetPop());
    }

private:
    ui64 TaskId;
    IPipeTaskRunner* TaskRunner;
    IInputStream& Input;
    IOutputStream& Output;
    mutable NKikimr::NMiniKQL::TType* InputType = nullptr;
    TDqAsyncInputBufferStats PushStats;
    TDqInputStats PopStats;
    i64 BufferSize;
    i64 FreeSpace;
};

/*______________________________________________________________________________________________*/

class TOutputChannel : public IOutputChannel {
public:
    TOutputChannel(ui64 taskId, ui64 channelId, IInputStream& input, IOutputStream& output)
        : TaskId(taskId)
        , ChannelId(channelId)
        , Input(input)
        , Output(output)
    { }

    [[nodiscard]]
    NDqProto::TPopResponse Pop(TDqSerializedBatch& data) override {
        NDqProto::TCommandHeader header;
        header.SetVersion(1);
        header.SetCommand(NDqProto::TCommandHeader::POP);
        header.SetTaskId(TaskId);
        header.SetChannelId(ChannelId);
        header.Save(&Output);

        NDqProto::TPopResponse response;
        response.Load(&Input);
        data.Clear();
        data.Proto = std::move(*response.MutableData());
        if (data.IsOOB()) {
            LoadRopeFromPipe(Input, data.Payload);
        }
        return response;
    }

    bool IsFinished() const override {
        if (Finished) {
            return true;
        }
        NDqProto::TCommandHeader header;
        header.SetVersion(1);
        header.SetCommand(NDqProto::TCommandHeader::IS_FINISHED);
        header.SetTaskId(TaskId);
        header.SetChannelId(ChannelId);
        header.Save(&Output);

        NDqProto::TIsFinishedResponse response;
        response.Load(&Input);
        Finished = response.GetResult();
        return Finished;
    }

private:
    mutable bool Finished = false;
    ui64 TaskId;
    ui64 ChannelId;

    IInputStream& Input;
    IOutputStream& Output;
};

class TDqOutputChannel: public IDqOutputChannel {
public:
    TDqOutputChannel(const IOutputChannel::TPtr& channel, ui64 taskId, ui64 channelId, ui32 dstStageId, IPipeTaskRunner* taskRunner)
        : Delegate(channel)
        , TaskId(taskId)
        , TaskRunner(taskRunner)
        , Input(TaskRunner->GetInput())
        , Output(TaskRunner->GetOutput())
    {
        Y_UNUSED(Input);
        PopStats.ChannelId = channelId;
        PopStats.DstStageId = dstStageId;
    }

    ui64 GetChannelId() const override {
        return PopStats.ChannelId;
    }

    ui64 GetValuesCount() const override {
        ythrow yexception() << "unimplemented";
    }

    const TDqOutputStats& GetPushStats() const override {
        return PushStats;
    }

    const TDqOutputChannelStats& GetPopStats() const override {
        return PopStats;
    }

    // <| producer methods
    [[nodiscard]]
    bool IsFull() const override {
        ythrow yexception() << "unimplemented";
    };

    // can throw TDqChannelStorageException
    void Push(NUdf::TUnboxedValue&& value) override {
        Y_UNUSED(value);
        ythrow yexception() << "unimplemented";
    }

    void WidePush(NUdf::TUnboxedValue* values, ui32 count) override {
        Y_UNUSED(values);
        Y_UNUSED(count);
        ythrow yexception() << "unimplemented";
    }

    void Push(NDqProto::TWatermark&& watermark) override {
        Y_UNUSED(watermark);
        ythrow yexception() << "unimplemented";
    }

    void Push(NDqProto::TCheckpoint&& checkpoint) override {
        Y_UNUSED(checkpoint);
        ythrow yexception() << "unimplemented";
    }

    void Finish() override {
        try {
            NDqProto::TCommandHeader header;
            header.SetVersion(3);
            header.SetCommand(NDqProto::TCommandHeader::FINISH_OUTPUT);
            header.SetTaskId(TaskId);
            header.SetChannelId(PopStats.ChannelId);
            header.Save(&Output);
        } catch (...) {
            TaskRunner->RaiseException();
        }
    }
    // |>

    // <| consumer methods

    bool HasData() const override {
        ythrow yexception() << "unimplemented";
    }

    bool IsFinished() const override {
        try {
            return Delegate->IsFinished();
        } catch (...) {
            TaskRunner->RaiseException();
        }
    }
    // can throw TDqChannelStorageException
    [[nodiscard]]
    bool Pop(TDqSerializedBatch& data) override {
        try {
            auto response = Delegate->Pop(data);
            return response.GetResult();
        } catch (...) {
            TaskRunner->RaiseException();
        }
    }

    bool Pop(NDqProto::TWatermark&) override {
        return false;
    }

    bool Pop(NDqProto::TCheckpoint&) override {
        return false;
    }

    // Only for data-queries
    // TODO: remove this method and create independent Data- and Stream-query implementations.
    //       Stream-query implementation should be without PopAll method.
    //       Data-query implementation should be one-shot for Pop (a-la PopAll) call and without ChannelStorage.
    // can throw TDqChannelStorageException
    [[nodiscard]]
    bool PopAll(TDqSerializedBatch& data) override {
        Y_UNUSED(data);
        ythrow yexception() << "unimplemented";
    }
    // |>

    ui64 Drop() override {
        try {
            NDqProto::TCommandHeader header;
            header.SetVersion(3);
            header.SetCommand(NDqProto::TCommandHeader::DROP_OUTPUT);
            header.SetTaskId(TaskId);
            header.SetChannelId(PopStats.ChannelId);
            header.Save(&Output);

            NDqProto::TDropOutputResponse response;
            response.Load(&Input);

            return response.GetResult();
        } catch (...) {
            TaskRunner->RaiseException();
        }
    }

    NKikimr::NMiniKQL::TType* GetOutputType() const override {
        Y_ABORT("Unimplemented");
        return nullptr;
    }

    void Terminate() override {
        try {
            NDqProto::TCommandHeader header;
            header.SetVersion(3);
            header.SetCommand(NDqProto::TCommandHeader::TERMINATE_OUTPUT);
            header.SetTaskId(TaskId);
            header.SetChannelId(PopStats.ChannelId);
            header.Save(&Output);
        } catch (...) {
            TaskRunner->RaiseException();
        }
    }

    template<typename T>
    void FromProto(const T& f)
    {
        YQL_ENSURE(PopStats.ChannelId == f.GetChannelId());
        LoadFromProto(PushStats, f.GetPush());
        LoadFromProto(PopStats, f.GetPop());
        PopStats.MaxMemoryUsage = f.GetMaxMemoryUsage();
    }

private:
    IOutputChannel::TPtr Delegate;
    ui64 TaskId;
    IPipeTaskRunner* TaskRunner;
    IInputStream& Input;
    IOutputStream& Output;
    TDqOutputStats PushStats;
    TDqOutputChannelStats PopStats;
};

class TDqSink : public IDqAsyncOutputBuffer {
public:
    TDqSink(ui64 taskId, ui64 outputIndex, TType* type, IPipeTaskRunner* taskRunner)
        : TaskId(taskId)
        , TaskRunner(taskRunner)
        , Input(TaskRunner->GetInput())
        , Output(TaskRunner->GetOutput())
        , OutputType(type)
    {
        PopStats.OutputIndex = outputIndex;
    }

    ui64 GetOutputIndex() const override {
        return PopStats.OutputIndex;
    }

    const TDqOutputStats& GetPushStats() const override {
        return PushStats;
    }
    
    const TDqAsyncOutputBufferStats& GetPopStats() const override {
        return PopStats;
    }

    ui64 Pop(NDq::TDqSerializedBatch& batch, ui64 bytes) override {
        try {
            NDqProto::TCommandHeader header;
            header.SetVersion(5);
            header.SetCommand(NDqProto::TCommandHeader::SINK_POP);
            header.SetTaskId(TaskId);
            header.SetChannelId(PopStats.OutputIndex);
            header.Save(&Output);

            NDqProto::TSinkPopRequest request;
            request.SetBytes(bytes);
            request.Save(&Output);

            NDqProto::TSinkPopResponse response;
            response.Load(&Input);

            batch.Proto = std::move(*response.MutableData());
            if (batch.IsOOB()) {
                LoadRopeFromPipe(Input, batch.Payload);
            }
            return response.GetBytes();
        } catch (...) {
            TaskRunner->RaiseException();
        }
    }

    ui64 Pop(NKikimr::NMiniKQL::TUnboxedValueBatch& batch, ui64 bytes) override {
        Y_UNUSED(batch);
        Y_UNUSED(bytes);
        Y_ABORT("Unimplemented");
        return 0;
    }

    bool IsFinished() const override {
        try {
            NDqProto::TCommandHeader header;
            header.SetVersion(5);
            header.SetCommand(NDqProto::TCommandHeader::SINK_IS_FINISHED);
            header.SetTaskId(TaskId);
            header.SetChannelId(PopStats.OutputIndex);
            header.Save(&Output);

            NDqProto::TIsFinishedResponse response;
            response.Load(&Input);

            return response.GetResult();
        } catch (...) {
            TaskRunner->RaiseException();
        }
    }

    NKikimr::NMiniKQL::TType* GetOutputType() const override {
        return OutputType;
    }

    void Finish() override {
        Y_ABORT("Unimplemented");
    }

    bool Pop(NDqProto::TWatermark& watermark) override {
        Y_UNUSED(watermark);
        Y_ABORT("Watermarks are not supported");
    }

    bool Pop(NDqProto::TCheckpoint& checkpoint) override {
        Y_UNUSED(checkpoint);
        Y_ABORT("Checkpoints are not supported");
    }

    bool IsFull() const override {
        Y_ABORT("Unimplemented");
    }

    void Push(NUdf::TUnboxedValue&& value) override {
        Y_UNUSED(value);
        Y_ABORT("Unimplemented");
    }

    void WidePush(NUdf::TUnboxedValue* values, ui32 count) override {
        Y_UNUSED(values);
        Y_UNUSED(count);
        Y_ABORT("Unimplemented");
    }

    void Push(NDqProto::TWatermark&& watermark) override {
        Y_UNUSED(watermark);
        Y_ABORT("Watermarks are not supported");
    }

    void Push(NDqProto::TCheckpoint&& checkpoint) override {
        Y_UNUSED(checkpoint);
        Y_ABORT("Checkpoints are not supported");
    }

    bool HasData() const override {
        Y_ABORT("Unimplemented");
    }

    template<typename T>
    void FromProto(const T& f)
    {
        YQL_ENSURE(PopStats.OutputIndex == f.GetOutputIndex());
        LoadFromProto(PopStats, f.GetPop());
        PopStats.MaxRowsInMemory = f.GetMaxRowsInMemory();
        PopStats.MaxMemoryUsage = f.GetMaxMemoryUsage();
        LoadFromProto(PushStats, f.GetPush());
    }

private:
    ui64 TaskId;
    IPipeTaskRunner* TaskRunner;

    IInputStream& Input;
    IOutputStream& Output;

    NKikimr::NMiniKQL::TType* OutputType;

    TDqOutputStats PushStats;
    TDqAsyncOutputBufferStats PopStats;
};

/*______________________________________________________________________________________________*/

class TTaskRunner: public IPipeTaskRunner {
public:
    TTaskRunner(
        std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc,
        const NDqProto::TDqTask& task,
        TFilesHolder::TPtr&& filesHolder,
        THolder<TChildProcess>&& command,
        ui64 stageId,
        const TString& traceId)
        : TraceId(traceId)
        , Task(task)
        , FilesHolder(std::move(filesHolder))
        , Alloc(alloc)
        , AllocatedHolder(std::make_optional<TAllocatedHolder>(*Alloc, "TDqTaskRunnerProxy"))
        , Running(true)
        , Command(std::move(command))
        , StderrReader(MakeHolder<TThread>([this] () { ReadStderr(); }))
        , Output(Command->GetStdin())
        , Input(Command->GetStdout())
        , TaskId(Task.GetId())
        , StageId(stageId)
    {
        StderrReader->Start();
        InitTaskMeta();
        InitChannels();
    }

    ~TTaskRunner() {
        auto guard = Guard(*Alloc);
        AllocatedHolder.reset();
        Command->Kill();
        Command->Wait(TDuration::Seconds(0));
    }

    void ReadStderr() {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(TraceId, ToString(StageId));
        auto& input = Command->GetStderr();
        char buf[1024];
        size_t size;
        while ((size = input.Read(buf, sizeof(buf))) > 0) {
            auto str = TString(buf, size);
            Stderr += str;
            YQL_CLOG(DEBUG, ProviderDq) << "stderr > `" << str << "`";
        }
        YQL_CLOG(DEBUG, ProviderDq) << "stderr finished";
    }

    ui64 GetTaskId() const override {
        return TaskId;
    }

    i32 GetProtocolVersion() override {
        if (ProtocolVersion < 0) {
            NDqProto::TCommandHeader header;
            header.SetVersion(1);
            header.SetCommand(NDqProto::TCommandHeader::VERSION);
            header.Save(&Output);

            NDqProto::TGetVersionResponse response;
            response.Load(&Input);

            ProtocolVersion = response.GetVersion();
        }

        return ProtocolVersion;
    }

    NYql::NDqProto::TPrepareResponse Prepare(const NDq::TDqTaskRunnerMemoryLimits& limits) override {
        ChannelBufferSize = limits.ChannelBufferSize;
        NDqProto::TCommandHeader header;
        header.SetVersion(1);
        header.SetCommand(NDqProto::TCommandHeader::PREPARE);
        header.Save(&Output);

        NDqProto::TPrepareRequest request;
        *request.MutableTask() = Task;
        request.Save(&Output);

        NYql::NDqProto::TPrepareResponse ret;
        ret.Load(&Input);

        auto state = TFailureInjector::GetCurrentState();
        for (auto& [k, v]: state) {
            NDqProto::TCommandHeader header;
            header.SetVersion(1);
            header.SetCommand(NDqProto::TCommandHeader::CONFIGURE_FAILURE_INJECTOR);
            header.Save(&Output);
            NYql::NDqProto::TConfigureFailureInjectorRequest request;
            request.SetName(k);
            request.SetSkip(v.Skip);
            request.SetFail(v.CountOfFails);
            request.Save(&Output);
        }

        return ret;
    }

    NYql::NDqProto::TRunResponse Run() override {
        NDqProto::TCommandHeader header;
        header.SetVersion(GetProtocolVersion() >= 6 ? 6 : 1);
        header.SetCommand(NDqProto::TCommandHeader::RUN);
        header.SetTaskId(Task.GetId());
        header.Save(&Output);

        NDqProto::TRunResponse response;
        response.Load(&Input);
        if (GetProtocolVersion() >= 6) {
            for (auto& space : response.GetChannelFreeSpace()) {
                auto channel = InputChannels.find(space.GetId()); YQL_ENSURE(channel != InputChannels.end());
                channel->second->SetFreeSpace(space.GetSpace());
            }
            for (auto& space : response.GetSourceFreeSpace()) {
                auto source = Sources.find(space.GetId()); YQL_ENSURE(source != Sources.end());
                source->second->SetFreeSpace(space.GetSpace());
            }
        }
        return response;
    }

    IInputChannel::TPtr GetInputChannel(ui64 channelId) override {
        auto channel = InputChannels.find(channelId);
        YQL_ENSURE(channel != InputChannels.end());
        return channel->second;
    }

    IOutputChannel::TPtr GetOutputChannel(ui64 channelId) override {
        return new TOutputChannel(Task.GetId(), channelId, Input, Output);
    }

    IDqAsyncInputBuffer* GetSource(ui64 index) override {
        auto source = Sources.find(index);
        YQL_ENSURE(source != Sources.end());
        return source->second.Get();
    }

    TDqSink::TPtr GetSink(ui64 index) override {
        return new TDqSink(Task.GetId(), index, OutputTypes.at(index), this);
    }

    const NMiniKQL::TTypeEnvironment& GetTypeEnv() const override {
        return AllocatedHolder->TypeEnv;
    }

    const NMiniKQL::THolderFactory& GetHolderFactory() const override {
        return AllocatedHolder->HolderFactory;
    }

    NMiniKQL::TScopedAlloc& GetAllocator() const {
        return *Alloc.get();
    }

    const THashMap<TString, TString>& GetSecureParams() const override {
        return SecureParams;
    }

    const THashMap<TString, TString>& GetTaskParams() const override {
        return TaskParams;
    }

    const TVector<TString>& GetReadRanges() const override {
        return ReadRanges;
    }

    TGuard<NKikimr::NMiniKQL::TScopedAlloc> BindAllocator(TMaybe<ui64> memoryLimit) override {
        auto guard = AllocatedHolder->TypeEnv.BindAllocator();
        if (memoryLimit) {
            guard.GetMutex()->SetLimit(*memoryLimit);
        }
        return guard;
    }

    bool IsAllocatorAttached() override {
        return AllocatedHolder->TypeEnv.GetAllocator().IsAttached();
    }

    void Kill() override {
        bool expected = true;
        if (!Running.compare_exchange_strong(expected, false)) {
            return;
        }

        Command->Kill();
        Command->Wait(TDuration::Seconds(0));
        StderrReader->Join();
    }

    TStatus GetStatus() override {
        bool expected = true;
        if (!Running.compare_exchange_strong(expected, false)) {
            return {Code, Stderr};
        }

        try {
            NDqProto::TCommandHeader header;
            header.SetVersion(1);
            header.SetCommand(NDqProto::TCommandHeader::STOP);
            header.Save(&Output);
        } catch (...) { }
        Code = Command->Wait();
        StderrReader->Join();
        return {Code, Stderr};
    }

    [[noreturn]]
    void RaiseException() override {
        auto status = GetStatus();
        TString message;
        message += "ExitCode: " + ToString(status.ExitCode) + "\n";
        message += "Stderr:\n" + status.Stderr + "\n";
        ythrow yexception() << message;
    }

    IOutputStream& GetOutput() override {
        return Output;
    }

    IInputStream& GetInput() override {
        return Input;
    }

private:
    void InitTaskMeta() {
        Yql::DqsProto::TTaskMeta taskMeta;
        Task.GetMeta().UnpackTo(&taskMeta);

        for (const auto& x : taskMeta.GetSecureParams()) {
            SecureParams[x.first] = x.second;
        }

        for (const auto& x : taskMeta.GetTaskParams()) {
            TaskParams[x.first] = x.second;
        }

        for (const auto& readRange : taskMeta.GetReadRanges()) {
            ReadRanges.push_back(readRange);
        }

        {
            auto guard = BindAllocator({});
            ProgramNode = DeserializeRuntimeNode(Task.GetProgram().GetRaw(), GetTypeEnv()); 
        }

        auto& programStruct = static_cast<TStructLiteral&>(*ProgramNode.GetNode());
        auto programType = programStruct.GetType();
        YQL_ENSURE(programType);
        auto programRootIdx = programType->FindMemberIndex("Program");
        YQL_ENSURE(programRootIdx);
        TRuntimeNode programRoot = programStruct.GetValue(*programRootIdx);

        auto programInputsIdx = programType->FindMemberIndex("Inputs");
        YQL_ENSURE(programInputsIdx);
        TRuntimeNode programInputs = programStruct.GetValue(*programInputsIdx);
        YQL_ENSURE(programInputs.IsImmediate() && programInputs.GetNode()->GetType()->IsTuple());
        auto& programInputsTuple = static_cast<TTupleLiteral&>(*programInputs.GetNode());
        auto programInputsCount = programInputsTuple.GetValuesCount();

        InputTypes.resize(programInputsCount);

        for (ui32 i = 0; i < programInputsCount; ++i) {
            auto input = programInputsTuple.GetValue(i);
            TType* type = input.GetStaticType();
            YQL_ENSURE(type->GetKind() == TType::EKind::Stream);
            TType* inputType = static_cast<TStreamType&>(*type).GetItemType();

            InputTypes[i] = inputType;
        }

        OutputTypes.resize(Task.OutputsSize());
        if (programRoot.GetNode()->GetType()->IsCallable()) {
            auto programResultType = static_cast<const TCallableType*>(programRoot.GetNode()->GetType());
            YQL_ENSURE(programResultType->GetReturnType()->IsStream());
            auto programResultItemType = static_cast<const TStreamType*>(programResultType->GetReturnType())->GetItemType();

            if (programResultItemType->IsVariant()) {
                auto variantType = static_cast<const TVariantType*>(programResultItemType);
                YQL_ENSURE(variantType->GetUnderlyingType()->IsTuple());
                auto variantTupleType = static_cast<const TTupleType*>(variantType->GetUnderlyingType());
                YQL_ENSURE(Task.OutputsSize() == variantTupleType->GetElementsCount(),
                    "" << Task.OutputsSize() << " != " << variantTupleType->GetElementsCount());
                for (ui32 i = 0; i < variantTupleType->GetElementsCount(); ++i) {
                    OutputTypes[i] = variantTupleType->GetElementType(i);
                }
            }
            else {
                YQL_ENSURE(Task.OutputsSize() == 1);
                OutputTypes[0] = programResultItemType;
            }
        } else {
            YQL_ENSURE(programRoot.GetNode()->GetType()->IsVoid());
            YQL_ENSURE(Task.OutputsSize() == 0);
        }
    }

    void InitChannels() {
        for (ui32 i = 0; i < Task.InputsSize(); ++i) {
            auto& inputDesc = Task.GetInputs(i);
            if (inputDesc.HasSource()) {
                Sources[i] = new TDqSource(Task.GetId(), i, InputTypes.at(i), ChannelBufferSize, this);
            } else {
                for (auto& inputChannelDesc : inputDesc.GetChannels()) {
                    ui64 channelId = inputChannelDesc.GetId();
                    InputChannels[channelId] = new TInputChannel(this, Task.GetId(), channelId, Input, Output, ChannelBufferSize);
                }
            }
        }
    }

private:
    const TString TraceId;
    NDqProto::TDqTask Task;
    TFilesHolder::TPtr FilesHolder;
    THashMap<TString, TString> SecureParams;
    THashMap<TString, TString> TaskParams;
    TVector<TString> ReadRanges;
    THashMap<ui64, TIntrusivePtr<TInputChannel>> InputChannels;
    THashMap<ui64, TIntrusivePtr<TDqSource>> Sources;
    i64 ChannelBufferSize = 0;

    std::shared_ptr <NKikimr::NMiniKQL::TScopedAlloc> Alloc;

    struct TAllocatedHolder {
        TAllocatedHolder(NKikimr::NMiniKQL::TScopedAlloc& alloc, const TStringBuf& memInfoTitle)
            : TypeEnv(alloc)
            , MemInfo(memInfoTitle)
            , HolderFactory(alloc.Ref(), MemInfo) {}

        NKikimr::NMiniKQL::TTypeEnvironment TypeEnv;
        NKikimr::NMiniKQL::TMemoryUsageInfo MemInfo;
        NKikimr::NMiniKQL::THolderFactory HolderFactory;
    };

    std::optional<TAllocatedHolder> AllocatedHolder;

    std::atomic<bool> Running;
    int Code = -1;
    TString Stderr;
    THolder<TChildProcess> Command;
    THolder<TThread> StderrReader;
    IOutputStream& Output;
    IInputStream& Input;

    i32 ProtocolVersion = -1;
    const ui64 TaskId;
    const ui64 StageId;

    NKikimr::NMiniKQL::TRuntimeNode ProgramNode;
    std::vector<TType*> InputTypes;
    std::vector<TType*> OutputTypes;
};

class TDqTaskRunner: public NDq::IDqTaskRunner {
public:
    TDqTaskRunner(
        std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc,
        const NDqProto::TDqTask& task,
        TFilesHolder::TPtr&& filesHolder,
        THolder<TChildProcess>&& command,
        ui64 stageId,
        const TString& traceId)
        : Delegate(new TTaskRunner(alloc, task, std::move(filesHolder), std::move(command), stageId, traceId))
        , Task(task)
    { }

    ui64 GetTaskId() const override {
        return Task.GetId();
    }

    void SetSpillerFactory(std::shared_ptr<ISpillerFactory>) override {
    }

    void Prepare(const TDqTaskSettings& task, const TDqTaskRunnerMemoryLimits& memoryLimits,
        const IDqTaskRunnerExecutionContext& execCtx) override
    {
        Y_UNUSED(execCtx);
        Y_ABORT_UNLESS(Task.GetId() == task.GetId());
        try {
            auto result = Delegate->Prepare(memoryLimits);
            Y_UNUSED(result);
        } catch (...) {
            Delegate->RaiseException();
        }
    }

    ERunStatus Run() override
    {
        try {
            auto response = Delegate->Run();
            return static_cast<NDq::ERunStatus>(response.GetResult());
        } catch (...) {
            Delegate->RaiseException();
        }
    }

    bool HasEffects() const override
    {
        return false;
    }

    IDqInputChannel::TPtr GetInputChannel(ui64 channelId) override
    {
        auto& channel = InputChannels[channelId];
        if (!channel) {
            channel = new TDqInputChannel(
                Delegate->GetInputChannel(channelId),
                Task.GetId(),
                channelId,
                Task.GetStageId(),
                Delegate.Get());
            Stats.InputChannels[Task.GetStageId()][channelId] = channel;
        }
        return channel;
    }

    IDqAsyncInputBuffer::TPtr GetSource(ui64 inputIndex) override {
        return Delegate->GetSource(inputIndex);
    }

    IDqOutputChannel::TPtr GetOutputChannel(ui64 channelId) override
    {
        auto& channel = OutputChannels[channelId];
        if (!channel) {
            channel = new TDqOutputChannel(
                Delegate->GetOutputChannel(channelId),
                Task.GetId(),
                channelId,
                Task.GetStageId(),
                Delegate.Get());
            Stats.OutputChannels[Task.GetStageId()][channelId] = channel;
        }
        return channel;
    }

    IDqAsyncOutputBuffer::TPtr GetSink(ui64 outputIndex) override {
        auto& sink = Sinks[outputIndex];
        if (!sink) {
            sink = static_cast<TDqSink*>(Delegate->GetSink(outputIndex).Get());
        }
        return sink;
    }

    std::optional<std::pair<NUdf::TUnboxedValue, IDqAsyncInputBuffer::TPtr>> GetInputTransform(ui64 /*inputIndex*/) override {
        return {};
    }

    std::pair<IDqAsyncOutputBuffer::TPtr, IDqOutputConsumer::TPtr> GetOutputTransform(ui64 /*outputIndex*/) override {
        return {};
    }

    const NKikimr::NMiniKQL::TTypeEnvironment& GetTypeEnv() const override {
        return Delegate->GetTypeEnv();
    }

    const NKikimr::NMiniKQL::THolderFactory& GetHolderFactory() const override {
        return Delegate->GetHolderFactory();
    }

    NKikimr::NMiniKQL::TScopedAlloc& GetAllocator() const override {
        return Delegate->GetAllocator();
    }

    const THashMap<TString, TString>& GetSecureParams() const override {
        return Delegate->GetSecureParams();
    }

    const THashMap<TString, TString>& GetTaskParams() const override {
        return Delegate->GetTaskParams();
    }

    const TVector<TString>& GetReadRanges() const override {
        return Delegate->GetReadRanges();
    }

    TGuard<NKikimr::NMiniKQL::TScopedAlloc> BindAllocator(TMaybe<ui64> memoryLimit) override {
        return Delegate->BindAllocator(memoryLimit);
    }

    bool IsAllocatorAttached() override {
        return Delegate->IsAllocatorAttached();
    }

    IRandomProvider* GetRandomProvider() const override {
        return nullptr;
    }

    const TDqMeteringStats* GetMeteringStats() const override {
        try {
            NDqProto::TCommandHeader header;
            header.SetVersion(3);
            header.SetCommand(NDqProto::TCommandHeader::GET_METERING_STATS);
            header.SetTaskId(Task.GetId());
            header.Save(&Delegate->GetOutput());

            NDqProto::TMeteringStatsResponse response;
            response.Load(&Delegate->GetInput());

            MeteringStats.Inputs.clear();
            for (auto input : response.GetInputs()) {
                auto i = MeteringStats.AddInputs();
                i.RowsConsumed = input.GetRowsConsumed();
                i.BytesConsumed = input.GetBytesConsumed();
            }
            return &MeteringStats;
        } catch (...) {
            Delegate->RaiseException();
        }
    }

    const TDqTaskRunnerStats* GetStats() const override
    {
        try {
            NDqProto::TCommandHeader header;
            header.SetVersion(3);
            header.SetCommand(NDqProto::TCommandHeader::GET_STATS);
            header.SetTaskId(Task.GetId());
            header.Save(&Delegate->GetOutput());

            NDqProto::TGetStatsResponse response;
            response.Load(&Delegate->GetInput());

            auto& protoStats = response.GetStats();

            Stats.BuildCpuTime = TDuration::MicroSeconds(protoStats.GetBuildCpuTimeUs());
            Stats.ComputeCpuTime = TDuration::MicroSeconds(protoStats.GetComputeCpuTimeUs());
            // Stats.RunStatusTimeMetrics.Load(ERunStatus::PendingInput, TDuration::MicroSeconds(f.GetPendingInputTimeUs()));
            // Stats.RunStatusTimeMetrics.Load(ERunStatus::PendingOutput, TDuration::MicroSeconds(f.GetPendingOutputTimeUs()));
            // Stats.RunStatusTimeMetrics.Load(ERunStatus::Finished, TDuration::MicroSeconds(f.GetFinishTimeUs()));
            // Stats.TotalTime = TDuration::MilliSeconds(f.GetTotalTime());
            // Stats.WaitTime = TDuration::MicroSeconds(f.GetWaitTimeUs());
            // Stats.WaitOutputTime = TDuration::MicroSeconds(f.GetWaitOutputTimeUs());

            // Stats.MkqlTotalNodes = f.GetMkqlTotalNodes();
            // Stats.MkqlCodegenFunctions = f.GetMkqlCodegenFunctions();
            // Stats.CodeGenTotalInstructions = f.GetCodeGenTotalInstructions();
            // Stats.CodeGenTotalFunctions = f.GetCodeGenTotalFunctions();
            // Stats.CodeGenFullTime = f.GetCodeGenFullTime();
            // Stats.CodeGenFinalizeTime = f.GetCodeGenFinalizeTime();
            // Stats.CodeGenModulePassTime = f.GetCodeGenModulePassTime();

            for (const auto& input : protoStats.GetInputChannels()) {
                InputChannels[input.GetChannelId()]->FromProto(input);
            }

            for (const auto& output : protoStats.GetOutputChannels()) {
                OutputChannels[output.GetChannelId()]->FromProto(output);
            }

            // todo: (whcrc) fill sources and ComputeCpuTimeByRun?

            return &Stats;
        } catch (...) {
            Delegate->RaiseException();
        }
    }

    TString Save() const override {
        ythrow yexception() << "unimplemented";
    }

    void Load(TStringBuf in) override {
        Y_UNUSED(in);
        ythrow yexception() << "unimplemented";
    }

    void SetWatermarkIn(TInstant) override {
        ythrow yexception() << "unimplemented";
    }

    const NKikimr::NMiniKQL::TWatermark& GetWatermark() const override {
        ythrow yexception() << "unimplemented";
    }

private:
    TIntrusivePtr<TTaskRunner> Delegate;
    NDqProto::TDqTask Task;
    mutable TDqTaskRunnerStats Stats;
    mutable TDqMeteringStats MeteringStats;

    mutable THashMap<ui64, TIntrusivePtr<TDqInputChannel>> InputChannels;
    mutable THashMap<ui64, TIntrusivePtr<TDqOutputChannel>> OutputChannels;
    THashMap<ui64, TIntrusivePtr<TDqSink>> Sinks;
};

/*______________________________________________________________________________________________*/

class TPipeFactory: public IProxyFactory {
    struct TJob: public TTaskScheduler::ITask {
        TJob(NThreading::TPromise<void> p)
            : Promise(std::move(p))
        { }

        TInstant Process() override {
            Promise.SetValue();
            return TInstant::Max();
        }

        NThreading::TPromise<void> Promise;
    };

    struct TStopJob: public TTaskScheduler::ITask {
        TList<THolder<TChildProcess>> StopList;

        TStopJob(TList<THolder<TChildProcess>>&& stopList)
            : StopList(std::move(stopList))
        { }

        TInstant Process() override {
            for (const auto& job : StopList) {
                job->Kill();
                job->Wait(TDuration::Seconds(1));
            }

            return TInstant::Max();
        }
    };

public:
    TPipeFactory(const TPipeFactoryOptions& options)
        : ExePath(options.ExecPath)
        , EnablePorto(options.EnablePorto)
        , PortoSettings({
                EnablePorto,
                Nothing(),
                options.PortoLayer,
                options.ContainerName
            })
        , FileCache(options.FileCache)
        , Args {"yql@child", "tasks_runner_proxy"}
        , Env(options.Env)
        , Revision(options.Revision
            ? *options.Revision
            : GetProgramCommitId())
        , TaskScheduler(1)
        , MaxProcesses(options.MaxProcesses)
        , PortoCtlPath(options.PortoCtlPath)
    {
        Start(ExePath, PortoSettings);
        TaskScheduler.Start();
    }

    ITaskRunner::TPtr GetOld(std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc, const NDq::TDqTaskSettings& tmp, const TString& traceId) override {
        Y_UNUSED(alloc);
        Yql::DqsProto::TTaskMeta taskMeta;
        tmp.GetMeta().UnpackTo(&taskMeta);
        ui64 stageId = taskMeta.GetStageId();
        auto result = GetExecutorForTask(taskMeta.GetFiles(), taskMeta.GetSettings());
        auto [task, filesHolder] = PrepareTask(tmp, result.Get());
        return new TTaskRunner(alloc, task, std::move(filesHolder), std::move(result), stageId, traceId);
    }

    TIntrusivePtr<NDq::IDqTaskRunner> Get(std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc, const NDq::TDqTaskSettings& tmp, NDqProto::EDqStatsMode statsMode, const TString& traceId) override
    {
        Y_UNUSED(statsMode);
        Y_UNUSED(alloc);
        Yql::DqsProto::TTaskMeta taskMeta;
        tmp.GetMeta().UnpackTo(&taskMeta);

        auto result = GetExecutorForTask(taskMeta.GetFiles(), taskMeta.GetSettings());
        auto [task, filesHolder] = PrepareTask(tmp, result.Get());
        return new TDqTaskRunner(alloc, task, std::move(filesHolder), std::move(result), taskMeta.GetStageId(), traceId);
    }

private:
    THolder<TChildProcess> StartOne(const TString& exePath, const TPortoSettings& portoSettings) {
        return CreateChildProcess(PortoCtlPath, FileCache->GetDir(), exePath, Args, Env, ContainerId++, portoSettings);
    }

    void Start(const TString& exePath, const TPortoSettings& portoSettings) {
        const auto key = GetKey(exePath, portoSettings);
        while (static_cast<int>(ProcessHolder.Size()) < MaxProcesses) {
            auto process = StartOne(exePath, portoSettings);
            ProcessHolder.Put(key, std::move(process));
        }
    }

    void ProcessJobs(const TString& exePath, const TPortoSettings& portoSettings) {
        NThreading::TPromise<void> promise = NThreading::NewPromise();

        promise.GetFuture().Apply([=](const NThreading::TFuture<void>&) mutable {
            Start(exePath, portoSettings);
        });

        Y_ABORT_UNLESS(TaskScheduler.Add(MakeIntrusive<TJob>(promise), TInstant()));
    }

    void StopJobs(TList<THolder<TChildProcess>>&& stopList) {
        Y_ABORT_UNLESS(TaskScheduler.Add(MakeIntrusive<TStopJob>(std::move(stopList)), TInstant()));
    }

    TString GetKey(const TString& exePath, const TPortoSettings& settings)
    {
        return exePath + "," + settings.ToString();
    }

    std::tuple<NDqProto::TDqTask, TFilesHolder::TPtr> PrepareTask(const NDq::TDqTaskSettings& tmp, TChildProcess* result) {
        // get files from fileCache
        auto task = tmp.GetSerializedTask();
        auto filesHolder = std::make_unique<TFilesHolder>(FileCache);
        Yql::DqsProto::TTaskMeta taskMeta;

        task.GetMeta().UnpackTo(&taskMeta);

        auto* files = taskMeta.MutableFiles();

        for (auto& file : *files) {
            if (file.GetObjectType() != Yql::DqsProto::TFile::EEXE_FILE) {
                auto maybeFile = FileCache->AcquireFile(file.GetObjectId());
                if (!maybeFile) {
                    throw std::runtime_error("Cannot find object `" + file.GetObjectId() + "' in cache");
                }
                filesHolder->Add(file.GetObjectId());
                auto name = file.GetName();

                switch (file.GetObjectType()) {
                    case Yql::DqsProto::TFile::EUDF_FILE:
                    case Yql::DqsProto::TFile::EUSER_FILE:
                        file.SetLocalPath(InitializeLocalFile(result->ExternalWorkDir(), *maybeFile, name));
                        break;
                    default:
                        Y_ABORT_UNLESS(false);
                }
            }
        }

        auto& taskParams = *taskMeta.MutableTaskParams();
        taskParams[WorkingDirectoryParamName] = result->InternalWorkDir();
        taskParams[WorkingDirectoryDontInitParamName] = "true";
        task.MutableMeta()->PackFrom(taskMeta);

        return std::make_tuple(task, std::move(filesHolder));
    }

    TFsPath MakeLocalPath(TString fileName) {
        TString localPath = fileName;
        if (localPath.StartsWith(TStringBuf("/home/"))) {
            localPath = localPath.substr(TStringBuf("/home").length()); // Keep leading slash
        }
        if (!localPath.StartsWith('/')) {
            localPath.prepend('/');
        }
        localPath.prepend('.');
        return localPath;
    }

    TString InitializeLocalFile(const TString& base, const TString& path, const TFsPath& name)
    {
        auto localPath = MakeLocalPath(name.GetPath());
        NFs::MakeDirectoryRecursive(base + "/" + localPath.Parent().GetPath(), NFs::FP_NONSECRET_FILE, false);
        YQL_CLOG(DEBUG, ProviderDq) << "HardLink '" << path << "-> '" << (base + "/" + localPath.GetPath()) << "'";
        YQL_ENSURE(NFs::HardLink(path, base + "/" + localPath.GetPath()));
        return localPath.GetPath();
    }

    template<typename T, typename S>
    THolder<TChildProcess> GetExecutorForTask(const T& files, const S& settings) {
        TString executorId;
        TPortoSettings portoSettings = PortoSettings;

        for (const auto& file : files) {
            if (file.GetObjectType() == Yql::DqsProto::TFile::EEXE_FILE) {
                executorId = file.GetObjectId();
                break;
            }
        }

        TDqConfiguration::TPtr conf = new TDqConfiguration;
        try {
            conf->Dispatch(settings);
        } catch (...) { /* ignore unknown settings */ }
        portoSettings.Enable = EnablePorto && conf->_EnablePorto.Get().GetOrElse(TDqSettings::TDefault::EnablePorto);
        portoSettings.MemoryLimit = conf->_PortoMemoryLimit.Get();
        if (portoSettings.Enable) {
            YQL_CLOG(DEBUG, ProviderDq) << "Porto enabled";
        }

        TString exePath;
        if (executorId.empty() || Revision.empty() || executorId == Revision) {
            exePath = ExePath;
        } else {
            auto maybeExeFile = FileCache->FindFile(executorId);
            if (!maybeExeFile) {
                throw std::runtime_error("Cannot find exe `" + executorId + "' in cache");
            }
            exePath = *maybeExeFile;
        }

        auto key = GetKey(exePath, portoSettings);
        TList<THolder<TChildProcess>> stopList;
        THolder<TChildProcess> result = ProcessHolder.Acquire(key, &stopList);
        if (!result) {
            result = StartOne(exePath, portoSettings);
        }
        ProcessJobs(exePath, portoSettings);
        StopJobs(std::move(stopList));
        return result;
    }

    static THolder<TChildProcess> CreateChildProcess(
        const TString& portoCtlPath,
        const TString& cacheDir,
        const TString& exePath,
        const TVector<TString>& args,
        const THashMap<TString, TString>& env,
        i64 containerId,
        const TPortoSettings& portoSettings)
    {
        THolder<TChildProcess> command;
        if (portoSettings.Enable) {
            command = MakeHolder<TPortoProcess>(portoCtlPath, exePath, args, env, cacheDir + "/Slot-" + ToString(containerId), portoSettings);
        } else {
            command = MakeHolder<TChildProcess>(exePath, args, env, cacheDir + "/Slot-" + ToString(containerId));
        }
        YQL_CLOG(DEBUG, ProviderDq) << "Executing " << exePath;
        for (const auto& arg: args) {
            YQL_CLOG(DEBUG, ProviderDq) << "Arg: " << arg;
        }
        command->Run();
        return command;
    }

    std::atomic<i64> ContainerId = 1;

    const TString ExePath;
    const bool EnablePorto;
    const TPortoSettings PortoSettings;
    const IFileCache::TPtr FileCache;
    const TString UdfsDir;
    const TVector<TString> Args;
    const THashMap<TString, TString> Env;

    const TString Revision;

    TProcessHolder ProcessHolder;
    TTaskScheduler TaskScheduler;
    const int MaxProcesses;
    const TString PortoCtlPath;
};

IProxyFactory::TPtr CreatePipeFactory(
    const TPipeFactoryOptions& options)
{
    return new TPipeFactory(options);
}

} // namespace NYql::NTaskRunnerProxy
