#pragma once

#include <atomic>
#include <thread>

#include <util/string/cast.h>
#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/generic/vector.h>

#include <library/cpp/porto/proto/rpc.pb.h>

namespace Porto {

constexpr int INFINITE_TIMEOUT = -1;
constexpr int DEFAULT_TIMEOUT = 300;        // 5min
constexpr int DEFAULT_DISK_TIMEOUT = 900;   // 15min

constexpr char SOCKET_PATH[] = "/run/portod.socket";

typedef std::function<void(const TWaitResponse &event)> TWaitCallback;

enum {
    GET_NONBLOCK = 1,   // try lock container state
    GET_SYNC = 2,       // refresh cached values, cache ttl 5s
    GET_REAL = 4,       // no faked or inherited values
};

struct DockerImage {
    std::string Id;
    std::vector<std::string> Tags;
    std::vector<std::string> Digests;
    std::vector<std::string> Layers;
    uint64_t Size;
    struct Config {
        std::vector<std::string> Cmd;
        std::vector<std::string> Env;
    } Config;

    DockerImage() = default;
    DockerImage(const TDockerImage &i);

    DockerImage(const DockerImage &i) = default;
    DockerImage(DockerImage &&i) = default;

    DockerImage& operator=(const DockerImage &i) = default;
    DockerImage& operator=(DockerImage &&i) = default;
};

class TPortoApi {
#ifdef __linux__
    friend class TAsyncWaiter;
#endif
private:
    int Fd = -1;
    int Timeout = DEFAULT_TIMEOUT;
    int DiskTimeout = DEFAULT_DISK_TIMEOUT;
    bool AutoReconnect = true;

    EError LastError = EError::Success;
    TString LastErrorMsg;

    /*
     * These keep last request and response. Method might return
     * pointers to Rsp innards -> pointers valid until next call.
     */
    TPortoRequest Req;
    TPortoResponse Rsp;

    std::vector<TString> AsyncWaitNames;
    std::vector<TString> AsyncWaitLabels;
    int AsyncWaitTimeout = INFINITE_TIMEOUT;
    TWaitCallback AsyncWaitCallback;
    bool AsyncWaitOneShot = false;

    EError SetError(const TString &prefix, int _errno) Y_WARN_UNUSED_RESULT;

    EError SetSocketTimeout(int direction, int timeout) Y_WARN_UNUSED_RESULT;

    EError Send(const TPortoRequest &req) Y_WARN_UNUSED_RESULT;

    EError Recv(TPortoResponse &rsp) Y_WARN_UNUSED_RESULT;

    EError Call(int extra_timeout = 0) Y_WARN_UNUSED_RESULT;

    EError CallWait(TString &result_state, int wait_timeout) Y_WARN_UNUSED_RESULT;

public:
    TPortoApi() { }
    ~TPortoApi();

    int GetFd() const {
        return Fd;
    }

    bool Connected() const {
        return Fd >= 0;
    }

    EError Connect(const char *socket_path = SOCKET_PATH) Y_WARN_UNUSED_RESULT;
    void Disconnect();

    /* Requires signal(SIGPIPE, SIG_IGN) */
    void SetAutoReconnect(bool auto_reconnect) {
        AutoReconnect = auto_reconnect;
    }

    /* Request and response timeout in seconds */
    int GetTimeout() const {
        return Timeout;
    }
    EError SetTimeout(int timeout);

    /* Extra timeout for disk operations in seconds */
    int GetDiskTimeout() const {
        return DiskTimeout;
    }
    EError SetDiskTimeout(int timeout);

    EError Error() const Y_WARN_UNUSED_RESULT {
        return LastError;
    }

    EError GetLastError(TString &msg) const Y_WARN_UNUSED_RESULT {
        msg = LastErrorMsg;
        return LastError;
    }

    /* Returns "LastError:(LastErrorMsg)" */
    TString GetLastError() const Y_WARN_UNUSED_RESULT;

    /* Returns text protobuf */
    TString GetLastRequest() const {
        return Req.DebugString();
    }
    TString GetLastResponse() const {
        return Rsp.DebugString();
    }

    /* To be used for next changed_since */
    uint64_t ResponseTimestamp() const Y_WARN_UNUSED_RESULT {
        return Rsp.timestamp();
    }

    // extra_timeout: 0 - none, -1 - infinite
    EError Call(const TPortoRequest &req,
                TPortoResponse &rsp,
                int extra_timeout = 0) Y_WARN_UNUSED_RESULT;

    EError Call(const TString &req,
                TString &rsp,
                int extra_timeout = 0) Y_WARN_UNUSED_RESULT;

    /* System */

    EError GetVersion(TString &tag, TString &revision) Y_WARN_UNUSED_RESULT;

    const TGetSystemResponse *GetSystem();

    EError SetSystem(const TString &key, const TString &val) Y_WARN_UNUSED_RESULT;

    /* Container */

    const TListPropertiesResponse *ListProperties();

    EError ListProperties(TVector<TString> &properties) Y_WARN_UNUSED_RESULT;

    const TListResponse *List(const TString &mask = "");

    EError List(TVector<TString> &names, const TString &mask = "") Y_WARN_UNUSED_RESULT;

    EError Create(const TString &name) Y_WARN_UNUSED_RESULT;

    EError CreateWeakContainer(const TString &name) Y_WARN_UNUSED_RESULT;

    EError Destroy(const TString &name) Y_WARN_UNUSED_RESULT;

    EError Start(const TString &name)Y_WARN_UNUSED_RESULT;

    // stop_timeout: time between SIGTERM and SIGKILL, -1 - default
    EError Stop(const TString &name, int stop_timeout = -1) Y_WARN_UNUSED_RESULT;

    EError Kill(const TString &name, int sig = 9) Y_WARN_UNUSED_RESULT;

    EError Pause(const TString &name) Y_WARN_UNUSED_RESULT;

    EError Resume(const TString &name) Y_WARN_UNUSED_RESULT;

    EError Respawn(const TString &name) Y_WARN_UNUSED_RESULT;

    // wait_timeout: 0 - nonblock, -1 - infinite
    EError WaitContainer(const TString &name,
                         TString &result_state,
                         int wait_timeout = INFINITE_TIMEOUT) Y_WARN_UNUSED_RESULT;

    EError WaitContainers(const TVector<TString> &names,
                          TString &result_name,
                          TString &result_state,
                          int wait_timeout = INFINITE_TIMEOUT) Y_WARN_UNUSED_RESULT;

    const TWaitResponse *Wait(const TVector<TString> &names,
                              const TVector<TString> &labels,
                              int wait_timeout = INFINITE_TIMEOUT) Y_WARN_UNUSED_RESULT;

    EError AsyncWait(const TVector<TString> &names,
                     const TVector<TString> &labels,
                     TWaitCallback callbacks,
                     int wait_timeout = INFINITE_TIMEOUT,
                     const TString &targetState = "") Y_WARN_UNUSED_RESULT;

    EError StopAsyncWait(const TVector<TString> &names,
                         const TVector<TString> &labels,
                         const TString &targetState = "") Y_WARN_UNUSED_RESULT;

    const TGetResponse *Get(const TVector<TString> &names,
                            const TVector<TString> &properties,
                            int flags = 0) Y_WARN_UNUSED_RESULT;

    /* Porto v5 api */
    EError GetContainerSpec(const TString &name, TContainer &container) Y_WARN_UNUSED_RESULT ;
    EError ListContainersBy(const TListContainersRequest &listContainersRequest, TVector<TContainer> &containers) Y_WARN_UNUSED_RESULT;
    EError CreateFromSpec(const TContainerSpec &container, TVector<TVolumeSpec> volumes, bool start = false) Y_WARN_UNUSED_RESULT;
    EError UpdateFromSpec(const TContainerSpec &container) Y_WARN_UNUSED_RESULT;

    EError GetProperty(const TString &name,
                       const TString &property,
                       TString &value,
                       int flags = 0) Y_WARN_UNUSED_RESULT;

    EError GetProperty(const TString &name,
                       const TString &property,
                       const TString &index,
                       TString &value,
                       int flags = 0) Y_WARN_UNUSED_RESULT {
        return GetProperty(name, property + "[" + index + "]", value, flags);
    }

    EError SetProperty(const TString &name,
                       const TString &property,
                       const TString &value) Y_WARN_UNUSED_RESULT;

    EError SetProperty(const TString &name,
                       const TString &property,
                       const TString &index,
                       const TString &value) Y_WARN_UNUSED_RESULT {
        return SetProperty(name, property + "[" + index + "]", value);
    }

    EError GetInt(const TString &name,
                  const TString &property,
                  const TString &index,
                  uint64_t &value) Y_WARN_UNUSED_RESULT;

    EError GetInt(const TString &name,
                       const TString &property,
                       uint64_t &value) Y_WARN_UNUSED_RESULT {
        return GetInt(name, property, "", value);
    }

    EError SetInt(const TString &name,
                  const TString &property,
                  const TString &index,
                  uint64_t value) Y_WARN_UNUSED_RESULT;

    EError SetInt(const TString &name,
                  const TString &property,
                  uint64_t value) Y_WARN_UNUSED_RESULT {
        return SetInt(name, property, "", value);
    }

    EError GetProcMetric(const TVector<TString> &names,
                         const TString &metric,
                         TMap<TString, uint64_t> &values);

    EError GetLabel(const TString &name,
                    const TString &label,
                    TString &value) Y_WARN_UNUSED_RESULT {
        return GetProperty(name, "labels", label, value);
    }

    EError SetLabel(const TString &name,
                    const TString &label,
                    const TString &value,
                    const TString &prev_value = " ") Y_WARN_UNUSED_RESULT;

    EError IncLabel(const TString &name,
                    const TString &label,
                    int64_t add,
                    int64_t &result) Y_WARN_UNUSED_RESULT;

    EError IncLabel(const TString &name,
                    const TString &label,
                    int64_t add = 1) Y_WARN_UNUSED_RESULT {
        int64_t result;
        return IncLabel(name, label, add, result);
    }

    EError ConvertPath(const TString &path,
                       const TString &src_name,
                       const TString &dst_name,
                       TString &result_path) Y_WARN_UNUSED_RESULT;

    EError AttachProcess(const TString &name, int pid,
                         const TString &comm = "") Y_WARN_UNUSED_RESULT;

    EError AttachThread(const TString &name, int pid,
                        const TString &comm = "") Y_WARN_UNUSED_RESULT;

    EError LocateProcess(int pid,
                         const TString &comm /* = "" */,
                         TString &name) Y_WARN_UNUSED_RESULT;

    /* Volume */

    const TListVolumePropertiesResponse *ListVolumeProperties();

    EError ListVolumeProperties(TVector<TString> &properties) Y_WARN_UNUSED_RESULT;

    const TListVolumesResponse *ListVolumes(const TString &path = "",
                                            const TString &container = "");

    EError ListVolumes(TVector<TString> &paths) Y_WARN_UNUSED_RESULT;

    const TVolumeDescription *GetVolumeDesc(const TString &path);

    /* Porto v5 api */
    EError ListVolumesBy(const TGetVolumeRequest &getVolumeRequest, TVector<TVolumeSpec> &volumes) Y_WARN_UNUSED_RESULT;
    EError CreateVolumeFromSpec(const TVolumeSpec &volume, TVolumeSpec &resultSpec) Y_WARN_UNUSED_RESULT;

    const TVolumeSpec *GetVolume(const TString &path);

    const TGetVolumeResponse *GetVolumes(uint64_t changed_since = 0);

    EError CreateVolume(TString &path,
                        const TMap<TString, TString> &config) Y_WARN_UNUSED_RESULT;

    EError LinkVolume(const TString &path,
                      const TString &container = "",
                      const TString &target = "",
                      bool read_only = false,
                      bool required = false) Y_WARN_UNUSED_RESULT;

    EError UnlinkVolume(const TString &path,
                        const TString &container = "",
                        const TString &target = "***",
                        bool strict = false) Y_WARN_UNUSED_RESULT;

    EError TuneVolume(const TString &path,
                      const TMap<TString, TString> &config) Y_WARN_UNUSED_RESULT;

    /* Layer */

    const TListLayersResponse *ListLayers(const TString &place = "",
                                          const TString &mask = "");

    EError ListLayers(TVector<TString> &layers,
                      const TString &place = "",
                      const TString &mask = "") Y_WARN_UNUSED_RESULT;

    EError ImportLayer(const TString &layer,
                       const TString &tarball,
                       bool merge = false,
                       const TString &place = "",
                       const TString &private_value = "",
                       bool verboseError = false) Y_WARN_UNUSED_RESULT;

    EError ExportLayer(const TString &volume,
                       const TString &tarball,
                       const TString &compress = "") Y_WARN_UNUSED_RESULT;

    EError ReExportLayer(const TString &layer,
                         const TString &tarball,
                         const TString &compress = "") Y_WARN_UNUSED_RESULT;

    EError RemoveLayer(const TString &layer,
                       const TString &place = "",
                       bool async = false) Y_WARN_UNUSED_RESULT;

    EError GetLayerPrivate(TString &private_value,
                           const TString &layer,
                           const TString &place = "") Y_WARN_UNUSED_RESULT;

    EError SetLayerPrivate(const TString &private_value,
                           const TString &layer,
                           const TString &place = "") Y_WARN_UNUSED_RESULT;

    /* Docker images */

    EError DockerImageStatus(DockerImage &image,
                             const TString &name,
                             const TString &place = "") Y_WARN_UNUSED_RESULT;

    EError ListDockerImages(std::vector<DockerImage> &images,
                            const TString &place = "",
                            const TString &mask = "") Y_WARN_UNUSED_RESULT;

    EError PullDockerImage(DockerImage &image,
                           const TString &name,
                           const TString &place = "",
                           const TString &auth_token = "",
                           const TString &auth_host = "",
                           const TString &auth_service = "") Y_WARN_UNUSED_RESULT;

    EError RemoveDockerImage(const TString &name,
                             const TString &place = "");

    /* Storage */

    const TListStoragesResponse *ListStorages(const TString &place = "",
                                              const TString &mask = "");

    EError ListStorages(TVector<TString> &storages,
                        const TString &place = "",
                        const TString &mask = "") Y_WARN_UNUSED_RESULT;

    EError RemoveStorage(const TString &storage,
                         const TString &place = "") Y_WARN_UNUSED_RESULT;

    EError ImportStorage(const TString &storage,
                         const TString &archive,
                         const TString &place = "",
                         const TString &compression = "",
                         const TString &private_value = "") Y_WARN_UNUSED_RESULT;

    EError ExportStorage(const TString &storage,
                         const TString &archive,
                         const TString &place = "",
                         const TString &compression = "") Y_WARN_UNUSED_RESULT;
};

#ifdef __linux__
class TAsyncWaiter {
    struct TCallbackData {
        const TWaitCallback Callback;
        const TString State;
    };

    enum class ERequestType {
        None,
        Add,
        Del,
        Stop,
    };

    THashMap<TString, TCallbackData> AsyncCallbacks;
    std::unique_ptr<std::thread> WatchDogThread;
    std::atomic<uint64_t> CallbacksCount;
    int EpollFd = -1;
    TPortoApi Api;

    int Sock, MasterSock;
    TString ReqCt;
    TString ReqState;
    TWaitCallback ReqCallback;

    std::function<void(const TString &error, int ret)> FatalCallback;
    bool FatalError = false;

    void MainCallback(const TWaitResponse &event);
    inline TWaitCallback GetMainCallback() {
        return [this](const TWaitResponse &event) {
            MainCallback(event);
        };
    }

    int Repair();
    void WatchDog();

    void SendInt(int fd, int value);
    int RecvInt(int fd);

    void HandleAddRequest();
    void HandleDelRequest();

    void Fatal(const TString &error, int ret) {
        FatalError = true;
        FatalCallback(error, ret);
    }

    public:
    TAsyncWaiter(std::function<void(const TString &error, int ret)> fatalCallback);
    ~TAsyncWaiter();

    int Add(const TString &ct, const TString &state, TWaitCallback callback);
    int Remove(const TString &ct);
    uint64_t InvocationCount() const {
        return CallbacksCount;
    }
};
#endif

} /* namespace Porto */
