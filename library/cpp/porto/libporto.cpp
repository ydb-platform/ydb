#include "libporto.hpp"
#include "metrics.hpp"

#include <google/protobuf/text_format.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/io/coded_stream.h>

extern "C" {
#include <errno.h>
#include <time.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>

#ifndef __linux__
#include <fcntl.h>
#else
#include <sys/epoll.h>
#endif
}

namespace Porto {

TPortoApi::~TPortoApi() {
    Disconnect();
}

EError TPortoApi::SetError(const TString &prefix, int _errno) {
    LastErrorMsg = prefix + ": " + strerror(_errno);

    switch (_errno) {
        case ENOENT:
            LastError = EError::SocketUnavailable;
            break;
        case EAGAIN:
            LastErrorMsg = prefix + ": Timeout exceeded. Timeout value: " + std::to_string(Timeout);
            LastError = EError::SocketTimeout;
            break;
        case EIO:
        case EPIPE:
            LastError = EError::SocketError;
            break;
        default:
            LastError = EError::Unknown;
            break;
    }

    Disconnect();
    return LastError;
}

TString TPortoApi::GetLastError() const {
    return EError_Name(LastError) + ":(" + LastErrorMsg + ")";
}

EError TPortoApi::Connect(const char *socket_path) {
    struct sockaddr_un peer_addr;
    socklen_t peer_addr_size;

    Disconnect();

#ifdef __linux__
    Fd = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (Fd < 0)
        return SetError("socket", errno);
#else
    Fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (Fd < 0)
        return SetError("socket", errno);
    if (fcntl(Fd, F_SETFD, FD_CLOEXEC) < 0)
        return SetError("fcntl FD_CLOEXEC", errno);
#endif

    if (Timeout > 0 && SetSocketTimeout(3, Timeout))
        return LastError;

    memset(&peer_addr, 0, sizeof(struct sockaddr_un));
    peer_addr.sun_family = AF_UNIX;
    strncpy(peer_addr.sun_path, socket_path, strlen(socket_path));

    peer_addr_size = sizeof(struct sockaddr_un);
    if (connect(Fd, (struct sockaddr *) &peer_addr, peer_addr_size) < 0)
        return SetError("connect", errno);

    /* Restore async wait state */
    if (!AsyncWaitNames.empty()) {
        for (auto &name: AsyncWaitNames)
            Req.mutable_asyncwait()->add_name(name);
        for (auto &label: AsyncWaitLabels)
            Req.mutable_asyncwait()->add_label(label);
        if (AsyncWaitTimeout >= 0)
            Req.mutable_asyncwait()->set_timeout_ms(AsyncWaitTimeout * 1000);
        return Call();
    }

    return EError::Success;
}

void TPortoApi::Disconnect() {
    if (Fd >= 0)
        close(Fd);
    Fd = -1;
}

EError TPortoApi::SetSocketTimeout(int direction, int timeout) {
    struct timeval tv;

    if (Fd < 0)
        return EError::Success;

    tv.tv_sec = timeout > 0 ? timeout : 0;
    tv.tv_usec = 0;

    if ((direction & 1) && setsockopt(Fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof tv))
        return SetError("setsockopt SO_SNDTIMEO", errno);

    if ((direction & 2) && setsockopt(Fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv))
        return SetError("setsockopt SO_RCVTIMEO", errno);

    return EError::Success;
}

EError TPortoApi::SetTimeout(int timeout) {
    Timeout = timeout ? timeout : DEFAULT_TIMEOUT;
    return SetSocketTimeout(3, Timeout);
}

EError TPortoApi::SetDiskTimeout(int timeout) {
    DiskTimeout = timeout ? timeout : DEFAULT_DISK_TIMEOUT;
    return EError::Success;
}

EError TPortoApi::Send(const TPortoRequest &req) {
    google::protobuf::io::FileOutputStream raw(Fd);

    if (!req.IsInitialized()) {
        LastError = EError::InvalidMethod;
        LastErrorMsg = "Request is not initialized";
        return EError::InvalidMethod;
    }

    {
        google::protobuf::io::CodedOutputStream output(&raw);

        output.WriteVarint32(req.ByteSize());
        req.SerializeWithCachedSizes(&output);
    }

    raw.Flush();

    int err = raw.GetErrno();
    if (err)
        return SetError("send", err);

    return EError::Success;
}

EError TPortoApi::Recv(TPortoResponse &rsp) {
    google::protobuf::io::FileInputStream raw(Fd);
    google::protobuf::io::CodedInputStream input(&raw);

    while (true) {
        uint32_t size;

        if (!input.ReadVarint32(&size))
            return SetError("recv", raw.GetErrno() ?: EIO);

        auto prev_limit = input.PushLimit(size);

        rsp.Clear();

        if (!rsp.ParseFromCodedStream(&input))
            return SetError("recv", raw.GetErrno() ?: EIO);

        input.PopLimit(prev_limit);

        if (rsp.has_asyncwait()) {
            if (AsyncWaitCallback)
                AsyncWaitCallback(rsp.asyncwait());

            if (AsyncWaitOneShot)
                return EError::Success;

            continue;
        }

        return EError::Success;
    }
}

EError TPortoApi::Call(const TPortoRequest &req,
                        TPortoResponse &rsp,
                        int extra_timeout) {
    bool reconnect = AutoReconnect;
    EError err = EError::Success;

    if (Fd < 0) {
        if (!reconnect)
            return SetError("Not connected", EIO);
        err = Connect();
        reconnect = false;
    }

    if (!err) {
        err = Send(req);
        if (err == EError::SocketError && reconnect) {
            err = Connect();
            if (!err)
                err = Send(req);
        }
    }

    if (!err && extra_timeout && Timeout > 0)
        err = SetSocketTimeout(2, extra_timeout > 0 ? (extra_timeout + Timeout) : -1);

    if (!err)
        err = Recv(rsp);

    if (extra_timeout && Timeout > 0) {
        EError err = SetSocketTimeout(2, Timeout);
        (void)err;
    }

    if (!err) {
        err = LastError = rsp.error();
        LastErrorMsg = rsp.errormsg();
    }

    return err;
}

EError TPortoApi::Call(int extra_timeout) {
    return Call(Req, Rsp, extra_timeout);
}

EError TPortoApi::Call(const TString &req,
                        TString &rsp,
                        int extra_timeout) {
    Req.Clear();
    if (!google::protobuf::TextFormat::ParseFromString(req, &Req)) {
        LastError = EError::InvalidMethod;
        LastErrorMsg = "Cannot parse request";
        rsp = "";
        return EError::InvalidMethod;
    }

    EError err = Call(Req, Rsp, extra_timeout);

    rsp = Rsp.DebugString();

    return err;
}

EError TPortoApi::GetVersion(TString &tag, TString &revision) {
    Req.Clear();
    Req.mutable_version();

    if (!Call()) {
        tag = Rsp.version().tag();
        revision = Rsp.version().revision();
    }

    return LastError;
}

const TGetSystemResponse *TPortoApi::GetSystem() {
    Req.Clear();
    Req.mutable_getsystem();
    if (!Call())
        return &Rsp.getsystem();
    return nullptr;
}

EError TPortoApi::SetSystem(const TString &key, const TString &val) {
    TString rsp;
    return Call("SetSystem {" + key + ":" + val + "}", rsp);
}

/* Container */

EError TPortoApi::Create(const TString &name) {
    Req.Clear();
    auto req = Req.mutable_create();
    req->set_name(name);
    return Call();
}

EError TPortoApi::CreateWeakContainer(const TString &name) {
    Req.Clear();
    auto req = Req.mutable_createweak();
    req->set_name(name);
    return Call();
}

EError TPortoApi::Destroy(const TString &name) {
    Req.Clear();
    auto req = Req.mutable_destroy();
    req->set_name(name);
    return Call();
}

const TListResponse *TPortoApi::List(const TString &mask) {
    Req.Clear();
    auto req = Req.mutable_list();

    if(!mask.empty())
        req->set_mask(mask);

    if (!Call())
        return &Rsp.list();

    return nullptr;
}

EError TPortoApi::List(TVector<TString> &list, const TString &mask) {
    Req.Clear();
    auto req = Req.mutable_list();
    if(!mask.empty())
        req->set_mask(mask);
    if (!Call())
        list = TVector<TString>(std::begin(Rsp.list().name()),
                                        std::end(Rsp.list().name()));
    return LastError;
}

const TListPropertiesResponse *TPortoApi::ListProperties() {
    Req.Clear();
    Req.mutable_listproperties();

    if (Call())
        return nullptr;

    bool has_data = false;
    for (const auto &prop: Rsp.listproperties().list()) {
        if (prop.read_only()) {
            has_data = true;
            break;
        }
    }

    if (!has_data) {
        TPortoRequest req;
        TPortoResponse rsp;

        req.mutable_listdataproperties();
        if (!Call(req, rsp)) {
            for (const auto &data: rsp.listdataproperties().list()) {
                auto d = Rsp.mutable_listproperties()->add_list();
                d->set_name(data.name());
                d->set_desc(data.desc());
                d->set_read_only(true);
            }
        }
    }

    return &Rsp.listproperties();
}

EError TPortoApi::ListProperties(TVector<TString> &properties) {
    properties.clear();
    auto rsp = ListProperties();
    if (rsp) {
        for (auto &prop: rsp->list())
            properties.push_back(prop.name());
    }
    return LastError;
}

const TGetResponse *TPortoApi::Get(const TVector<TString> &names,
                                   const TVector<TString> &vars,
                                   int flags) {
    Req.Clear();
    auto get = Req.mutable_get();

    for (const auto &n : names)
        get->add_name(n);

    for (const auto &v : vars)
        get->add_variable(v);

    if (flags & GET_NONBLOCK)
        get->set_nonblock(true);
    if (flags & GET_SYNC)
        get->set_sync(true);
    if (flags & GET_REAL)
        get->set_real(true);

    if (!Call())
        return &Rsp.get();

    return nullptr;
}

EError TPortoApi::GetContainerSpec(const TString &name, TContainer &container) {
    Req.Clear();
    TListContainersRequest req;
    auto filter = req.add_filters();
    filter->set_name(name);

    TVector<TContainer> containers;

    auto ret = ListContainersBy(req, containers);
    if (containers.empty())
        return EError::ContainerDoesNotExist;

    if (!ret)
        container = containers[0];

    return ret;
}

EError TPortoApi::ListContainersBy(const TListContainersRequest &listContainersRequest, TVector<TContainer> &containers) {
    Req.Clear();
    auto req = Req.mutable_listcontainersby();
    *req = listContainersRequest;

    auto ret = Call();
    if (ret)
        return ret;

    for (auto &ct : Rsp.listcontainersby().containers())
        containers.push_back(ct);

    return EError::Success;
}

EError TPortoApi::CreateFromSpec(const TContainerSpec &container, TVector<TVolumeSpec> volumes, bool start) {
    Req.Clear();
    auto req = Req.mutable_createfromspec();

    auto ct = req->mutable_container();
    *ct = container;

    for  (auto &volume : volumes) {
        auto v = req->add_volumes();
        *v = volume;
    }

    req->set_start(start);

    return Call();
}

EError TPortoApi::UpdateFromSpec(const TContainerSpec &container) {
    Req.Clear();
    auto req = Req.mutable_updatefromspec();

    auto ct = req->mutable_container();
    *ct = container;

    return Call();
}

EError TPortoApi::GetProperty(const TString &name,
                              const TString &property,
                              TString &value,
                              int flags) {
    Req.Clear();
    auto req = Req.mutable_getproperty();

    req->set_name(name);
    req->set_property(property);
    if (flags & GET_SYNC)
        req->set_sync(true);
    if (flags & GET_REAL)
        req->set_real(true);

    if (!Call())
        value = Rsp.getproperty().value();

    return LastError;
}

EError TPortoApi::SetProperty(const TString &name,
                              const TString &property,
                              const TString &value) {
    Req.Clear();
    auto req = Req.mutable_setproperty();

    req->set_name(name);
    req->set_property(property);
    req->set_value(value);

    return Call();
}

EError TPortoApi::GetInt(const TString &name,
                         const TString &property,
                         const TString &index,
                         uint64_t &value) {
    TString key = property, str;
    if (index.size())
        key = property + "[" + index + "]";
    if (!GetProperty(name, key, str)) {
        const char *ptr = str.c_str();
        char *end;
        errno = 0;
        value = strtoull(ptr, &end, 10);
        if (errno || end == ptr || *end) {
            LastError = EError::InvalidValue;
            LastErrorMsg = " value: " + str;
        }
    }
    return LastError;
}

EError TPortoApi::SetInt(const TString &name,
                         const TString &property,
                         const TString &index,
                         uint64_t value) {
    TString key = property;
    if (index.size())
        key = property + "[" + index + "]";
    return SetProperty(name, key, ToString(value));
}

EError TPortoApi::GetProcMetric(const TVector<TString> &names,
                                const TString &metric,
                                TMap<TString, uint64_t> &values) {
    auto it = ProcMetrics.find(metric);

    if (it == ProcMetrics.end()) {
        LastError = EError::InvalidValue;
        LastErrorMsg = " Unknown metric: " + metric;
        return LastError;
    }

    LastError = it->second->GetValues(names, values, *this);

    if (LastError)
        LastErrorMsg = "Unknown error on Get() method";

    return LastError;
}

EError TPortoApi::SetLabel(const TString &name,
                           const TString &label,
                           const TString &value,
                           const TString &prev_value) {
    Req.Clear();
    auto req = Req.mutable_setlabel();

    req->set_name(name);
    req->set_label(label);
    req->set_value(value);
    if (prev_value != " ")
        req->set_prev_value(prev_value);

    return Call();
}

EError TPortoApi::IncLabel(const TString &name,
                           const TString &label,
                           int64_t add,
                           int64_t &result) {
    Req.Clear();
    auto req = Req.mutable_inclabel();

    req->set_name(name);
    req->set_label(label);
    req->set_add(add);

    EError err = Call();

    if (Rsp.has_inclabel())
        result = Rsp.inclabel().result();

    return err;
}

EError TPortoApi::Start(const TString &name) {
    Req.Clear();
    auto req = Req.mutable_start();

    req->set_name(name);

    return Call();
}

EError TPortoApi::Stop(const TString &name, int stop_timeout) {
    Req.Clear();
    auto req = Req.mutable_stop();

    req->set_name(name);
    if (stop_timeout >= 0)
        req->set_timeout_ms(stop_timeout * 1000);

    return Call(stop_timeout > 0 ? stop_timeout : 0);
}

EError TPortoApi::Kill(const TString &name, int sig) {
    Req.Clear();
    auto req = Req.mutable_kill();

    req->set_name(name);
    req->set_sig(sig);

    return Call();
}

EError TPortoApi::Pause(const TString &name) {
    Req.Clear();
    auto req = Req.mutable_pause();

    req->set_name(name);

    return Call();
}

EError TPortoApi::Resume(const TString &name) {
    Req.Clear();
    auto req = Req.mutable_resume();

    req->set_name(name);

    return Call();
}

EError TPortoApi::Respawn(const TString &name) {
    Req.Clear();
    auto req = Req.mutable_respawn();

    req->set_name(name);

    return Call();
}

EError TPortoApi::CallWait(TString &result_state, int wait_timeout) {
    time_t deadline = 0;
    time_t last_retry = 0;

    if (wait_timeout >= 0) {
        deadline = time(nullptr) + wait_timeout;
        Req.mutable_wait()->set_timeout_ms(wait_timeout * 1000);
    }

retry:
    if (!Call(wait_timeout)) {
        if (Rsp.wait().has_state())
            result_state = Rsp.wait().state();
        else if (Rsp.wait().name() == "")
            result_state = "timeout";
        else
            result_state = "dead";
    } else if (LastError == EError::SocketError && AutoReconnect) {
        time_t now = time(nullptr);

        if (wait_timeout < 0 || now < deadline) {
            if (wait_timeout >= 0) {
                wait_timeout = deadline - now;
                Req.mutable_wait()->set_timeout_ms(wait_timeout * 1000);
            }
            if (last_retry == now)
                sleep(1);
            last_retry = now;
            goto retry;
        }

        result_state = "timeout";
    } else
        result_state = "unknown";

    return LastError;
}

EError TPortoApi::WaitContainer(const TString &name,
                                TString &result_state,
                                int wait_timeout) {
    Req.Clear();
    auto req = Req.mutable_wait();

    req->add_name(name);

    return CallWait(result_state, wait_timeout);
}

EError TPortoApi::WaitContainers(const TVector<TString> &names,
                                 TString &result_name,
                                 TString &result_state,
                                 int wait_timeout) {
    Req.Clear();
    auto req = Req.mutable_wait();

    for (auto &c : names)
        req->add_name(c);

    EError err = CallWait(result_state, wait_timeout);

    result_name = Rsp.wait().name();

    return err;
}

const TWaitResponse *TPortoApi::Wait(const TVector<TString> &names,
                                     const TVector<TString> &labels,
                                     int wait_timeout) {
    Req.Clear();
    auto req = Req.mutable_wait();
    TString result_state;

    for (auto &c : names)
        req->add_name(c);
    for (auto &label: labels)
        req->add_label(label);

    EError err = CallWait(result_state, wait_timeout);
    (void)err;

    if (Rsp.has_wait())
        return &Rsp.wait();

    return nullptr;
}

EError TPortoApi::AsyncWait(const TVector<TString> &names,
                             const TVector<TString> &labels,
                             TWaitCallback callback,
                             int wait_timeout,
                             const TString &targetState) {
    Req.Clear();
    auto req = Req.mutable_asyncwait();

    AsyncWaitNames.clear();
    AsyncWaitLabels.clear();
    AsyncWaitTimeout = wait_timeout;
    AsyncWaitCallback = callback;

    for (auto &name: names)
        req->add_name(name);
    for (auto &label: labels)
        req->add_label(label);
    if (wait_timeout >= 0)
        req->set_timeout_ms(wait_timeout * 1000);
    if (!targetState.empty()) {
        req->set_target_state(targetState);
        AsyncWaitOneShot = true;
    } else
        AsyncWaitOneShot = false;

    if (Call()) {
        AsyncWaitCallback = nullptr;
    } else {
        AsyncWaitNames = names;
        AsyncWaitLabels = labels;
    }

    return LastError;
}

EError TPortoApi::StopAsyncWait(const TVector<TString> &names,
                                const TVector<TString> &labels,
                                const TString &targetState) {
    Req.Clear();
    auto req = Req.mutable_stopasyncwait();

    AsyncWaitNames.clear();
    AsyncWaitLabels.clear();

    for (auto &name: names)
        req->add_name(name);
    for (auto &label: labels)
        req->add_label(label);
    if (!targetState.empty()) {
        req->set_target_state(targetState);
    }

    return Call();
}

EError TPortoApi::ConvertPath(const TString &path,
                              const TString &src,
                              const TString &dest,
                              TString &res) {
    Req.Clear();
    auto req = Req.mutable_convertpath();

    req->set_path(path);
    req->set_source(src);
    req->set_destination(dest);

    if (!Call())
        res = Rsp.convertpath().path();

    return LastError;
}

EError TPortoApi::AttachProcess(const TString &name, int pid,
                                const TString &comm) {
    Req.Clear();
    auto req = Req.mutable_attachprocess();

    req->set_name(name);
    req->set_pid(pid);
    req->set_comm(comm);

    return Call();
}

EError TPortoApi::AttachThread(const TString &name, int pid,
                               const TString &comm) {
    Req.Clear();
    auto req = Req.mutable_attachthread();

    req->set_name(name);
    req->set_pid(pid);
    req->set_comm(comm);

    return Call();
}

EError TPortoApi::LocateProcess(int pid, const TString &comm,
                                TString &name) {
    Req.Clear();
    auto req = Req.mutable_locateprocess();

    req->set_pid(pid);
    req->set_comm(comm);

    if (!Call())
        name = Rsp.locateprocess().name();

    return LastError;
}

/* Volume */

const TListVolumePropertiesResponse *TPortoApi::ListVolumeProperties() {
    Req.Clear();
    Req.mutable_listvolumeproperties();

    if (!Call())
        return &Rsp.listvolumeproperties();

    return nullptr;
}

EError TPortoApi::ListVolumeProperties(TVector<TString> &properties) {
    properties.clear();
    auto rsp = ListVolumeProperties();
    if (rsp) {
        for (auto &prop: rsp->list())
            properties.push_back(prop.name());
    }
    return LastError;
}

EError TPortoApi::CreateVolume(TString &path,
                               const TMap<TString, TString> &config) {
    Req.Clear();
    auto req = Req.mutable_createvolume();

    req->set_path(path);

    *(req->mutable_properties()) =
        google::protobuf::Map<TString, TString>(config.begin(), config.end());

    if (!Call(DiskTimeout) && path.empty())
        path = Rsp.createvolume().path();

    return LastError;
}

EError TPortoApi::TuneVolume(const TString &path,
                             const TMap<TString, TString> &config) {
    Req.Clear();
    auto req = Req.mutable_tunevolume();

    req->set_path(path);

    *(req->mutable_properties()) =
        google::protobuf::Map<TString, TString>(config.begin(), config.end());

    return Call(DiskTimeout);
}

EError TPortoApi::LinkVolume(const TString &path,
                             const TString &container,
                             const TString &target,
                             bool read_only,
                             bool required) {
    Req.Clear();
    auto req = (target.empty() && !required) ? Req.mutable_linkvolume() :
                                               Req.mutable_linkvolumetarget();

    req->set_path(path);
    if (!container.empty())
        req->set_container(container);
    if (target != "")
        req->set_target(target);
    if (read_only)
        req->set_read_only(read_only);
    if (required)
        req->set_required(required);

    return Call();
}

EError TPortoApi::UnlinkVolume(const TString &path,
                               const TString &container,
                               const TString &target,
                               bool strict) {
    Req.Clear();
    auto req = (target == "***") ? Req.mutable_unlinkvolume() :
                                   Req.mutable_unlinkvolumetarget();

    req->set_path(path);
    if (!container.empty())
        req->set_container(container);
    if (target != "***")
        req->set_target(target);
    if (strict)
        req->set_strict(strict);

    return Call(DiskTimeout);
}

const TListVolumesResponse *
TPortoApi::ListVolumes(const TString &path,
                       const TString &container) {
    Req.Clear();
    auto req = Req.mutable_listvolumes();

    if (!path.empty())
        req->set_path(path);

    if (!container.empty())
        req->set_container(container);

    if (Call())
        return nullptr;

    auto list = Rsp.mutable_listvolumes();

    /* compat */
    for (auto v: *list->mutable_volumes()) {
        if (v.links().size())
            break;
        for (auto &ct: v.containers())
            v.add_links()->set_container(ct);
    }

    return list;
}

EError TPortoApi::ListVolumes(TVector<TString> &paths) {
    Req.Clear();
    auto rsp = ListVolumes();
    paths.clear();
    if (rsp) {
        for (auto &v : rsp->volumes())
            paths.push_back(v.path());
    }
    return LastError;
}

const TVolumeDescription *TPortoApi::GetVolumeDesc(const TString &path) {
    Req.Clear();
    auto rsp = ListVolumes(path);

    if (rsp && rsp->volumes().size())
        return &rsp->volumes(0);

    return nullptr;
}

const TVolumeSpec *TPortoApi::GetVolume(const TString &path) {
    Req.Clear();
    auto req = Req.mutable_getvolume();

    req->add_path(path);

    if (!Call() && Rsp.getvolume().volume().size())
        return &Rsp.getvolume().volume(0);

    return nullptr;
}

const TGetVolumeResponse *TPortoApi::GetVolumes(uint64_t changed_since) {
    Req.Clear();
    auto req = Req.mutable_getvolume();

    if (changed_since)
        req->set_changed_since(changed_since);

    if (!Call() && Rsp.has_getvolume())
        return &Rsp.getvolume();

    return nullptr;
}


EError TPortoApi::ListVolumesBy(const TGetVolumeRequest &getVolumeRequest, TVector<TVolumeSpec> &volumes) {
    Req.Clear();
    auto req = Req.mutable_getvolume();
    *req = getVolumeRequest;

    auto ret = Call();
    if (ret)
        return ret;

    for (auto volume : Rsp.getvolume().volume())
        volumes.push_back(volume);
    return EError::Success;
}

EError TPortoApi::CreateVolumeFromSpec(const TVolumeSpec &volume, TVolumeSpec &resultSpec) {
    Req.Clear();
    auto req = Req.mutable_newvolume();
    auto vol = req->mutable_volume();
    *vol = volume;

    auto ret = Call();
    if (ret)
        return ret;

    resultSpec =  Rsp.newvolume().volume();

    return ret;
}

/* Layer */

EError TPortoApi::ImportLayer(const TString &layer,
                              const TString &tarball,
                              bool merge,
                              const TString &place,
                              const TString &private_value,
                              bool verboseError) {
    Req.Clear();
    auto req = Req.mutable_importlayer();

    req->set_layer(layer);
    req->set_tarball(tarball);
    req->set_merge(merge);
    req->set_verbose_error(verboseError);
    if (place.size())
        req->set_place(place);
    if (private_value.size())
        req->set_private_value(private_value);

    return Call(DiskTimeout);
}

EError TPortoApi::ExportLayer(const TString &volume,
                              const TString &tarball,
                              const TString &compress) {
    Req.Clear();
    auto req = Req.mutable_exportlayer();

    req->set_volume(volume);
    req->set_tarball(tarball);
    if (compress.size())
        req->set_compress(compress);

    return Call(DiskTimeout);
}

EError TPortoApi::ReExportLayer(const TString &layer,
                                const TString &tarball,
                                const TString &compress) {
    Req.Clear();
    auto req = Req.mutable_exportlayer();

    req->set_volume("");
    req->set_layer(layer);
    req->set_tarball(tarball);
    if (compress.size())
        req->set_compress(compress);

    return Call(DiskTimeout);
}

EError TPortoApi::RemoveLayer(const TString &layer,
                              const TString &place,
                              bool async) {
    Req.Clear();
    auto req = Req.mutable_removelayer();

    req->set_layer(layer);
    req->set_async(async);
    if (place.size())
        req->set_place(place);

    return Call(DiskTimeout);
}

const TListLayersResponse *TPortoApi::ListLayers(const TString &place,
                                                 const TString &mask) {
    Req.Clear();
    auto req = Req.mutable_listlayers();

    if (place.size())
        req->set_place(place);
    if (mask.size())
        req->set_mask(mask);

    if (Call())
        return nullptr;

    auto list = Rsp.mutable_listlayers();

    /* compat conversion */
    if (!list->layers().size() && list->layer().size()) {
        for (auto &name: list->layer()) {
            auto l = list->add_layers();
            l->set_name(name);
            l->set_owner_user("");
            l->set_owner_group("");
            l->set_last_usage(0);
            l->set_private_value("");
        }
    }

    return list;
}

EError TPortoApi::ListLayers(TVector<TString> &layers,
                             const TString &place,
                             const TString &mask) {
    Req.Clear();
    auto req = Req.mutable_listlayers();

    if (place.size())
        req->set_place(place);
    if (mask.size())
        req->set_mask(mask);

    if (!Call())
        layers = TVector<TString>(std::begin(Rsp.listlayers().layer()),
                                      std::end(Rsp.listlayers().layer()));

    return LastError;
}

EError TPortoApi::GetLayerPrivate(TString &private_value,
                                  const TString &layer,
                                  const TString &place) {
    Req.Clear();
    auto req = Req.mutable_getlayerprivate();

    req->set_layer(layer);
    if (place.size())
        req->set_place(place);

    if (!Call())
        private_value = Rsp.getlayerprivate().private_value();

    return LastError;
}

EError TPortoApi::SetLayerPrivate(const TString &private_value,
                                  const TString &layer,
                                  const TString &place) {
    Req.Clear();
    auto req = Req.mutable_setlayerprivate();

    req->set_layer(layer);
    req->set_private_value(private_value);
    if (place.size())
        req->set_place(place);

    return Call();
}

/* Docker images */

DockerImage::DockerImage(const TDockerImage &i) {
    Id = i.id();
    for (const auto &tag: i.tags())
        Tags.emplace_back(tag);
    for (const auto &digest: i.digests())
        Digests.emplace_back(digest);
    for (const auto &layer: i.layers())
        Layers.emplace_back(layer);
    if (i.has_size())
        Size = i.size();
    if (i.has_config()) {
        auto &cfg = i.config();
        for (const auto &cmd: cfg.cmd())
            Config.Cmd.emplace_back(cmd);
        for (const auto &env: cfg.env())
            Config.Env.emplace_back(env);
    }
}

EError TPortoApi::DockerImageStatus(DockerImage &image,
                                     const TString &name,
                                     const TString &place) {
    auto req = Req.mutable_dockerimagestatus();
    req->set_name(name);
    if (!place.empty())
        req->set_place(place);
    EError ret = Call();
    if (!ret && Rsp.dockerimagestatus().has_image())
        image = DockerImage(Rsp.dockerimagestatus().image());
    return ret;
}

EError TPortoApi::ListDockerImages(std::vector<DockerImage> &images,
                                    const TString &place,
                                    const TString &mask) {
    auto req = Req.mutable_listdockerimages();
    if (place.size())
        req->set_place(place);
    if (mask.size())
        req->set_mask(mask);
    EError ret = Call();
    if (!ret) {
        for (const auto &i: Rsp.listdockerimages().images())
            images.emplace_back(i);
    }
    return ret;
}

EError TPortoApi::PullDockerImage(DockerImage &image,
                                   const TString &name,
                                   const TString &place,
                                   const TString &auth_token,
                                   const TString &auth_path,
                                   const TString &auth_service) {
    auto req = Req.mutable_pulldockerimage();
    req->set_name(name);
    if (place.size())
        req->set_place(place);
    if (auth_token.size())
        req->set_auth_token(auth_token);
    if (auth_path.size())
        req->set_auth_path(auth_path);
    if (auth_service.size())
        req->set_auth_service(auth_service);
    EError ret = Call();
    if (!ret && Rsp.pulldockerimage().has_image())
        image = DockerImage(Rsp.pulldockerimage().image());
    return ret;
}

EError TPortoApi::RemoveDockerImage(const TString &name,
                                     const TString &place) {
    auto req = Req.mutable_removedockerimage();
    req->set_name(name);
    if (place.size())
        req->set_place(place);
    return Call();
}

/* Storage */

const TListStoragesResponse *TPortoApi::ListStorages(const TString &place,
                                                     const TString &mask) {
    Req.Clear();
    auto req = Req.mutable_liststorages();

    if (place.size())
        req->set_place(place);
    if (mask.size())
        req->set_mask(mask);

    if (Call())
        return nullptr;

    return &Rsp.liststorages();
}

EError TPortoApi::ListStorages(TVector<TString> &storages,
                               const TString &place,
                               const TString &mask) {
    Req.Clear();
    auto req = Req.mutable_liststorages();

    if (place.size())
        req->set_place(place);
    if (mask.size())
        req->set_mask(mask);

    if (!Call()) {
        storages.clear();
        for (auto &storage: Rsp.liststorages().storages())
            storages.push_back(storage.name());
    }

    return LastError;
}

EError TPortoApi::RemoveStorage(const TString &storage,
                                const TString &place) {
    Req.Clear();
    auto req = Req.mutable_removestorage();

    req->set_name(storage);
    if (place.size())
        req->set_place(place);

    return Call(DiskTimeout);
}

EError TPortoApi::ImportStorage(const TString &storage,
                                const TString &archive,
                                const TString &place,
                                const TString &compression,
                                const TString &private_value) {
    Req.Clear();
    auto req = Req.mutable_importstorage();

    req->set_name(storage);
    req->set_tarball(archive);
    if (place.size())
        req->set_place(place);
    if (compression.size())
        req->set_compress(compression);
    if (private_value.size())
        req->set_private_value(private_value);

    return Call(DiskTimeout);
}

EError TPortoApi::ExportStorage(const TString &storage,
                                const TString &archive,
                                const TString &place,
                                const TString &compression) {
    Req.Clear();
    auto req = Req.mutable_exportstorage();

    req->set_name(storage);
    req->set_tarball(archive);
    if (place.size())
        req->set_place(place);
    if (compression.size())
        req->set_compress(compression);

    return Call(DiskTimeout);
}

#ifdef __linux__
void TAsyncWaiter::MainCallback(const TWaitResponse &event) {
    CallbacksCount++;

    auto it = AsyncCallbacks.find(event.name());
    if (it != AsyncCallbacks.end() && it->second.State == event.state()) {
        it->second.Callback(event);
        AsyncCallbacks.erase(it);
    }
}

int TAsyncWaiter::Repair() {
    for (const auto &it : AsyncCallbacks) {
        int ret = Api.AsyncWait({it.first}, {}, GetMainCallback(), -1, it.second.State);
        if (ret)
            return ret;
    }
    return 0;
}

void TAsyncWaiter::WatchDog() {
    int ret;
    auto apiFd = Api.Fd;

    while (true) {
        struct epoll_event events[2];
        int nfds = epoll_wait(EpollFd, events, 2, -1);

        if (nfds < 0) {
            if (errno == EINTR)
                continue;

            Fatal("Can not make epoll_wait", errno);
            return;
        }

        for (int n = 0; n < nfds; ++n) {
            if (events[n].data.fd == apiFd) {
                TPortoResponse rsp;
                ret = Api.Recv(rsp);
                // portod reloaded - async_wait must be repaired
                if (ret == EError::SocketError) {
                    ret = Api.Connect();
                    if (ret) {
                        Fatal("Can not connect to porto api", ret);
                        return;
                    }

                    ret = Repair();
                    if (ret) {
                        Fatal("Can not repair", ret);
                        return;
                    }

                    apiFd = Api.Fd;

                    struct epoll_event portoEv;
                    portoEv.events = EPOLLIN;
                    portoEv.data.fd = apiFd;
                    if (epoll_ctl(EpollFd, EPOLL_CTL_ADD, apiFd, &portoEv)) {
                        Fatal("Can not epoll_ctl", errno);
                        return;
                    }
                }
            } else if (events[n].data.fd == Sock) {
                ERequestType requestType = static_cast<ERequestType>(RecvInt(Sock));

                switch (requestType) {
                    case ERequestType::Add:
                        HandleAddRequest();
                        break;
                    case ERequestType::Del:
                        HandleDelRequest();
                        break;
                    case ERequestType::Stop:
                        return;
                    case ERequestType::None:
                    default:
                        Fatal("Unknown request", static_cast<int>(requestType));
                }
            }
        }
    }
}

void TAsyncWaiter::SendInt(int fd, int value) {
    int ret = write(fd, &value, sizeof(value));
    if (ret != sizeof(value))
        Fatal("Can not send int", errno);
}

int TAsyncWaiter::RecvInt(int fd) {
    int value;
    int ret = read(fd, &value, sizeof(value));
    if (ret != sizeof(value))
        Fatal("Can not recv int", errno);

    return value;
}

void TAsyncWaiter::HandleAddRequest() {
    int ret = 0;

    auto it = AsyncCallbacks.find(ReqCt);
    if (it != AsyncCallbacks.end()) {
        ret = Api.StopAsyncWait({ReqCt}, {}, it->second.State);
        AsyncCallbacks.erase(it);
    }

    AsyncCallbacks.insert(std::make_pair(ReqCt, TCallbackData({ReqCallback, ReqState})));

    ret = Api.AsyncWait({ReqCt}, {}, GetMainCallback(), -1, ReqState);
    SendInt(Sock, ret);
}

void TAsyncWaiter::HandleDelRequest() {
    int ret = 0;

    auto it = AsyncCallbacks.find(ReqCt);
    if (it != AsyncCallbacks.end()) {
        ret = Api.StopAsyncWait({ReqCt}, {}, it->second.State);
        AsyncCallbacks.erase(it);
    }

    SendInt(Sock, ret);
}

TAsyncWaiter::TAsyncWaiter(std::function<void(const TString &error, int ret)> fatalCallback)
    : CallbacksCount(0ul)
      , FatalCallback(fatalCallback)
{
    int socketPair[2];
    int ret = socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, socketPair);
    if (ret) {
        Fatal("Can not make socketpair", ret);
        return;
    }

    MasterSock = socketPair[0];
    Sock = socketPair[1];

    ret = Api.Connect();
    if (ret) {
        Fatal("Can not connect to porto api", ret);
        return;
    }

    auto apiFd = Api.Fd;

    EpollFd = epoll_create1(EPOLL_CLOEXEC);

    if (EpollFd == -1) {
        Fatal("Can not epoll_create", errno);
        return;
    }

    struct epoll_event pairEv;
    pairEv.events = EPOLLIN;
    pairEv.data.fd = Sock;

    struct epoll_event portoEv;
    portoEv.events = EPOLLIN;
    portoEv.data.fd = apiFd;

    if (epoll_ctl(EpollFd, EPOLL_CTL_ADD, Sock, &pairEv)) {
        Fatal("Can not epoll_ctl", errno);
        return;
    }

    if (epoll_ctl(EpollFd, EPOLL_CTL_ADD, apiFd, &portoEv)) {
        Fatal("Can not epoll_ctl", errno);
        return;
    }

    WatchDogThread = std::unique_ptr<std::thread>(new std::thread(&TAsyncWaiter::WatchDog, this));
}

TAsyncWaiter::~TAsyncWaiter() {
    SendInt(MasterSock, static_cast<int>(ERequestType::Stop));
    WatchDogThread->join();

    // pedantic check, that porto api is watching by epoll
    if (epoll_ctl(EpollFd, EPOLL_CTL_DEL, Api.Fd, nullptr) || epoll_ctl(EpollFd, EPOLL_CTL_DEL, Sock, nullptr)) {
        Fatal("Can not epoll_ctl_del", errno);
    }

    close(EpollFd);
    close(Sock);
    close(MasterSock);
}

int TAsyncWaiter::Add(const TString &ct, const TString &state, TWaitCallback callback) {
    if (FatalError)
        return -1;

    ReqCt = ct;
    ReqState = state;
    ReqCallback = callback;

    SendInt(MasterSock, static_cast<int>(ERequestType::Add));
    return RecvInt(MasterSock);
}

int TAsyncWaiter::Remove(const TString &ct) {
    if (FatalError)
        return -1;

    ReqCt = ct;

    SendInt(MasterSock, static_cast<int>(ERequestType::Del));
    return RecvInt(MasterSock);
}
#endif

} /* namespace Porto */
