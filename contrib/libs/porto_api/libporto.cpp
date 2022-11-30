#include "libporto.hpp"

#include <google/protobuf/text_format.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/io/coded_stream.h>

#include <contrib/libs/porto_api/protos/rpc.pb.h>

using ::rpc::EError;

extern "C" {
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>
}

#include <errno.h>

namespace Porto {

static const char PortoSocket[] = "/run/portod.socket";

class Connection::ConnectionImpl {
public:
    int Fd = -1;
    int Timeout = 0;

    rpc::TContainerRequest Req;
    rpc::TContainerResponse Rsp;

    TVector<TString> AsyncWaitContainers;
    int AsyncWaitTimeout = -1;
    std::function<void(AsyncWaitEvent &event)> AsyncWaitCallback;

    int LastError = 0;
    TString LastErrorMsg;

    int Error(int err, const TString &prefix) {
        switch (err) {
        case ENOENT:
            LastError = EError::SocketUnavailable;
            break;
        case EAGAIN:
            LastError = EError::SocketTimeout;
            break;
        case EIO:
            LastError = EError::SocketError;
            break;
        default:
            LastError = EError::Unknown;
            break;
        }
        LastErrorMsg = TString(prefix + ": " + strerror(err));
        Close();
        return LastError;
    }

    ConnectionImpl() { }

    ~ConnectionImpl() {
        Close();
    }

    int Connect();

    int SetTimeout(int direction, int timeout);

    void Close() {
        if (Fd >= 0)
            close(Fd);
        Fd = -1;
    }

    int Send(const rpc::TContainerRequest &req);
    int Recv(rpc::TContainerResponse &rsp);
    int Call(const rpc::TContainerRequest &req, rpc::TContainerResponse &rsp);
    int Call();
};

int Connection::ConnectionImpl::Connect()
{
    struct sockaddr_un peer_addr;
    socklen_t peer_addr_size;

    if (Fd >= 0)
        Close();

    Fd = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (Fd < 0)
        return Error(errno, "socket");

    if (Timeout && SetTimeout(3, Timeout))
        return LastError;

    memset(&peer_addr, 0, sizeof(struct sockaddr_un));
    peer_addr.sun_family = AF_UNIX;
    strncpy(peer_addr.sun_path, PortoSocket, sizeof(PortoSocket) - 1);

    peer_addr_size = sizeof(struct sockaddr_un);
    if (connect(Fd, (struct sockaddr *) &peer_addr, peer_addr_size) < 0)
        return Error(errno, "connect");

    /* restore async wait */
    if (!AsyncWaitContainers.empty()) {
        for (auto &name: AsyncWaitContainers)
            Req.mutable_asyncwait()->add_name(name);
        if (AsyncWaitTimeout >= 0)
            Req.mutable_asyncwait()->set_timeout_ms(AsyncWaitTimeout * 1000);
        return Call();
    }

    return EError::Success;
}

int Connection::ConnectionImpl::SetTimeout(int direction, int timeout)
{
    struct timeval tv;

    tv.tv_sec = timeout;
    tv.tv_usec = 0;

    if ((direction & 1) && setsockopt(Fd, SOL_SOCKET,
                SO_SNDTIMEO, &tv, sizeof tv))
        return Error(errno, "set send timeout");

    if ((direction & 2) && setsockopt(Fd, SOL_SOCKET,
                SO_RCVTIMEO, &tv, sizeof tv))
        return Error(errno, "set recv timeout");

    return EError::Success;
}

int Connection::ConnectionImpl::Send(const rpc::TContainerRequest &req) {
    google::protobuf::io::FileOutputStream raw(Fd);

    {
        google::protobuf::io::CodedOutputStream output(&raw);

        output.WriteVarint32(req.ByteSize());
        req.SerializeWithCachedSizes(&output);
    }

    raw.Flush();

    int err = raw.GetErrno();
    if (err)
        return Error(err, "send");

    return EError::Success;
}

int Connection::ConnectionImpl::Recv(rpc::TContainerResponse &rsp) {
    google::protobuf::io::FileInputStream raw(Fd);
    google::protobuf::io::CodedInputStream input(&raw);

    while (true) {
        uint32_t size;

        if (!input.ReadVarint32(&size))
            return Error(raw.GetErrno() ?: EIO, "recv");

        auto prev_limit = input.PushLimit(size);

        rsp.Clear();

        if (!rsp.ParseFromCodedStream(&input))
            return Error(raw.GetErrno() ?: EIO, "recv");

        input.PopLimit(prev_limit);

        if (rsp.has_asyncwait()) {
            if (AsyncWaitCallback) {
                AsyncWaitEvent event = {
                    static_cast<time_t>(rsp.asyncwait().when()),
                    rsp.asyncwait().name(),
                    rsp.asyncwait().state(),
                    rsp.asyncwait().label(),
                    rsp.asyncwait().value(),
                };
                AsyncWaitCallback(event);
            }
        } else
            return EError::Success;
    }
}


int Connection::ConnectionImpl::Call(const rpc::TContainerRequest &req,
                                     rpc::TContainerResponse &rsp) {
    int ret = 0;

    if (Fd < 0)
        ret = Connect();

    if (!ret)
        ret = Send(req);

    if (!ret)
        ret = Recv(rsp);

    if (!ret) {
        LastErrorMsg = rsp.errormsg();
        ret = LastError = (int)rsp.error();
    }

    return ret;
}

int Connection::ConnectionImpl::Call() {
    int ret = Call(Req, Rsp);
    Req.Clear();
    return ret;
}

Connection::Connection() : Impl(new ConnectionImpl()) { }

Connection::~Connection() {
    Impl = nullptr;
}

int Connection::Connect() {
    return Impl->Connect();
}

int Connection::GetTimeout() const {
    return Impl->Timeout;
}

int Connection::SetTimeout(int timeout) {
    Impl->Timeout = timeout;
    if (Impl->Fd >= 0)
        return Impl->SetTimeout(3, timeout);
    return EError::Success;
}

void Connection::Close() {
    Impl->Close();
}

int Connection::Call(const rpc::TContainerRequest &req, rpc::TContainerResponse &rsp) {

    if (!req.IsInitialized()) {
        Impl->LastError = EError::InvalidMethod;
        Impl->LastErrorMsg = "Request is not initialized";
        return (int)EError::InvalidMethod;
    }

    return Impl->Call(req, rsp);
}

int Connection::Call(const TString &req, TString &rsp) {

    if (!google::protobuf::TextFormat::ParseFromString(req, &Impl->Req)) {
        Impl->LastError = EError::InvalidMethod;
        Impl->LastErrorMsg = "Cannot parse request";
        return (int)EError::InvalidMethod;
    }

    int ret = Call(Impl->Req, Impl->Rsp);
    Impl->Req.Clear();
    rsp = Impl->Rsp.DebugString();

    return ret;
}

int Connection::Create(const TString &name) {
    Impl->Req.mutable_create()->set_name(name);

    return Impl->Call();
}

int Connection::CreateWeakContainer(const TString &name) {
    Impl->Req.mutable_createweak()->set_name(name);

    return Impl->Call();
}

int Connection::Destroy(const TString &name) {
    Impl->Req.mutable_destroy()->set_name(name);

    return Impl->Call();
}

int Connection::List(TVector<TString> &list, const TString &mask) {
    Impl->Req.mutable_list();

    if(!mask.empty())
        Impl->Req.mutable_list()->set_mask(mask);

    int ret = Impl->Call();
    if (!ret)
        list = TVector<TString>(std::begin(Impl->Rsp.list().name()),
                                        std::end(Impl->Rsp.list().name()));

    return ret;
}

int Connection::ListProperties(TVector<Property> &list) {
    Impl->Req.mutable_propertylist();

    int ret = Impl->Call();
    bool has_data = false;
    int i = 0;

    if (!ret) {
        list.resize(Impl->Rsp.propertylist().list_size());
        for (const auto &prop: Impl->Rsp.propertylist().list()) {
            list[i].Name = prop.name();
            list[i].Description = prop.desc();
            list[i].ReadOnly =  prop.read_only();
            list[i].Dynamic =  prop.dynamic();
            has_data |= list[i].ReadOnly;
            i++;
        }
    }

    if (!has_data) {
        Impl->Req.mutable_datalist();
        ret = Impl->Call();
        if (!ret) {
            list.resize(list.size() + Impl->Rsp.datalist().list_size());
            for (const auto &data: Impl->Rsp.datalist().list()) {
                list[i].Name = data.name();
                list[i++].Description = data.desc();
            }
        }
    }

    return ret;
}

int Connection::Get(const TVector<TString> &name,
                   const TVector<TString> &variable,
                   TMap<TString, TMap<TString, GetResponse>> &result,
                   int flags) {
    auto get = Impl->Req.mutable_get();

    for (const auto &n : name)
        get->add_name(n);
    for (const auto &v : variable)
        get->add_variable(v);

    if (flags & GetFlags::NonBlock)
        get->set_nonblock(true);
    if (flags & GetFlags::Sync)
        get->set_sync(true);
    if (flags & GetFlags::Real)
        get->set_real(true);

    int ret = Impl->Call();
    if (!ret) {
         for (int i = 0; i < Impl->Rsp.get().list_size(); i++) {
             const auto &entry = Impl->Rsp.get().list(i);

             for (int j = 0; j < entry.keyval_size(); j++) {
                 auto keyval = entry.keyval(j);

                 GetResponse resp;
                 resp.Error = 0;
                 if (keyval.has_error())
                     resp.Error = keyval.error();
                 if (keyval.has_errormsg())
                     resp.ErrorMsg = keyval.errormsg();
                 if (keyval.has_value())
                     resp.Value = keyval.value();

                 result[entry.name()][keyval.variable()] = resp;
             }
         }
    }

    return ret;
}

int Connection::GetProperty(const TString &name, const TString &property,
                           TString &value, int flags) {
    auto* get_property = Impl->Req.mutable_getproperty();
    get_property->set_name(name);
    get_property->set_property(property);
    if (flags & GetFlags::Sync)
        get_property->set_sync(true);
    if (flags & GetFlags::Real)
        get_property->set_real(true);

    int ret = Impl->Call();
    if (!ret)
        value.assign(Impl->Rsp.getproperty().value());

    return ret;
}

int Connection::SetProperty(const TString &name, const TString &property,
                           TString value) {
    auto* set_property = Impl->Req.mutable_setproperty();
    set_property->set_name(name);
    set_property->set_property(property);
    set_property->set_value(value);

    return Impl->Call();
}

int Connection::IncLabel(const TString &name, const TString &label,
                         int64_t add, int64_t &result) {
    auto cmd = Impl->Req.mutable_inclabel();
    cmd->set_name(name);
    cmd->set_label(label);
    cmd->set_add(add);
    int ret = Impl->Call();
    if (Impl->Rsp.has_inclabel())
        result = Impl->Rsp.inclabel().result();
    return ret;
}

int Connection::GetVersion(TString &tag, TString &revision) {
    Impl->Req.mutable_version();

    int ret = Impl->Call();
    if (!ret) {
        tag = Impl->Rsp.version().tag();
        revision = Impl->Rsp.version().revision();
    }

    return ret;
}

int Connection::Start(const TString &name) {
    Impl->Req.mutable_start()->set_name(name);

    return Impl->Call();
}

int Connection::Stop(const TString &name, int timeout) {
    auto stop = Impl->Req.mutable_stop();

    stop->set_name(name);
    if (timeout >= 0)
        stop->set_timeout_ms(timeout * 1000);

    return Impl->Call();
}

int Connection::Kill(const TString &name, int sig) {
    Impl->Req.mutable_kill()->set_name(name);
    Impl->Req.mutable_kill()->set_sig(sig);

    return Impl->Call();
}

int Connection::Pause(const TString &name) {
    Impl->Req.mutable_pause()->set_name(name);

    return Impl->Call();
}

int Connection::Resume(const TString &name) {
    Impl->Req.mutable_resume()->set_name(name);

    return Impl->Call();
}

int Connection::Respawn(const TString &name) {
    Impl->Req.mutable_respawn()->set_name(name);
    return Impl->Call();
}

int Connection::WaitContainers(const TVector<TString> &containers,
                               const TVector<TString> &labels,
                               TString &name, int timeout) {
    auto wait = Impl->Req.mutable_wait();
    int ret, recv_timeout = 0;

    for (const auto &c : containers)
        wait->add_name(c);

    for (auto &label: labels)
        wait->add_label(label);

    if (timeout >= 0) {
        wait->set_timeout_ms(timeout * 1000);
        recv_timeout = timeout + (Impl->Timeout ?: timeout);
    }

    if (Impl->Fd < 0 && Connect())
        return Impl->LastError;

    if (timeout && Impl->SetTimeout(2, recv_timeout))
        return Impl->LastError;

    ret = Impl->Call();

    if (timeout && Impl->Fd >= 0)
        Impl->SetTimeout(2, Impl->Timeout);

    name.assign(Impl->Rsp.wait().name());
    return ret;
}

int Connection::AsyncWait(const TVector<TString> &containers,
                          const TVector<TString> &labels,
                          std::function<void(AsyncWaitEvent &event)> callback,
                          int timeout) {
    Impl->AsyncWaitContainers.clear();
    Impl->AsyncWaitTimeout = timeout;
    Impl->AsyncWaitCallback = callback;
    for (auto &name: containers)
        Impl->Req.mutable_asyncwait()->add_name(name);
    for (auto &label: labels)
        Impl->Req.mutable_asyncwait()->add_label(label);
    if (timeout >= 0)
        Impl->Req.mutable_asyncwait()->set_timeout_ms(timeout * 1000);
    int ret = Impl->Call();
    if (ret)
        Impl->AsyncWaitCallback = nullptr;
    else
        Impl->AsyncWaitContainers = containers;
    return ret;
}

int Connection::Recv() {
    return Impl->Recv(Impl->Rsp);
}

TString Connection::GetLastError() const {
    return rpc::EError_Name((EError)Impl->LastError) + ":(" + Impl->LastErrorMsg + ")";
}

void Connection::GetLastError(int &error, TString &msg) const {
    error = Impl->LastError;
    msg = Impl->LastErrorMsg;
}

TString Connection::TextError() const {
    return rpc::EError_Name((EError)Impl->LastError) + ":" + Impl->LastErrorMsg;
}

int Connection::ListVolumeProperties(TVector<Property> &list) {
    Impl->Req.mutable_listvolumeproperties();

    int ret = Impl->Call();
    if (!ret) {
        int i = 0;
        list.resize(Impl->Rsp.volumepropertylist().properties_size());
        for (const auto &prop: Impl->Rsp.volumepropertylist().properties()) {
            list[i].Name = prop.name();
            list[i++].Description =  prop.desc();
        }
    }

    return ret;
}

int Connection::CreateVolume(const TString &path,
                            const TMap<TString, TString> &config,
                            Volume &result) {
    auto req = Impl->Req.mutable_createvolume();

    req->set_path(path);

    for (const auto &kv: config) {
        auto prop = req->add_properties();
        prop->set_name(kv.first);
        prop->set_value(kv.second);
    }

    int ret = Impl->Call();
    if (!ret) {
        const auto &volume = Impl->Rsp.volume();
        result.Path = volume.path();
        for (const auto &p: volume.properties())
            result.Properties[p.name()] = p.value();
    }
    return ret;
}

int Connection::CreateVolume(TString &path,
                            const TMap<TString, TString> &config) {
    Volume result;
    int ret = CreateVolume(path, config, result);
    if (!ret && path.empty())
        path = result.Path;
    return ret;
}

int Connection::LinkVolume(const TString &path, const TString &container,
        const TString &target, bool read_only, bool required) {
    auto req = (target == "" && !required) ? Impl->Req.mutable_linkvolume() :
                                             Impl->Req.mutable_linkvolumetarget();
    req->set_path(path);
    if (!container.empty())
        req->set_container(container);
    if (target != "")
        req->set_target(target);
    if (read_only)
        req->set_read_only(read_only);
    if (required)
        req->set_required(required);
    return Impl->Call();
}

int Connection::UnlinkVolume(const TString &path,
                             const TString &container,
                             const TString &target,
                             bool strict) {
    auto req = (target == "***") ? Impl->Req.mutable_unlinkvolume() :
                                   Impl->Req.mutable_unlinkvolumetarget();

    req->set_path(path);
    if (!container.empty())
        req->set_container(container);
    if (target != "***")
        req->set_target(target);
    if (strict)
        req->set_strict(strict);

    return Impl->Call();
}

int Connection::ListVolumes(const TString &path,
                           const TString &container,
                           TVector<Volume> &volumes) {
    auto req = Impl->Req.mutable_listvolumes();

    if (!path.empty())
        req->set_path(path);

    if (!container.empty())
        req->set_container(container);

    int ret = Impl->Call();
    if (!ret) {
        const auto &list = Impl->Rsp.volumelist();

        volumes.resize(list.volumes().size());
        int i = 0;

        for (const auto &v: list.volumes()) {
            volumes[i].Path = v.path();
            int l = 0;
            if (v.links().size()) {
                volumes[i].Links.resize(v.links().size());
                for (auto &link: v.links()) {
                    volumes[i].Links[l].Container = link.container();
                    volumes[i].Links[l].Target = link.target();
                    volumes[i].Links[l].ReadOnly = link.read_only();
                    volumes[i].Links[l].Required = link.required();
                    ++l;
                }
            } else {
                volumes[i].Links.resize(v.containers().size());
                for (auto &ct: v.containers())
                    volumes[i].Links[l++].Container = ct;
            }
            for (const auto &p: v.properties())
                volumes[i].Properties[p.name()] = p.value();
            i++;
        }
    }

    return ret;
}

int Connection::TuneVolume(const TString &path,
                          const TMap<TString, TString> &config) {
    auto req = Impl->Req.mutable_tunevolume();

    req->set_path(path);

    for (const auto &kv: config) {
        auto prop = req->add_properties();
        prop->set_name(kv.first);
        prop->set_value(kv.second);
    }

    return Impl->Call();
}

int Connection::ImportLayer(const TString &layer,
                            const TString &tarball, bool merge,
                            const TString &place,
                            const TString &private_value) {
    auto req = Impl->Req.mutable_importlayer();

    req->set_layer(layer);
    req->set_tarball(tarball);
    req->set_merge(merge);
    if (place.size())
        req->set_place(place);
    if (private_value.size())
        req->set_private_value(private_value);
    return Impl->Call();
}

int Connection::ExportLayer(const TString &volume,
                           const TString &tarball,
                           const TString &compress) {
    auto req = Impl->Req.mutable_exportlayer();

    req->set_volume(volume);
    req->set_tarball(tarball);
    if (compress.size())
        req->set_compress(compress);
    return Impl->Call();
}

int Connection::RemoveLayer(const TString &layer, const TString &place) {
    auto req = Impl->Req.mutable_removelayer();

    req->set_layer(layer);
    if (place.size())
        req->set_place(place);
    return Impl->Call();
}

int Connection::ListLayers(TVector<Layer> &layers,
                           const TString &place,
                           const TString &mask) {
    auto req = Impl->Req.mutable_listlayers();
    if (place.size())
        req->set_place(place);
    if (mask.size())
        req->set_mask(mask);
    int ret = Impl->Call();
    if (!ret) {
        if (Impl->Rsp.layers().layers().size()) {
            for (auto &layer: Impl->Rsp.layers().layers()) {
                Layer l;
                l.Name = layer.name();
                l.OwnerUser = layer.owner_user();
                l.OwnerGroup = layer.owner_group();
                l.PrivateValue = layer.private_value();
                l.LastUsage = layer.last_usage();
                layers.push_back(l);
            }
        } else {
            for (auto &layer: Impl->Rsp.layers().layer()) {
                Layer l;
                l.Name = layer;
                layers.push_back(l);
            }
        }
    }
    return ret;
}

int Connection::GetLayerPrivate(TString &private_value,
                                const TString &layer,
                                const TString &place) {
    auto req = Impl->Req.mutable_getlayerprivate();
    req->set_layer(layer);
    if (place.size())
        req->set_place(place);
    int ret = Impl->Call();
    if (!ret) {
        private_value = Impl->Rsp.layer_private().private_value();
    }
    return ret;
}

int Connection::SetLayerPrivate(const TString &private_value,
                                const TString &layer,
                                const TString &place) {
    auto req = Impl->Req.mutable_setlayerprivate();
    req->set_layer(layer);
    req->set_private_value(private_value);
    if (place.size())
        req->set_place(place);
    return Impl->Call();
}

const rpc::TStorageListResponse *Connection::ListStorage(const TString &place, const TString &mask) {
    auto req = Impl->Req.mutable_liststorage();
    if (place.size())
        req->set_place(place);
    if (mask.size())
        req->set_mask(mask);
    if (Impl->Call())
        return nullptr;
    return &Impl->Rsp.storagelist();
}

int Connection::RemoveStorage(const TString &name,
                              const TString &place) {
    auto req = Impl->Req.mutable_removestorage();

    req->set_name(name);
    if (place.size())
        req->set_place(place);
    return Impl->Call();
}

int Connection::ImportStorage(const TString &name,
                              const TString &archive,
                              const TString &place,
                              const TString &compression,
                              const TString &private_value) {
    auto req = Impl->Req.mutable_importstorage();

    req->set_name(name);
    req->set_tarball(archive);
    if (place.size())
        req->set_place(place);
    if (compression.size())
        req->set_compress(compression);
    if (private_value.size())
        req->set_private_value(private_value);
    return Impl->Call();
}

int Connection::ExportStorage(const TString &name,
                              const TString &archive,
                              const TString &place,
                              const TString &compression) {
    auto req = Impl->Req.mutable_exportstorage();

    req->set_name(name);
    req->set_tarball(archive);
    if (place.size())
        req->set_place(place);
    if (compression.size())
        req->set_compress(compression);
    return Impl->Call();
}

int Connection::ConvertPath(const TString &path, const TString &src,
                           const TString &dest, TString &res) {
    auto req = Impl->Req.mutable_convertpath();
    req->set_path(path);
    req->set_source(src);
    req->set_destination(dest);

    auto ret = Impl->Call();
    if (!ret)
        res = Impl->Rsp.convertpath().path();
    return ret;
}

int Connection::AttachProcess(const TString &name,
                              int pid, const TString &comm) {
    auto req = Impl->Req.mutable_attachprocess();
    req->set_name(name);
    req->set_pid(pid);
    req->set_comm(comm);
    return Impl->Call();
}

int Connection::AttachThread(const TString &name,
                              int pid, const TString &comm) {
    auto req = Impl->Req.mutable_attachthread();
    req->set_name(name);
    req->set_pid(pid);
    req->set_comm(comm);
    return Impl->Call();
}

int Connection::LocateProcess(int pid, const TString &comm,
                              TString &name) {
    Impl->Req.mutable_locateprocess()->set_pid(pid);
    Impl->Req.mutable_locateprocess()->set_comm(comm);

    int ret = Impl->Call();

    if (ret)
        return ret;

    name = Impl->Rsp.locateprocess().name();

    return ret;
}

} /* namespace Porto */
