#pragma once

#include <util/generic/map.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace rpc {
    class TContainerRequest;
    class TContainerResponse;
    class TStorageListResponse;
}

namespace Porto {

constexpr int NO_TIMEOUT = 0;       // default
constexpr int SANE_TIMEOUT = 300;   // 5min
constexpr int DISK_TIMEOUT = 600;   // 10min

struct Property {
    TString Name;
    TString Description;
    bool ReadOnly = false;
    bool Dynamic = false;
};

struct VolumeLink {
    TString Container;
    TString Target;
    bool ReadOnly = false;
    bool Required = false;
};

struct Volume {
    TString Path;
    TMap<TString, TString> Properties;
    TVector<VolumeLink> Links;
};

struct Layer {
    TString Name;
    TString OwnerUser;
    TString OwnerGroup;
    TString PrivateValue;
    uint64_t LastUsage;
};

struct Storage {
    TString Name;
    TString OwnerUser;
    TString OwnerGroup;
    TString PrivateValue;
    uint64_t LastUsage;
};

struct GetResponse {
    TString Value;
    int Error;
    TString ErrorMsg;
};

struct AsyncWaitEvent {
    time_t When;
    TString Name;
    TString State;
    TString Label;
    TString Value;
};

enum GetFlags {
    NonBlock = 1,
    Sync = 2,
    Real = 4,
};

class Connection {
    class ConnectionImpl;

    std::unique_ptr<ConnectionImpl> Impl;

    Connection(const Connection&) = delete;
    void operator=(const Connection&) = delete;

public:
    Connection();
    ~Connection();

    /* each rpc call does auto-connect/reconnect */
    int Connect();
    void Close();

    /* request timeout in seconds */
    int GetTimeout() const;
    int SetTimeout(int timeout);

    TString GetLastError() const;
    void GetLastError(int &error, TString &msg) const;

    int Call(const rpc::TContainerRequest &req, rpc::TContainerResponse &rsp);
    int Call(const TString &req, TString &rsp);

    int Create(const TString &name);
    int CreateWeakContainer(const TString &name);
    int Destroy(const TString &name);

    int Start(const TString &name);
    int Stop(const TString &name, int timeout = -1);
    int Kill(const TString &name, int sig);
    int Pause(const TString &name);
    int Resume(const TString &name);
    int Respawn(const TString &name);

    int WaitContainers(const TVector<TString> &containers,
                       const TVector<TString> &labels,
                       TString &name, int timeout);

    int AsyncWait(const TVector<TString> &containers,
                  const TVector<TString> &labels,
                  std::function<void(AsyncWaitEvent &event)> callbacks,
                  int timeout = -1);
    int Recv();

    int List(TVector<TString> &list,
             const TString &mask = "");
    int ListProperties(TVector<Property> &list);

    int Get(const TVector<TString> &name,
            const TVector<TString> &variable,
            TMap<TString, TMap<TString, GetResponse>> &result,
            int flags = 0);

    int GetProperty(const TString &name,
            const TString &property, TString &value, int flags = 0);
    int SetProperty(const TString &name,
            const TString &property, TString value);

    int GetData(const TString &name, const TString &property,
                TString &value) {
        return GetProperty(name, property, value);
    }

    int IncLabel(const TString &name, const TString &label,
                 int64_t add, int64_t &result);

    int IncLabel(const TString &name, const TString &label,
                 int64_t add = 1) {
        int64_t result;
        return IncLabel(name, label, add, result);
    }

    int GetVersion(TString &tag, TString &revision);

    TString TextError() const;

    int ListVolumeProperties(TVector<Property> &list);
    int CreateVolume(const TString &path,
                     const TMap<TString, TString> &config,
                     Volume &result);
    int CreateVolume(TString &path,
                     const TMap<TString, TString> &config);
    int LinkVolume(const TString &path,
                   const TString &container = "",
                   const TString &target = "",
                   bool read_only = false,
                   bool required = false);
    int UnlinkVolume(const TString &path,
                     const TString &container = "",
                     const TString &target = "***",
                     bool strict = false);
    int ListVolumes(const TString &path, const TString &container,
                    TVector<Volume> &volumes);
    int ListVolumes(TVector<Volume> &volumes) {
        return ListVolumes(TString(), TString(), volumes);
    }
    int TuneVolume(const TString &path,
                   const TMap<TString, TString> &config);

    int ImportLayer(const TString &layer, const TString &tarball,
                    bool merge = false, const TString &place = "",
                    const TString &private_value = "");

    int ExportLayer(const TString &volume, const TString &tarball,
                    const TString &compress = "");
    int RemoveLayer(const TString &layer, const TString &place = "");
    int ListLayers(TVector<Layer> &layers,
                   const TString &place = "",
                   const TString &mask = "");

    int GetLayerPrivate(TString &private_value, const TString &layer,
                        const TString &place = "");
    int SetLayerPrivate(const TString &private_value, const TString &layer,
                        const TString &place = "");

    const rpc::TStorageListResponse *ListStorage(const TString &place = "", const TString &mask = "");

    int RemoveStorage(const TString &name, const TString &place = "");
    int ImportStorage(const TString &name,
                      const TString &archive,
                      const TString &place = "",
                      const TString &compression = "",
                      const TString &private_value = "");
    int ExportStorage(const TString &name,
                      const TString &archive,
                      const TString &place = "",
                      const TString &compression = "");

    int ConvertPath(const TString &path, const TString &src,
                    const TString &dest, TString &res);

    int AttachProcess(const TString &name,
                      int pid, const TString &comm);
    int AttachThread(const TString &name,
                     int pid, const TString &comm);
    int LocateProcess(int pid, const TString &comm, TString &name);
};

} /* namespace Porto */
