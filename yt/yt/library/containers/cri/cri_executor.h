#pragma once

#include "public.h"
#include "config.h"
#include "cri_api.h"

#include <yt/yt/library/process/process.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NContainers::NCri {

////////////////////////////////////////////////////////////////////////////////

struct TCriDescriptor
{
    TString Name;
    TString Id;
};

struct TCriPodDescriptor
{
    TString Name;
    TString Id;
};

struct TCriImageDescriptor
{
    TString Image;
};

void FormatValue(TStringBuilderBase* builder, const TCriDescriptor& descriptor, TStringBuf spec);
void FormatValue(TStringBuilderBase* builder, const TCriPodDescriptor& descriptor, TStringBuf spec);
void FormatValue(TStringBuilderBase* builder, const TCriImageDescriptor& descriptor, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

struct TCriContainerResources
{
    std::optional<double> CpuLimit;
    std::optional<double> CpuRequest;
    std::optional<i64> MemoryLimit;
    std::optional<i64> MemoryRequest;
};

struct TCriPodSpec
    : public TRefCounted
{
    TString Name;
    TCriContainerResources Resources;
};

DEFINE_REFCOUNTED_TYPE(TCriPodSpec)

struct TCriBindMount
{
    TString ContainerPath;
    TString HostPath;
    bool ReadOnly;
};

struct TCriCredentials
{
    std::optional<i64> Uid;
    std::optional<i64> Gid;
    std::vector<i64> Groups;
};

struct TCriContainerSpec
    : public TRefCounted
{
    TString Name;

    THashMap<TString, TString> Labels;

    TCriImageDescriptor Image;

    bool ReadOnlyRootFS;

    std::vector<TCriBindMount> BindMounts;

    TCriCredentials Credentials;

    TCriContainerResources Resources;

    //! Command to execute (i.e., entrypoint for docker).
    std::vector<TString> Command;

    //! Arguments for the Command (i.e., command for docker).
    std::vector<TString> Arguments;

    //! Current working directory of the command.
    TString WorkingDirectory;

    //! Environment variable to set in the container.
    THashMap<TString, TString> Environment;
};

DEFINE_REFCOUNTED_TYPE(TCriContainerSpec)

////////////////////////////////////////////////////////////////////////////////

//! Wrapper around CRI gRPC API
//!
//! @see yt/yt/contrib/cri-api/k8s.io/cri-api/pkg/apis/runtime/v1/api.proto
//! @see https://github.com/kubernetes/cri-api
struct ICriExecutor
    : public TRefCounted
{
    //! Returns status of the CRI runtime.
    //! @param verbose fill field "info" with runtime-specific debug.
    virtual TFuture<TCriRuntimeApi::TRspStatusPtr> GetRuntimeStatus(bool verbose = false) = 0;

    // PodSandbox

    virtual TString GetPodCgroup(TString podName) const = 0;

    virtual TFuture<TCriRuntimeApi::TRspListPodSandboxPtr> ListPodSandbox(
        std::function<void(NProto::PodSandboxFilter&)> initFilter = nullptr) = 0;

    virtual TFuture<TCriRuntimeApi::TRspListContainersPtr> ListContainers(
        std::function<void(NProto::ContainerFilter&)> initFilter = nullptr) = 0;

    virtual TFuture<void> ForEachPodSandbox(
        const TCallback<void(const TCriPodDescriptor&, const NProto::PodSandbox&)>& callback,
        std::function<void(NProto::PodSandboxFilter&)> initFilter = nullptr) = 0;

    virtual TFuture<void> ForEachContainer(
        const TCallback<void(const TCriDescriptor&, const NProto::Container&)>& callback,
        std::function<void(NProto::ContainerFilter&)> initFilter = nullptr) = 0;

    //! Returns status of the pod.
    //! @param verbose fill field "info" with runtime-specific debug.
    virtual TFuture<TCriRuntimeApi::TRspPodSandboxStatusPtr> GetPodSandboxStatus(
        const TCriPodDescriptor& pod, bool verbose = false) = 0;

    //! Returns status of the container.
    //! @param verbose fill "info" with runtime-specific debug information.
    virtual TFuture<TCriRuntimeApi::TRspContainerStatusPtr> GetContainerStatus(
        const TCriDescriptor& ct, bool verbose = false) = 0;

    virtual TFuture<TCriPodDescriptor> RunPodSandbox(TCriPodSpecPtr podSpec) = 0;
    virtual TFuture<void> StopPodSandbox(const TCriPodDescriptor& pod) = 0;
    virtual TFuture<void> RemovePodSandbox(const TCriPodDescriptor& pod) = 0;
    virtual TFuture<void> UpdatePodResources(
        const TCriPodDescriptor& pod,
        const TCriContainerResources& resources) = 0;

    //! Remove all pods and containers in namespace managed by executor.
    virtual void CleanNamespace() = 0;

    //! Remove all containers in one pod.
    virtual void CleanPodSandbox(const TCriPodDescriptor& pod) = 0;

    virtual TFuture<TCriDescriptor> CreateContainer(
        TCriContainerSpecPtr containerSpec,
        const TCriPodDescriptor& pod,
        TCriPodSpecPtr podSpec) = 0;

    virtual TFuture<void> StartContainer(const TCriDescriptor& ct) = 0;

    //! Stops container if it's running.
    //! @param timeout defines timeout for graceful stop,  timeout=0 - kill instantly.
    virtual TFuture<void> StopContainer(
        const TCriDescriptor& ct,
        TDuration timeout = TDuration::Zero()) = 0;

    virtual TFuture<void> RemoveContainer(const TCriDescriptor& ct) = 0;

    virtual TFuture<void> UpdateContainerResources(
        const TCriDescriptor& ct,
        const TCriContainerResources& resources) = 0;

    virtual TFuture<TCriImageApi::TRspListImagesPtr> ListImages(
        std::function<void(NProto::ImageFilter&)> initFilter = nullptr) = 0;

    //! Returns status of the image.
    //! @param verbose fill field "info" with runtime-specific debug.
    virtual TFuture<TCriImageApi::TRspImageStatusPtr> GetImageStatus(
        const TCriImageDescriptor& image,
        bool verbose = false) = 0;

    virtual TFuture<TCriImageDescriptor> PullImage(
        const TCriImageDescriptor& image,
        bool always = false,
        TCriAuthConfigPtr authConfig = nullptr,
        TCriPodSpecPtr podSpec = nullptr) = 0;

    virtual TFuture<void> RemoveImage(const TCriImageDescriptor& image) = 0;

    // FIXME(khlebnikov): temporary compat
    virtual TProcessBasePtr CreateProcess(
        const TString& path,
        TCriContainerSpecPtr containerSpec,
        const TCriPodDescriptor& pod,
        TCriPodSpecPtr podSpec) = 0;
};

DEFINE_REFCOUNTED_TYPE(ICriExecutor)

////////////////////////////////////////////////////////////////////////////////

ICriExecutorPtr CreateCriExecutor(TCriExecutorConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers::NCri
