#pragma once

#include <yt/yt/core/rpc/client.h>

#include <k8s.io/cri-api/pkg/apis/runtime/v1/api.grpc.pb.h>

namespace NYT::NContainers::NCri {

////////////////////////////////////////////////////////////////////////////////

namespace NProto = ::runtime::v1;

//! Reasonable default for CRI gRPC socket address.
constexpr TStringBuf DefaultCriEndpoint = "unix:///run/containerd/containerd.sock";

//! RuntimeReady means the runtime is up and ready to accept basic containers.
constexpr TStringBuf RuntimeReady = "RuntimeReady";

//! NetworkReady means the runtime network is up and ready to accept containers which require network.
constexpr TStringBuf NetworkReady = "NetworkReady";

//! CRI uses cgroupfs notation for systemd slices, but each name must ends with ".slice".
constexpr TStringBuf SystemdSliceSuffix = ".slice";

////////////////////////////////////////////////////////////////////////////////

//! CRI labels for pods and containers managed by YT
constexpr TStringBuf YTPodNamespaceLabel = "tech.ytsaurus.pod.namespace";
constexpr TStringBuf YTPodNameLabel = "tech.ytsaurus.pod.name";
constexpr TStringBuf YTContainerNameLabel = "tech.ytsaurus.container.name";
constexpr TStringBuf YTJobIdLabel = "tech.ytsaurus.job.id";

////////////////////////////////////////////////////////////////////////////////

#define DEFINE_CRI_API_METHOD(method, ...) \
    DEFINE_RPC_PROXY_METHOD_GENERIC(method, NProto::method##Request, NProto::method##Response, __VA_ARGS__)

//! See https://github.com/kubernetes/cri-api
class TCriRuntimeApi
    : public NRpc::TProxyBase
{
public:
    explicit TCriRuntimeApi(NRpc::IChannelPtr channel);

    static const NRpc::TServiceDescriptor& GetDescriptor();

    DEFINE_CRI_API_METHOD(Version);
    DEFINE_CRI_API_METHOD(RunPodSandbox);
    DEFINE_CRI_API_METHOD(StopPodSandbox);
    DEFINE_CRI_API_METHOD(RemovePodSandbox);
    DEFINE_CRI_API_METHOD(PodSandboxStatus);
    DEFINE_CRI_API_METHOD(ListPodSandbox);
    DEFINE_CRI_API_METHOD(CreateContainer);
    DEFINE_CRI_API_METHOD(StartContainer);
    DEFINE_CRI_API_METHOD(StopContainer);
    DEFINE_CRI_API_METHOD(RemoveContainer);
    DEFINE_CRI_API_METHOD(ListContainers);
    DEFINE_CRI_API_METHOD(ContainerStatus);
    DEFINE_CRI_API_METHOD(UpdateContainerResources);
    DEFINE_CRI_API_METHOD(ReopenContainerLog);
    DEFINE_CRI_API_METHOD(ExecSync);
    DEFINE_CRI_API_METHOD(Exec);
    DEFINE_CRI_API_METHOD(Attach);
    DEFINE_CRI_API_METHOD(PortForward);
    DEFINE_CRI_API_METHOD(ContainerStats);
    DEFINE_CRI_API_METHOD(ListContainerStats);
    DEFINE_CRI_API_METHOD(PodSandboxStats);
    DEFINE_CRI_API_METHOD(ListPodSandboxStats);
    DEFINE_CRI_API_METHOD(UpdateRuntimeConfig);
    DEFINE_CRI_API_METHOD(Status);
    DEFINE_CRI_API_METHOD(CheckpointContainer);
    DEFINE_CRI_API_METHOD(ListMetricDescriptors);
    DEFINE_CRI_API_METHOD(ListPodSandboxMetrics);

    // FIXME(khlebnikov): figure out streaming results
    // DEFINE_RPC_PROXY_METHOD_GENERIC(GetContainerEvents, NProto::GetEventsRequest, NProto::ContainerEventResponse,
    //    .SetStreamingEnabled(true));
};

////////////////////////////////////////////////////////////////////////////////

class TCriImageApi
    : public NRpc::TProxyBase
{
public:
    explicit TCriImageApi(NRpc::IChannelPtr channel);

    static const NRpc::TServiceDescriptor& GetDescriptor();

    DEFINE_CRI_API_METHOD(ListImages);
    DEFINE_CRI_API_METHOD(ImageStatus);
    DEFINE_CRI_API_METHOD(PullImage);
    DEFINE_CRI_API_METHOD(RemoveImage);
    DEFINE_CRI_API_METHOD(ImageFsInfo);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers::NCri
