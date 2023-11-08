package podagent

import (
	"encoding/json"
	"net"
)

type BoxStatus struct {
	ID       string `json:"id"`
	Revision uint32 `json:"revision"`
}

type WorkloadStatus struct {
	ID       string `json:"id"`
	Revision uint32 `json:"revision"`
}

type PodStatusResponse struct {
	Boxes     []BoxStatus      `json:"boxes"`
	Workloads []WorkloadStatus `json:"workloads"`
}

type MemoryResource struct {
	Guarantee uint64 `json:"memory_guarantee_bytes"`
	Limit     uint64 `json:"memory_limit_bytes"`
}

type CPUResource struct {
	Guarantee float64 `json:"cpu_guarantee_millicores"`
	Limit     float64 `json:"cpu_limit_millicores"`
}

type ResourceRequirements struct {
	Memory MemoryResource `json:"memory"`
	CPU    CPUResource    `json:"cpu"`
}

type NodeMeta struct {
	DC      string `json:"dc"`
	Cluster string `json:"cluster"`
	FQDN    string `json:"fqdn"`
}

type PodMeta struct {
	PodID       string          `json:"pod_id"`
	PodSetID    string          `json:"pod_set_id"`
	Annotations json.RawMessage `json:"annotations"`
	Labels      json.RawMessage `json:"labels"`
}

type Resources struct {
	Boxes map[string]ResourceRequirements `json:"box_resource_requirements"`
	Pod   ResourceRequirements            `json:"resource_requirements"`
}

type InternetAddress struct {
	Address net.IP `json:"ip4_address"`
	ID      string `json:"id"`
}

type VirtualService struct {
	IPv4Addrs []net.IP `json:"ip4_addresses"`
	IPv6Addrs []net.IP `json:"ip6_addresses"`
}

type IPAllocation struct {
	InternetAddress InternetAddress   `json:"internet_address"`
	TransientFQDN   string            `json:"transient_fqdn"`
	PersistentFQDN  string            `json:"persistent_fqdn"`
	Addr            net.IP            `json:"address"`
	VlanID          string            `json:"vlan_id"`
	VirtualServices []VirtualService  `json:"virtual_services"`
	Labels          map[string]string `json:"labels"`
}

type PodAttributesResponse struct {
	NodeMeta          NodeMeta                        `json:"node_meta"`
	PodMeta           PodMeta                         `json:"metadata"`
	BoxesRequirements map[string]ResourceRequirements `json:"box_resource_requirements"`
	PodRequirements   ResourceRequirements            `json:"resource_requirements"`
	IPAllocations     []IPAllocation                  `json:"ip6_address_allocations"`
}
