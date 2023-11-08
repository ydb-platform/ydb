package yplite

type PodSpec struct {
	DNS                   PodDNS                 `json:"dns"`
	ResourceRequests      ResourceRequest        `json:"resourceRequests"`
	PortoProperties       []PortoProperty        `json:"portoProperties"`
	IP6AddressAllocations []IP6AddressAllocation `json:"ip6AddressAllocations"`
}

type PodAttributes struct {
	ResourceRequirements struct {
		CPU struct {
			Guarantee uint64 `json:"cpu_guarantee_millicores,string"`
			Limit     uint64 `json:"cpu_limit_millicores,string"`
		} `json:"cpu"`
		Memory struct {
			Guarantee uint64 `json:"memory_guarantee_bytes,string"`
			Limit     uint64 `json:"memory_limit_bytes,string"`
		} `json:"memory"`
	} `json:"resource_requirements"`
}

type ResourceRequest struct {
	CPUGuarantee         uint64 `json:"vcpuGuarantee,string"`
	CPULimit             uint64 `json:"vcpuLimit,string"`
	MemoryGuarantee      uint64 `json:"memoryGuarantee,string"`
	MemoryLimit          uint64 `json:"memoryLimit,string"`
	AnonymousMemoryLimit uint64 `json:"anonymousMemoryLimit,string"`
}

type IP6AddressAllocation struct {
	Address        string `json:"address"`
	VlanID         string `json:"vlanId"`
	PersistentFQDN string `json:"persistentFqdn"`
	TransientFQDN  string `json:"transientFqdn"`
}

type PortoProperty struct {
	Name  string `json:"key"`
	Value string `json:"value"`
}

type PodDNS struct {
	PersistentFqdn string `json:"persistentFqdn"`
	TransientFqdn  string `json:"transientFqdn"`
}
