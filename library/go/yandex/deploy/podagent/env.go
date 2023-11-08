package podagent

import "os"

// Box/Workload environment variable names, documentation references:
//   - https://deploy.yandex-team.ru/docs/concepts/pod/box#systemenv
//   - https://deploy.yandex-team.ru/docs/concepts/pod/workload/workload#system_env
const (
	EnvWorkloadIDKey  = "DEPLOY_WORKLOAD_ID"
	EnvContainerIDKey = "DEPLOY_CONTAINER_ID"
	EnvBoxIDKey       = "DEPLOY_BOX_ID"
	EnvPodIDKey       = "DEPLOY_POD_ID"
	EnvProjectIDKey   = "DEPLOY_PROJECT_ID"
	EnvStageIDKey     = "DEPLOY_STAGE_ID"
	EnvUnitIDKey      = "DEPLOY_UNIT_ID"

	EnvLogsEndpointKey = "DEPLOY_LOGS_ENDPOINT"
	EnvLogsNameKey     = "DEPLOY_LOGS_DEFAULT_NAME"
	EnvLogsSecretKey   = "DEPLOY_LOGS_SECRET"

	EnvNodeClusterKey = "DEPLOY_NODE_CLUSTER"
	EnvNodeDCKey      = "DEPLOY_NODE_DC"
	EnvNodeFQDNKey    = "DEPLOY_NODE_FQDN"

	EnvPodPersistentFQDN = "DEPLOY_POD_PERSISTENT_FQDN"
	EnvPodTransientFQDN  = "DEPLOY_POD_TRANSIENT_FQDN"
)

// UnderPodAgent returns true if application managed by pod-agent.
func UnderPodAgent() bool {
	_, ok := os.LookupEnv(EnvPodIDKey)
	return ok
}
