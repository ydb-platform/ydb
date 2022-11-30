package maxprocs

import (
	"context"
	"os"
	"runtime"
	"strings"

	"a.yandex-team.ru/library/go/yandex/deploy/podagent"
	"a.yandex-team.ru/library/go/yandex/yplite"
)

const (
	SafeProc = 4
	MinProc  = 2
	MaxProc  = 8

	GoMaxProcEnvName      = "GOMAXPROCS"
	QloudCPUEnvName       = "QLOUD_CPU_GUARANTEE"
	InstancectlCPUEnvName = "CPU_GUARANTEE"
	DeloyBoxIDName        = podagent.EnvBoxIDKey
)

// Adjust adjust the maximum number of CPUs that can be executing.
// Takes a minimum between n and CPU counts and returns the previous setting
func Adjust(n int) int {
	if n < MinProc {
		n = MinProc
	}

	nCPU := runtime.NumCPU()
	if n < nCPU {
		return runtime.GOMAXPROCS(n)
	}

	return runtime.GOMAXPROCS(nCPU)
}

// AdjustAuto automatically adjust the maximum number of CPUs that can be executing to safe value
// and returns the previous setting
func AdjustAuto() int {
	if val, ok := getEnv(GoMaxProcEnvName); ok {
		return applyIntStringLimit(val)
	}

	if isCgroupsExists() {
		return AdjustCgroup()
	}

	if val, ok := getEnv(InstancectlCPUEnvName); ok {
		return applyFloatStringLimit(strings.TrimRight(val, "c"))
	}

	if val, ok := getEnv(QloudCPUEnvName); ok {
		return applyFloatStringLimit(val)
	}

	if boxID, ok := os.LookupEnv(DeloyBoxIDName); ok {
		return adjustYPBox(boxID)
	}

	if yplite.IsAPIAvailable() {
		return AdjustYPLite()
	}

	return Adjust(SafeProc)
}

// AdjustQloud automatically adjust the maximum number of CPUs in case of Qloud env
// and returns the previous setting
func AdjustQloud() int {
	if val, ok := getEnv(GoMaxProcEnvName); ok {
		return applyIntStringLimit(val)
	}

	if val, ok := getEnv(QloudCPUEnvName); ok {
		return applyFloatStringLimit(val)
	}

	return Adjust(MaxProc)
}

// AdjustYP automatically adjust the maximum number of CPUs in case of YP/Y.Deploy/YP.Hard env
// and returns the previous setting
func AdjustYP() int {
	if val, ok := getEnv(GoMaxProcEnvName); ok {
		return applyIntStringLimit(val)
	}

	if isCgroupsExists() {
		return AdjustCgroup()
	}

	return adjustYPBox(os.Getenv(DeloyBoxIDName))
}

func adjustYPBox(boxID string) int {
	resources, err := podagent.NewClient().PodAttributes(context.Background())
	if err != nil {
		return Adjust(SafeProc)
	}

	var cpuGuarantee float64
	if boxResources, ok := resources.BoxesRequirements[boxID]; ok {
		cpuGuarantee = boxResources.CPU.Guarantee / 1000
	}

	if cpuGuarantee <= 0 {
		// if we don't have guarantees for current box, let's use pod guarantees
		cpuGuarantee = resources.PodRequirements.CPU.Guarantee / 1000
	}

	return applyFloatLimit(cpuGuarantee)
}

// AdjustYPLite automatically adjust the maximum number of CPUs in case of YP.Lite env
// and returns the previous setting
func AdjustYPLite() int {
	if val, ok := getEnv(GoMaxProcEnvName); ok {
		return applyIntStringLimit(val)
	}

	podAttributes, err := yplite.FetchPodAttributes()
	if err != nil {
		return Adjust(SafeProc)
	}

	return applyFloatLimit(float64(podAttributes.ResourceRequirements.CPU.Guarantee / 1000))
}

// AdjustInstancectl automatically adjust the maximum number of CPUs
// and returns the previous setting
// WARNING: supported only instancectl v1.177+ (https://wiki.yandex-team.ru/runtime-cloud/nanny/instancectl-change-log/#1.177)
func AdjustInstancectl() int {
	if val, ok := getEnv(GoMaxProcEnvName); ok {
		return applyIntStringLimit(val)
	}

	if val, ok := getEnv(InstancectlCPUEnvName); ok {
		return applyFloatStringLimit(strings.TrimRight(val, "c"))
	}

	return Adjust(MaxProc)
}

// AdjustCgroup automatically adjust the maximum number of CPUs based on the CFS quota
// and returns the previous setting.
func AdjustCgroup() int {
	if val, ok := getEnv(GoMaxProcEnvName); ok {
		return applyIntStringLimit(val)
	}

	quota, err := getCFSQuota()
	if err != nil {
		return Adjust(SafeProc)
	}

	return applyFloatLimit(quota)
}
