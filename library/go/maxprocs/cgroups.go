package maxprocs

import (
	"errors"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/prometheus/procfs"

	"a.yandex-team.ru/library/go/slices"
)

const (
	unifiedHierarchy = "unified"
	cpuHierarchy     = "cpu"
)

var ErrNoCgroups = errors.New("no suitable cgroups were found")

func isCgroupsExists() bool {
	mounts, err := procfs.GetMounts()
	if err != nil {
		return false
	}

	for _, m := range mounts {
		if m.FSType == "cgroup" || m.FSType == "cgroup2" {
			return true
		}
	}

	return false
}

func parseCgroupsMountPoints() (map[string]string, error) {
	mounts, err := procfs.GetMounts()
	if err != nil {
		return nil, err
	}

	out := make(map[string]string)
	for _, mount := range mounts {
		switch mount.FSType {
		case "cgroup2":
			out[unifiedHierarchy] = mount.MountPoint
		case "cgroup":
			for opt := range mount.SuperOptions {
				if opt == cpuHierarchy {
					out[cpuHierarchy] = mount.MountPoint
					break
				}
			}
		}
	}

	return out, nil
}

func getCFSQuota() (float64, error) {
	self, err := procfs.Self()
	if err != nil {
		return 0, err
	}

	selfCgroups, err := self.Cgroups()
	if err != nil {
		return 0, fmt.Errorf("parse self cgroups: %w", err)
	}

	cgroups, err := parseCgroupsMountPoints()
	if err != nil {
		return 0, fmt.Errorf("parse cgroups: %w", err)
	}

	if len(selfCgroups) == 0 || len(cgroups) == 0 {
		return 0, ErrNoCgroups
	}

	for _, cgroup := range selfCgroups {
		var quota float64
		switch {
		case cgroup.HierarchyID == 0:
			// for the cgroups v2 hierarchy id is always 0
			mp, ok := cgroups[unifiedHierarchy]
			if !ok {
				continue
			}

			quota, _ = parseV2CPUQuota(mp, cgroup.Path)
		case slices.ContainsString(cgroup.Controllers, cpuHierarchy):
			mp, ok := cgroups[cpuHierarchy]
			if !ok {
				continue
			}

			quota, _ = parseV1CPUQuota(mp, cgroup.Path)
		}

		if quota > 0 {
			return quota, nil
		}
	}

	return 0, ErrNoCgroups
}

func parseV1CPUQuota(mountPoint string, cgroupPath string) (float64, error) {
	basePath := filepath.Join(mountPoint, cgroupPath)
	cfsQuota, err := readFileInt(filepath.Join(basePath, "cpu.cfs_quota_us"))
	if err != nil {
		return -1, fmt.Errorf("parse cpu.cfs_quota_us: %w", err)
	}

	// A value of -1 for cpu.cfs_quota_us indicates that the group does not have any
	// bandwidth restriction in place
	// https://www.kernel.org/doc/Documentation/scheduler/sched-bwc.txt
	if cfsQuota == -1 {
		return float64(runtime.NumCPU()), nil
	}

	cfsPeriod, err := readFileInt(filepath.Join(basePath, "cpu.cfs_period_us"))
	if err != nil {
		return -1, fmt.Errorf("parse cpu.cfs_period_us: %w", err)
	}

	return float64(cfsQuota) / float64(cfsPeriod), nil
}

func parseV2CPUQuota(mountPoint string, cgroupPath string) (float64, error) {
	/*
		https://www.kernel.org/doc/Documentation/cgroup-v2.txt

		cpu.max
			A read-write two value file which exists on non-root cgroups.
			The default is "max 100000".

			The maximum bandwidth limit.  It's in the following format::
			  $MAX $PERIOD

			which indicates that the group may consume upto $MAX in each
			$PERIOD duration.  "max" for $MAX indicates no limit.  If only
			one number is written, $MAX is updated.
	*/
	rawCPUMax, err := ioutil.ReadFile(filepath.Join(mountPoint, cgroupPath, "cpu.max"))
	if err != nil {
		return -1, fmt.Errorf("read cpu.max: %w", err)
	}

	parts := strings.Fields(string(rawCPUMax))
	if len(parts) != 2 {
		return -1, fmt.Errorf("invalid cpu.max format: %s", string(rawCPUMax))
	}

	// "max" for $MAX indicates no limit
	if parts[0] == "max" {
		return float64(runtime.NumCPU()), nil
	}

	cpuMax, err := strconv.Atoi(parts[0])
	if err != nil {
		return -1, fmt.Errorf("parse cpu.max[max] (%q): %w", parts[0], err)
	}

	cpuPeriod, err := strconv.Atoi(parts[1])
	if err != nil {
		return -1, fmt.Errorf("parse cpu.max[period] (%q): %w", parts[1], err)
	}

	return float64(cpuMax) / float64(cpuPeriod), nil
}
