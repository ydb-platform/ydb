package collect

import (
	"context"
	"os"
	"runtime"
	"runtime/debug"
	"time"

	"github.com/prometheus/procfs"

	"a.yandex-team.ru/library/go/core/buildinfo"
	"a.yandex-team.ru/library/go/core/metrics"
)

var _ Func = GoMetrics

func GoMetrics(_ context.Context, r metrics.Registry, c metrics.CollectPolicy) {
	if r == nil {
		return
	}
	r = r.WithPrefix("go")

	var stats debug.GCStats
	stats.PauseQuantiles = make([]time.Duration, 5) // Minimum, 25%, 50%, 75%, and maximum pause times.
	var numGoroutine, numThread int
	var ms runtime.MemStats

	c.AddCollect(func(context.Context) {
		debug.ReadGCStats(&stats)
		runtime.ReadMemStats(&ms)

		numThread, _ = runtime.ThreadCreateProfile(nil)
		numGoroutine = runtime.NumGoroutine()
	})

	gcRegistry := r.WithPrefix("gc")
	gcRegistry.FuncCounter("num", c.RegisteredCounter(func() int64 {
		return stats.NumGC
	}))
	gcRegistry.FuncCounter(r.ComposeName("pause", "total", "ns"), c.RegisteredCounter(func() int64 {
		return stats.PauseTotal.Nanoseconds()
	}))
	gcRegistry.FuncGauge(r.ComposeName("pause", "quantile", "min"), c.RegisteredGauge(func() float64 {
		return stats.PauseQuantiles[0].Seconds()
	}))
	gcRegistry.FuncGauge(r.ComposeName("pause", "quantile", "25"), c.RegisteredGauge(func() float64 {
		return stats.PauseQuantiles[1].Seconds()
	}))
	gcRegistry.FuncGauge(r.ComposeName("pause", "quantile", "50"), c.RegisteredGauge(func() float64 {
		return stats.PauseQuantiles[2].Seconds()
	}))
	gcRegistry.FuncGauge(r.ComposeName("pause", "quantile", "75"), c.RegisteredGauge(func() float64 {
		return stats.PauseQuantiles[3].Seconds()
	}))
	gcRegistry.FuncGauge(r.ComposeName("pause", "quantile", "max"), c.RegisteredGauge(func() float64 {
		return stats.PauseQuantiles[4].Seconds()
	}))
	gcRegistry.FuncGauge(r.ComposeName("last", "ts"), c.RegisteredGauge(func() float64 {
		return float64(ms.LastGC)
	}))
	gcRegistry.FuncCounter(r.ComposeName("forced", "num"), c.RegisteredCounter(func() int64 {
		return int64(ms.NumForcedGC)
	}))

	r.FuncGauge(r.ComposeName("goroutine", "num"), c.RegisteredGauge(func() float64 {
		return float64(numGoroutine)
	}))
	r.FuncGauge(r.ComposeName("thread", "num"), c.RegisteredGauge(func() float64 {
		return float64(numThread)
	}))

	memRegistry := r.WithPrefix("mem")
	memRegistry.FuncCounter(r.ComposeName("alloc", "total"), c.RegisteredCounter(func() int64 {
		return int64(ms.TotalAlloc)
	}))
	memRegistry.FuncGauge("sys", c.RegisteredGauge(func() float64 {
		return float64(ms.Sys)
	}))
	memRegistry.FuncCounter("lookups", c.RegisteredCounter(func() int64 {
		return int64(ms.Lookups)
	}))
	memRegistry.FuncCounter("mallocs", c.RegisteredCounter(func() int64 {
		return int64(ms.Mallocs)
	}))
	memRegistry.FuncCounter("frees", c.RegisteredCounter(func() int64 {
		return int64(ms.Frees)
	}))
	memRegistry.FuncGauge(r.ComposeName("heap", "alloc"), c.RegisteredGauge(func() float64 {
		return float64(ms.HeapAlloc)
	}))
	memRegistry.FuncGauge(r.ComposeName("heap", "sys"), c.RegisteredGauge(func() float64 {
		return float64(ms.HeapSys)
	}))
	memRegistry.FuncGauge(r.ComposeName("heap", "idle"), c.RegisteredGauge(func() float64 {
		return float64(ms.HeapIdle)
	}))
	memRegistry.FuncGauge(r.ComposeName("heap", "inuse"), c.RegisteredGauge(func() float64 {
		return float64(ms.HeapInuse)
	}))
	memRegistry.FuncGauge(r.ComposeName("heap", "released"), c.RegisteredGauge(func() float64 {
		return float64(ms.HeapReleased)
	}))
	memRegistry.FuncGauge(r.ComposeName("heap", "objects"), c.RegisteredGauge(func() float64 {
		return float64(ms.HeapObjects)
	}))

	memRegistry.FuncGauge(r.ComposeName("stack", "inuse"), c.RegisteredGauge(func() float64 {
		return float64(ms.StackInuse)
	}))
	memRegistry.FuncGauge(r.ComposeName("stack", "sys"), c.RegisteredGauge(func() float64 {
		return float64(ms.StackSys)
	}))

	memRegistry.FuncGauge(r.ComposeName("span", "inuse"), c.RegisteredGauge(func() float64 {
		return float64(ms.MSpanInuse)
	}))
	memRegistry.FuncGauge(r.ComposeName("span", "sys"), c.RegisteredGauge(func() float64 {
		return float64(ms.MSpanSys)
	}))

	memRegistry.FuncGauge(r.ComposeName("cache", "inuse"), c.RegisteredGauge(func() float64 {
		return float64(ms.MCacheInuse)
	}))
	memRegistry.FuncGauge(r.ComposeName("cache", "sys"), c.RegisteredGauge(func() float64 {
		return float64(ms.MCacheSys)
	}))

	memRegistry.FuncGauge(r.ComposeName("buck", "hash", "sys"), c.RegisteredGauge(func() float64 {
		return float64(ms.BuckHashSys)
	}))
	memRegistry.FuncGauge(r.ComposeName("gc", "sys"), c.RegisteredGauge(func() float64 {
		return float64(ms.GCSys)
	}))
	memRegistry.FuncGauge(r.ComposeName("other", "sys"), c.RegisteredGauge(func() float64 {
		return float64(ms.OtherSys)
	}))
	memRegistry.FuncGauge(r.ComposeName("gc", "next"), c.RegisteredGauge(func() float64 {
		return float64(ms.NextGC)
	}))

	memRegistry.FuncGauge(r.ComposeName("gc", "cpu", "fraction"), c.RegisteredGauge(func() float64 {
		return ms.GCCPUFraction
	}))
}

var _ Func = ProcessMetrics

func ProcessMetrics(_ context.Context, r metrics.Registry, c metrics.CollectPolicy) {
	if r == nil {
		return
	}
	buildVersion := buildinfo.Info.ArcadiaSourceRevision
	r.WithTags(map[string]string{"revision": buildVersion}).Gauge("build").Set(1.0)

	pid := os.Getpid()
	proc, err := procfs.NewProc(pid)
	if err != nil {
		return
	}

	procRegistry := r.WithPrefix("proc")

	var ioStat procfs.ProcIO
	var procStat procfs.ProcStat
	var fd int
	var cpuWait uint64

	const clocksPerSec = 100

	c.AddCollect(func(ctx context.Context) {
		if gatheredFD, err := proc.FileDescriptorsLen(); err == nil {
			fd = gatheredFD
		}

		if gatheredIOStat, err := proc.IO(); err == nil {
			ioStat.SyscW = gatheredIOStat.SyscW
			ioStat.WriteBytes = gatheredIOStat.WriteBytes
			ioStat.SyscR = gatheredIOStat.SyscR
			ioStat.ReadBytes = gatheredIOStat.ReadBytes
		}

		if gatheredStat, err := proc.Stat(); err == nil {
			procStat.UTime = gatheredStat.UTime
			procStat.STime = gatheredStat.STime
			procStat.RSS = gatheredStat.RSS
		}

		if gatheredSched, err := proc.Schedstat(); err == nil {
			cpuWait = gatheredSched.WaitingNanoseconds
		}
	})

	procRegistry.FuncGauge("fd", c.RegisteredGauge(func() float64 {
		return float64(fd)
	}))

	ioRegistry := procRegistry.WithPrefix("io")
	ioRegistry.FuncCounter(r.ComposeName("read", "count"), c.RegisteredCounter(func() int64 {
		return int64(ioStat.SyscR)
	}))
	ioRegistry.FuncCounter(r.ComposeName("read", "bytes"), c.RegisteredCounter(func() int64 {
		return int64(ioStat.ReadBytes)
	}))
	ioRegistry.FuncCounter(r.ComposeName("write", "count"), c.RegisteredCounter(func() int64 {
		return int64(ioStat.SyscW)
	}))
	ioRegistry.FuncCounter(r.ComposeName("write", "bytes"), c.RegisteredCounter(func() int64 {
		return int64(ioStat.WriteBytes)
	}))

	cpuRegistry := procRegistry.WithPrefix("cpu")
	cpuRegistry.FuncCounter(r.ComposeName("total", "ns"), c.RegisteredCounter(func() int64 {
		return int64(procStat.UTime+procStat.STime) * (1_000_000_000 / clocksPerSec)
	}))
	cpuRegistry.FuncCounter(r.ComposeName("user", "ns"), c.RegisteredCounter(func() int64 {
		return int64(procStat.UTime) * (1_000_000_000 / clocksPerSec)
	}))
	cpuRegistry.FuncCounter(r.ComposeName("system", "ns"), c.RegisteredCounter(func() int64 {
		return int64(procStat.STime) * (1_000_000_000 / clocksPerSec)
	}))
	cpuRegistry.FuncCounter(r.ComposeName("wait", "ns"), c.RegisteredCounter(func() int64 {
		return int64(cpuWait)
	}))

	procRegistry.FuncGauge(r.ComposeName("mem", "rss"), c.RegisteredGauge(func() float64 {
		return float64(procStat.RSS)
	}))
}
