GO_LIBRARY()

SRCS(
    exp_arenas_off.go
    exp_boringcrypto_off.go
    exp_cacheprog_off.go
    exp_cgocheck2_off.go
    exp_coverageredesign_on.go
    exp_fieldtrack_off.go
    exp_heapminimum512kib_off.go
    exp_loopvar_off.go
    exp_pagetrace_off.go
    exp_preemptibleloops_off.go
    exp_regabiargs_on.go
    exp_regabiwrappers_on.go
    exp_staticlockranking_off.go
    flags.go
)

END()
