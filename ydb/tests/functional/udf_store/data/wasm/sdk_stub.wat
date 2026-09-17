;; Stub runtime library uploaded as modules type=LIBRARY name "sdk".
;; First entry in required_libraries → AddSdk / "env" (must export malloc/free).
;; Heap base matches DefaultRegistrySdkWast: reserve [0, 65536) for UDF data.
;; "sbrk" is what the host moves to fence off the bridge resident arena, so it
;; has to exist and it has to grow memory: after the fence the break sits at
;; the top of linear memory and a pure bump would hand out unmapped offsets.
(module
    (import "env" "memory" (memory i64 8 2097152))

    (global $heap (mut i64) (i64.const 65536))

    (func $sbrk (param $n i64) (result i64)
        (local $p i64)
        (local $break i64)
        (local $pages i64)
        (local.set $p (global.get $heap))
        (local.set $break
            (i64.and
                (i64.add (i64.add (local.get $p) (local.get $n)) (i64.const 7))
                (i64.const -8)))
        (local.set $pages
            (i64.sub
                (i64.shr_u
                    (i64.add (local.get $break) (i64.const 65535))
                    (i64.const 16))
                (memory.size)))
        (if (i64.gt_s (local.get $pages) (i64.const 0))
            (then
                (if (i64.eq (memory.grow (local.get $pages)) (i64.const -1))
                    (then (return (i64.const -1))))))
        (global.set $heap (local.get $break))
        (local.get $p)
    )

    (func $malloc (param $n i64) (result i64)
        (call $sbrk (local.get $n))
    )

    (func $free (param $p i64))

    (export "sbrk" (func $sbrk))
    (export "malloc" (func $malloc))
    (export "free" (func $free))
)
