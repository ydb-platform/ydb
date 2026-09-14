;; Stub runtime library uploaded as modules type=LIBRARY name "sdk".
;; First entry in required_libraries → AddSdk / "env" (must export malloc/free).
;; Heap base matches DefaultRegistrySdkWast: reserve [0, 65536) for UDF data.
(module
    (import "env" "memory" (memory i64 8 2097152))

    (global $heap (mut i64) (i64.const 65536))

    (func $malloc (param $n i64) (result i64)
        (local $p i64)
        (local.set $p (global.get $heap))
        (global.set $heap
            (i64.and
                (i64.add (i64.add (local.get $p) (local.get $n)) (i64.const 7))
                (i64.const -8)))
        (local.get $p)
    )

    (func $free (param $p i64))

    (export "malloc" (func $malloc))
    (export "free" (func $free))
)
