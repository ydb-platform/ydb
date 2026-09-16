;; UDF module: imports env (sdk) + helpers library, exports scale.
(module
    (import "env" "memory" (memory i64 8 2097152))
    (import "helpers" "helpers_scale" (func $helpers_scale (param i64) (result i64)))

    (type $t_scale (func (param i64 i64 i64)))

    (func $scale (type $t_scale)
        (param $context i64)
        (param $result i64)
        (param $arg0 i64)

        (i32.store8
            (i64.add (local.get $result) (i64.const 2))
            (i32.const 3))
        (i64.store
            (i64.add (local.get $result) (i64.const 8))
            (call $helpers_scale
                (i64.load (i64.add (local.get $arg0) (i64.const 8)))))
    )

    (export "scale" (func $scale))
)
