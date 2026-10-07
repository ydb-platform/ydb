;; Intermediate library uploaded as modules type=LIBRARY name "helpers".
(module
    (func $helpers_scale (param $value i64) (result i64)
        (i64.mul (local.get $value) (i64.const 3))
    )
    (export "helpers_scale" (func $helpers_scale))
)
