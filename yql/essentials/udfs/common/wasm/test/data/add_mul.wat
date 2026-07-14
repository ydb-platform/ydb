(module
    (type (;0;) (func (param i64 i64) (result i64)))

    (func $add (type 0) (param $first i64) (param $second i64) (result i64)
        (i64.add
            (local.get $first)
            (local.get $second)
        )
    )

    (func $mul (type 0) (param $0 i64) (param $1 i64) (result i64)
        (local.get $1)
        (local.get $0)
        (i64.mul)
    )

    (export "add" (func $add))
    (export "mul" (func $mul))
)
