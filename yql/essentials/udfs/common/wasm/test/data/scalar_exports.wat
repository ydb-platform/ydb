(module
    (type $t_i64_2 (func (param i64 i64) (result i64)))
    (type $t_i64_1 (func (param i64) (result i64)))
    (type $t_i64_0 (func (result i64)))
    (type $t_f64_1 (func (param f64) (result f64)))
    (type $t_i32_2 (func (param i32 i32) (result i32)))
    (type $t_void (func))

    (func $add (type $t_i64_2) (param $a i64) (param $b i64) (result i64)
        (i64.add
            (local.get $a)
            (local.get $b)
        )
    )

    (func $inc (type $t_i64_1) (param $x i64) (result i64)
        (i64.add
            (local.get $x)
            (i64.const 1)
        )
    )

    (func $const42 (type $t_i64_0) (result i64)
        (i64.const 42)
    )

    (func $square_f (type $t_f64_1) (param $x f64) (result f64)
        (local.get $x)
        (local.get $x)
        (f64.mul)
    )

    (func $add_i32 (type $t_i32_2) (param $a i32) (param $b i32) (result i32)
        (i32.add
            (local.get $a)
            (local.get $b)
        )
    )

    (func $nop (type $t_void))

    (export "add" (func $add))
    (export "inc" (func $inc))
    (export "const42" (func $const42))
    (export "square_f" (func $square_f))
    (export "add_i32" (func $add_i32))
    (export "nop" (func $nop))
)
