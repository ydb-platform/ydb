(module
    (import "env" "memory" (memory i64 8 2097152))

    ;; Static data lives below the default bump-heap base (65536 in
    ;; DefaultRegistrySdkWast / sdk_stub). Marshalling malloc must not land here.
    (data (i64.const 1024) "\08\07\06\05\04\03\02\01")

    (type $t_add (func (param i64 i64 i64 i64)))
    (type $t_strlen (func (param i64 i64 i64)))
    (type $t_rodata_cookie (func (param i64 i64)))

    (func $udf_add (type $t_add)
        (param $context i64)
        (param $result i64)
        (param $left i64)
        (param $right i64)

        (i32.store8
            (i64.add (local.get $result) (i64.const 2))
            (i32.const 3))
        (i64.store
            (i64.add (local.get $result) (i64.const 8))
            (i64.add
                (i64.load (i64.add (local.get $left) (i64.const 8)))
                (i64.load (i64.add (local.get $right) (i64.const 8)))))
    )

    (func $udf_strlen (type $t_strlen)
        (param $context i64)
        (param $result i64)
        (param $value i64)

        (if
            (i32.eq
                (i32.load8_u (i64.add (local.get $value) (i64.const 2)))
                (i32.const 16))
            (then
                (i32.store8
                    (i64.add (local.get $result) (i64.const 2))
                    (i32.const 4))
                (i64.store
                    (i64.add (local.get $result) (i64.const 8))
                    (i64.extend_i32_u
                        (i32.load (i64.add (local.get $value) (i64.const 4))))))
            (else
                (i32.store8
                    (i64.add (local.get $result) (i64.const 2))
                    (i32.const 2))))
    )

    (func $udf_rodata_cookie (type $t_rodata_cookie)
        (param $context i64)
        (param $result i64)

        (i32.store8
            (i64.add (local.get $result) (i64.const 2))
            (i32.const 3))
        (i64.store
            (i64.add (local.get $result) (i64.const 8))
            (i64.load (i64.const 1024)))
    )

    (export "udf_add" (func $udf_add))
    (export "udf_strlen" (func $udf_strlen))
    (export "udf_rodata_cookie" (func $udf_rodata_cookie))
)
