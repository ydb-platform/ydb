(module
    (import "env" "memory" (memory i64 8 2097152))

    (type $t_i64_2 (func (param i64 i64) (result i64)))

    (func $sum_i64 (type $t_i64_2) (param $ptr i64) (param $length i64) (result i64)
        (local $result i64)
        (local $index i64)

        (local.set $result (i64.const 0))
        (local.set $index (i64.const 0))

        (block $end
            (br_if $end (i64.eqz (local.get $length)))
            (loop $loop
                (local.set $result
                    (i64.add
                        (local.get $result)
                        (i64.load (local.get $ptr))))
                (local.set $ptr (i64.add (local.get $ptr) (i64.const 8)))
                (local.set $index (i64.add (local.get $index) (i64.const 1)))
                (br_if $loop (i64.lt_u (local.get $index) (local.get $length)))
            )
        )

        (local.get $result)
    )

    (func $count_a (type $t_i64_2) (param $ptr i64) (param $length i64) (result i64)
        (local $result i64)
        (local $index i64)

        (local.set $result (i64.const 0))
        (local.set $index (i64.const 0))

        (block $end
            (br_if $end (i64.eqz (local.get $length)))
            (loop $loop
                (if (i32.eq (i32.load8_u (local.get $ptr)) (i32.const 97))
                    (then
                        (local.set $result (i64.add (local.get $result) (i64.const 1)))
                    )
                )
                (local.set $ptr (i64.add (local.get $ptr) (i64.const 1)))
                (local.set $index (i64.add (local.get $index) (i64.const 1)))
                (br_if $loop (i64.lt_u (local.get $index) (local.get $length)))
            )
        )

        (local.get $result)
    )

    (func $hello (result i64)
        (i64.const 1024)
    )

    (data (i64.const 1024) "hello from wasm\00")

    (export "sum_i64" (func $sum_i64))
    (export "count_a" (func $count_a))
    (export "hello" (func $hello))
)
