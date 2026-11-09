# Ouroboros

[![Ouroboros on Crates.IO](https://img.shields.io/crates/v/ouroboros)](https://crates.io/crates/ouroboros)
[![Documentation](https://img.shields.io/badge/documentation-link-success)](https://docs.rs/ouroboros)


Easy self-referential struct generation for Rust. 
Dual licensed under MIT / Apache 2.0.

While this crate is `no_std` compatible, it still requires the `alloc` crate.

Version notes:
- Version `0.13.0` and later contain checks for additional situations which
  cause undefined behavior if not caught.
- Version `0.11.0` and later place restrictions on derive macros, earlier
  versions allowed using them in ways which could lead to undefined behavior if
  not used properly.
- Version `0.10.0` and later automatically box every field. This is done
  to prevent undefined behavior, but has the side effect of making the library
  easier to work with.

Tests are located in the examples/ folder because they need to be in a crate
outside of `ouroboros` for the `self_referencing` macro to work properly.

```rust
use ouroboros::self_referencing;

#[self_referencing]
struct MyStruct {
    int_data: i32,
    float_data: f32,
    #[borrows(int_data)]
    // the 'this lifetime is created by the #[self_referencing] macro
    // and should be used on all references marked by the #[borrows] macro
    int_reference: &'this i32,
    #[borrows(mut float_data)]
    float_reference: &'this mut f32,
}

fn main() {
    // The builder is created by the #[self_referencing] macro 
    // and is used to create the struct
    let mut my_value = MyStructBuilder {
        int_data: 42,
        float_data: 3.14,

        // Note that the name of the field in the builder 
        // is the name of the field in the struct + `_builder` 
        // ie: {field_name}_builder
        // the closure that assigns the value for the field will be passed 
        // a reference to the field(s) defined in the #[borrows] macro
	
        int_reference_builder: |int_data: &i32| int_data,
        float_reference_builder: |float_data: &mut f32| float_data,
    }.build();

    // The fields in the original struct can not be accesed directly
    // The builder creates accessor methods which are called borrow_{field_name}()

    // Prints 42
    println!("{:?}", my_value.borrow_int_data());
    // Prints 3.14
    println!("{:?}", my_value.borrow_float_reference());
    // Sets the value of float_data to 84.0
    my_value.with_mut(|fields| {
        **fields.float_reference = (**fields.int_reference as f32) * 2.0;
    });

    // We can hold on to this reference...
    let int_ref = *my_value.borrow_int_reference();
    println!("{:?}", *int_ref);
    // As long as the struct is still alive.
    drop(my_value);
    // This will cause an error!
    // println!("{:?}", *int_ref);
}
```
