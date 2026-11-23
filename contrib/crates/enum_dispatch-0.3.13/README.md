# enum_dispatch

[![crates.io](https://img.shields.io/crates/v/enum_dispatch.svg)](https://crates.io/crates/enum_dispatch)
[![Docs](https://docs.rs/enum_dispatch/badge.svg)](https://docs.rs/enum_dispatch/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)]()

`enum_dispatch` transforms your trait objects into concrete compound types, increasing their method call speed up to 10x.

## example

If you have the following code...

```rust
// We already have defined MyImplementorA and MyImplementorB, and implemented MyBehavior for each.

trait MyBehavior {
    fn my_trait_method(&self);
}

// Any pointer type -- Box, &, etc.
let a: Box<dyn MyBehavior> = Box::new(MyImplementorA::new());

a.my_trait_method();    //dynamic dispatch
```

...then you can improve its performance using `enum_dispatch` like this:

```rust
#[enum_dispatch]
enum MyBehaviorEnum {
    MyImplementorA,
    MyImplementorB,
}

#[enum_dispatch(MyBehaviorEnum)]
trait MyBehavior {
    fn my_trait_method(&self);
}

let a: MyBehaviorEnum = MyImplementorA::new().into();

a.my_trait_method();    //no dynamic dispatch
```

Notice the differences:

1. The new enum, `MyBehaviorEnum`, whose variants are simply types implementing the trait `MyBehavior`.
2. The new `enum_dispatch` attributes applied to the enum and trait, linking the two together.
3. The removal of the `Box` allocation.
4. Faster trait method calls!

## how to use

0. Add `enum_dispatch` as a Cargo.toml dependency, and `use enum_dispatch::enum_dispatch` in your code.
1. Create a new enum whose variants are any in-scope trait implementors you've defined.
2. Add an `#[enum_dispatch]` attribute to either the enum or trait definition. This will "register" it with the `enum_dispatch` library. Take note of the name of the enum or trait it was applied to -- we'll call it `FirstBlockName`.
3. Add an `#[enum_dispatch(FirstBlockName)]` attribute to the remaining definition. This will "link" it with the previously registered definition.
4. Update your dynamic types to use the new enum instead. You can use `.into()` from any trait implementor to automatically turn it into an enum variant.

## performance

More information on performance can be found in the [docs](https://docs.rs/enum_dispatch/), and benchmarks are available in the `benches` directory.
The following benchmark results give a taste of what can be achieved using `enum_dispatch`.
They compare the speed of repeatedly accessing method calls on a `Vec` of 1024 trait objects of randomized concrete types using either `Box`ed trait objects, `&` referenced trait objects, or `enum_dispatch`ed enum types.

```text
test benches::boxdyn_homogeneous_vec       ... bench:   5,900,191 ns/iter (+/- 95,169)
test benches::refdyn_homogeneous_vec       ... bench:   5,658,461 ns/iter (+/- 137,128)
test benches::enumdispatch_homogeneous_vec ... bench:     479,630 ns/iter (+/- 3,531)
```

## bonus features

### serialization compatibility

While `enum_dispatch` was built with performance in mind, the transformations it applies make all your data structures much more visible to the compiler.
That means you can use [`serde`](https://crates.io/crates/serde) or other similar tools on your trait objects!

### automatic `From` and `TryInto` implementations

`enum_dispatch` will generate a `From` implementation for all inner types to make it easy to instantiate your custom enum.
In addition, it will generate a `TryInto` implementation for all inner types to make it easy to convert back into the original, unwrapped types.

### attribute support

You can use use `#[cfg(...)]` attributes on `enum_dispatch` variants to conditionally include or exclude their corresponding `enum_dispatch` implementations.
Other attributes will be passed straight through to the underlying generated enum, allowing compatibility with other procedural macros.

### `no_std` support

`enum_dispatch` is supported in `no_std` environments.
It's a great fit for embedded devices, where it's super useful to be able to allocate collections of trait objects on the stack.

## tweaks and options

### custom variant names

By default, `enum_dispatch` will expand each enum variant into one with a single unnamed field of the same name as the internal type.
If for some reason you'd like to use a custom name for a particular type in an `enum_dispatch` variant, you can do so as shown below:

```rust
#[enum_dispatch]
enum MyTypes {
    TypeA,
    CustomVariantName(TypeB),
}

let mt: MyTypes = TypeB::new().into();
match mt {
    TypeA(a) => { /* `a` is a TypeA */ },
    CustomVariantName(b) => { /* `b` is a TypeB */ },
}
```

Custom variant names are required for enums and traits with generic type arguments, which can also be optimized by `enum_dispatch`.
Check out [this generics example](tests/generics.rs) to see how that works.

### specify multiple enums at once

If you want to use `enum_dispatch` to implement the same trait for multiple enums, you may specify them all in the same attribute:

```rust
#[enum_dispatch(Widgets, Tools, Gadgets)]
trait CommonFunctionality {
    // ...
}
```

### specify multiple traits at once

Similarly to above, you may use a single attribute to implement multiple traits for a single enum:

```rust
#[enum_dispatch(CommonFunctionality, WidgetFunctionality)]
enum Widget {
    // ...
}
```

### generic enums and traits

`enum_dispatch` can operate on enums and traits with generic parameters.
When linking these, be sure to include the generic parameters in the attribute argument, like below:

```rust
#[enum_dispatch]
trait Foo<T, U> { /* ... */ }

#[enum_dispatch(Foo<T, U>)]
enum Bar<T: Clone, U: Hash> { /* ... */ }
```

The names of corresponding generic parameters should match between the definition of the enum and trait.

[This example](tests/complex_generics.rs) demonstrates this in more detail.

## troubleshooting

### no impls created?

Be careful not to forget an attribute or mistype the name in a linking attribute.
If parsing is completed before a linking attribute is found, no implementations will be generated.
Due to technical limitations of the macro system, it's impossible to properly warn the user in this scenario.

### can't parse enum?

Types must be fully in scope to be usable as an enum variant. For example, the following will fail to compile:

```rust
#[enum_dispatch]
enum Fails {
    crate::A::TypeA,
    crate::B::TypeB,
}
```

This is because the enum must be correctly parsable before macro expansion. Instead, import the types first:

```rust
use crate::A::TypeA;
use crate::B::TypeB;

#[enum_dispatch]
enum Succeeds {
    TypeA,
    TypeB,
}
```

## technical details

`enum_dispatch` is a procedural macro that implements a trait for a fixed set of types in the form of an enum.
This is faster than using dynamic dispatch because type information will be "built-in" to each enum, avoiding a costly vtable lookup.

Since `enum_dispatch` is a procedural macro, it works by processing and expanding attributed code at compile time.
The folowing sections explain how the example above might be transformed.

### enum processing

There's no way to define an enum whose variants are actual concrete types.
To get around this, `enum_dispatch` rewrites its body by generating a name for each variant and using the provided type as its single tuple-style argument.
The name for each variant isn't particularly important for most purposes, but `enum_dispatch` will currently just use the name of the provided type.

```rust
enum MyBehaviorEnum {
    MyImplementorA(MyImplementorA),
    MyImplementorB(MyImplementorB),
}
```

### trait processing

`enum_dispatch` doesn't actually process annotated traits!
However, it still requires access to the trait definition so it can take note of the trait's name, as well as the function signatures of any methods inside it.

### trait impl creation

Whenever `enum_dispatch` is able to "link" two definitions together, it will generate an `impl` block, implementing the trait for the enum.
In the above example, the linkage is completed by the `MyBehavior` trait definition, so `impl` blocks will be generated directly below that trait.
The generated impl block might look something like this:

```rust
impl MyBehavior for MyBehaviorEnum {
    fn my_trait_method(&self) {
        match self {
            MyImplementorA(inner) => inner.my_trait_method(),
            MyImplementorB(inner) => inner.my_trait_method(),
        }
    }
}
```

Additional trait methods would be expanded accordingly, and additional enum variants would correspond to additional match arms in each method definition.
It's easy to see how quickly this can become unmanageable in manually written code!

### 'From' impl creation

Normally, it would be impossible to initialize one of the new enum variants without knowing its name.
However, with implementations of `From<T>` for each variant, that requirement is alleviated.
The generated implementations could look like the following:

```rust
impl From<MyImplementorA> for MyBehaviorEnum {
    fn from(inner: MyImplementorA) -> MyBehaviorEnum {
        MyBehaviorEnum::MyImplementorA(inner)
    }
}

impl From<MyImplementorB> for MyBehaviorEnum {
    fn from(inner: MyImplementorB) -> MyBehaviorEnum {
        MyBehaviorEnum::MyImplementorB(inner)
    }
}
```

As with above, having a large number of possible type variants would make this very difficult to maintain by hand.

### registry and linkage

Anyone closely familiar with writing macros will know that they must be processed locally, with no context about the surrounding source code.
Additionally, parsed syntax items in `syn` are `!Send` and `!Sync`.
This is for good reason -- with multithreaded compilation and macro expansion, there are no guarantees on the order or lifetime of a reference to any given block of code.
Unfortunately, it also prevents referencing syntax between separate macro invocations.

In the interest of convenience, `enum_dispatch` circumvents these restrictions by converting syntax into a `String` and storing it in `once_cell` lazily initialized `Mutex<HashMap<String, String>>`s whose keys are either the trait or enum names.

There is also a similar `HashMap` dedicated to "deferred" links, since definitions in different files could be encountered in arbitrary orders.
If a linking attribute (with one argument) occurs before the corresponding registry attribute (with no arguments), the argument will be stored as a deferred link.
Once that argument's definition is encountered, impl blocks can be created as normal.

Because of the link deferral mechanism, it's not an error to encounter a linking attribute without being able to implement it.
`enum_dispatch` will simply expect to find the corresponding registry attribute later in parsing.
However, there's no way to insert a callback to check that all deferred links have been processed once all the original source code has been parsed, explaining the impossibility of warning the user of unlinked attributes.
