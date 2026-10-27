# Typetag

[<img alt="github" src="https://img.shields.io/badge/github-dtolnay/typetag-8da0cb?style=for-the-badge&labelColor=555555&logo=github" height="20">](https://github.com/dtolnay/typetag)
[<img alt="crates.io" src="https://img.shields.io/crates/v/typetag.svg?style=for-the-badge&color=fc8d62&logo=rust" height="20">](https://crates.io/crates/typetag)
[<img alt="docs.rs" src="https://img.shields.io/badge/docs.rs-typetag-66c2a5?style=for-the-badge&labelColor=555555&logo=docs.rs" height="20">](https://docs.rs/typetag)
[<img alt="build status" src="https://img.shields.io/github/actions/workflow/status/dtolnay/typetag/ci.yml?branch=master&style=for-the-badge" height="20">](https://github.com/dtolnay/typetag/actions?query=branch%3Amaster)

**Serde serializable and deserializable trait objects.**

This crate provides a macro for painless serialization of `&dyn Trait` trait
objects and serialization + deserialization of `Box<dyn Trait>` trait objects.

Let's dive into the example and I'll explain some more below.

```toml
[dependencies]
typetag = "0.2"
```

<br>

## Example

Suppose I have a trait `WebEvent` and I require that every implementation of the
trait be serializable and deserializable so that I can send them to my
ad-serving AI. Here are just the types and trait impls to start with:

```rust
trait WebEvent {
    fn inspect(&self);
}

#[derive(Serialize, Deserialize)]
struct PageLoad;

impl WebEvent for PageLoad {
    fn inspect(&self) {
        println!("200 milliseconds or bust");
    }
}

#[derive(Serialize, Deserialize)]
struct Click {
    x: i32,
    y: i32,
}

impl WebEvent for Click {
    fn inspect(&self) {
        println!("negative space between the ads: x={} y={}", self.x, self.y);
    }
}
```

We'll need to be able to send an arbitrary web event as JSON to the AI:

```rust
fn send_event_to_money_factory(event: &dyn WebEvent) -> Result<()> {
    let json = serde_json::to_string(event)?;
    somehow_send_json(json)?;
    Ok(())
}
```

and receive an arbitrary web event as JSON on the server side:

```rust
fn process_event_from_clickfarm(json: &str) -> Result<()> {
    let event: Box<dyn WebEvent> = serde_json::from_str(json)?;
    overanalyze(event)?;
    Ok(())
}
```

The introduction claimed that this would be painless but I'll let you be the
judge.

First stick an attribute on top of the trait.

```rust
#[typetag::serde(tag = "type")]
trait WebEvent {
    fn inspect(&self);
}
```

Then stick a similar attribute on all those impl blocks too.

```rust
#[typetag::serde]
impl WebEvent for PageLoad {
    fn inspect(&self) {
        println!("200 milliseconds or bust");
    }
}

#[typetag::serde]
impl WebEvent for Click {
    fn inspect(&self) {
        println!("negative space between the ads: x={} y={}", self.x, self.y);
    }
}
```

And now it works as described. All in all, three lines were added!

<br>

# What?

Trait objects are serialized by this library like Serde enums. Every impl of the
trait (anywhere in the program) looks like one variant of the enum.

All three of Serde's tagged [enum representations] are supported. The one shown
above is the "internally tagged" style so our two event types would be
represented in JSON as:

[enum representations]: https://serde.rs/enum-representations.html

```json
{"type":"PageLoad"}
{"type":"Click","x":10,"y":10}
```

The choice of enum representation is controlled by the attribute that goes on
the trait definition. Let's check out the "adjacently tagged" style:

```rust
#[typetag::serde(tag = "type", content = "value")]
trait WebEvent {
    fn inspect(&self);
}
```

```json
{"type":"PageLoad","value":null}
{"type":"Click","value":{"x":10,"y":10}}
```

and the "externally tagged" style, which is Serde's default for enums:

```rust
#[typetag::serde]
trait WebEvent {
    fn inspect(&self);
}
```

```json
{"PageLoad":null}
{"Click":{"x":10,"y":10}}
```

Separately, the value of the tag for a given trait impl may be defined as part
of the attribute that goes on the trait impl. By default the tag will be the
type name when no name is specified explicitly.

```rust
#[typetag::serde(name = "mouse_button_down")]
impl WebEvent for Click {
    fn inspect(&self) {
        println!("negative space between the ads: ({}, {})", self.x, self.y);
    }
}
```

```json
{"type":"mouse_button_down","x":10,"y":10}
```

Conceptually all you're getting with this crate is that we build for you an enum
in which every impl of the trait in your program is automatically registered as
an enum variant. The behavior is the same as if you had written the enum
yourself and implemented Serialize and Deserialize for the dyn Trait object in
terms of the enum.

```rust
// generated (conceptually)
#[derive(Serialize, Deserialize)]
enum WebEvent {
    PageLoad(PageLoad),
    Click(Click),
    /* ... */
}
```

<br>

## So many questions

- *Does it work if the trait impls are spread across different crates?* **Yes**

  Serialization and deserialization both support every single impl of the trait
  across the dependency graph of the final program binary.

- *Does it work in non-self-describing data formats like Bincode?* **Yes**

  All three choices of enum representation will round-trip correctly through
  compact binary formats including Bincode.

- *Does it support non-struct types?* **Yes**

  The implementations of the trait can be structs, enums, primitives, or
  anything else supported by Serde. The Serialize and Deserialize impls may be
  derived or handwritten.

- *Even in WebAssembly?* **Yes**

  Wasm is supported but with some extra setup described [here][wasm].

  [wasm]: https://docs.rs/inventory/0.3/inventory/index.html#webassembly-and-constructors

- *Didn't someone explain to me why this wasn't possible?* **Yes**

  It might have been me.

- *Then how does it work?*

  We use the [`inventory`] crate to produce a registry of impls of your trait,
  which is built on the [`ctor`] crate to hook up initialization functions that
  insert into the registry. The first `Box<dyn Trait>` deserialization will
  perform the work of iterating the registry and building a map of tags to
  deserialization functions. Subsequent deserializations find the right
  deserialization function in that map. The [`erased-serde`] crate is also
  involved, to do this all in a way that does not break object safety.

[`inventory`]: https://github.com/dtolnay/inventory
[`ctor`]: https://github.com/mmastrac/rust-ctor
[`erased-serde`]: https://github.com/dtolnay/erased-serde

<br>

#### License

<sup>
Licensed under either of <a href="LICENSE-APACHE">Apache License, Version
2.0</a> or <a href="LICENSE-MIT">MIT license</a> at your option.
</sup>

<br>

<sub>
Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in this crate by you, as defined in the Apache-2.0 license, shall
be dual licensed as above, without any additional terms or conditions.
</sub>
