# Changelog

## 0.3.13

- Fix namespace collision with imports named `core` (!35)

## 0.3.12

- Update to syn 2.0 (#69)

## 0.3.11

- Add support for trait methods that return `Self` (#59, !29)

## 0.3.10

- Add support for async trait methods (#62)

## 0.3.9

- Add support for const generics (#51, !25)

## 0.3.8

- Preserve attributes from inner fields of enum variants (!27)

## 0.3.7

- Support trait methods with late bound lifetime arguments (#34)

## 0.3.6

- Remove `extra-traits` feature from `syn` dependency (!24)
- Support trait methods with pattern arguments (#44)

## 0.3.5

- Compatibility with `syn >= 1.0.58` (#37, !21)

## 0.3.4

- Support enum variants named `Error` (#36)

## 0.3.3

### Compatibility warning
Users who had previously used an `#[enum_dispatch(...)]` attribute containing the name of a generic trait will need to update the attribute to include matching generic arguments.
See #35 for details.

- Support trait methods with generic type parameters (#28)
- Officially support linking generic traits (#26)

## 0.3.2

- Support `cfg` attributes on enum variants and trait items (#24)

## 0.3.1

- Support multiple comma separated traits or enums in `enum_dispatch` attributes (#3, !14)
- Pass attributes from trait methods to the generated implementations (!15)

## 0.3.0

Rerelease of 0.2.3 to undo unintentional semver incompatibility (#16)

## 0.2.4

Rerelease of 0.2.2 to undo unintentional semver incompatibility (#16)

## 0.2.3

- Support identical method names across traits and base structs (!12)

## 0.2.2

- Support multiple trait implementations per enum (!13)

## 0.2.1

- Pass attributes from enum variants to the generated enums (#14)
- Support enum variants with generic parameters (!4)

## 0.2.0

- Generate implementations of `TryInto` instead of `TryFrom`, which cannot be implemented on foreign types (#10)
