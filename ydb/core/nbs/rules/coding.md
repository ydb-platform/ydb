# C++ coding rules

Apply these when writing or reviewing C++ in any workspace. They are an
informal distillation of the [Google C++ style guide](https://google.github.io/styleguide/cppguide.html)
and internal practice.

## Formatting

- Format with `ya style ydb/core/nbs/cloud/blockstore`. When its output reads poorly, adding a comment forces a line break where you want one.

## Comments

- Comment everything non-trivial.
- Every class needs a comment above it: what it does, which problem it solves.
- Every public method or function needs a comment saying what it does.
- Every enum needs a comment per element explaining what that element means.

## Header files

- Do not put implementations in headers. Test helpers included by few files are
  the only exception, and even there prefer splitting header from
  implementation.
- Include only what is needed. Be especially ruthless about removing includes
  from `*.h`. Watch the IDE's unused-include hints in `.cpp` files, though they
  sometimes lie.
- When a header does not need the complete type (the class is passed by
  reference or pointer), do not include its header. Prefer a `public.h` with
  forward declarations, including smart-pointer aliases.
- Declaration order within a header: enums, then structs, then classes, then
  free functions.
- One class per file. Exception: very simple, logically related
  structs/classes.

## Implementation files

- Keep the order of definitions identical to the order of declarations.
- Put all free functions in a single anonymous namespace section at the top of
  the file.
- Separate implementations of different classes with a `///` divider.

## Classes

- Think hard about the class name, and write the comment.
- People read top to bottom, so put the most important information first: the
  public part other classes use comes first, internal structure last.

```cpp
// What this class does, how it is meant to be used, what guarantees it gives.
class TClass
    : public IInterface1
    , public IInterface2
{
public:
    // nested type declarations + usings
    // constructors
    // destructor
    // assignment operators
    // static methods
    // interface implementations
    // regular methods
protected:
    // same order as the public section, but avoid protected
    // const fields
    // fields
private:
    // same order as the public section
    // const fields
    // fields
};
```

- Declaration order must match definition order.
- Mark static method definitions with a `// static` comment.
- Trivial getters/setters may be implemented in the header. Trivial means a
  single assignment and nothing else. Nothing at all.
- Group methods with related meaning together.
- When a condition combines knowledge from several fields, extract it into a
  const method with a descriptive name, e.g. `bool IsValid() const`.
- Keep the public interface compact. Every public method deserves a test, and
  that cost should discourage making things public.
- A public method call moves the object to a new state, and that state must be
  valid and preserve internal invariants.
- Check invariants at the start of methods.
- Do not write unused methods or code.
- Extract code into a free function in the anonymous namespace whenever
  possible.
- Suffix test-only methods with `DebugOnly`. When tests need private fields,
  add a dedicated accessor class and make it a `friend`.
- Minimize stateful fields. If state can be computed, expose a const method
  instead of adding a field like `bool IsValid`.
- Initialize fields in the header to keep constructors compact.
- Think twice before using inheritance.
- Mark classes `final`.

## Structs

- A struct has no methods: state changes go directly through its fields. If two
  fields must change together, convert it to a class.
- Static factory methods are allowed.
- Const methods that compute something are allowed.

## Variables and constness

- Make everything that can be `constexpr` be `constexpr`.
- Make everything that can be `const` be `const`.
- A variable computable at initialization and never reassigned is `const`.
- Name variables for what they mean; it makes reading easier.

## Lambdas

- Do not couple components through `std::function`. Extract an interface
  instead.
- A lambda local to a method that never escapes it may capture by `&`.
- A long-lived lambda captures every variable explicitly; never use `=`.

## Arguments and return values

- Return a single value.
- To return several, define a struct. `std::pair`/`std::tuple` are bad: the
  field order becomes easy to confuse later.
- Argument order: inputs first (by const reference or by value), then in/out
  (by reference or pointer), then outputs last.
- Pass output parameters by pointer, so the call site immediately shows the
  argument may be modified.
- Replace `bool` with an enum with meaningful names. It makes adding a new state
  easy, reduces mistakes, and makes dependent code searchable.

## General

- Write the simplest, clearest code you can. Anyone can write something
  complicated; solving the problem simply takes effort.
- We read more than we write, so a long but clear variable name is good.
- Debugging is harder than writing. Code written at the limit of the author's
  cognitive capacity cannot be debugged by that author, by definition.
- Reread your code after writing it. Better names and duplication become
  visible.
- Read a class, ideally in full, before changing it, and understand what the
  author intended. This is why class and method comments help.
- Resist solving a problem by adding one more `if` when there are already many.
  This piles up when a task is specified incrementally and many people each add
  one branch. Once the whole task is known, read the code, understand the
  original intent, and write a much simpler solution.
- Split very large methods.
- Destructors are a C++ strength: use RAII for all kinds of things, not just
  freeing memory.
- When an `if` condition is complex, compute it into named variables; the names
  explain the intent.
- Extract logic into a separate class when possible. The usual obstacle is
  needing to call back into the parent; extract an interface from the parent and
  pass it to the helper. Such helpers are easy to test by mocking that
  interface.
- Take clang-tidy hints seriously; they usually have a point.
