# YDB Coding Style

Follow the coding style of the file you are modifying. If the style is unclear
(e.g. a new file), apply the rules below.

## Names

A name should reflect the data, type, or action it represents. Use only common
abbreviations. Single-letter names (`i`, `j`, `k`) are allowed only for loop
counters and iterators.

Structs follow the same rules as classes unless stated otherwise.

### Variables and identifiers

- Local and global variables: lowercase.
- Functions and class methods: PascalCase (leading capital letter), no suffix.
- Function arguments, lambda captures, and function-pointer variables: lowercase.
- Class data members: PascalCase; private and protected members end with `_`,
  public members have no suffix.
- Class and typedef names: prefix `T` + PascalCase (e.g. `TVector`).
- Virtual interfaces: prefix `I` + PascalCase. A virtual interface has at least
  one pure virtual method (including inherited) and no data members (including
  inherited).
- Global constants and macros: `UPPER_CASE` with underscores between tokens.
- Compound names: capitalize the first letter of each token without underscores
  (e.g. `GetQueueSize`). In `UPPER_CASE` constants, separate tokens with `_`.
- Do not start a name with `_`.
- Do not use Hungarian notation.

```cpp
class TClass {
public:
    int Size;
    int GetSize() {
        return Size;
    }
};

TClass object;
int GetValue();
void SetValue(int val);
```

**Interop exception**: when interfacing with external code, add required methods
or free functions even if they break local naming (e.g. STL iterator protocol
`.begin()` / `.end()`, buffer access `.data()` / `.size()`, external `swap()`).

### Macros

If you must use a macro, make it unique (e.g. reflect the directory path).
Public API macros use the `Y_` prefix (see `util/system/compiler.h`).

### Enumerations

- Global enums: prefix `E` + PascalCase (same rules as classes).
- Global enum members: `UPPER_CASE`, prefixed by enum initials
  (e.g. `EFetchType` → `FT_SIMPLE`).
- Class enum members: same rules as other class members (PascalCase).
- Unnamed enums are allowed only as class members.
- `enum class` follows the same rules as class enums.

```cpp
enum EFetchType {
    FT_SIMPLE,
    FT_RELFORM_DEBUG,
};

class TIndicator {
public:
    enum EStatus {
        Created,
        Running,
        Finished
    };
};

enum class EStatus {
    Created,
    Running,
    Finished
};
```

Do not hand-roll enum ↔ `TString` conversion. Use `GENERATE_ENUM_SERIALIZATION`
or `GENERATE_ENUM_SERIALIZATION_WITH_HEADER` in `ya.make` (see
`tools/enum_parser/enum_serialization_runtime/README.md`).

## Formatting

### Indentation and whitespace

- Indent with **4 spaces**. Never use tab characters for indentation.
- No trailing whitespace at end of line.
- Files must end with a single newline.

### Braces

Use [1TBS / K&R style](https://en.wikipedia.org/wiki/Indentation_style#Variant:_1TBS)
for block statements. Pick one function-brace style per file and stay consistent:

```cpp
if (something) {
    One();
    Two();
} else {
    Three();
}

for (int i = 0; i < N; ++i) {
    // ...
}

// Either style is OK within a file:
void Func1(int a, int b) {
}

void Func2(int a, int b)
{
}
```

Multi-line conditions break before the opening brace:

```cpp
if (a && b && c &&
    d && e)
{
    Op();
}
```

Even one-line statement bodies start on a new line and use braces:

```cpp
if (something) {
    A();
}
```

Do not write empty loop bodies (`for (...);` or `for (...) ;`).

Do not put multiple statements on one line.

Leave blank lines between logical blocks of code.

### Operators and spacing

- Binary operators and assignment: spaces on both sides (`a = b`, `x += 3`).
- Unary operators and member access: no extra spaces.
- `noexcept` attaches to the declaration: `void F() noexcept { }`.
- Pointer and reference markers attach to the **type**: `const T& value`, `int* p`.
- No space between function name and `(`: `Func(a, b, c)`.
- Space between keyword and `(`: `if (`, `for (`, `while (`.
- No spaces inside `()`: `(a + b)`.
- Template angle brackets without spaces: `TVector<TVector<int>>`.

### Lambdas

Single-line lambdas are allowed only at the point of use and only if they do not
violate other rules (one statement per line, braces for bodies):

```cpp
Sort(a.begin(), a.end(), [](int x, int y) { return x < y; }); // OK

auto cmp = [](int x, int y) {
    return x < y;
};
Sort(a.begin(), a.end(), cmp);
```

## Variables and classes

### Variable declarations

Prefer one declaration per line. Multiple variables of the same type on one line
are acceptable. Do not mix arrays, pointers, references, and scalars in one
declaration. Do not break a declaration across lines.

```cpp
int level;                  // preferred
int size;

int level, size;            // OK

int level,                  // not OK: line break
    size;

int level, array[16], *p;   // not OK: mixed kinds
```

### Class layout

- `struct` may contain only public members; omit `public:`.
  If it has anything beyond members plus ctor/dtor, prefer `class`.
- Access specifiers align with the class declaration column. Always declare
  `private:` / `protected:` explicitly, including the first private section.
- Do not mix data members and methods in the same access section; repeat the
  access specifier to separate them. Use the minimum number of access sections.
- Within one access section: constructors → destructor → overridden operators →
  other methods.
- Public methods precede `protected` / `private` methods.
- Data members at the beginning or end of the class; nested types may precede
  data members.
- Prefer explicit default member initializers (`= nullptr`, `= 0`) over `= {}`
  when possible; match the style already used in the file.
- Start `template` on its own line.

```cpp
class TClass {
public:
    TClass();
    ~TClass();

private:
    int Member_ = 0;
    int* OtherMember_ = nullptr;
};
```

### Constructors

```cpp
TClass::TClass()
    : FieldA(1)
    , FieldB("value")
    , FieldC(true)
{
}
```

Use either in-class initializers or constructor initializer lists consistently —
do not mix both styles for the same members.

## Namespaces

Namespace names start with `N` + PascalCase. Closing brace gets a comment with
the namespace name (`// namespace` for anonymous namespaces). Fix incorrect
comments when editing.

```cpp
namespace NStl {
    namespace NPrivate {
    } // namespace NPrivate

    class TVector {
    };
} // namespace NStl

namespace {
} // namespace
```

## Includes

Remove unnecessary `#include` directives in both `.cpp` and `.h` files.

Header files must compile standalone. For unknown types in a header:
- standard types → include the minimal standard header (`<cstddef>`, `<cstdio>`);
- class/struct/enum used by pointer or reference → forward-declare in the header;
- otherwise → include the declaring header.

Do not use `using namespace` in headers except for standard-library literals.

Use `#pragma once` for include guards.

### Include order

Break includes into groups (less general before more general). Separate groups
with a blank line. Sort alphabetically within each group.

1. Paired header (`.cpp` only): `#include "same_name.h"` first if present.
2. Local module headers in quotes: `#include "..."`.
3. `ydb/`
4. `yql/`
5. `library/`
6. `util/`
7. Other external includes (e.g. `contrib/`, `yt/`), excluding the standard library.
8. Standard library: `#include <vector>`, `#include <cstddef>`, etc.

Non-local paths use angle brackets.

```cpp
#include "sqs.h"
#include "ymq.h"

#include <ydb/core/base/path.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <yql/essentials/public/issue/yql_issue_message.h>

#include <library/cpp/scheme/scheme.h>

#include <util/string/cast.h>

#include <vector>
```

## Comments

Comments explain non-obvious code. Do not comment out dead code — delete it and
rely on version control.

Single-line `//` comments: one space after `//`.

```cpp
// Single comment

void Function() { // trailing comment
}
```

Write comments in English (preferred) or Russian (UTF-8). Doxygen-style comments
are welcome.

TODO format:

```cpp
// TODO(username): fix me later
// TODO(PROJECT-1234): fix me later
```

## Files and preprocessor

- File names: lowercase only. C++ extensions: `.cpp`, `.h`.
- Preprocessor directives use the same 4-space indent as code; `#` moves with
  the indent level.
- Mid-file `#ifdef` starts at column 0 only when it wraps a top-level block;
  nested conditions indent with code.
- Avoid preprocessor when templates, `if constexpr`, or virtual functions suffice.

## C++ features

- Use `nullptr`, not `0` or `NULL`.
- Prefer `using` over `typedef` in new code; use `typedef` when `using` breaks
  MSVC with variadic function types.
- In derived classes use `override` without repeating `virtual`.
- Use trailing return type syntax (`auto f() -> T`) only when necessary.
