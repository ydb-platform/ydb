# Contributing to Boost.Graph

## We are welcoming contributions

- Bug fixes and test cases for them
- New algorithms (with tests, documentation, complexity notes, and a reference)
- Documentation, examples, performance work
- Concept refinements (handle with care as these are API-visible)

## Maintainers

| Maintainer | Role | Focus | Availability | Contact |
|---|---|---|---|---|
| **Jeremy W. Murphy** | Principal maintainer. Holds merge authority and final say on design decisions. | Algorithms, technical review, library architecture. 10+ years on BGL. | Part-time, best-effort. | `jeremy.william.murphy -at- gmail.com` |
| **Arnaud Becheler** | Assistant maintainer. Triage, review, and contributor support; defers to Jeremy on merge decisions. | Documentation, modernization, contributor onboarding. | Full-time, funded by the [C++ Alliance](https://cppalliance.org) to assist Jeremy. | `arnaud.becheler -at- gmail.com` |

The authoritative maintainer list lives in [meta/libraries.json](meta/libraries.json); keep this section in sync with it. Maintainers aim to provide first-pass review on new PRs within two weeks.

## Getting set up

Clone the [Boost superproject](https://github.com/boostorg/boost):

```bash
git clone --depth 1 https://github.com/boostorg/boost
cd boost
git submodule update --init --depth 1
./bootstrap.sh
./b2 headers
```

Then replace `libs/graph/` with your fork:

```bash
rm -rf libs/graph
git clone https://github.com/<you>/graph libs/graph
cd libs/graph
git remote add upstream https://github.com/boostorg/graph
```

## Building and testing

- Headers only: `./b2 headers` from boost root
- Compiled components: `./b2` from `libs/graph`
- All tests: `./b2` from `libs/graph/test` (takes ~10 min)
- Single test: `./b2 cycle_canceling_test` from `libs/graph/test`
- Different C++ standard: `./b2 cxxstd=20`
- Different compiler: `./b2 toolset=clang`

### Naming conventions

BGL follows the standard Boost / STL conventions:

| Kind | Style | Example |
|---|---|---|
| Functions, types, variables, files | `snake_case` | `dijkstra_visitor`, `add_edge`, `adjacency_list.hpp` |
| Member functions / methods | `snake_case` (same as free functions) | `get_edge`, `out_edges` |
| Private member variables | `m_` prefix + `snake_case` | `m_matrix`, `m_num_edges`, `m_property` |
| Concept names | `PascalCase` | `IncidenceGraph`, `VertexListGraph` |
| Template parameters | `PascalCase` | `template <class Graph, class WeightMap>` |
| Tag-dispatch types | `snake_case` + `S` suffix | `vecS`, `directedS` |
| Macros | `BOOST_GRAPH_` prefix, `SCREAMING_SNAKE_CASE` | `BOOST_GRAPH_DECLARE_EDGE_PROPERTY` |

### Formatting

Low-level formatting (braces, column width, spaces in angle brackets, etc.) is captured by the [`.clang-format`](.clang-format) file at the repo root (WebKit preset with BGL-specific overrides: Allman braces, 80-column limit, `Cpp03`). It is **not** enforced by CI, but contributors are encouraged to run `clang-format -i` on files they touch before opening a PR.

## Using other Boost libraries

When writing new code:

- **Prefer `std::`** when the supported C++ standard has an equivalent. Use `std::function`, `std::shared_ptr`, `std::tuple` over their Boost counterparts. Exceptions are welcome with justification in the PR description, in particular Boost containers with no std equivalent in our standard window (e.g. `boost::unordered_flat_map`, `boost::bimap`, `boost::multi_index_container`).
- **Avoid pulling in new heavy dependencies** (Spirit, property_tree, xpressive, etc.) without justification: BGL is gradually migrating away from heavy Boost dependencies. Open a discussion before adding one.
- **Concept checks** (`boost::concept`) are encouraged for any new public template. New public APIs should respect the existing concept hierarchy. 
- Track BGL's position in the Boost dependency DAG via [boostdep](https://github.com/boostorg/boostdep)'s hosted reports. The long-term goal is to reduce BGL's level over time:
    - **[Module levels](https://pdimov.github.io/boostdep-report/master/module-levels.html#graph)** shows BGL's depth in the global DAG. A rising level means new transitive Boost dependencies have been pulled in.
    - **[Per-library page](https://pdimov.github.io/boostdep-report/develop/graph.html)** shows BGL's direct deps and per-dep `#include` counts.

## Boost macro usage

BGL has accumulated a wide vocabulary of `BOOST_*` macros from its pre-C++14 days. The table below captures the current guidance for each one used in the codebase: **Keep** as-is, **Replace** with a language or std equivalent, or **Remove** along with the dead `#if defined(...)` branch the macro guards. When in doubt, follow the recommendation here or ask before introducing new uses.

| Macro | Recommendation |
|---|---|
| `BOOST_ASSERT` | Keep, more useful and configurable than `assert` |
| `BOOST_BORLANDC` | Remove |
| `BOOST_CONCEPT_ASSERT` | Keep (pre C++20) |
| `BOOST_CONCEPT_REQUIRES` | Keep (pre C++20) |
| `BOOST_CONCEPT_USAGE` | Keep (pre C++20) |
| `BOOST_DEDUCED_TYPENAME` | Remove |
| `BOOST_FOREACH` | Replace with an appropriate algorithm or range-based `for` loop |
| `BOOST_FWD_REF` | Replace with `T&&` |
| `BOOST_JOIN` | Keep |
| `BOOST_MPL_HAS_XXX_TRAIT_DEF` | Consider writing the same utility without dependency on MPL |
| `BOOST_MSVC` | Keep |
| `BOOST_NO_ARGUMENT_DEPENDENT_LOOKUP` | Remove |
| `BOOST_NO_AUTO_PTR` | Remove and assume `auto_ptr` is not available |
| `BOOST_NO_CXX11_ALLOCATOR` | Remove |
| `BOOST_NO_CXX11_RVALUE_REFERENCES` | Remove |
| `BOOST_NO_CXX11_SMART_PTR` | Remove |
| `BOOST_NO_CXX17_STRUCTURED_BINDINGS` | Keep |
| `BOOST_NO_MEMBER_TEMPLATE_FRIENDS` | Remove |
| `BOOST_NO_SFINAE` | Remove |
| `BOOST_NO_STDC_NAMESPACE` | Possibly keep |
| `BOOST_NO_STD_ALLOCATOR` | Remove |
| `BOOST_NO_STD_ITERATOR_TRAITS` | Remove |
| `BOOST_NO_TEMPLATED_ITERATOR_CONSTRUCTORS` | Remove |
| `BOOST_OVERRIDE` | Possibly remove |
| `BOOST_PARAMETER_KEYWORD` | Keep |
| `BOOST_PARAMETER_NAME` | Keep |
| `BOOST_PP_CAT` | Use if Boost.PP can't be replaced with variadics |
| `BOOST_PP_COMMA_IF` | Use if Boost.PP can't be replaced with variadics |
| `BOOST_PP_DEC` | Use if Boost.PP can't be replaced with variadics |
| `BOOST_PP_ENUM_BINARY_PARAMS` | Use if Boost.PP can't be replaced with variadics |
| `BOOST_PP_ENUM_BINARY_PARAMS_Z` | Use if Boost.PP can't be replaced with variadics |
| `BOOST_PP_ENUM_PARAMS` | Use if Boost.PP can't be replaced with variadics |
| `BOOST_PP_ENUM_PARAMS_Z` | Use if Boost.PP can't be replaced with variadics |
| `BOOST_PP_ENUM_TRAILING_BINARY_PARAMS_Z` | Use if Boost.PP can't be replaced with variadics |
| `BOOST_PP_ENUM_TRAILING_PARAMS_Z` | Use if Boost.PP can't be replaced with variadics |
| `BOOST_PP_EXPR_IF` | Use if Boost.PP can't be replaced with variadics |
| `BOOST_PP_INC` | Use if Boost.PP can't be replaced with variadics |
| `BOOST_PP_LPAREN_IF` | Use if Boost.PP can't be replaced with variadics |
| `BOOST_PP_REPEAT` | Use if Boost.PP can't be replaced with variadics |
| `BOOST_PP_REPEAT_FROM_TO` | Use if Boost.PP can't be replaced with variadics |
| `BOOST_PP_RPAREN_IF` | Use if Boost.PP can't be replaced with variadics |
| `BOOST_PP_SEQ_ELEM` | Use if Boost.PP can't be replaced with variadics |
| `BOOST_PP_SUB` | Use if Boost.PP can't be replaced with variadics |
| `BOOST_PP_TUPLE_EAT` | Use if Boost.PP can't be replaced with variadics |
| `BOOST_PREVENT_MACRO_SUBSTITUTION` | Keep |
| `BOOST_SERIALIZATION_NVP` | Consider removing if you can eliminate the dependency on Boost.Serialization via `<boost/core/serialization.hpp>` |
| `BOOST_SPIRIT_CLOSURE_LIMIT` | Keep |
| `BOOST_SPIRIT_DEBUG` | Keep |
| `BOOST_SPIRIT_DEBUG_RULE` | Keep |
| `BOOST_STATIC_ASSERT` | Replace with `static_assert(..., msg)` |
| `BOOST_STATIC_ASSERT_MSG` | Replace with `static_assert(..., msg)` |
| `BOOST_STATIC_CONSTANT` | Remove in favour of in-class `static constexpr` initialization |
| `BOOST_SYMBOL_EXPORT` | Keep |
| `BOOST_SYMBOL_IMPORT` | Keep |
| `BOOST_SYMBOL_VISIBLE` | Keep |
| `BOOST_TESTED_AT` | Keep |
| `BOOST_THROW_EXCEPTION` | Keep |
| `BOOST_TTI_HAS_MEMBER_FUNCTION` | Consider writing the same utility without dependency on TTI |
| `BOOST_USING_STD_MAX` | Replace with `(std::max)(...)` |
| `BOOST_USING_STD_MIN` | Replace with `(std::min)(...)` |
| `BOOST_WORKAROUND` | Keep |

When you mark a `BOOST_NO_*` flag as **Remove**, also delete the `#if defined(...)` block it gates — the alternate branch is always taken on the supported toolchains.

## Pull request process

- Fork, branch from `develop`, PR back to `develop`
- One logical change per PR; rebase before requesting review
- Tests required for new features and bug fixes
- **Open (non-draft) PRs are assumed ready for review.** Use GitHub's Draft state while iterating, then mark the PR as *Ready for review* when you want maintainers to look at it.
- A maintainer will review within ~2 weeks (see Maintainers below)
- Squash on merge by default

## Merge criteria

Before a PR is merged, all of the following must hold:

1. CI is green on the full matrix (gcc-14 + clang-19, C++14/17/20/23).
2. New behavior has tests.
3. Bug fixes have a regression test.
4. Documentation under [doc/](doc/) is updated if the public API changed.
5. No new compiler warnings on the supported toolchains.
6. The PR is rebased on current `develop` with a clean commit history.
7. The principal maintainer has approved.

## Reporting bugs

1. Search [existing issues](https://github.com/boostorg/graph/issues) first
2. Note compiler, version, OS, Boost version
3. Minimal reproducer on [Compiler Explorer](https://godbolt.org/z/37dPWd5bs)
4. Expected Output versus Actual Output is explained.

## Security

See [SECURITY.md](SECURITY.md).

## License

By contributing, you agree your contribution is licensed under the [Boost Software License 1.0](https://www.boost.org/LICENSE_1_0.txt).
