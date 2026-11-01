# Promela style guide

## Layout and names

1. Order new files as: model contract, bounds/constants, channels/shared state,
   inline helpers, component/environment processes, property macros/LTL, init.
2. Use four spaces, no tabs, one statement per line, and a final newline. Aim for
   100 columns. Keep braces with their construct and separate sections with a
   blank line. Avoid decorative separators.
3. Use UPPER_SNAKE_CASE for constants and predicate macros, PascalCase for
   processes, and snake_case for variables, channels, helpers, and claims.
   Prefer stable `safe_*` and `live_*` claim names. Preserve existing names when
   formatting; semantic label prefixes have runtime meaning.
4. Start preprocessor directives in column one. A continuation backslash must
   be the last character on its line. Do not rewrite macro boundaries or
   separators as a formatting operation.

## Guards and atomicity

1. Align each `::` within its `if` or `do`. Keep the guard and arrow together
   when possible; wrap long expressions without introducing state variables.
2. Preserve option boundaries, `else`, statement order, labels, and blocking
   expressions. Moving them can change enabled transitions.
3. Use `atomic` only for an implementation step or an explicitly justified
   abstraction. A blocked statement relinquishes atomic execution. Use `d_step`
   only when the deterministic execution and collapsed intermediate states
   match the intended model; it is not interchangeable with `atomic`.
4. Do not replace polling with a blocking guard as a style change. Such a
   stuttering abstraction changes scheduling and needs a property-specific
   justification and verification.

## State and properties

1. Group state by owner. Document shared writers and bounds; choose the smallest
   type that covers the domain without overflow. Do not change types just to
   improve appearance.
2. Use named sentinels. Do not introduce exact counters or combine state updates
   into atomic blocks unless the modeled implementation or abstraction warrants it.
3. Place assertions beside relevant updates. Parenthesize predicate macro
   arguments and keep them free of side effects. A helper must not silently
   hard-code process zero when it claims to be generic.
4. Explain abstraction, publication order, ownership, and fairness assumptions
   in comments. `timeout` represents global absence of executable transitions,
   not a wall-clock timer.
5. Keep `end*`, `accept*`, and `progress*` labels only for their Spin semantics.
   Do not move them between statements during formatting.
6. Prefer stutter-invariant LTL for partial-order reduction. If using `X`, verify
   the search supports it and disable incompatible reduction. Use `xr`/`xs`
   only with justified exclusive channel ownership.

See the [Promela reference](https://spinroot.com/spin/Man/promela.html) for
language semantics and [Pan options](https://spinroot.com/spin/Man/Pan.html)
for search modes. Do not commit generated verifier files or incidental trails.
