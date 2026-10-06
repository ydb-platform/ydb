---
name: ydb-promela-code-style
description: "Write, format, or review Promela models across YDB while preserving guards, atomicity, labels, and checked properties."
---

# Promela Code Style

1. Read the model contract, local instructions, and neighboring models. Follow
   explicit task requirements and the component's established style first.
2. Read the [style guide](references/promela-style-guide.md) for new models or
   formatting reviews. Keep unrelated legacy models out of the diff.
3. Separate formatting from semantic changes. Guards, option boundaries,
   statement order, blocking expressions, labels, `atomic`, `d_step`, variable
   types, and LTL formulas affect the transition system. Changing them requires
   a modeling explanation and renewed verification.
4. For formatting alone, preserve those constructs exactly. Do not add temporary
   variables for line wrapping or rename `end*`, `progress*`, or `accept*` labels.
5. Check whitespace and parse the model with Spin in a separate temporary tree
   containing its includes. Keep generated `pan.*` outside the source directory.
   A syntax check is not a property proof.
6. For semantic changes, use the [verification skill](../ydb-spin-promela-verifier/SKILL.md)
   and repeat the affected safety and LTL checks. Even after formatting alone,
   do not claim a verified new input hash without a new exhaustive run.
7. Report changed files, semantic changes, and the checks actually completed.
