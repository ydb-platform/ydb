# Versioning

This library follows a three-part version scheme, `X.Y.Z`
(`Major.Minor.Patch`).

## What each part means

- **X — Major.** A significant change that is expected to break backwards
  compatibility. Adopting a new major version may require effort on your part.
- **Y — Minor.** A moderate change, such as a significant non-breaking feature
  addition. A minor bump may also include a backwards-incompatible ABI change;
  when it does, this is noted and explained in the release notes. Minor updates
  are low effort to adopt: consumers need to rebuild against the new headers,
  but source code changes are typically not required.
- **Z — Patch.** A small change that does not break backwards compatibility. A
  patch release may warn of an upcoming breaking change. Patch updates can be
  picked up automatically.

## Branch stability

Untagged branches (for example, `main`) are **not** subject to any API or ABI
stability policy. Stability guarantees apply only to tagged releases.

## What this means for you

We recommend running the latest release. Use the version parts as a guide to how
much effort an upgrade requires:

- **Major (X):** expect adoption effort; review the release notes.
- **Minor (Y):** low effort; rebuild against the new headers when the ABI changes.
- **Patch (Z):** pick up automatically.
