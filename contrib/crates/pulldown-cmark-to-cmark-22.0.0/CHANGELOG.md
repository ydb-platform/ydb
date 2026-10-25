# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## 22.0.0 (2025-12-23)

### New Features

 - <csr-id-9b55c822ae8c824954452d1414e1dc33eadc5b73/> Add #![deny(missing_docs)] attribute to enable missing documentation checks and add comprehensive documentation for all public items

### New Features (BREAKING)

 - <csr-id-ffa22b3e25ef79aad66774f1c08b1a4a1b1bc4f4/> Add support for super/subscript without html
   Superscript and subscript can be presented in markdown as:
   ^superscript^ and ~subscript~ respectively. This is not always turned on,
   and many people prefer the HTML tag form, however in order to round-trip
   markdown which uses the symbolic form we need an option to enable that.
   
   This feature is enabled via a new field in the `Options` struct.

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 7 commits contributed to the release.
 - 43 days passed between releases.
 - 2 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - Merge pull request #107 from kinnison/super-sub-not-html ([`15e7a74`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/15e7a74364cf05e437029d84ec6b78a6662fd137))
    - Refactor ([`aba929c`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/aba929c5068f69de5ae2e92b397fd901922f2c0d))
    - Add testing support for symbolic super/subscript ([`463b65a`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/463b65a0ede1c99d921862645931c8ecc117c159))
    - Add support for super/subscript without html ([`ffa22b3`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/ffa22b3e25ef79aad66774f1c08b1a4a1b1bc4f4))
    - Merge pull request #108 from Byron/copilot/add-documentation-and-deny-attrs ([`077d5d0`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/077d5d0f7b464d4fdd251bc4adfe5679fb6a2736))
    - Refactor ([`6d09f44`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/6d09f445a72555fbd70812b21a38ba65bba0e42b))
    - Add #![deny(missing_docs)] attribute to enable missing documentation checks and add comprehensive documentation for all public items ([`9b55c82`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/9b55c822ae8c824954452d1414e1dc33eadc5b73))
</details>

## 21.1.0 (2025-11-09)

<csr-id-5b93e7e7b51000632bfecf6d7a49e48d9fb3bc19/>
<csr-id-942c42ba6fc517b595130603e9fd4aa1309ee755/>
<csr-id-30b706b719fd2554b664bc7d2c40bcb7e7095a8c/>
<csr-id-31a49d0d45c1c32bfcf1c653121ee3ca0a1d4fe8/>
<csr-id-3ab278b9db5546f405bcd13bdf5c3f0d59d2fb8a/>

### New Features

 - <csr-id-91ffc9508b129cd603b870d38fd9bd88240d07fe/> Improve CommonMark conformance testing output
   This is a major improvement to the rendering of the failing
   CommonMark conformance test output, that should make it easier
   to read and understand when pulldown-cmark-to-cmark is producing
   erroneous output.
   
   Additionally, this introduces a way to force the printing of the
   CommonMark failure report from the command line:
   
   $ FULL_CMARK_RESULTS=true cargo test
   
   and documents in CONTRIBUTING.md that new contributors may want
   to start with trying to improve conformance.
   
   This removes `#[should_panic]` from the main CommonMark test. Instead,
   we now hard-code in how many of the CommonMark tests are known to pass.
   
   If the number that actually passes increases or decreases, this test
   will fail, informing the user either that they have introduced a bug
   (decrease), or that they've successfully improved conformance (increase).
   
   Previously, if a change accidentally reduce the conformance, there was
   no easy way to know, since that information was not stored/tracked
   in any way.

### Other

 - <csr-id-5b93e7e7b51000632bfecf6d7a49e48d9fb3bc19/> Drop comment headers

### Refactor

 - <csr-id-942c42ba6fc517b595130603e9fd4aa1309ee755/> Add Repeated utility to simplify writing repeated content
 - <csr-id-30b706b719fd2554b664bc7d2c40bcb7e7095a8c/> Move and group related code
   My impression reading through the code in this project is that
   top-level statements have gotten a bit intertangled over time;
   this tries to reorder things to have a clearer flow and
   structure, grouping related things together.
   
   For example, this moves the recently added Error enum up above
   all of the API functions, instead of leaving it somewhat
   arbitrarily nestled between `cmark_resume_with_options()` and
   `cmark_resume_one_event()`.
   
   No code changes were made to the body of functions or types.
   This also does not change any of the public APIs, only where
   code is located.
   
   * Group padded newline logic at top of text_modifications.rs
   
   * Rename `padding_of()` to `list_item_padding_of()`
   
   * Add 'Public API Functions' header in lib.rs
   
   * Consolidate public enum and struct types at the
     top of lib.rs, instead of leaving them scattered
     amongst the `cmark*()` public functions.
   
   * Consolidate the two `impl State<'_>` statements
   
   * Move `cmark_resume()`, `cmark()` and `cmark_with_options()`
     from the bottom of lib.rs up to the top, before the ~600
     implementation of `cmark_resume_one_event()` that makes up
     the bulk of lib.rs
 - <csr-id-31a49d0d45c1c32bfcf1c653121ee3ca0a1d4fe8/> Add State::set_minimum_newlines_before_start()
   This should help with readability. As an example, I didn't realize
   initially that all of the `if` statements updated in this PR
   were actually modifying the same field.
   
   But once I started doing the more generic refactoring of adding
   a `set_option_minimum()` method, I looked at every line and
   realized they were all the same.
   
   Factoring this to a method should clarify to future readers that
   updating newlines_before_start specifically is a recurring operation.
 - <csr-id-3ab278b9db5546f405bcd13bdf5c3f0d59d2fb8a/> Use `write_padded_string()` in a couple additional places
   Additionally, while in the neighhborhood, add `pub(crate)` to the other
   text_modifications.rs private helpers
   
   These being marked as `pub` is misleading since the `text_modifications`
   module is not actually externally public, and these functions
   are never re-exported publicly.

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 13 commits contributed to the release.
 - 264 days passed between releases.
 - 6 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - Release pulldown-cmark-to-cmark v21.1.0 ([`4fa1f9e`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/4fa1f9eab4754267e2d6580fe7e64f8aa4c113fa))
    - Release pulldown-cmark-to-cmark v21.0.0 ([`d69f748`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/d69f748198d26828f74c0e1a0faed6a986cb4b97))
    - Merge pull request #104 from ConnorGray/connorgray/refactor-5 ([`bf34a3c`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/bf34a3cac68e6f82a24ee3d44224a9e2ef2bcd0d))
    - Add Repeated utility to simplify writing repeated content ([`942c42b`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/942c42ba6fc517b595130603e9fd4aa1309ee755))
    - Merge pull request #103 from ConnorGray/connorgray/refactor-4 ([`3005f1b`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/3005f1b10548a08a412086ecddc22b907ad0b60d))
    - Drop comment headers ([`5b93e7e`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/5b93e7e7b51000632bfecf6d7a49e48d9fb3bc19))
    - Move and group related code ([`30b706b`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/30b706b719fd2554b664bc7d2c40bcb7e7095a8c))
    - Merge pull request #102 from ConnorGray/connorgray/revamp-conformance-testing ([`3169307`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/3169307f9dba2683eaa0f425177b2905669cd705))
    - Merge pull request #101 from ConnorGray/connorgray/refactor-3 ([`ac3aaec`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/ac3aaec506e3eb5bdd85ff3a2bc9b1f2cee0f5c9))
    - Merge pull request #100 from ConnorGray/connorgray/refactor-2 ([`29c8c20`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/29c8c20bb8632e70d6ed10eb43fa68f673988748))
    - Improve CommonMark conformance testing output ([`91ffc95`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/91ffc9508b129cd603b870d38fd9bd88240d07fe))
    - Add State::set_minimum_newlines_before_start() ([`31a49d0`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/31a49d0d45c1c32bfcf1c653121ee3ca0a1d4fe8))
    - Use `write_padded_string()` in a couple additional places ([`3ab278b`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/3ab278b9db5546f405bcd13bdf5c3f0d59d2fb8a))
</details>

## 21.0.0 (2025-02-18)

<csr-id-05e247e5c7a74ac7bf9a906417724b8a275cabcc/>
<csr-id-2252ba1d9cf93256a8bcdd721813ed2397e32464/>
<csr-id-942c42ba6fc517b595130603e9fd4aa1309ee755/>
<csr-id-30b706b719fd2554b664bc7d2c40bcb7e7095a8c/>
<csr-id-31a49d0d45c1c32bfcf1c653121ee3ca0a1d4fe8/>
<csr-id-3ab278b9db5546f405bcd13bdf5c3f0d59d2fb8a/>
<csr-id-5b93e7e7b51000632bfecf6d7a49e48d9fb3bc19/>

The breaking release is to avoid side-effects with different `pulldown-cmark` versions.
This crate now comes with version 13.

### Refactor

 - <csr-id-942c42ba6fc517b595130603e9fd4aa1309ee755/> Add Repeated utility to simplify writing repeated content
 - <csr-id-30b706b719fd2554b664bc7d2c40bcb7e7095a8c/> Move and group related code
   My impression reading through the code in this project is that
   top-level statements have gotten a bit intertangled over time;
   this tries to reorder things to have a clearer flow and
   structure, grouping related things together.
   
   For example, this moves the recently added Error enum up above
   all of the API functions, instead of leaving it somewhat
   arbitrarily nestled between `cmark_resume_with_options()` and
   `cmark_resume_one_event()`.
   
   No code changes were made to the body of functions or types.
   This also does not change any of the public APIs, only where
   code is located.
   
   * Group padded newline logic at top of text_modifications.rs
   
   * Rename `padding_of()` to `list_item_padding_of()`
   
   * Add 'Public API Functions' header in lib.rs
   
   * Consolidate public enum and struct types at the
     top of lib.rs, instead of leaving them scattered
     amongst the `cmark*()` public functions.
   
   * Consolidate the two `impl State<'_>` statements
   
   * Move `cmark_resume()`, `cmark()` and `cmark_with_options()`
     from the bottom of lib.rs up to the top, before the ~600
     implementation of `cmark_resume_one_event()` that makes up
     the bulk of lib.rs
 - <csr-id-31a49d0d45c1c32bfcf1c653121ee3ca0a1d4fe8/> Add State::set_minimum_newlines_before_start()
   This should help with readability. As an example, I didn't realize
   initially that all of the `if` statements updated in this PR
   were actually modifying the same field.
   
   But once I started doing the more generic refactoring of adding
   a `set_option_minimum()` method, I looked at every line and
   realized they were all the same.
   
   Factoring this to a method should clarify to future readers that
   updating newlines_before_start specifically is a recurring operation.
 - <csr-id-3ab278b9db5546f405bcd13bdf5c3f0d59d2fb8a/> Use `write_padded_string()` in a couple additional places
   Additionally, while in the neighhborhood, add `pub(crate)` to the other
   text_modifications.rs private helpers
   
   These being marked as `pub` is misleading since the `text_modifications`
   module is not actually externally public, and these functions
   are never re-exported publicly.

### Other

 - <csr-id-5b93e7e7b51000632bfecf6d7a49e48d9fb3bc19/> Drop comment headers

### New Features

 - <csr-id-91ffc9508b129cd603b870d38fd9bd88240d07fe/> Improve CommonMark conformance testing output
   This is a major improvement to the rendering of the failing
   CommonMark conformance test output, that should make it easier
   to read and understand when pulldown-cmark-to-cmark is producing
   erroneous output.
   
   Additionally, this introduces a way to force the printing of the
   CommonMark failure report from the command line:
   
   $ FULL_CMARK_RESULTS=true cargo test
   
   and documents in CONTRIBUTING.md that new contributors may want
   to start with trying to improve conformance.
   
   This removes `#[should_panic]` from the main CommonMark test. Instead,
   we now hard-code in how many of the CommonMark tests are known to pass.
   
   If the number that actually passes increases or decreases, this test
   will fail, informing the user either that they have introduced a bug
   (decrease), or that they've successfully improved conformance (increase).
   
   Previously, if a change accidentally reduce the conformance, there was
   no easy way to know, since that information was not stored/tracked
   in any way.

### Other

 - <csr-id-05e247e5c7a74ac7bf9a906417724b8a275cabcc/> Adjust wording + typos

### Refactor

 - <csr-id-2252ba1d9cf93256a8bcdd721813ed2397e32464/> Add helper to factor out newline + padding pattern
   When writing a newline into the generated Markdown content
   to start a new line of output, it is (almost) always necessary
   to output the "padding" characters used to indent the content
   at the current location in the document, based on the hierarchy
   of block-level elements the output cursor is "inside" of.
   
   Since writing a newline and writing the current padding are
   always paired, factoring them out into a function should help
   with readability and consistency in performing this minor
   two-step dance correctly.

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 9 commits contributed to the release.
 - 6 days passed between releases.
 - 2 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - Release pulldown-cmark-to-cmark v21.0.0 ([`c1347d1`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/c1347d140499a637c98bcb9dc6f324731919dcf6))
    - Update changelog prior to release ([`475478a`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/475478a781fafc42b3e0879d9fe12993c3d89cb4))
    - Bump version to 21 for pulldown-cmark 13 ([`ed16be5`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/ed16be5ad4d32d3d17a5ac86a60f7e0a2cbf2c91))
    - Merge pull request #99 from danieleades/cmark-13 ([`fb9bbd6`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/fb9bbd6a4201f917041e7ca887e1845463be03df))
    - Merge pull request #98 from ConnorGray/connorgray/refactor-1 ([`b47d6c9`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/b47d6c936b2d940107103e6bee3b12ab822f2d0f))
    - Update doc-string so `State::padding` is shown conventionally ([`e10010f`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/e10010f198a9cba9c0691def19978cf444740aa2))
    - Update to pulldown-cmark 13 ([`efbdd3a`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/efbdd3a712119d1899e54ac69b79830bd5a5a541))
    - Adjust wording + typos ([`05e247e`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/05e247e5c7a74ac7bf9a906417724b8a275cabcc))
    - Add helper to factor out newline + padding pattern ([`2252ba1`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/2252ba1d9cf93256a8bcdd721813ed2397e32464))
</details>

## 20.0.1 (2025-02-12)

### Bug Fixes

 - <csr-id-27cbc1635ca77acae37fe4723daf75dd1c8733a0/> definition list block indentation now works correctly.

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 8 commits contributed to the release.
 - 58 days passed between releases.
 - 1 commit was understood as [conventional](https://www.conventionalcommits.org).
 - 1 unique issue was worked on: [#97](https://github.com/Byron/pulldown-cmark-to-cmark/issues/97)

### Thanks Clippy

<csr-read-only-do-not-edit/>

[Clippy](https://github.com/rust-lang/rust-clippy) helped 1 time to make code idiomatic. 

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **[#97](https://github.com/Byron/pulldown-cmark-to-cmark/issues/97)**
    - Definition list block indentation now works correctly. ([`27cbc16`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/27cbc1635ca77acae37fe4723daf75dd1c8733a0))
 * **Uncategorized**
    - Release pulldown-cmark-to-cmark v20.0.1 ([`80f82f4`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/80f82f42e82d050859eb466e9bf5715e67915320))
    - Merge branch 'definition_list' ([`435307d`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/435307d55579296addebb3a4adb9a580728e4148))
    - Thanks clippy ([`abb85a6`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/abb85a61515b1e9ccb9f40fb6f15b409f9d4602e))
    - Treat definition list entries as blocks ([`261c23b`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/261c23b18c00a0122bbcc00cb93f16ceb3bd467c))
    - Merge pull request #96 from mgsloan/improve-naming-in-code-escaping-and-add-comments ([`1ffd7c8`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/1ffd7c86380de1d558d27364168ff2a78de4b4b7))
    - Add comments explaining inline code escaping ([`60e1854`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/60e185422a15f3d5e33e889e9874bfb44ff25833))
    - Improve naming in function for counting max consecutive chars ([`32628b3`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/32628b3562225a8bec3e7a9ed54467dad5d7c2ef))
</details>

## 20.0.0 (2024-12-15)

### Bug Fixes (BREAKING)

 - <csr-id-fdc5859ff9823189c6966d5448468bff1d82d899/> Add structured error handling for cmark(), cmark_resume() etc
   When `cmark_resume_with_options()`, which serves as basis for the other
   `cmark*` functions, finds an inconsistent event stream (for example, two
   consecutive heading start tags), it panics.
   
   Introduce a custom error type `Error` in the crate and change the return
   type of all public functions from `fmt::Result<_>` to `Result<_, Error>`.
   
   The next commit adds an integration test.

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 5 commits contributed to the release.
 - 4 days passed between releases.
 - 1 commit was understood as [conventional](https://www.conventionalcommits.org).
 - 1 unique issue was worked on: [#91](https://github.com/Byron/pulldown-cmark-to-cmark/issues/91)

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **[#91](https://github.com/Byron/pulldown-cmark-to-cmark/issues/91)**
    - Revert "fix: harden again manufactured input which could previously trigger assertion failures." ([`a442910`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/a442910b6e82f17f0e044ceb2069dd4bd76ffa34))
 * **Uncategorized**
    - Release pulldown-cmark-to-cmark v20.0.0 ([`3c850de`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/3c850de2d3d1d79f19ca5f375e1089a653cf3ff7))
    - Merge pull request #93 from cip999/resume-error-handling ([`21f0f5d`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/21f0f5d472307f4c8c8403505d90259a922ae624))
    - Add integration test for CmarkError::UnexpectedEvent ([`4766cc1`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/4766cc1c4dc569841e89068d3292b28d3c57e2e6))
    - Add structured error handling for cmark(), cmark_resume() etc ([`fdc5859`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/fdc5859ff9823189c6966d5448468bff1d82d899))
</details>

## 19.0.1 (2024-12-11)

### Bug Fixes

 - <csr-id-a368f0f4ddbfeb8728abf92feaf0d4ed0e81b858/> harden again manufactured input which could previously trigger assertion failures.

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 4 commits contributed to the release.
 - 19 days passed between releases.
 - 1 commit was understood as [conventional](https://www.conventionalcommits.org).
 - 1 unique issue was worked on: [#91](https://github.com/Byron/pulldown-cmark-to-cmark/issues/91)

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **[#91](https://github.com/Byron/pulldown-cmark-to-cmark/issues/91)**
    - Harden again manufactured input which could previously trigger assertion failures. ([`a368f0f`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/a368f0f4ddbfeb8728abf92feaf0d4ed0e81b858))
 * **Uncategorized**
    - Release pulldown-cmark-to-cmark v19.0.1 ([`aefd5cb`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/aefd5cbea4b79e21a5a1c2836b41a007e750d7ce))
    - Merge pull request #92 from Byron/fuzz-test ([`c61fdd4`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/c61fdd4917b8ee41af0bae69d99c721d7d337328))
    - Refactor tests ([`b526d7c`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/b526d7cf0ad6df2ce8278a5fd634afb2067c484e))
</details>

## 19.0.0 (2024-11-22)

### New Features (BREAKING)

 - <csr-id-13785fe11cc8851f6f939b1763ae99d655a2d5dc/> Make `State` struct non-exhaustive
   That way, future additions to the rather internal state won't be a reason for breaking changes anymore.

### Bug Fixes (BREAKING)

 - <csr-id-76c24a16cf34d1675bef52aa8b4c58edebefbfa8/> drasticly improve escaping `!`, `#`, and `|`
   Raises spec tests from 578 to 580. It handles cases that look like these
   
   Link, not image: \![a](b)
   
   This header ends with hashes, not an ATX trailer ###
   ====================================================
   
   | a \| b | a \| c |
   |--------|--------|

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 12 commits contributed to the release.
 - 2 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - Release pulldown-cmark-to-cmark v19.0.0 ([`ccc8a52`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/ccc8a522c738fc69eb937c7c0f7e8564741ee584))
    - Merge pull request #90 from chriskrycho/escape-code-in-tables ([`daef56b`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/daef56bba9c244c2e08ef28ec09e99fa7b8c309d))
    - Add fixtures for pipes-in-code which should *not* be escaped ([`a85fe39`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/a85fe394d52c236e25cec640e69df83228041fe1))
    - Use `Cow<'a, str>` to minimize allocations for pipe escaping ([`022fa11`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/022fa11e3a39c6b6bc0b2dcd9728bf205f00d5d1))
    - Correct comment/note on when pipe characters are/not escaped ([`c9ecdd2`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/c9ecdd24024c0807ceb424f9dfae3b1b22072336))
    - Make `State` struct non-exhaustive ([`13785fe`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/13785fe11cc8851f6f939b1763ae99d655a2d5dc))
    - Improve test expectations for escaping pipes in inline code ([`e7f0784`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/e7f078461e94c621e817a7f25eece571a0ae90de))
    - Escape special characters in inline code in tables ([`37e8dcb`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/37e8dcbf9af753542c39bd077d62e6799fac1f83))
    - Add fixture for tables with escaped characters ([`edcac27`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/edcac270a3d6cc92db9098aa2ce10ea88e52d235))
    - Minor improvements ([`f6fba5f`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/f6fba5f43d29fa5a617124dbfcba7fad12bcecbc))
    - Merge branch 'notriddle/main' ([`4690885`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/469088549b3ff9c665a1e4143498aaf2ebf55414))
    - Drasticly improve escaping `!`, `#`, and `|` ([`76c24a1`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/76c24a16cf34d1675bef52aa8b4c58edebefbfa8))
</details>

## 18.0.0 (2024-10-16)

<csr-id-b3d7df263409e2f9ebdaae843e087c714b994367/>

### Chore

 - <csr-id-b3d7df263409e2f9ebdaae843e087c714b994367/> use `TextMergeStream` for round-trip tests
   This increases the amount of passing tests to 578.
   It doesn't strictly improve the quality of implementation,
   but since separate text events aren't supposed to be semantic,
   these failures seem spurrious.

### New Features

 - <csr-id-d8f22c18562ebc50c1ca6960bd745adab706a5b6/> angle brackets if link paren is unbalanced
   Increases passing spec tests from 473 to 477.

### Bug Fixes

 - <csr-id-2f75de249b1925dad69a43add3a22ace27e42904/> write block quote kind only once

### New Features (BREAKING)

 - <csr-id-db767944612b0e0068d7872ec515e3f0c23865b4/> correctly round-trip code blocks with no trailing newline
 - <csr-id-8a9573d0b1b6095248ba2869c63da8399d31acfb/> round-trip indented code blocks
   This raises the number of passing spec tests from 459 to 473.

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 11 commits contributed to the release.
 - 40 days passed between releases.
 - 5 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - Release pulldown-cmark-to-cmark v18.0.0 ([`6c8bc4d`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/6c8bc4d0e44aeee768d3a4ee8e2b4d500a3c0aeb))
    - Merge pull request #87 from notriddle/notriddle/link-tweaks ([`81ac29b`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/81ac29bb629c79bc7408cba9d5e91314fc5fe618))
    - Use `TextMergeStream` for round-trip tests ([`b3d7df2`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/b3d7df263409e2f9ebdaae843e087c714b994367))
    - Angle brackets if link paren is unbalanced ([`d8f22c1`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/d8f22c18562ebc50c1ca6960bd745adab706a5b6))
    - Merge branch 'blockquote-kinds' ([`d5323a8`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/d5323a8f490649f74a21f158268abbf56fdc9ae9))
    - Remove allocation ([`33ce10d`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/33ce10da1ad5fac7d0f87aec1ce3f469ad10c29f))
    - Write block quote kind only once ([`2f75de2`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/2f75de249b1925dad69a43add3a22ace27e42904))
    - Merge branch 'notriddle/main' ([`848abc3`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/848abc3cee47d139d44eaa951dbbb0e3d1c26d73))
    - Run cargo fmt and assure it doesn't regress anymore. ([`3fdf308`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/3fdf308560c7db1046bdca82ccd316cf16082ff5))
    - Correctly round-trip code blocks with no trailing newline ([`db76794`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/db767944612b0e0068d7872ec515e3f0c23865b4))
    - Round-trip indented code blocks ([`8a9573d`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/8a9573d0b1b6095248ba2869c63da8399d31acfb))
</details>

## 17.0.0 (2024-09-06)

### New Features (BREAKING)

 - <csr-id-3170061202dcdbb291abbb467fbee1de970d78bd/> retain more details on reference links
   This raises the number of passing spec tests from 436 to 459.

### Bug Fixes (BREAKING)

 - <csr-id-c3a0f322dfc3450e1973ea3ed79e55a8c3b8b718/> spurrious code blocks when char escapes start text

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 6 commits contributed to the release.
 - 14 days passed between releases.
 - 2 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - Release pulldown-cmark-to-cmark v17.0.0 ([`55fe0bc`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/55fe0bc8436b3a9241a2c5a5e6884009e6559218))
    - Merge branch 'notriddle/reference-links' ([`98818fa`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/98818faf31c0137d95fd8edfb3bb9158f3a1cb13))
    - Merge branch 'notriddle/main' ([`ed4ff6f`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/ed4ff6fd871277d354867570163a73369b835d7a))
    - Retain more details on reference links ([`3170061`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/3170061202dcdbb291abbb467fbee1de970d78bd))
    - Spurrious code blocks when char escapes start text ([`c3a0f32`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/c3a0f322dfc3450e1973ea3ed79e55a8c3b8b718))
    - Fix incorrect variable name ([`210a0be`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/210a0be86892446caa2cda11d0f712d6d0ff5078))
</details>

## 16.0.1 (2024-08-22)

<csr-id-21a7f26a611e7a5b99ab33e9386acd30f0188960/>

### Other

 - <csr-id-21a7f26a611e7a5b99ab33e9386acd30f0188960/> Lower MSRV to 1.71.1
   This project builds successfully with the MSRV of `pulldown-cmark`. Therefore we can lower `rust-version` to 1.71.1.
   
   Add documentation for the current MSRV and that this project follows MSRV policy of `pulldown-cmark`.
   
   Use `cargo hack` for MSRV CI job, to automatically install rustc according to `rust-version` and then execute `cargo check`.

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 16 commits contributed to the release.
 - 2 days passed between releases.
 - 1 commit was understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - Release pulldown-cmark-to-cmark v16.0.1 ([`ab22a57`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/ab22a57249aa03f73c4fbd09d4b2fb42572a326f))
    - Merge pull request #81 from caspermeijn/msrv ([`85ca9d4`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/85ca9d4d63aab14b3193a2438959baa7fc6e4b05))
    - Make clear that the rust-version field follows `pulldown-cmark`'s MSRV. ([`7269c4f`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/7269c4f8aabb8ca3fdb63f4ba95b207899f9f845))
    - Lower MSRV to 1.71.1 ([`21a7f26`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/21a7f26a611e7a5b99ab33e9386acd30f0188960))
    - Merge pull request #79 from danieleades/clippy ([`c6899bc`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/c6899bce8ca1b018c03bda2515892762ed53f933))
    - Use 'Self' keyword (clippy::use_self) ([`1a3029f`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/1a3029f41f4b97ddd1d2c0e373c63e1e388c40d9))
    - Remove unnecessary negation (clippy::if_not_else) ([`5745ce5`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/5745ce5a6574434fd673533586b3cfb6d5123ea3))
    - Remove unneeded 'ref' bindings (clippy::ref_binding_to_reference) ([`820b661`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/820b661395d9855bf8c7a8c58e906f86741ce168))
    - Nest or patterns (clippy::unnested_or_patterns) ([`43c6e8a`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/43c6e8a3ddffb91277cec72c11f5e9f714266b51))
    - Explicitly match unit values (clippy::ignored_unit_patterns) ([`d636dc6`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/d636dc68f0b72088d761774d60e8762b26562cc4))
    - Avoid explicit iter loops (clippy::explicit_iter_loop) ([`a8c7fa5`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/a8c7fa5a77145021fe9d67741e4a00d40851919f))
    - Use inline format args (clippy::uninlined_format_args) ([`d97f8f5`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/d97f8f5268f67f612474b68613eb5ff2941cb607))
    - Remove needless raw string hashes (clippy::needless_raw_string_hashes) ([`6e2ae15`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/6e2ae15a167e3622993d7a96ca87619b48751281))
    - Use semicolons if nothing returned (clippy::semicolon_if_nothing_returned) ([`8f9d4bf`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/8f9d4bff7ca827b61ab1ca4ab7ca08d98721244f))
    - Merge pull request #80 from danieleades/msrv ([`08ffcb8`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/08ffcb896f605eb94d05fc07bd921efbd1b18666))
    - Add MSRV check to CI ([`66b9ed2`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/66b9ed28d08fd622f411ce0c9e8720443bda1e0e))
</details>

## 16.0.0 (2024-08-20)

Add support for `pulldown-cmark` v0.12, and an understanding of markdown Definition Lists.

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 8 commits contributed to the release.
 - 64 days passed between releases.
 - 0 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - Release pulldown-cmark-to-cmark v16.0.0 ([`4dd27c5`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/4dd27c5620531d157b0002dde42b9a940c29890d))
    - Bump major version to help isolage the `pulldown-cmark` crate at v0.12 ([`17b5308`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/17b5308e2592ef593e84a89cebbb28955024769b))
    - Update changelog prior to release ([`7b74f1b`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/7b74f1b301db131f615b42e7645d413cc6c161cf))
    - Merge branch 'cmark-12' ([`f1f18d4`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/f1f18d44147dfcffee8caf9f46d870f1e651b208))
    - Fix test ([`173ae85`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/173ae8507f34d4163c735c9ca5f6b4b83a338948))
    - Add failing test case ([`7674b54`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/7674b549f0fb865c65d432ce4a5acfcc04af9c24))
    - Add test for definition list item start ([`e686a8f`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/e686a8f7ee35fe83fe1b7f8f6466b62d0af35fd9))
    - Add 'todo' placeholders ([`f1a8a3e`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/f1a8a3efd57a9bff2073796387d70eba49d93399))
</details>

## 15.0.1 (2024-06-16)

### Bug Fixes

 - <csr-id-2ac74720c4d3fc8ccda8eaae86508bd62c28b43d/> tables render more correctly

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 3 commits contributed to the release.
 - 1 day passed between releases.
 - 1 commit was understood as [conventional](https://www.conventionalcommits.org).
 - 2 unique issues were worked on: [#70](https://github.com/Byron/pulldown-cmark-to-cmark/issues/70), [#76](https://github.com/Byron/pulldown-cmark-to-cmark/issues/76)

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **[#70](https://github.com/Byron/pulldown-cmark-to-cmark/issues/70)**
    - Tables render more correctly ([`2ac7472`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/2ac74720c4d3fc8ccda8eaae86508bd62c28b43d))
 * **[#76](https://github.com/Byron/pulldown-cmark-to-cmark/issues/76)**
    - Tables render more correctly ([`2ac7472`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/2ac74720c4d3fc8ccda8eaae86508bd62c28b43d))
 * **Uncategorized**
    - Release pulldown-cmark-to-cmark v15.0.1 ([`7cc274e`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/7cc274e52d44f4901465796f4bfcc69f0a71f01a))
    - Fix emitting `::` for centered column in table. ([`33811dc`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/33811dc085da284c82bb74da590b14066ff44402))
</details>

## 15.0.0 (2024-06-15)

> [!NOTE]
> This release adds support for Math expressions, and notification boxes like the one this text is written in.

### New Features (BREAKING)

 - <csr-id-bff28af3c51a3a55675d5a64d8f3bda2d37701aa/> Update to pulldown-cmark v0.11 for math-expression support and special block quotes
   Update pulldown cmark

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 20 commits contributed to the release.
 - 5 days passed between releases.
 - 1 commit was understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - Release pulldown-cmark-to-cmark v15.0.0 ([`e6c8c3e`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/e6c8c3ec310f79c080fe466183baf6842e66608c))
    - Prepare changelog prior to release ([`d72bfd3`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/d72bfd376a12412e4c394fa16fc99f8b8fb0e14e))
    - Upgrade dev-dependencies ([`d8cbd37`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/d8cbd37355cacdf58625e08765cf4000914d1057))
    - Update to pulldown-cmark v0.11 for math-expression support and special block quotes ([`bff28af`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/bff28af3c51a3a55675d5a64d8f3bda2d37701aa))
    - Merge blockquote kind parsing ([`6d05a28`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/6d05a28029087187bbbbe085aa77689c6ed89a07))
    - Remove unused import ([`89d5dd5`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/89d5dd56d07fdc373ec901bc535862c5b2fac155))
    - Remove support for dollar surrounding backtick ([`d55ac5d`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/d55ac5dc61e2b5afc098f817737668acd8743700))
    - Generate snapshot ([`c8632c4`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/c8632c4c7c462e5dd6446b8883f527fc8e415ef7))
    - Add suggested fixture ([`f961f76`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/f961f7686b24838607e2508bf2ff65f12bed4225))
    - Use escaped comparison ([`74a5b59`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/74a5b5989c59c3d4dd1bff8f66b79be62bce0052))
    - Update math tests ([`e94e9e4`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/e94e9e4a259aa4747334f123c352a4812a14c42a))
    - Fix alternate math notation ([`8e09ddb`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/8e09ddb9d0c28873a5ae9c116a0a1f310179dc85))
    - Regenerate snapshots ([`153ff68`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/153ff68c4ac5e804211f41eb702fa00f7a16a311))
    - Add mathematical expressions ([`42049d9`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/42049d99af1bbfda53f5c50a59b56b0fae947b53))
    - Regenerate snapshots ([`ea04970`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/ea0497047b69fcd962568bce34b55e938d3f5db0))
    - Fix incorrect formatting ([`1d047be`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/1d047be50ca18ffd5a28ca64475d1100b6068cca))
    - Add blockquote kinds ([`dcf769c`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/dcf769c1d1f57d7e540e172988d18da84fa95492))
    - Update tests ([`8723b1f`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/8723b1ffcf2248d8fbad77c044d6eff96dda2fec))
    - Handle new types and change ([`c81d969`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/c81d969679b77dd0e9f9bf065190cfddc57f3dd3))
    - Update pulldown-cmark ([`173b8a4`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/173b8a40761d571772815f4227e3962a619bd7fd))
</details>

## 14.0.1 (2024-06-10)

This release contains a fix to avoid escaping characters within codeblocks. 
See https://github.com/Byron/pulldown-cmark-to-cmark/issues/73 for details.

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 4 commits contributed to the release.
 - 0 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 1 unique issue was worked on: [#73](https://github.com/Byron/pulldown-cmark-to-cmark/issues/73)

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **[#73](https://github.com/Byron/pulldown-cmark-to-cmark/issues/73)**
    - Fixdo not disturb code block detection when preventing escape ([`7376d42`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/7376d42ae3599c907061eba6b3a7676f79408bdb))
 * **Uncategorized**
    - Release pulldown-cmark-to-cmark v14.0.1 ([`fd7815a`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/fd7815a480181fa2f442ce8969111038927f49b6))
    - Prepare changelog prior to release. ([`857ff07`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/857ff07f8ae962de7d287008d875cc3ca342e510))
    - Merge pull request #74 from SichangHe/fix-unnecessary-range-escape ([`a6045c6`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/a6045c664fda1c56edf8d749601a6562d94a3871))
</details>

## 14.0.0 (2024-06-10)

This release adds support for keeping escapes in the original source for the input events,
assuming the new `cmark_resume_with_source_range_and_options()` function is used.

Please also note that for most this won't be a breaking change as only a field was added
to `State`, which isn't used unless you do incremental event serialization.

### Bug Fixes (BREAKING)

 - <csr-id-6c8297c03eb82ac95fa9dd559e3337e470d70c72/> clarify what `last_event_end_index` indices into and how it's used
   This is marked as breaking change as the field itself is new in `State`
   and could be breaking for some.

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 17 commits contributed to the release.
 - 1 commit was understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - Release pulldown-cmark-to-cmark v14.0.0 ([`1978c0c`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/1978c0c0fb42e20bc0d015611c1336975bd1965c))
    - Fix manifest includes to allow publishing ([`cfcabc4`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/cfcabc44bf5b73ab1567f062787be917d1933877))
    - Release pulldown-cmark-to-cmark v14.0.0 ([`8c6bee4`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/8c6bee4c037c3aff3c211c87146909ab0cd85aaf))
    - Fix doc-links ([`d9d81c0`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/d9d81c0bcac2471a26be35e21414f51b4a136b45))
    - Prepare changelog prior to release ([`c332f98`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/c332f98d21c2f35638e88145ee224d5511ddc2d4))
    - Merge pull request #71 from SichangHe/no-unnecessary-escape ([`386e663`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/386e6634421373c6b9841a59c7639e416215566c))
    - Clarify what `last_event_end_index` indices into and how it's used ([`6c8297c`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/6c8297c03eb82ac95fa9dd559e3337e470d70c72))
    - Dedup test code ([`f66724b`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/f66724b2f74814fe6a8ddb168425b9097bb84135))
    - Attempt to address @Bryon first comments https://github.com/Byron/pulldown-cmark-to-cmark/pull/71#pullrequestreview-2081814286 ([`bebff1e`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/bebff1e520f2da982cb1a7f9ba97ce13c9accab0))
    - Test `<` escape handling ([`542bc1f`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/542bc1fad3d06d39d59c8842225afc864d7539c3))
    - Restore `State` derives ([`ea17e27`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/ea17e2763d066bd454cfab0ae0a0768fdaf83bd1))
    - Rm test that fail for old implementation ([`78965c4`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/78965c4520e031930e232758d942077ed40b7a1d))
    - Better explain why `cmark_resume_with_source_range_and_options` is different ([`49f60d5`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/49f60d58ae2a1fd868797191c47d968f11c913df))
    - `source_range` persist special character escape by peeking source ([`92ad2c8`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/92ad2c8f291df7e165e1110d05c638661627c5d6))
    - Placeholder functions not using source range ([`a234ed5`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/a234ed5ece9d02eb8e9cb3c03432ca2a73165b5e))
    - Extract nested functionalities ([`93cbb33`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/93cbb33bad931ac3f787cd13438a812c6adb3b7a))
    - Failing rustdoc test <https://github.com/Byron/pulldown-cmark-to-cmark/issues/51> ([`8c6ccdb`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/8c6ccdbb01e015acc3a7e688bac2dad8319d03f8))
</details>

## 13.0.0 (2024-03-17)

<csr-id-5d940367d7d720a176c361b99ab8699897a53313/>

### New Features

 - <csr-id-8d39a30b20b621a0db6274ee6bb58755c5198d01/> add newlines_after_metadata option

### Bug Fixes

 - <csr-id-7b27baad538570400eeb651b2e9b2b786ad43189/> set correct yaml metablock end tag

### Test

 - <csr-id-5d940367d7d720a176c361b99ab8699897a53313/> add frontmatter snapshot

### New Features (BREAKING)

 - <csr-id-834c74b9df9b9451000b069988d8259e2b881692/> support for controlling the amount of newlines after metadata blocks.

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 6 commits contributed to the release.
 - 4 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - Release pulldown-cmark-to-cmark v13.0.0 ([`daeb618`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/daeb61841f171b1fdc1a51c6cc1a48adafa852ff))
    - Support for controlling the amount of newlines after metadata blocks. ([`834c74b`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/834c74b9df9b9451000b069988d8259e2b881692))
    - Refactor ([`b8ae359`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/b8ae35906a62e5e6d69b5412fff6a9a93918925e))
    - Add frontmatter snapshot ([`5d94036`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/5d940367d7d720a176c361b99ab8699897a53313))
    - Add newlines_after_metadata option ([`8d39a30`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/8d39a30b20b621a0db6274ee6bb58755c5198d01))
    - Set correct yaml metablock end tag ([`7b27baa`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/7b27baad538570400eeb651b2e9b2b786ad43189))
</details>

## 12.0.0 (2024-03-16)

<csr-id-b90defbf6ade8b3c1c0585f7933421f086eeb123/>

### Other

 - <csr-id-b90defbf6ade8b3c1c0585f7933421f086eeb123/> update pulldown-cmark dependency

### New Features (BREAKING)

 - <csr-id-640148b354eb7325e1fbeec2acbd4867d3c536fc/> upgrade to pulldown-cmark v0.10
   This comes with a variety of changes to the `State` type, which is the reason
   this is a breaking release.
   
   The overall correctness is improved though, bringing the amount of successful
   tests of the spec to 435/649 (up by 4).

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 18 commits contributed to the release.
 - 48 days passed between releases.
 - 2 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - Release pulldown-cmark-to-cmark v12.0.0 ([`002be03`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/002be03e5ec58fce34879e26118689b0e9269441))
    - Upgrade to pulldown-cmark v0.10 ([`640148b`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/640148b354eb7325e1fbeec2acbd4867d3c536fc))
    - Avoid allocations ([`366f304`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/366f304104ec27e96f185ead57658532ecaecaa7))
    - Allow State to borrow from events ([`b3bfd40`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/b3bfd404951c19314e110ee2764ea69b8cd4a9e2))
    - Remove State::last_was_html ([`42739b5`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/42739b5996a2490c61b792cfc32b980614892ae0))
    - Test custom attributes ([`8152f81`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/8152f815f8692feec1a5b194feb77028a39bc5dd))
    - Adjust snapshots ([`1c21b70`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/1c21b70685bd1e85a9c0ed2862d6f6fb2f97f142))
    - Remove trailing whitespace in snapshots (seems to be a pulldown-cmark parsing change) ([`b09ce73`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/b09ce73a997f4bbb0abe237bfc603980f532753d))
    - Fix handling of HTML blocks ([`6afb4cd`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/6afb4cdf66344b3701cdc30d393aec7d0356480a))
    - Clippy ([`4a66292`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/4a6629280441b14f85222f133efda26ae8ec46b0))
    - Fix EndTag tests ([`38bb5dd`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/38bb5dddc8b5603849c6ce006f96d4f93f83784c))
    - Comment out end tag tests that aren't triviably updateable ([`82110a4`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/82110a4cc62d2fac7fa5d12842807b9264e45deb))
    - Support heading attributes ([`fc30c6d`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/fc30c6dbc2457bf2c68ea70b66be80fd64016d5a))
    - Fix test ([`ea2608e`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/ea2608e88cabcbc9962b08360e624e176b780c19))
    - Fix easily-updateable tests ([`ebf9b69`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/ebf9b6914c9eccbc0efc9bfed3d26ccfb64ff3c5))
    - Add link/image stacks and track current header ([`3090b8a`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/3090b8a7c3026e2a5fa3b2fbaf4fe9753ec662a9))
    - Metadata blocks ([`a26731c`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/a26731caaaa89960528b8c72d6c051c7c30d4660))
    - Update pulldown-cmark dependency ([`b90defb`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/b90defbf6ade8b3c1c0585f7933421f086eeb123))
</details>

## 11.2.0 (2024-01-27)

### New Features

 - <csr-id-e33936543e4db169fb7734cc0ce83127a0c04e8b/> introduce `calculate_code_block_token_count()`, along with `DEFAULT_CODE_BLOCK_TOKEN_COUNT`.
   That way it's possible to correctly determine the amount of fenced code-block tokens
   needed to render all given events such that code-blocks are retained.

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 4 commits contributed to the release.
 - 1 day passed between releases.
 - 1 commit was understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - Release pulldown-cmark-to-cmark v11.2.0 ([`11344a7`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/11344a70243c5affbc0861669db9575209a6f612))
    - Merge branch 'fix_count_code_block_tokens' ([`ee42335`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/ee4233527dd46b963d5a553c18bcc7b876f75fa1))
    - Introduce `calculate_code_block_token_count()`, along with `DEFAULT_CODE_BLOCK_TOKEN_COUNT`. ([`e339365`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/e33936543e4db169fb7734cc0ce83127a0c04e8b))
    - Change the return type of count_code_block_tokens ([`ef1c276`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/ef1c2766c1f91ba226063e4f914646f0d32172da))
</details>

## 11.1.0 (2024-01-25)

### New Features

 - <csr-id-10b855fc6433ba1aa70add57be261d8be3fc2249/> add `count_code_block_tokens()` function.
   It allows to calculate the amount of backticks around a code block for given events
   to assure code-blocks are reproduced correctly when rendering the same events.

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 6 commits contributed to the release.
 - 61 days passed between releases.
 - 1 commit was understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - Release pulldown-cmark-to-cmark v11.1.0 ([`d236e29`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/d236e29395304f40c988813b4bf7b76e8dd2bfbe))
    - Add `count_code_block_tokens()` function. ([`10b855f`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/10b855fc6433ba1aa70add57be261d8be3fc2249))
    - Refactor ([`c0f631d`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/c0f631d68a9373bf662302296044d90eaac4c8b6))
    - Add check_code_block_token_count function ([`f119d77`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/f119d7735fa1ed762fedf4a614a76bd0c7fbb33f))
    - Merge pull request #64 from max-heller/readme-links ([`b926264`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/b9262648473b2ec8c0aaf5fc2f23c41ccdfcc58a))
    - Update pulldown-cmark and mdbook links in README ([`31e8bb5`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/31e8bb56781633b25dbe685d622f209afaa097ec))
</details>

## 11.0.2 (2023-11-24)

### Bug Fixes

 - <csr-id-9a64f6bc8fafe22bfd4d3fc6704a097719a10566/> Escape double quotes in link titles.

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 5 commits contributed to the release.
 - 4 days passed between releases.
 - 1 commit was understood as [conventional](https://www.conventionalcommits.org).
 - 1 unique issue was worked on: [#62](https://github.com/Byron/pulldown-cmark-to-cmark/issues/62)

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **[#62](https://github.com/Byron/pulldown-cmark-to-cmark/issues/62)**
    - Escape double quotes in link titles. ([`9a64f6b`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/9a64f6bc8fafe22bfd4d3fc6704a097719a10566))
 * **Uncategorized**
    - Release pulldown-cmark-to-cmark v11.0.2 ([`befdaab`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/befdaabc5c024d6d22332cf6e6e65baaa3b92a72))
    - Address code review comments ([`4767b2f`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/4767b2fe2dea1c4f0a563d3e71cdb54bd8efc8bc))
    - Escape double quotes in link titles ([`74f0abe`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/74f0abee0dcfde1148af57e2acc0451b12ce1308))
    - Failing test for link titles with interior punctuation ([`e891ea4`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/e891ea423ee7e1425dd4866e0d6e0fd74554a9e6))
</details>

## 11.0.1 (2023-11-19)

### Fix 

 - properly format multi-line footnotes. Merge branch 'multiline-footnotes' ([`f6b5348`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/f6b53488417f65a30bf833c165c9112a37b393b7))

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 5 commits contributed to the release.
 - 0 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 1 unique issue was worked on: [#61](https://github.com/Byron/pulldown-cmark-to-cmark/issues/61)

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **[#61](https://github.com/Byron/pulldown-cmark-to-cmark/issues/61)**
    - Fix: properly format multi-line footnotes. Merge branch 'multiline-footnotes' ([`f6b5348`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/f6b53488417f65a30bf833c165c9112a37b393b7))
 * **Uncategorized**
    - Release pulldown-cmark-to-cmark v11.0.1 ([`b1f77e0`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/b1f77e0526b59a04234c0874cf00493720da550a))
    - Prepare changelog ([`323cbc0`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/323cbc077267bb7ef370687d5c9d589758853e32))
    - Multiline footnote fix ([`4768ad5`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/4768ad54fee48046974dc7b76d3768b62161e959))
    - Failing multiline footnote test ([`e187313`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/e18731327065297e4d2ac9d8e24bb6b6db5bbaf0))
</details>

## 11.0.0 (2023-08-18)

### New Features

 - <csr-id-d9eb45477fc23da0960cdb24f9f20d67bfac0784/> add apache-2 license text to crate as well

### Bug Fixes

 - <csr-id-c2a0113cc649af32288c916026c47e63fbccb6ec/> improve handling of inline code spans with multiple backticks
   Merge pull request #56 from mgeisler/inline-code

### New Features (BREAKING)

 - <csr-id-2263c2f33d9fb546881c0837f391bf266528f8d2/> Options to choose the ordered list marker and to auto-increment the ordered list numbers.
   - Based on the commonmark list spec an ordered list can use `.` or `)`
   as the ordered list marker. Now users can configure the marker they want
   to use with the `ordered_list_token` option.

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 14 commits contributed to the release.
 - 3 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Thanks Clippy

<csr-read-only-do-not-edit/>

[Clippy](https://github.com/rust-lang/rust-clippy) helped 2 times to make code idiomatic. 

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - Release pulldown-cmark-to-cmark v11.0.0 ([`6f23621`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/6f23621a4f9ab79c7eeb10f7f2c201dccef8dffd))
    - Merge branch 'lic' ([`b889953`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/b889953135053db9763458cb471d5b7bd55c3272))
    - Add apache-2 license text to crate as well ([`d9eb454`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/d9eb45477fc23da0960cdb24f9f20d67bfac0784))
    - Add license text ([`9ea32f4`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/9ea32f403d250ee5f3dc68373b3e37c8f690436b))
    - Improve handling of inline code spans with multiple backticks ([`c2a0113`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/c2a0113cc649af32288c916026c47e63fbccb6ec))
    - Refactor ([`1cd68a4`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/1cd68a41be3079b55179507ca3ececa385b59698))
    - Thanks clippy ([`8f81a30`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/8f81a30c659f21a14d814a6a4505699ac4eda757))
    - Use necessary amount of backticks for inline code ([`3437734`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/343773455fb08f20817f6de74b0bf8f067a90ba1))
    - Always use backticks for inline code blocks ([`1e76c95`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/1e76c955ebd59190a3198ff5a62cd840023486d1))
    - Options to choose the ordered list marker and to auto-increment the ordered list numbers. ([`2263c2f`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/2263c2f33d9fb546881c0837f391bf266528f8d2))
    - Thanks clippy ([`6bde374`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/6bde37483b7578ef554952775e9e0c3332ecf4aa))
    - Correct indentation for a nested list in a doc comment ([`ce1fb72`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/ce1fb72ab7d9db2647e142916e8af0bccc0ec97e))
    - Add option to increment ordered list number ([`f2fae4b`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/f2fae4b5c64d44d624cb2dff16a70b31306e60ce))
    - Allow users to specify a custom ordered list marker ([`981c86a`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/981c86adc847eb8df0c1616b0e7377536f7f59e2))
</details>

## 10.0.4 (2022-09-10)

### Bug Fixes

 - <csr-id-f3b43705ecf5c3760f964680c2975b14cf92a990/> add newlines where needed to produce valid codeblocks

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 6 commits contributed to the release.
 - 1 day passed between releases.
 - 1 commit was understood as [conventional](https://www.conventionalcommits.org).
 - 1 unique issue was worked on: [#48](https://github.com/Byron/pulldown-cmark-to-cmark/issues/48)

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **[#48](https://github.com/Byron/pulldown-cmark-to-cmark/issues/48)**
    - Add newlines where needed to produce valid codeblocks ([`f3b4370`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/f3b43705ecf5c3760f964680c2975b14cf92a990))
 * **Uncategorized**
    - Release pulldown-cmark-to-cmark v10.0.4 ([`5f6ec45`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/5f6ec453d5c44d45d09100749d1c1e2891e05bf7))
    - Cargo fmt ([`89b557a`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/89b557a46786f2efebccec010f8c115ad21910e3))
    - Fix typo ([`1833cfb`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/1833cfb3f45591cc866851ebd0ca22d4cf6b4819))
    - Inserts a newline if there is no newline before code block termination fence ([`ef4c401`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/ef4c4014b4bc2d6e26240a99a9afd49e60b320f3))
    - Add a test case to verify that indented code blocks without trailing `\n` are rendered correctly ([`6c6f492`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/6c6f4926df13496bf307f7059af6342647e889d3))
</details>

## 10.0.3 (2022-09-09)

<csr-id-e214cc1be5305f9b43738affff4d1ca22242af37/>

### Bug Fixes

 - <csr-id-ff4050f4981f300d79b4d3214bdfe6c3c99ef205/> duplicated shortcut link definitions are only printed once.

### Other

 - <csr-id-e214cc1be5305f9b43738affff4d1ca22242af37/> Replace `.travis.yml` with `.github/workflows/rust.yml`
   We have already migrated from Travis CI to GitHub Actions.

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 5 commits contributed to the release.
 - 73 days passed between releases.
 - 2 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 1 unique issue was worked on: [#46](https://github.com/Byron/pulldown-cmark-to-cmark/issues/46)

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **[#46](https://github.com/Byron/pulldown-cmark-to-cmark/issues/46)**
    - Duplicated shortcut link definitions are only printed once. ([`ff4050f`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/ff4050f4981f300d79b4d3214bdfe6c3c99ef205))
 * **Uncategorized**
    - Release pulldown-cmark-to-cmark v10.0.3 ([`4e7c91b`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/4e7c91bb7d032921d56330663c3f4a9f6027e722))
    - De-duplicate shortcut link definitions ([`32d9466`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/32d94668ec4c294f5161ccc8cdba94ada78713d4))
    - Add a test case to verify that repeated reference links are rendered correctly ([`eb6ab63`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/eb6ab631b1dac766012625db44384eeb3df74f70))
    - Replace `.travis.yml` with `.github/workflows/rust.yml` ([`e214cc1`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/e214cc1be5305f9b43738affff4d1ca22242af37))
</details>

## 10.0.2 (2022-06-28)

### Fixes

- support for backticks within inline-backticks, see this [this
  comment](https://github.com/Byron/pulldown-cmark-to-cmark/issues/20#issuecomment-1165798752)
  for details.

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 4 commits contributed to the release.
 - 108 days passed between releases.
 - 0 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - Release pulldown-cmark-to-cmark v10.0.2 ([`341f46e`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/341f46e63d7a4276cda013251bf43b18b493494f))
    - Update changelog ([`0fa8688`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/0fa8688e422cd18839d06d400660df9a457ed34c))
    - Add space aroudn backticks ([`6f68331`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/6f683310f2e6965c6d1d3ee09c3b36eefff5c1a3))
    - Fix double-backtick issue ([`09a35a9`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/09a35a96002cb2ca00e0769c0ae4cc8927698ccd))
</details>

## 10.0.1 (2022-03-12)

### Fixes

- add `cmark_resume()`, completing the API transition started with the major version change.

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 6 commits contributed to the release.
 - 42 days passed between releases.
 - 0 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - Release pulldown-cmark-to-cmark v10.0.1 ([`d2d1e6e`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/d2d1e6ee90543aba85d83a7a08f1adda6c8ba0da))
    - Update changelog ([`eb88b2e`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/eb88b2e2f3c4de46bff0c55418f3359eb62bf747))
    - Add clippy to Makefile/CI ([`7bb9a68`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/7bb9a68a98bcec55879379dcda669593129a006a))
    - Fix misc Clippy lints ([`118df17`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/118df1747be21cd4b323bf76d22c6736117af0f9))
    - Re-order to logically group cmark* functions ([`16f06eb`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/16f06eb10bfc084c7e79e6d6e76703452517ae0f))
    - Add cmark_with_options() and fix comments ([`53efeb8`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/53efeb83c158978331303d6889a24f6192e40dea))
</details>

## 10.0.0 (2022-01-28)

Note that the breaking change is fixable by removing the last parameter from the `cmark()` function if it was `None`, 
or using `cmark_resume` instead.

### New Features

- Formatting within links is no being preserved.

### New Features (BREAKING)

 - <csr-id-7166abeb0f89ee1dfb7790923d0d5fc6edc394c5/> Simplify `cmark(…)` by removing `State` parameter, introduce `cmark_resume(…)`.
   
   The reason for doing it this way around causing a breaking change is
   the change of behaviour for `cmark_resume`, which now returns a state
   instance that needs a `finalize()` call to flush certain caches.
   
   Currently this is only used to write link blocks, after which the state
   is still usable for future invocations if necessary.
   
   That way the caller has control over where to put link blocks, or other
   items that should be placed at the end of a logical section.

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 6 commits contributed to the release over the course of 28 calendar days.
 - 28 days passed between releases.
 - 1 commit was understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - Release pulldown-cmark-to-cmark v10.0.0 ([`44c5286`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/44c52860ab7300f47b60ae72f9760f302c9d5758))
    - Write changelog prior to release ([`e9f809f`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/e9f809f45f637a385d8b4db248409195df1c9380))
    - Merge pull request #41 from aogier/bugfix/40-shortcut_code ([`7ee11da`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/7ee11da6906bbc59c2bd91413ba617bf8104685e))
    - Simplify `cmark(…)` by removing `State` parameter, introduce `cmark_resume(…)`. ([`7166abe`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/7166abeb0f89ee1dfb7790923d0d5fc6edc394c5))
    - Properly render shortcut links w/ code formatting ([`6a42312`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/6a423123f409612ba7798f6fcbd91373562e6aa7))
    - Fix changelog ([`84acaf3`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/84acaf3c4f0c77f774aa38f96ccee661d6bfe5d8))
</details>

## 9.0.0 (2021-12-31)

<csr-id-93f2d11e467221418748c672a3c493dcfbf9d6e7/>

### Bug Fixes

 - <csr-id-0a6f80d67eed9234170c95d8282cbc76ae66d47c/> Shortcut handling is now using the `State`.
   
   This makes resumable event processing work as expected, even though
   the output won't be similar as it will print shortcuts right after
   all events have been processed and it's impossible to know
   how often it will be called.
 - Simplify `State`.
  
   Use an `Option` to get rid ofa  boolean and make isuse impossible.

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 6 commits contributed to the release.
 - 5 days passed between releases.
 - 2 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 1 unique issue was worked on: [#39](https://github.com/Byron/pulldown-cmark-to-cmark/issues/39)

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **[#39](https://github.com/Byron/pulldown-cmark-to-cmark/issues/39)**
    - Shortcut handling is now using the `State` ([`0a6f80d`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/0a6f80d67eed9234170c95d8282cbc76ae66d47c))
 * **Uncategorized**
    - Release pulldown-cmark-to-cmark v9.0.0 ([`c14d084`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/c14d0840bcef5fe1e5f9bb5df96a12e29e0ca81b))
    - Write changelog ([`ddff119`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/ddff11953a25c9c77bd741371328a810ca678bfb))
    - Simplify `State` ([`93f2d11`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/93f2d11e467221418748c672a3c493dcfbf9d6e7))
    - Refactor ([`c0f14c0`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/c0f14c058f3639e37ef9cd7971d9fb3965320fe7))
    - Refactor ([`9283d48`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/9283d487546bdcef57b602a782c777462d7caa19))
</details>

## 8.0.0 (2021-12-26)

This release adds support for `pulldown-cmark v0.9`, [see the tracking issue](https://github.com/Byron/pulldown-cmark-to-cmark/issues/37) for more information.

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 4 commits contributed to the release.
 - 20 days passed between releases.
 - 0 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - Release pulldown-cmark-to-cmark v8.0.0 ([`cf469e2`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/cf469e28f829864350d9838424370e85ac30329c))
    - Adjust changelog prior to release ([`0ed0c69`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/0ed0c6960de20934ceebc11f8d54719c90516a45))
    - Cargo fmt ([`c696de3`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/c696de36d0d4a1dc6cfd0c86704844366009697a))
    - Support the new Heading tag in pulldown-cmark 0.9 ([`1c9ea44`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/1c9ea447a3a43a19a93382fc8d3304556b79641e))
</details>

## 7.1.1 (2021-12-05)

### Fixes

- Multiple shortcut links are now separated by newline.

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 3 commits contributed to the release.
 - 10 days passed between releases.
 - 0 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - Release pulldown-cmark-to-cmark v7.1.1 ([`e95115e`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/e95115e066a3a8bdbc1a5ca2cc422a03ba9c4f9b))
    - Update changelog prior to patch release ([`e540024`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/e540024bec698b7ebd28e76c1673f0b90c553f36))
    - Fix shortcuts rendering ([`3bbc763`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/3bbc7638fa3210efa09e78d7ac423710fe9ea68e))
</details>

## 7.1.0 (2021-11-25)

### New Features

* Shortcut links are now printed at the end of the stream like one would expect ([#33](https://github.com/Byron/pulldown-cmark-to-cmark/pull/33))
* email links are retained ([#34](https://github.com/Byron/pulldown-cmark-to-cmark/pull/34))
* Now 422 of 649 common mark spec tests pass, up from 402.

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 6 commits contributed to the release.
 - 7 days passed between releases.
 - 0 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - Release pulldown-cmark-to-cmark v7.1.0 ([`68a9b6a`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/68a9b6a1cd58e60b31f989c8c5b25654f923dbeb))
    - Update changelog ([`bbc22fe`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/bbc22fee8fbde87c23647704d253656585e97eeb))
    - Use match as expression ([`2d5d4e1`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/2d5d4e1e78c36a8188a79cc5aac944d4ee2f291b))
    - Merge branch 'feature/32-shortcut-links' of https://github.com/aogier/pulldown-cmark-to-cmark into aogier-feature/32-shortcut-links ([`81b1eb4`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/81b1eb46509047430dde0efa0874ec4fdecf82af))
    - Retain autolink/email link format ([`e25f14d`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/e25f14df072ac4f700d097136069331e61fb781e))
    - Shortcut links implementation ([`25cc3d1`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/25cc3d1afd6d6d675d6d29ebcdd40c0d22d992fe))
</details>

## 7.0.0 (2021-11-18)

The `Options` type was updated in a **breaking** fashion to allow configuring certain tokens, e.g. the `*` list token can now be configured as `-` if desired.

Thanks to the author of [this PR](https://github.com/Byron/pulldown-cmark-to-cmark/pull/31).

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 7 commits contributed to the release over the course of 53 calendar days.
 - 53 days passed between releases.
 - 0 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - Release pulldown-cmark-to-cmark v7.0.0 ([`cc82b0f`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/cc82b0fdcc020aece451f0feeee1d941ce79097e))
    - Update changelog prior to release ([`0b795a7`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/0b795a70379f334c0dc08a8208691ee824c2ee87))
    - Refactor ([`06fe6cb`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/06fe6cb1e35e8aca0cd8a4bfe9daacbdd2771698))
    - More efficient handling of reconfigured characters ([`fa14750`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/fa147502384bf82fe192719d0c8cc81ba125cc7f))
    - Cleanup typing for special characters ([`9aef43f`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/9aef43ff21d41bc66df6096d392d0e8a035d11fc))
    - Change options to support custom syntax and add test ([`db6e12c`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/db6e12c57753cfeccb010ecdb3104e3d0bc48b41))
    - Format with nightly ([`9313bac`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/9313bac4d0946a2e8bd0c8ca68d5fd3573891e45))
</details>

## v6.0.4 (2021-09-25)

* Improves newline handling when line ends with 
  [inline HTML](https://github.com/Byron/pulldown-cmark-to-cmark/issues/28)

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 3 commits contributed to the release.
 - 17 days passed between releases.
 - 0 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - Release pulldown-cmark-to-cmark v6.0.4 ([`72e5ca5`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/72e5ca549d0acb9bd59f1d8b1c1c8f55eac604ef))
    - Prepare changelog ([`50b4188`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/50b4188dafcf1b61b7103861b733e7b886fc8da9))
    - Don't output extra newlines after HTML before a SoftBreak ([`1a40792`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/1a407920006e554a4533c2ed9de1fe3bae57f146))
</details>

## v6.0.3 (2021-09-07)

* handle spaces in links [better](https://github.com/Byron/pulldown-cmark-to-cmark/pull/27)

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 3 commits contributed to the release.
 - 75 days passed between releases.
 - 0 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - Release pulldown-cmark-to-cmark v6.0.3 ([`100c41a`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/100c41a1106032ee84c8fcdbcf788cd4426573a5))
    - Prepare release ([`b3015aa`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/b3015aa080aab57b2f15920bce1cc2fd8eb0e1a9))
    - Handle spaces in link URIs ([`9f8f17b`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/9f8f17ba67b6f2c6d0ed1eb849f5cadc7c6fcef0))
</details>

## v6.0.2 (2021-06-23)

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 4 commits contributed to the release.
 - 19 days passed between releases.
 - 0 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - (cargo-release) version 6.0.2 ([`c1cd8ce`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/c1cd8ce70ce1f7cda2f1f55afb60ea92676b223b))
    - Prepare patch release ([`1872953`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/187295308f3632cf31b73c31ce54596320625892))
    - Adjust test expecations - it's probably more correct now ([`6bc1f11`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/6bc1f11eeb311183ff3537f60d324177a6d10bfa))
    - Don't add a newline after an HTML block if we're just ending another tag ([`89cc1ec`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/89cc1ec8d4f41d3b251ed08fe5127a8b81463ef0))
</details>

## v6.0.1 (2021-06-04)

* handline [html in tables correctly](https://github.com/Byron/pulldown-cmark-to-cmark/pull/26).

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 14 commits contributed to the release over the course of 133 calendar days.
 - 233 days passed between releases.
 - 0 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Thanks Clippy

<csr-read-only-do-not-edit/>

[Clippy](https://github.com/rust-lang/rust-clippy) helped 1 time to make code idiomatic. 

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - (cargo-release) version 6.0.1 ([`7782189`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/77821898001b0a89e030464719256b943218b810))
    - Adjust test expectations - it's OK to degenerate information I suppose ([`f733496`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/f733496f82a67faae140cc8a1c090e4eec4f87b8))
    - Prepare release ([`874edba`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/874edba1715ca045c5ae6e97a656dc3fa0256137))
    - Also escape closing brackets (oversight); fixes #25 ([`7fdf0da`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/7fdf0dac696e609be6cbb6c0cd78c0f9c4f24b64))
    - Merge remote-tracking branch 'origin/main' into crlf0710/main ([`934cfab`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/934cfab09843e306a336d5324fc5c106fd9c0ce4))
    - Allow the commonmark-spec test to fail right now… ([`65f5949`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/65f59491389db1ac5e820412455c810ecda019f9))
    - Re-enable test-suite :D ([`c2a2de3`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/c2a2de3825dda8d899ef2e5795f63f81ba0fb120))
    - Add a test that verify current implementation against all the examples from CommonMark spec. ([`f27acce`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/f27acced939680713f613b90aa878351f15eb8a0))
    - Dependency update ([`59db24f`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/59db24fb51c2e0a441a083b0129cfae9cce1f48f))
    - Use `make` in CI ([`4b53d20`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/4b53d20a79ee83b09e768b35654c850d502f3651))
    - Use modern doc string type links ([`cac031a`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/cac031a3b4acd3a179c56e2537217c3c4c27a16c))
    - Thanks clippy ([`16e66da`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/16e66da8308f66e38d9821f51c09b1cde1d5802b))
    - Cargo fmt ([`9d2dc90`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/9d2dc9082088845a9eecdae9ac7b1c4a4a096f78))
    - Add makefile for convenience ([`45bfb5e`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/45bfb5ec52d8ce407be96885053c61bd1dda8549))
</details>

## v6.0.1 (2021-06-04)

* also escape ']' characters

## v6.0.0 (2020-10-14)

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 2 commits contributed to the release over the course of 42 calendar days.
 - 72 days passed between releases.
 - 0 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - Bump pulldown-cmark ([`3ae8ddc`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/3ae8ddc6d04806adc68bf7474b18afba11c06d06))
    - Update dependencies ([`ba51fda`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/ba51fdae86390a2db7324a2d469d79de541ffdfa))
</details>

## v5.0.0 (2020-08-02)

* Allow configuring the [amount of backticks used in code blocks](https://github.com/Byron/pulldown-cmark-to-cmark/pull/18). 
  May **break** code relying on the amount of fields in the configuration struct.

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 3 commits contributed to the release.
 - 58 days passed between releases.
 - 0 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - (cargo-release) version 5.0.0 ([`a5f644a`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/a5f644a904bc2e343dd2d8e88235eba0a4ab6345))
    - Update changelog ([`5f7be5e`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/5f7be5e541491091dae331123296a1acd95d996c))
    - Make the number of backticks in codeblocks configurable ([`c9267b5`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/c9267b55bebaff48f27e04d64da239607b7fdb4d))
</details>

## v4.0.2 (2020-06-05)

* Fixed table header handling ([see PR for details](https://github.com/Byron/pulldown-cmark-to-cmark/pull/15))

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 4 commits contributed to the release over the course of 9 calendar days.
 - 20 days passed between releases.
 - 0 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - Bump patch level ([`1240a59`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/1240a5925cc6dad0201348f4b4f157f7c77e9807))
    - Avoid integer underflow ([`8774c43`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/8774c4397676673db370ac1cafdbacfb81bdf02c))
    - Capture inline-code as table header ([`025d8c2`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/025d8c297f845c3b89296a1e962e04aa18503168))
    - Run `cargo diet` ([`8618b3d`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/8618b3dcc1380efd6215a1bd53e3e3817d34b1df))
</details>

## v4.0.1 (2020-05-15)

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 2 commits contributed to the release.
 - 23 days passed between releases.
 - 0 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - Bump patch level ([`03f4ab8`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/03f4ab8cde0172fa21d0419eec54fd72294df64d))
    - Add a newline before code blocks if necessary ([`061f7b8`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/061f7b84bea240f3fa2ba53f7793c21fdf3ab274))
</details>

## v4.0.0 (2020-04-22)

* BREAKING: Move all types from `pulldown_cmark_to_cmark::fmt::*` into `pulldown_cmark_to_cmark::*` for simplicity. 
  For most common use-cases, this means that users of `pulldown_cmark_to_cmark::fmt::cmark` now use `pulldown_cmark_to_cmark::cmark` instead.

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 1 commit contributed to the release.
 - 0 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - Simplify library layout: move fmt::* into crate root ([`7070f33`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/7070f334ca796d85c3ea9bb164c42939daad6314))
</details>

## v3.0.1 (2020-04-22)

* support for markdown embedded in HTML tags, like

  ```markdown
  <article>
    
    * list
    * list

    **bold**

  </article>
  ```

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 10 commits contributed to the release over the course of 15 calendar days.
 - 81 days passed between releases.
 - 0 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - Bump patch level; add changelog ([`b53fbe5`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/b53fbe529d68146e6a0185ffea868d0efb90b02a))
    - Use Rust Edition 2018 QoL improvements ([`0b414d0`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/0b414d08b65ac1a7ccdf16994beb5d02a10b6044))
    - Move last_was_html into state definition (allowing resumes) ([`59c5a3b`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/59c5a3b8466c7967f1aa04d2c1fd9e42b366017d))
    - Add newlines after HTML elemets if followed by more markdown ([`fcd32d0`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/fcd32d02c3a87c5d07d66efe69d2607b874d87bc))
    - Bye bye travis, we had a good time ([`2ea28f7`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/2ea28f76ce02b3a1331e243e7147a00dcd210595))
    - Add github actions ([`6b3f51c`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/6b3f51c8be99ab10f80cef12348dc8d8635bdc06))
    - Update minor version of pulldown-cmark ([`3e856c9`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/3e856c997530d4f136e097aeb0f5f19d23b7c512))
    - Update tests to use new code block kind ([`5b1c7c6`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/5b1c7c6cabf79d193b73b5a6d16218c59c94e727))
    - Expose pulldown_cmark dependency to users ([`0174671`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/01746716d7256c9e2251abd5f10cb25d8ab3034e))
    - Upgrade cmark dependency and handle new code block ([`5786f7e`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/5786f7e491770f5b050c62394179b7a648dd1271))
</details>

## v2.0.1 (2020-01-31)

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 2 commits contributed to the release.
 - 0 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - Bump patch ([`63ac5ca`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/63ac5ca0446734373c2d88d4e7df8417dcc485b2))
    - Dumber, byte oriented escape code ([`b3dce1a`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/b3dce1a9cc1650f0c354efa35d154231089cb5df))
</details>

## v2.0.0 (2020-01-31)

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 8 commits contributed to the release.
 - 60 days passed between releases.
 - 0 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - Upgrade to edition 2018 ([`c6fb144`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/c6fb144d64db6ff1229c0d2d98f789abdfac57b3))
    - Optimize release ([`17e9f4e`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/17e9f4ed5ec5ae168a41ea6c29ec4b5ccf51369e))
    - Add real-world tests, related to #8 ([`8d7378f`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/8d7378f4d233c81ad3c0a43f8d817c45dbc9561d))
    - Add support for escapes, fixes #8 ([`91df9a2`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/91df9a24c160d32b2cc97fbebb9533fef1e0cc35))
    - Run cargo fmt ([`d68f033`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/d68f0335136d1a38f15bbcff2cc31e4bc382a81f))
    - Bump our version to 2.0 ([`d64853c`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/d64853c519e319b4c74164f3611aa6b54e23bae0))
    - Fix inline-html output ([`63f4e4f`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/63f4e4f215ad238ae1475f9a6f02137d7dd53888))
    - Update to pulldown-cmark 0.6.1 ([`4900624`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/49006242485932e9513b981678c6b9c7a8a3d9b2))
</details>

## v1.2.4 (2019-12-02)

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 5 commits contributed to the release.
 - 0 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - Bump version to 1.2.4 ([`ebd8e8e`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/ebd8e8e7c6b8bfe39f1435876ed7131dc3042ee6))
    - Merge pull request #7 from dylanowen/blockquotes ([`fee9004`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/fee90046b84216ede421e8cd467b2a1390ba265e))
    - Updated authors ([`58e5137`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/58e51373d1d517fc5e3f71ab1540411d17c87d2d))
    - Better support for blockquotes ([`6f0e4b5`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/6f0e4b547348da8e9d1f450ccb7183a7f4334287))
    - Added blockquote test cases ([`2dd8024`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/2dd8024ba73860efcc41f637696dd0b8fdf1c42f))
</details>

## v1.2.3 (2019-10-28)

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 3 commits contributed to the release.
 - 0 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - Bump version ([`6f2382e`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/6f2382ef118afcbebe558034f63a5825870cf60b))
    - Better approximation of contained characters ([`83fdbc0`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/83fdbc084d03a85108b13be83cf236f38e9b167e))
    - Assure to never have truly empty header fields ([`00442f9`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/00442f9df50059c9c2560086dc3c082c763718d4))
</details>

## v1.2.2 (2019-07-03)

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 2 commits contributed to the release.
 - 0 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - Minor version bump ([`83c6edc`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/83c6edc8f63c1780985c3c1b80a6e24260da6d58))
    - Upgrade to pulldown-cmark 0.5.2; with clear regressions ([`c8e2b9b`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/c8e2b9ba0696aea9acf4165d283a4effd64ac6e7))
</details>

## v1.2.1 (2019-07-03)

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 6 commits contributed to the release.
 - 0 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - Upgrade to pulldown-cmark 0.4 ([`27909e0`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/27909e0318095a03b55dab2c81a44601a1f46981))
    - Merge pull request #3 from integer32llc/pulldown-cmark-0.4 ([`a92f729`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/a92f729f872f60fe672c06df0c10923052dc4f5d))
    - Add a test that generated markdown parses equivalently ([`90f134e`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/90f134ee042614421da3cafc97bee264cbdda2b3))
    - Update table snapshot to remove whitespace in table cells ([`148c789`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/148c7892011c6ce2d11f7da43d7a19280b9a137f))
    - Remove trailing spaces from the table test fixture ([`c6490bc`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/c6490bc346010af103928569cb67bb05298a1468))
    - Upgrade to pulldown-cmark 0.4.1 ([`9a87b4e`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/9a87b4e328766b8754b686a8344f7155aae37c8d))
</details>

## v1.2.0 (2018-12-20)

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 4 commits contributed to the release over the course of 299 calendar days.
 - 309 days passed between releases.
 - 0 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - Bump version after supporting the latest pulldown-cmark 0.2 ([`108c355`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/108c355c252d4b2dd420812ee63cd6f002a66c95))
    - Merge pull request #1 from maralorn/master ([`7a4f8a5`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/7a4f8a5e72def83496ec695a8c6fa16602954b51))
    - Bump dependency versions ([`5c7ddd5`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/5c7ddd5321cc39327d94ea6f9f64c4f6bd750fd5))
    - Add crates badge ([`b3ec0d9`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/b3ec0d990d2ba83a7fdcf73acd00d0882a5164d4))
</details>

## v1.1.0 (2018-02-13)

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 5 commits contributed to the release over the course of 1 calendar day.
 - 2 days passed between releases.
 - 0 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - Bump minor ([`ba3d313`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/ba3d3133105eb5ca98dc808b224d1ad428fedb95))
    - Support for codeblocks in codeblocks ([`a594c33`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/a594c33ec3dd5e399a0fff99faad246eeebdc9a0))
    - Add example for codeblock in codeblock ([`fa9d980`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/fa9d980cfab8484cff0b23b3cc4043702a956f23))
    - Some more infos for the README ([`9705eb8`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/9705eb866be7003a6dff48fb2e7b751e1e27c583))
    - Add depndency info badge ([`1869129`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/18691296d5fc12e4f05b7a42bfec5ba56d633138))
</details>

## v1.0.0 (2018-02-11)

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 6 commits contributed to the release.
 - 0 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - Add tests-title ([`34598b8`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/34598b8e11b00248a15e234fc981ac71566488cb))
    - Add project affiliations ([`55a5563`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/55a5563cf140269f90159196b2ba9b0f0f692bc5))
    - First bunch of API docs ([`f315ab7`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/f315ab75743e40607acc1010514cad4d8b1eabc3))
    - Update all links ([`b1e0978`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/b1e0978fa9f0c5b57a756fd8567eba436daface2))
    - First minor adjustment before all links change. ([`5b4c1ef`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/5b4c1ef9617acc3151a862ed1e536809846f0ed2))
    - Move everything from 'termbook'. ([`7666772`](https://github.com/Byron/pulldown-cmark-to-cmark/commit/76667725b61be24890fbdfed5e7ecdb4c1ad1dc8))
</details>

