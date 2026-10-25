# Contributing to Temporal SDKs

Thanks for your interest in contributing to Temporal SDKs.

This guide describes expectations that apply across Temporal SDK repositories. Each
repository may have additional local conventions, but the guidance below should help
you open issues and pull requests that maintainers can evaluate efficiently.

## Before You Open an Issue

Search the existing issues first. If you find an issue that describes the same bug,
feature request, or design topic, add any relevant details there instead of opening a
duplicate. Use an upvote on the issue to show that it affects you too.

Issues are assigned to people when they are actively working on them. Before taking
on an issue, check whether it is already assigned so you do not duplicate someone
else's work.

Use GitHub issues for actionable bugs and feature work. For usage questions, help
debugging an application, or general discussion, join the relevant
language-specific channel in the
[Temporal community Slack](https://temporal.io/slack) or use the support channel
available to you.

## Bug Reports

When reporting a bug, include enough detail for someone else to reproduce or
understand the problem:

* A short summary of the problem.
* A minimal reproduction, preferably as code that can be copied into a small
  project or test.
* What you expected to happen and what actually happened.
* The SDK version.
* The language runtime version.
* The operating system and architecture.
* Temporal Server or Temporal Cloud details, if the issue depends on service
  behavior.
* Logs, stack traces, workflow histories, or other diagnostics that show the
  failure.
* Whether the behavior is a regression, and the last version where it worked if
  known.

## Feature Requests and Design Changes

Open or join a GitHub issue before starting substantial feature work, behavior
changes, or API design changes. This gives maintainers and other SDK users a chance
to discuss the approach before you invest in a larger implementation.

The relevant language-specific channel in Temporal community Slack is also a good
place for early discussion, but important decisions should still be captured in a
GitHub issue so they are visible and searchable.

Small bug fixes, documentation fixes, and narrowly scoped maintenance changes can go
straight to a pull request.

## Pull Requests

Good pull requests are focused and easy to review:

* Keep each pull request scoped to one logical change.
* Include tests for behavior changes.
* Update public API documentation or doc comments when public behavior changes.
* Add a high-level changelog entry for user-facing changes, under the `## Unreleased`
  heading. The two changelogs are split by audience: the repository-root `CHANGELOG.md`
  serves users of the Rust SDK, and `crates/sdk-core/CHANGELOG.md` serves users of the
  other Temporal SDKs, whose workers and clients run on Core. The test is whether a user
  of that SDK can observe the change — behavior, an option they set, a log or metric, a
  different interaction with the server. Because the Rust SDK runs on Core too, a
  user-observable change in Core behavior normally belongs in both files; an internal Rust
  API change that a language SDK absorbs inside its own bridge is invisible to its users
  and belongs in neither. Write each entry from the perspective of the file's audience,
  wording a change that goes in both files separately for each. The comment at the top of
  either file lists the available headings.
* Describe what changed, why it changed, and what validation you ran.

Run the relevant local checks when practical. CI must pass before a pull request can
be merged.

## Things to Avoid

Avoid changes that make review harder without improving the contribution:

* Unrelated refactors mixed into a behavior change.
* Style-only churn.
* Large feature pull requests that were not discussed first.
* License, copyright, or other legal changes without maintainer discussion.

## AI-Generated Contributions

Using AI tools while contributing is acceptable. You are responsible for the
correctness, quality, and maintainability of everything you submit.

Contributors must fully understand the issue they are fixing and be able to explain
the proposed change. We expect that human understanding to be evident in pull
request responses and design discussions. If a contribution's interaction appears
entirely AI-driven, maintainers may close it: it does not provide a benefit over
maintainers using AI tooling themselves.

Thoroughly self-review AI-generated code and documentation before opening a pull
request. Make sure it is correct, tested where appropriate, and consistent with the
style and patterns of the codebase.

Keep AI-assisted changes concise and scoped. Avoid verbose generated prose,
unnecessary comments, or broad rewrites that make the change harder to review.

## Contributor License Agreement

All contributors must complete the Temporal Contributor License Agreement (CLA)
before changes can be merged. A link to the CLA will be posted in the pull request.

## Security Issues

Do not open public GitHub issues for suspected security vulnerabilities. Report them
to security@temporal.io instead.

## Review and CI

Maintainers review pull requests for correctness, compatibility, test coverage,
documentation, and long-term maintainability. Review may require changes before a
pull request can be merged, and it may take maintainers some time to review a
contribution.

CI is the final validation gate. If CI fails, update the pull request or ask for help
if the failure appears unrelated to your change. Some CI gates may wait for a
maintainer to approve or run them.

## Inactive Pull Requests

Maintainers may close inactive pull requests after follow-up if they are no longer
moving forward. If that happens, you are welcome to reopen the pull request or open a
new one when you are ready to continue.

## Community Conduct

Keep discussions respectful, constructive, and focused on the work. Clear context,
specific examples, and patience with review feedback help everyone move faster.
