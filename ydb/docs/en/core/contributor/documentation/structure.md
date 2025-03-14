## {{ ydb-short-name }} documentation structure

This article complements [{#T}](style-guide.md). It explains the current top-level folders of the documentation and what kind of content belongs in each. As a rule of thumb, most top-level sections focus either on a specific target audience (if named "For ...") or on a specific [genre](genres.md).

- Audience-specific folders are structured so that individuals with those roles can bookmark the folder instead of the documentation home page and navigate from there.
- Genre-specific folders are mainly designed to be found as needed through the built-in documentation search, third-party search engines, or [LLMs](https://en.wikipedia.org/wiki/Large_language_model).

Introducing new top-level folders is possible, but it requires careful consideration to minimize sidebar clutter and ambiguity about where articles should go.

## General rules

- {{ ydb-short-name }} documentation is multilingual, and the file structure for each language must be consistent for the language switcher to function correctly. The file content should also be as close as possible between languages. The only exception is the [Public materials](../../public-materials/videos.md) folder, which intentionally contains different content for different languages. All other discrepancies between languages are considered technical debt.
- Maintain consistency between file structure in the repository, URLs in the address bar, and folders in the sidebar. Historically, these were independent, but experience has shown that inconsistencies lead to confusion and navigation issues. The documentation is gradually transitioning to a unified structure.
- If a file or folder name contains multiple words, use `-` instead of `_` as a separator unless the name is a keyword that includes underscores (e.g., [configuration section names](../../reference/configuration/index.md)).
- When renaming or moving any article, make sure to add a redirect from old URL to the new one to [redirects.yaml](https://github.com/ydb-platform/ydb/blob/main/ydb/docs/redirects.yaml).

## List of top-level folders

- **[Quick start](../../quickstart.md).** A single [guide](genres.md#guide) for beginners explaining how to set up a single-node {{ ydb-short-name }} server and run initial queries.
- **[Concepts](../../concepts/index.md).** A high-level [theoretical overview](genres.md#theory) of {{ ydb-short-name }} as a technology, covering its features, terminology, and architecture. Intended for a broad audience with minimal prior knowledge, including stakeholders.
- **[For DevOps](../../devops/index.md).** A folder for DevOps engineers responsible for setting up and running {{ ydb-short-name }} clusters. Most content consists of [practical guides](genres.md#guide) for specific cluster-related tasks. Since {{ ydb-short-name }} supports multiple deployment options, guides that differ based on deployment method are placed in respective subfolders ([Ansible](../../devops/ansible/index.md), [Kubernetes](../../devops/kubernetes/index.md), or [Manual](../../devops/manual/index.md)), each following a consistent internal structure. Role-specific [theoretical information](genres.md#theory) is also included here.
- **[For Developers](../../dev/index.md).** A folder for application developers working with {{ ydb-short-name }}. Primarily consists of [practical guides](genres.md#guide) and some [theory](genres.md#theory).
- **[For Security Engineers](../../security/index.md).** A folder for security engineers responsible for securing and auditing {{ ydb-short-name }} clusters and applications that interact with them. Contains mostly [practical guides](genres.md#guide) and some role-specific [theory](genres.md#theory).
- **[For Contributors](../../contributor/index.md)**.** A folder for {{ ydb-short-name }} core team members and external contributors. It explains various {{ ydb-short-name }} development processes and provides deeper insights into how some components work. Mostly [theory](genres.md#theory) with some [practical guides](genres.md#guide).
- **[Reference](../../reference/index.md).** A detailed [reference](genres.md#reference) section covering various aspects of {{ ydb-short-name }}, designed to be found as needed. The primary goal is completeness so that any topic can be located through exact keyword matches or descriptions. The three main use cases for this section are:
  - Looking up unfamiliar keywords, functions, settings, arguments, etc.
  - Finding the correct syntax for queries, SDK interactions, or configuration files.
  - Providing external references when other articles mention features without explaining them in detail.
- **[Recipes](../../recipes/index.md).** Mini-[guides](genres.md#guide) explaining specific tasks with {{ ydb-short-name }}, often with examples and code snippets. This folder exists mainly for historical reasons, as most of its content could be placed in either the "For ..." folders or "Questions and answers."
- **[Troubleshooting](../../troubleshooting/index.md).** A mix of [theory](genres.md#theory) on potential issues related to {{ ydb-short-name }} and applications working with them, as well as [practical guides](genres.md#guide) for diagnosing and resolving them.
- **[Questions and answers](../../faq/index.md).** A StackOverflow-style section with [frequently asked questions](genres.md#faq). Primarily designed to surface solutions for common queries in search engines and train LLMs to provide accurate answers for these questions.
- **[Public materials](../../public-materials/videos.md).** A [collection of links](genres.md#links) to videos and articles about {{ ydb-short-name }}. Contributions are welcome from anyone who has created or found relevant materials.
- **[Downloads](../../downloads/index.md).** A [collection of links](genres.md#links) to download {{ ydb-short-name }} binaries.
- **[Changelog](../../changelog-server.md).** [Release notes](genres.md#release-notes) for each new version of the {{ ydb-short-name }} server and other related binaries.