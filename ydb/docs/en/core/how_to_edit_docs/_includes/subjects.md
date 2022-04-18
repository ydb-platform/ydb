# Structure of {{ ydb-short-name }} documentation subject directories

## Introduction {#intro}

Documentation articles are arranged in a hierarchical structure of subject directories. A subject directory combines a set of articles on a certain general topic.

A topic description includes:

1. The target audience of the article. The audience depends on the role of a potential reader when working with {{ ydb-short-name }}, with the following three basic audiences:
   - Developer of applications for {{ ydb-short-name }}
   - {{ ydb-short-name }} administrator
   - {{ ydb-short-name }} developer/contributor
2. Purpose: The reader's task or problem the solution of which is described in the content.

In general, the directory structure directly affects the structure of the documentation table of contents. Typical exceptions are:

- Intermediate stages of documentation creation. A new directory is created immediately to preserve the referential integrity in the future, but its content is still insufficient to be designed as a separate submenu in the table of contents. In this case, articles can be temporarily included in existing submenus.
- Historically established directories whose transfer is undesirable due to the loss of referential integrity.

## Level 1 structure

The following subject directories are placed on level 1:

| Name | Audience | Purpose |
| --- | ---------- | ---------- |
| getting_started | All | Provide quick solutions to standard problems that arise when starting to work with a new database: what is it, how to install it, how to connect, how to perform elementary operations with data, where to go further in the documentation and on what issues. |
| concepts | Application developer | Describe the main structural components of {{ ydb-short-name }} that one will deal with when developing applications for {{ ydb-short-name }}. Provide an insight into the role of each of these components in developing applications. Provide detailed information about component configuration options available to application developers. |
| reference, yql | Application developer | A reference guide for daily use on tools for accessing {{ ydb-short-name }} functions: CLI, YQL, and SDK |
| best_practices | Application developer | Common approaches to addressing the main challenges that arise when developing applications |
| troubleshooting | ? | Tools for identifying the cause of issues |
| how_to_edit_docs | Developers {{ ydb-short-name }} | How to update the {{ ydb-short-name }} documentation |
| deploy | All | Deploying and configuring {{ ydb-short-name }} databases and clusters. Cloud, orchestrated, and manual deployments. |
| maintenance | All | How to maintain {{ ydb-short-name }} databases and clusters: backups, monitoring, logs, and disk swap. |
| faq | All | Questions and answers |

