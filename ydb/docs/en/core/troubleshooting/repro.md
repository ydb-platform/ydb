# How to prepare a reproducible example (repro)

A reproducible example (**repro**) is a minimal, self-contained set of steps: YQL, CLI commands, a script, or a small project, with which the issue reproduces consistently. A good repro helps identify whether the problem is in {{ ydb-short-name }} or in the application code. It speeds up diagnosis and makes it easier to add regression tests.

## When you need a repro {#when-needed}

Prepare a repro when you report a problem with {{ ydb-short-name }}:

- a query error or unexpected result;
- an SDK or CLI failure;
- different behavior of the same scenario when you run it in different ways ({{ ydb-ui-name }}, the CLI, or an SDK).

{% note tip %}

If you cannot reproduce the issue consistently, still report it: describe the circumstances and environment, and attach logs.

{% endnote %}

## What to include in the issue {#must-include}

An **issue** is a problem report in a GitHub repository. Structure it using the sections below:

1. **Bug description** — briefly state what is wrong. Preferably attach the error message itself as text, not as a screenshot.
2. **Steps to reproduce** — a numbered list of actions and minimal YQL or code. State where you ran the scenario: [{{ ydb-ui-name }}](../reference/ydb-ui/index.md), [{{ ydb-short-name }} CLI](../reference/ydb-cli/index.md), or the [{{ ydb-short-name }} SDK](../reference/ydb-sdk/index.md).
3. **Expected behavior** — what should happen when the system works correctly.
4. **Actual behavior** — what actually happened. State whether the issue reproduces consistently.
5. **Environment** — {{ ydb-short-name }} version or branch, how it is deployed, and the client version if relevant. For example, state a Docker image and tag, a binary build, a cluster, or another environment.

Make the scenario self-contained and minimal. Leave only what is necessary for reproduction: the required objects, test data, a query, or code.

Sometimes the issue does not reproduce with a single query and appears only in client code. This happens, for example, with session or topic handling. In that case, prepare a short SDK script or a small project. If needed, include a link to a public repository and the run command.

Also attach a screenshot or a log fragment if the steps would otherwise be ambiguous.

{% note warning %}

The issue you created on GitHub is visible to everyone. Do not publish tokens, passwords, connection strings, internal hostnames, or real data—replace them with test values.

{% endnote %}

## Example layout {#minimal-example}

Below is a sample issue layout for learning. It shows how to structure the sections and does not describe a real bug. The values in **Environment** are examples: use the {{ ydb-short-name }} version and deployment method that you actually use.

{% cut "Example repro" %}

**Bug description**:

A query with `ORDER BY ... DESC` returns rows in ascending order instead of descending order.

**Steps to reproduce**:

1. Open [{{ ydb-ui-name }}](../reference/ydb-ui/index.md).
2. Create a table:

    ```yql
    DROP TABLE IF EXISTS `demo_items`;

    CREATE TABLE `demo_items` (
        `id` Uint64 NOT NULL,
        `price` Uint64 NOT NULL,
        PRIMARY KEY (`id`)
    );
    ```

3. Insert data and run the select:

    ```yql
    UPSERT INTO `demo_items` (`id`, `price`) VALUES
        (1, 10),
        (2, 30),
        (3, 20);

    SELECT `id`, `price`
    FROM `demo_items`
    ORDER BY `price` DESC;
    ```

**Expected behavior**:

Rows are returned in descending `price` order: `(2, 30)`, `(3, 20)`, `(1, 10)`.

**Actual behavior**:

Rows are returned in ascending `price` order: `(1, 10)`, `(3, 20)`, `(2, 30)`. The issue reproduces on every run.

**Environment**:

- {{ ydb-short-name }}: `25.4`
- Deployment: Docker image `{{ ydb_local_docker_image }}:25.4`

{% endcut %}

## Common mistakes {#anti-patterns}

- Only phrases like "it does not work" or "it sometimes fails", or a single pull request link without a detailed problem description.
- Links to private resources, for example an internal wiki or a private repository.
- An entire application instead of a minimal scenario.

## Where to report a bug {#where-to-report}

Before you open an issue, search for similar ones among the [existing issues](https://github.com/ydb-platform/ydb/issues).

If there is no similar issue, [open a new one](https://github.com/ydb-platform/ydb/issues/new) in the [{{ ydb-short-name }} repository](https://github.com/ydb-platform/ydb) on GitHub.

{% note warning %}

Do not report security vulnerabilities in public issues. Write to [security@ydb.tech](mailto:security@ydb.tech) according to the [security policy](https://github.com/ydb-platform/ydb/blob/main/SECURITY.md).

{% endnote %}
