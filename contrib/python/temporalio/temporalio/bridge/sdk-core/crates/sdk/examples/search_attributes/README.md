# Search Attributes

This sample demonstrates reading and upserting workflow search attributes. Search attributes allow filtering workflows in the Temporal UI and via list/count APIs.

The workflow reads an initial search attribute set at start time, then upserts additional ones.

### Prerequisites

Custom search attributes must be registered before running this example:

```bash
  temporal operator search-attribute create --name CustomKeywordField --type Keyword
  temporal operator search-attribute create --name CustomIntField --type Int
```

### Running this sample

1. `temporal server start-dev` to start the Temporal server.
2. Register the search attributes (see above).
3. In another terminal, start the worker:

```bash
  cargo run --features examples --example search-attributes-worker
```

4. In another terminal, run the workflow:

```bash
  cargo run --features examples --example search-attributes-starter
```

The starter should print:

    Workflow result: initial_keyword=initial-value, upserted CustomIntField=42
