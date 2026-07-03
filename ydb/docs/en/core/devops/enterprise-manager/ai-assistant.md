# Enabling AI assistant in YDB EM

This guide shows how to enable AI assistant in {{ ydb-short-name }} Enterprise Manager (YDB EM). After the setup, users will see the assistant in the YDB EM web interface. The assistant sends model requests through Gateway and can use Model Context Protocol (MCP) tools provided by Gateway.

## Before you start {#before-start}

You can use this guide before the first YDB EM deployment or when updating an existing installation. For a new deployment, add the variables to the inventory before running the initial setup playbook. For deployment instructions, see [{#T}](initial-deployment.md).

Make sure that you have:

1. Access to the Ansible inventory used to deploy YDB EM.
1. An OpenAI-compatible model endpoint that will be reachable from the Gateway host.
1. A way to add a named model credential entry to the Gateway token file. The tokenator in Gateway reads this file and uses the `Token` value of the entry whose `Name` equals `ydb_em_ai_model_token_name`, for example `model-token`, as the upstream `Authorization` header.
1. If you are updating an existing installation, access to the deployed Gateway host.

For an existing installation, first confirm that the Gateway host is managed by the same Ansible inventory. Check the active service manager unit, process owner, config path, token file, and listening port before applying the playbook. If the host uses a custom or manual Gateway layout, apply the same settings through that installation's operational procedure instead of running the playbook blindly.

{% note warning %}

Do not put model API keys, OAuth tokens, or other secrets into `ydb_em_ai_assistant_client_runtime_config`. Gateway returns this value to the browser from `GET /meta/ai_assistant_client_config`.

{% endnote %}

## How it works {#how-it-works}

The browser works with the assistant through Gateway:

1. The UI checks `GET /capabilities`. The assistant button is shown when `Settings.Proxy.Model` is `true` and the user setting for AI assistant is enabled. When the backend capability first appears, YDB EM initializes this user setting to enabled.
1. The UI gets runtime settings from `GET /meta/ai_assistant_client_config`.
1. The UI sends model requests to `/proxy/model/...`.
1. Gateway forwards model requests to `ydb_em_ai_model_endpoint` and adds the Authorization header from tokenator.
1. The UI sends MCP requests to `/meta/mcp`. Gateway exposes MCP tools through this endpoint and runs them against YDB EM APIs and cluster proxy endpoints. When documentation search is configured, `search_docs` is exposed through the same MCP endpoint and can be used by the assistant.

## Configure model access {#configure-model-access}

`ydb_em_ai_model_token_name` is the tokenator entry name that Gateway uses for model requests. It must not be the raw token value. In the token file, this is the `Name` field of an entry whose `Token` value is the full HTTP `Authorization` header expected by the model endpoint.

Example tokenator entry:

```textproto
StaticTokenInfo {
  Name: "model-token"
  Token: "Bearer <model-api-token>"
}
```

The token delivery method depends on your deployment process. The requirement is that the Gateway tokenator file contains an entry whose `Name` matches `ydb_em_ai_model_token_name`.

{% note info %}

The default YDB EM Ansible token template renders only the `meta` credentials entry. If your installation uses that template unchanged, extend or override the tokenator file through your deployment process so that it also contains `model-token` and, if different, the embeddings token.

{% endnote %}

## Configure Ansible variables {#configure-ansible-variables}

Add the AI assistant variables to the YDB EM inventory, for example to `examples/inventory/50-inventory.yaml` or another inventory file used by your deployment.

```yaml
ydb_em_ai_assistant_enabled: true
ydb_em_ai_model_endpoint: "https://llm.example.com"
ydb_em_ai_model_token_name: "model-token"

ydb_em_ai_assistant_client_runtime_config:
  llm:
    baseURL: "/proxy/model/v1"
    model: "<model-name>"
    temperature: 0.2
  mcp:
    - id: "ydb-meta"
      url: "/meta/mcp"
      transport: "streamable"
      toolCallTimeoutMs: 600000
```

Parameter | Description
--- | ---
`ydb_em_ai_assistant_enabled` | Enables AI assistant settings in Gateway.
`ydb_em_ai_model_endpoint` | Upstream model API endpoint used by Gateway.
`ydb_em_ai_model_token_name` | Tokenator entry name for model requests.
`ydb_em_ai_assistant_client_runtime_config.llm.baseURL` | Browser-visible model URL. Use Gateway proxy, usually `/proxy/model/v1`.
`ydb_em_ai_assistant_client_runtime_config.llm.model` | Model name sent to the OpenAI-compatible API.
`ydb_em_ai_assistant_client_runtime_config.mcp` | MCP servers available to the assistant. For YDB EM, use `/meta/mcp`.

Gateway appends the request suffix from `/proxy/model/...` to `ydb_em_ai_model_endpoint`. With `baseURL: "/proxy/model/v1"`, a chat completion request is forwarded to `/v1/chat/completions` on the upstream endpoint. Configure the endpoint so that this path is valid for your model provider.

## Configure documentation search {#configure-docs-search}

This step is optional. Enable it only if the assistant should have the `search_docs` MCP tool. Gateway calls an OpenAI-compatible embeddings endpoint for this tool and appends `/embeddings` to the configured base URL when the suffix is missing. When documentation search is enabled, the assistant gets `search_docs` through the configured `/meta/mcp` server.

```yaml
ydb_em_docs_search_enabled: true
ydb_em_docs_search_embeddings_upstream_base_url: "https://llm.example.com/v1"
ydb_em_docs_search_embeddings_token_name: "model-token"
ydb_em_docs_search_embeddings_model: "<embeddings-model-name>"
ydb_em_docs_search_vector_size: 0
ydb_em_docs_search_limit: 6
ydb_em_docs_search_score: 0.6
```

Parameter | Description
--- | ---
`ydb_em_docs_search_enabled` | Configures the semantic docs search backend and exposes `search_docs` through MCP when all required search settings are valid.
`ydb_em_docs_search_embeddings_upstream_base_url` | OpenAI-compatible embeddings endpoint base URL.
`ydb_em_docs_search_embeddings_token_name` | Tokenator entry name for embeddings requests.
`ydb_em_docs_search_embeddings_model` | Embeddings model name.
`ydb_em_docs_search_vector_size` | Optional embedding vector dimension override. Leave it unset to use the Ansible default `0`. With `0`, Gateway omits the OpenAI-compatible `dimensions` field and skips the response size check. Set a positive value only when the embeddings provider and model support explicit dimensions and the expected size is known.
`ydb_em_docs_search_limit` | Maximum number of documents returned.
`ydb_em_docs_search_score` | Minimum similarity score from `0` to `1`. Documents with a lower score are filtered out.

## Apply the configuration {#apply-configuration}

After updating the inventory, run the YDB EM playbook from the directory with your inventory. The same playbook is used for initial deployment and for applying this change to an existing installation:

```bash
ansible-playbook ydb_platform.ydb_em.initial_setup
```

If your vault file is encrypted, add the vault option used in your deployment:

```bash
ansible-playbook ydb_platform.ydb_em.initial_setup --ask-vault-pass
```

The role renders the Gateway config and the default token file and ensures that the Gateway service is started. If you deliver additional model credentials through an overridden token file or another deployment step, make sure the deployed token file contains the configured entry name. When applying these settings to an already running deployment, restart the Gateway service using your operational procedure if your Ansible run does not restart it after config or token changes.

## Verify the setup {#verify}

Open the YDB EM web interface:

```text
https://<gateway-host>:8789/ui/clusters
```

Check that model proxy is enabled:

```bash
curl -k https://<gateway-host>:8789/capabilities
```

The response should contain:

```json
{
  "Settings": {
    "Proxy": {
      "Model": true
    }
  }
}
```

Check the runtime config returned to the UI:

```bash
curl -k https://<gateway-host>:8789/meta/ai_assistant_client_config
```

The response should contain the `llm` block and should not contain model secrets.

Check the model proxy:

```bash
curl -k https://<gateway-host>:8789/proxy/model/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"<model-name>","messages":[{"role":"user","content":"ping"}],"max_tokens":1}'
```

If your deployment requires user authentication, add the same authentication headers that are used for the YDB EM UI.

If documentation search is enabled, also check that `/meta/mcp` exposes `search_docs` and run a harmless documentation-search request through your MCP client or assistant tooling. This verifies both the MCP registration and the embeddings and metabase path.

## Troubleshooting {#troubleshooting}

### Assistant button is not shown {#button-not-shown}

Check that `ydb_em_ai_assistant_enabled` is `true` and that `/capabilities` contains `Settings.Proxy.Model: true`. Also check the user setting for AI assistant in the YDB EM UI: YDB EM initializes it to enabled when the backend capability is present, but a user can turn it off.

### Runtime config is invalid {#runtime-config-invalid}

Check `GET /meta/ai_assistant_client_config`. For an enabled assistant, the response must be a JSON object with `llm.baseURL` and `llm.model` string fields. A `null` response means that Gateway did not load an enabled AI assistant client config.

### Model proxy returns an authorization error {#model-authorization-error}

Check that `ydb_em_ai_model_token_name` matches a tokenator entry and that the entry returns the Authorization header expected by the model endpoint. If tokenator cannot find the entry, Gateway sends the upstream request without the expected Authorization header.

### Model proxy calls the wrong upstream path {#wrong-upstream-path}

Check `ydb_em_ai_model_endpoint` together with `llm.baseURL`. Gateway appends the path from `/proxy/model/...` to the configured endpoint. A duplicated `/v1` path often causes upstream `404` errors.

### MCP tools are unavailable {#mcp-tools-unavailable}

Check that the runtime config contains `url: "/meta/mcp"` and that user requests can reach `/meta/mcp` through Gateway. Gateway registers `/meta/mcp` only when MCP is enabled; it is enabled by default, but custom Gateway config can disable it.

### Documentation search is unavailable {#docs-search-unavailable}

Check the `ydb_em_docs_search_*` settings, the embeddings endpoint path, and the tokenator entry used for embeddings requests. Also make sure documentation vectors are available in the YDB EM metabase. If docs search is not configured, the `search_docs` tool is not exposed.

### Embeddings endpoint rejects vector size {#embeddings-vector-size}

If `search_docs` fails with `variable embedding size not supported`, set `ydb_em_docs_search_vector_size: 0` and verify the embeddings endpoint and model.

### Documentation search returns 504 {#docs-search-504}

If `search_docs` returns `504 Gateway Timeout` and Gateway logs for `/meta/docs` contain `Member not found: doc_id`, check the YDB EM metabase table `ydb/Docs.db`. The current Gateway docs search query uses the `doc_id`, `embedding`, and `payload` columns. For useful results, the table should also contain rows and be built for an embeddings model compatible with `ydb_em_docs_search_embeddings_model`.
