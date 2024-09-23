# Installing {{ ydb-short-name }} DSTool

To install and configure {{ ydb-short-name }} DSTool:

1. Install the `ydb-dstool` Python package:

   ```bash
   pip install ydb-dstool
   ```

1. Configure the environment:

   ```bash
   export PATH=${PATH}:${HOME}/.local/bin
   ```

1. Test it by running the command that shows cluster information:

   ```bash
   ydb-dstool -e <bs_endpoint> cluster list
   ```

   * `bs_endpoint`: URI of the interface for {{ ydb-short-name }} cluster distributed storage management. The interface is accessible over HTTP on any cluster node on port 8765 by default. URI example: `http://localhost:8765`.

   Result:

   ```text
   ┌───────┬───────┬───────┬────────┬────────┬───────┬────────┐
   │ Hosts │ Nodes │ Pools │ Groups │ VDisks │ Boxes │ PDisks │
   ├───────┼───────┼───────┼────────┼────────┼───────┼────────┤
   │ 8     │ 16    │ 1     │ 5      │ 40     │ 1     │ 32     │
   └───────┴───────┴───────┴────────┴────────┴───────┴────────┘
   ```
