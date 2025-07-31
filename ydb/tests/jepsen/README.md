<img width="64" src="https://raw.githubusercontent.com/ydb-platform/ydb/main/ydb/docs/_assets/logo.svg" /><br/>

[![License](https://img.shields.io/badge/License-EPL%2D%2D2.0-blue.svg)](https://github.com/ydb-platform/jepsen.ydb/blob/main/LICENSE)

# jepsen.ydb

A command line utility for testing YDB with Jepsen.

## Usage

1. Install `gnuplot-nox` and `graphviz` packages at the control node.
2. Install JDK (arcadia one is too old), for example [adoptium](https://adoptium.net/temurin).
3. Set `JAVA_CMD` env:
```bash
export JAVA_CMD=/home/iddqd/tmp/jdk-21.0.8+9/bin/java
```
4. Download [lean](https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein) to `/usr/local/bin/`
5. Create a `~/ydb-nodes.txt` file that lists your YDB cluster nodes.

6. Example command for running the test:
```bash
lein run test \
    --nodes-file ~/ydb-nodes.txt \
    --db-name /your/db/name \
    --no-ssh \
    --concurrency 10n \
    --key-count 15 \
    --max-writes-per-key 1000 \
    --max-txn-length 4 \
    --batch-ops-probability 0.85 \
    --batch-commit-probability 0.5 \
    --ballast-size 1024 \
    --store-type row
```
7. Run http server for observe results:
```bash
lein run serve -p 9000
```

## License

Copyright © 2024 YANDEX LLC

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
