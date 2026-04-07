#!/bin/bash

set -euo pipefail

find . -path "*/opentelemetry/proto/*" -name "*.proto" -type f -exec sed -i '
    # Заменяем go_package с go.opentelemetry.io на a.yandex-team.ru
    s|option go_package = "go\.opentelemetry\.io/proto/otlp/[^"]*";|option go_package = "a.yandex-team.ru/contrib/libs/opentelemetry-proto";|g
' {} \;
