#!/usr/bin/env bash

set -Eeuo pipefail

: "${IMAGE:?Set IMAGE to the Docker image under test}"
: "${EXPECTED_REVISION:?Set EXPECTED_REVISION to the ydbd Git revision}"

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
TEST_ROOT=$(mktemp -d "${RUNNER_TEMP:-/tmp}/local-ydb-acceptance.XXXXXX")
ARTIFACTS_DIR=${ACCEPTANCE_ARTIFACTS:-"${TEST_ROOT}/artifacts"}
NAME_PREFIX="local-ydb-acceptance-${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-1}-$$"

# The default-startup test prepares fixtures consumed by later scenarios.
BACKUP_VOLUME="${NAME_PREFIX}-backup-volume"
GENERATED_CONFIG="${TEST_ROOT}/config.yaml"
GENERATED_CERTS="${TEST_ROOT}/generated-certs"

declare -a CONTAINERS=()
declare -a VOLUMES=()
FOREGROUND_DOCKER_PID=""

mkdir -p "$ARTIFACTS_DIR"

# shellcheck source=acceptance/common.sh
source "${SCRIPT_DIR}/acceptance/common.sh"
trap cleanup EXIT

# Keep this list explicit: it documents execution order and shared-fixture dependencies.
# shellcheck source=acceptance/test_image_metadata.sh
source "${SCRIPT_DIR}/acceptance/test_image_metadata.sh"
# shellcheck source=acceptance/test_default_startup.sh
source "${SCRIPT_DIR}/acceptance/test_default_startup.sh"
# shellcheck source=acceptance/test_custom_config.sh
source "${SCRIPT_DIR}/acceptance/test_custom_config.sh"
# shellcheck source=acceptance/test_default_tls.sh
source "${SCRIPT_DIR}/acceptance/test_default_tls.sh"
# shellcheck source=acceptance/test_custom_tls.sh
source "${SCRIPT_DIR}/acceptance/test_custom_tls.sh"
# shellcheck source=acceptance/test_partial_tls.sh
source "${SCRIPT_DIR}/acceptance/test_partial_tls.sh"
# shellcheck source=acceptance/test_tls_disabled.sh
source "${SCRIPT_DIR}/acceptance/test_tls_disabled.sh"
# shellcheck source=acceptance/test_init_scripts.sh
source "${SCRIPT_DIR}/acceptance/test_init_scripts.sh"
# shellcheck source=acceptance/test_backup_restore.sh
source "${SCRIPT_DIR}/acceptance/test_backup_restore.sh"
# shellcheck source=acceptance/test_failing_init_script.sh
source "${SCRIPT_DIR}/acceptance/test_failing_init_script.sh"
# shellcheck source=acceptance/test_interactive.sh
source "${SCRIPT_DIR}/acceptance/test_interactive.sh"

test_image_metadata
test_default_startup
test_custom_config
test_default_tls
test_custom_tls
test_partial_tls
test_tls_disabled
test_init_scripts
test_backup_restore
test_failing_init_script
test_interactive

printf '\nAll Docker image acceptance scenarios passed.\n'
