# Base constants
BUILD_DIRNAME = "build"
BUNDLE_DIRNAME = "bundle"
NODE_MODULES_DIRNAME = "node_modules"
NODE_MODULES_WORKSPACE_BUNDLE_FILENAME = "workspace_node_modules.tar"
NPM_REGISTRY_URL = "http://npm.yandex-team.ru"
OUTPUT_TAR_FILENAME = "output.tar"
OUTPUT_TAR_UUID_FILENAME = "output.tar.uuid"
PACKAGE_JSON_FILENAME = "package.json"

# PNPM-specific constants
PNPM_WS_FILENAME = "pnpm-workspace.yaml"
PNPM_LOCKFILE_FILENAME = "pnpm-lock.yaml"

# This is a name of intermediate file that is used in TS_PREPARE_DEPS.
# This file has a structure same to pnpm-lock.yaml, but all tarballs
# a set relative to the build root.
PNPM_PRE_LOCKFILE_FILENAME = "pre.pnpm-lock.yaml"
PNPM_BUILD_BACKUP_LOCKFILE_FILENAME = "build.bkp.pnpm-lock.yaml"

# File is to store the last install status hash to avoid installing the same thing
LOCAL_PNPM_INSTALL_HASH_FILENAME = ".__install_hash__"
# File is to syncronize processes using the local nm_store for the project simultaneously
LOCAL_PNPM_INSTALL_MUTEX_FILENAME = ".__install_mutex__"

STORE_DIRNAME = "pnpm_store"
VIRTUAL_STORE_DIRNAME = ".pnpm"
