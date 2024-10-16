set -xue

sh ./run.sh --target-platform default-darwin-x86_64
sh ./run.sh --target-platform default-darwin-arm64
sh ./run.sh --target-platform default-linux-x86_64 --musl
sh ./run.sh --target-platform windows
