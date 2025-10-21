set -e

export CONAN_USER_HOME=/ydbwork/build
export CCACHE_SLOPPINESS=locale
export CCACHE_BASEDIR=/ydbwork/

export CONAN_USER_HOME=/ydbwork/build

export CC=/usr/bin/clang-14
export CC_FOR_BUILD=$CC

mkdir /ydbwork/build
cd /ydbwork/build

echo "::group::cmake"
cmake -G Ninja -DCMAKE_BUILD_TYPE=Release \
-DCCACHE_PATH=/usr/local/bin/ccache \
-DCMAKE_TOOLCHAIN_FILE=../ydb/clang.toolchain \
-DCMAKE_PROJECT_TOP_LEVEL_INCLUDES=./cmake/conan_provider.cmake \
../ydb
echo "::endgroup::"


echo "::group::ninja"
#ninja ydb/apps/ydb/all
ninja
echo "::endgroup::"

ccache -s
