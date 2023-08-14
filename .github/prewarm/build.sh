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
-DCMAKE_C_COMPILER_LAUNCHER=/usr/local/bin/ccache -DCMAKE_CXX_COMPILER_LAUNCHER=/usr/local/bin/ccache \
-DCMAKE_TOOLCHAIN_FILE=../ydb/clang.toolchain \
-DCMAKE_C_FLAGS_RELEASE="-O2 -UNDEBUG" \
-DCMAKE_CXX_FLAGS_RELEASE="-O2 -UNDEBUG" \
../ydb
echo "::endgroup::"


echo "::group::ninja"
#ninja ydb/apps/ydb/all
ninja
echo "::endgroup::"

ccache -s
