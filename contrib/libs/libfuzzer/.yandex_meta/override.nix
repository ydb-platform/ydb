pkgs: attrs: with pkgs; with attrs; rec {
  version = "18.1.8";

  src = fetchFromGitHub {
    owner = "llvm";
    repo = "llvm-project";
    rev = "llvmorg-${version}";
    hash = "sha256-iiZKMRo/WxJaBXct9GdAcAT3cz9d9pnAcO1mmR6oPNE=";
  };

  sourceRoot = "source/compiler-rt";

  patches = [
    ./cmake-afl.patch
    ./no-fuchsia.patch
  ];

  cmakeFlags = [
    "-DCOMPILER_RT_DEFAULT_TARGET_ONLY=ON"
    "-DCMAKE_C_COMPILER_TARGET=${stdenv.hostPlatform.config}"

    # Build only necessary subset (i. e. libfuzzer)
    "-DCOMPILER_RT_BUILD_LIBFUZZER=ON"
    "-DCOMPILER_RT_BUILD_SANITIZERS=OFF"
    "-DCOMPILER_RT_BUILD_PROFILE=OFF"
    "-DCOMPILER_RT_BUILD_MEMPROF=OFF"
    "-DCOMPILER_RT_BUILD_BUILTINS=OFF"
    "-DCOMPILER_RT_BUILD_CRT=OFF"
    "-DCOMPILER_RT_BUILD_XRAY=OFF"
    "-DCOMPILER_RT_BUILD_ORC=OFF"
    "-DCOMPILER_RT_BUILD_GWP_ASAN=OFF"

    # Link against external libcxx
    "-DCOMPILER_RT_USE_LIBCXX=OFF"
  ];

  # Remove SCUDO_DEFAULT_OPTIONS
  env = {};
  NIX_CFLAGS_COMPILE = [];
}
