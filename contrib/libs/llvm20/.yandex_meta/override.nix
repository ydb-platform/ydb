pkgs: attrs: with pkgs; with attrs; rec {
  version = "20.1.4";

  src = fetchFromGitHub {
    owner = "llvm";
    repo = "llvm-project";
    rev = "llvmorg-${version}";
    hash = "sha256-/WomqG2DdnUHwlVsMfpzaK/dhGV3zychfU0wLmihQac=";
  };

  patches = [
    ./disable-arc4random.patch
  ];

  cmakeFlags = attrs.cmakeFlags ++ [
    "-DCMAKE_BUILD_TYPE=MinSizeRel"
    "-DBUILD_SHARED_LIBS=ON"
    "-DLLVM_BUILD_LLVM_DYLIB=OFF"
    "-DLLVM_ENABLE_LIBPFM=OFF"
    "-DLLVM_ENABLE_LIBXML2=OFF"
    "-DLLVM_ENABLE_TERMINFO=OFF"
    "-DLLVM_LINK_LLVM_DYLIB=OFF"
    "-DLLVM_USE_PERF=ON"
    "-DLLVM_TARGETS_TO_BUILD=AArch64;ARM;BPF;PowerPC;X86;NVPTX;LoongArch;WebAssembly"
    "-DLLVM_EXPERIMENTAL_TARGETS_TO_BUILD="
    "-DHAVE_MALLINFO2="
    "-DPOLLY_ENABLE_GPGPU_CODEGEN=ON"
    "-ULLVM_TABLEGEN"
  ];
}
