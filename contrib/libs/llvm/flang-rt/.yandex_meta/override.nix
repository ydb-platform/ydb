pkgs: attrs: with pkgs; with attrs; rec {
  version = "23.1.3";

  src = fetchFromGitHub {
    owner = "llvm";
    repo = "llvm-project";
    rev = "llvmorg-${version}";
    hash = "sha256-+e5YvYEbCvvEQbLVIQP5hwPp4tEk6EB270bSEMKv0t4=";
  };

  patches = [
    ./disable-fortran.patch
  ];
  postPatch = "";

  sourceRoot = "source";

  # Remove -DSCUDO_DEFAULT_OPTIONS set in llvmPackages.compiler-rt
  env.NIX_FLAGS_COMPILE = "";
}
