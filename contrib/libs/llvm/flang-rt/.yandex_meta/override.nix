pkgs: attrs: with pkgs; with attrs; rec {
  version = "22.1.7";

  src = fetchFromGitHub {
    owner = "llvm";
    repo = "llvm-project";
    rev = "llvmorg-${version}";
    hash = "sha256-AmozlrL8AAlfr+F7OrJqr3ecd/KhBx5Bngj3SopPdyY=";
  };

  patches = [
    ./disable-fortran.patch
  ];
  postPatch = "";

  sourceRoot = "source";

  # Remove -DSCUDO_DEFAULT_OPTIONS set in llvmPackages.compiler-rt
  env.NIX_FLAGS_COMPILE = "";
}
