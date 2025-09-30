pkgs: attrs: with pkgs; with attrs; rec {
  version = "21.1.1";

  src = fetchFromGitHub {
    owner = "llvm";
    repo = "llvm-project";
    rev = "llvmorg-${version}";
    hash = "sha256-IB9Z3bIMwfgw2W2Vxo89CmtCM9DfOyV2Ei64nqgHrgc=";
  };

  patches = [
    ./disable-fortran.patch
  ];
  postPatch = "";

  sourceRoot = "source";

  # Remove -DSCUDO_DEFAULT_OPTIONS set in llvmPackages.compiler-rt
  env.NIX_FLAGS_COMPILE = "";
}
