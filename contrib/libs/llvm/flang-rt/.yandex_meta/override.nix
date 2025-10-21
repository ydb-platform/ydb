pkgs: attrs: with pkgs; with attrs; rec {
  version = "21.1.2";

  src = fetchFromGitHub {
    owner = "llvm";
    repo = "llvm-project";
    rev = "llvmorg-${version}";
    hash = "sha256-SgZdBL0ivfv6/4EqmPQ+I57qT2t6i/rqnm20+T1BsFY=";
  };

  patches = [
    ./disable-fortran.patch
  ];
  postPatch = "";

  sourceRoot = "source";

  # Remove -DSCUDO_DEFAULT_OPTIONS set in llvmPackages.compiler-rt
  env.NIX_FLAGS_COMPILE = "";
}
