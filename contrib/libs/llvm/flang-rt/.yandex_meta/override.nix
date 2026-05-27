pkgs: attrs: with pkgs; with attrs; rec {
  version = "22.1.5";

  src = fetchFromGitHub {
    owner = "llvm";
    repo = "llvm-project";
    rev = "llvmorg-${version}";
    hash = "sha256-eunfMOH+HVpefZJ+CG7hXDoM+pi6iYvHpD3DoSAsjoE=";
  };

  patches = [
    ./disable-fortran.patch
  ];
  postPatch = "";

  sourceRoot = "source";

  # Remove -DSCUDO_DEFAULT_OPTIONS set in llvmPackages.compiler-rt
  env.NIX_FLAGS_COMPILE = "";
}
