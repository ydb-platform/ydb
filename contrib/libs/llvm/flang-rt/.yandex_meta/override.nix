pkgs: attrs: with pkgs; with attrs; rec {
  version = "22.1.0";

  src = fetchFromGitHub {
    owner = "llvm";
    repo = "llvm-project";
    rev = "llvmorg-${version}";
    hash = "sha256-tG0JuBfOyYCqGGS2N2zpJxhkb1CUnGtfZ5nBesQzK/A=";
  };

  patches = [
    ./disable-fortran.patch
  ];
  postPatch = "";

  sourceRoot = "source";

  # Remove -DSCUDO_DEFAULT_OPTIONS set in llvmPackages.compiler-rt
  env.NIX_FLAGS_COMPILE = "";
}
