pkgs: attrs: with pkgs; with attrs; rec {
  pname = "openmp";
  version = "21.1.6";

  src = fetchFromGitHub {
    owner = "llvm";
    repo = "llvm-project";
    rev = "llvmorg-${version}";
    hash = "sha256-mqZLJYDEs6FXAjbSOruR2ATZZxemNMagNG9SMjSWBFE=";
  };

  buildInputs = [ pkgs.python3 ];

  sourceRoot = "source/openmp";
}
