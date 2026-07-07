pkgs: attrs: with pkgs; with attrs; rec {
  pname = "openmp";
  version = "22.1.8";

  src = fetchFromGitHub {
    owner = "llvm";
    repo = "llvm-project";
    rev = "llvmorg-${version}";
    hash = "sha256-SF7wFuh4kXZTytpdgX7vUZItKtRobnVICm+ixze4iG0=";
  };

  buildInputs = [ pkgs.python3 ];

  sourceRoot = "source/openmp";
}
