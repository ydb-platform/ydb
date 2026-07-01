pkgs: attrs: with pkgs; with attrs; rec {
  pname = "openmp";
  version = "22.1.7";

  src = fetchFromGitHub {
    owner = "llvm";
    repo = "llvm-project";
    rev = "llvmorg-${version}";
    hash = "sha256-AmozlrL8AAlfr+F7OrJqr3ecd/KhBx5Bngj3SopPdyY=";
  };

  buildInputs = [ pkgs.python3 ];

  sourceRoot = "source/openmp";
}
