pkgs: attrs: with pkgs; with attrs; rec {
  pname = "openmp";
  version = "21.1.2";

  src = fetchFromGitHub {
    owner = "llvm";
    repo = "llvm-project";
    rev = "llvmorg-${version}";
    hash = "sha256-SgZdBL0ivfv6/4EqmPQ+I57qT2t6i/rqnm20+T1BsFY=";
  };

  buildInputs = [ pkgs.python3 ];

  sourceRoot = "source/openmp";
}
