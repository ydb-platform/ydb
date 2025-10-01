pkgs: attrs: with pkgs; with attrs; rec {
  pname = "openmp";
  version = "21.1.1";

  src = fetchFromGitHub {
    owner = "llvm";
    repo = "llvm-project";
    rev = "llvmorg-${version}";
    hash = "sha256-IB9Z3bIMwfgw2W2Vxo89CmtCM9DfOyV2Ei64nqgHrgc=";
  };

  buildInputs = [ pkgs.python3 ];

  sourceRoot = "source/openmp";
}
