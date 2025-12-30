pkgs: attrs: with pkgs; with attrs; rec {
  pname = "openmp";
  version = "21.1.7";

  src = fetchFromGitHub {
    owner = "llvm";
    repo = "llvm-project";
    rev = "llvmorg-${version}";
    hash = "sha256-SaRJ7+iZMhhBdcUDuJpMAY4REQVhrvYMqI2aq3Kz08o=";
  };

  buildInputs = [ pkgs.python3 ];

  sourceRoot = "source/openmp";
}
