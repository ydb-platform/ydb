pkgs: attrs: with pkgs; with attrs; rec {
  pname = "openmp";
  version = "21.1.4";

  src = fetchFromGitHub {
    owner = "llvm";
    repo = "llvm-project";
    rev = "llvmorg-${version}";
    hash = "sha256-jcMYtoa5Rsf+CxRSrqR9PMxnYAEfaoQZXHgYFpZucWs=";
  };

  buildInputs = [ pkgs.python3 ];

  sourceRoot = "source/openmp";
}
