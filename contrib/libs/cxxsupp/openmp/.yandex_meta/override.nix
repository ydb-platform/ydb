pkgs: attrs: with pkgs; with attrs; rec {
  pname = "openmp";
  version = "21.1.5";

  src = fetchFromGitHub {
    owner = "llvm";
    repo = "llvm-project";
    rev = "llvmorg-${version}";
    hash = "sha256-3OZKcYSJeecSE9RrPCDKpsF4AiLszmb4LLmw0h7Sjjs=";
  };

  buildInputs = [ pkgs.python3 ];

  sourceRoot = "source/openmp";
}
