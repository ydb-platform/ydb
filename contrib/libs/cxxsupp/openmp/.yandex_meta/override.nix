pkgs: attrs: with pkgs; with attrs; rec {
  pname = "openmp";
  version = "20.1.7";

  src = fetchFromGitHub {
    owner = "llvm";
    repo = "llvm-project";
    rev = "llvmorg-${version}";
    hash = "sha256-OSd26CLKziKo/eM/5rhtcWd0AxdtJk0ELA5YIxqINKs=";
  };

  buildInputs = [ pkgs.python3 ];

  sourceRoot = "source/openmp";
}
