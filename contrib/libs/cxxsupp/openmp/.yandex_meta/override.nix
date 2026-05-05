pkgs: attrs: with pkgs; with attrs; rec {
  pname = "openmp";
  version = "22.1.3";

  src = fetchFromGitHub {
    owner = "llvm";
    repo = "llvm-project";
    rev = "llvmorg-${version}";
    hash = "sha256-jlSQBwXxLvqp65qP366C2ZverDJa4r5Ux5JXh0IQAnQ=";
  };

  buildInputs = [ pkgs.python3 ];

  sourceRoot = "source/openmp";
}
