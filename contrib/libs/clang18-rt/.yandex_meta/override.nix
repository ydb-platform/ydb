pkgs: attrs: with pkgs; with attrs; rec {
  version = "18.1.8";

  src = fetchFromGitHub {
    owner = "llvm";
    repo = "llvm-project";
    rev = "llvmorg-${version}";
    hash = "sha256-iiZKMRo/WxJaBXct9GdAcAT3cz9d9pnAcO1mmR6oPNE=";
  };

  sourceRoot = "source/compiler-rt";

  patches = [];
}
