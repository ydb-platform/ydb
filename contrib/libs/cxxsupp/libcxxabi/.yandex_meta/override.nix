pkgs: attrs: with pkgs; with attrs; rec {
  version = "19.1.7";

  src = fetchFromGitHub {
    owner = "llvm";
    repo = "llvm-project";
    rev = "llvmorg-${version}";
    hash = "sha256-cZAB5vZjeTsXt9QHbP5xluWNQnAHByHtHnAhVDV0E6I=";
  };

  patches = [];
  postPatch = "";

  sourceRoot = "source/libcxxabi";
}
