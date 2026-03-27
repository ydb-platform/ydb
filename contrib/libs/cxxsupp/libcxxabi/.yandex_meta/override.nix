pkgs: attrs: with pkgs; with attrs; rec {
  version = "21.1.8";

  src = fetchFromGitHub {
    owner = "llvm";
    repo = "llvm-project";
    rev = "llvmorg-${version}";
    hash = "sha256-pgd8g9Yfvp7abjCCKSmIn1smAROjqtfZaJkaUkBSKW0=";
  };

  patches = [];
  postPatch = "";

  sourceRoot = "source/libcxxabi";
}
