pkgs: attrs: with pkgs; with attrs; rec {
  version = "17.0.6";

  src = fetchFromGitHub {
    owner = "llvm";
    repo = "llvm-project";
    rev = "llvmorg-${version}";
    hash = "sha256-8MEDLLhocshmxoEBRSKlJ/GzJ8nfuzQ8qn0X/vLA+ag=";
  };

  patches = [];

  sourceRoot = "source/libcxxabi";
}
