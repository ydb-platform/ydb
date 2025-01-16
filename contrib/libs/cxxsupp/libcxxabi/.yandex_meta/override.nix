pkgs: attrs: with pkgs; with attrs; rec {
  version = "16.0.6";

  src = fetchFromGitHub {
    owner = "llvm";
    repo = "llvm-project";
    rev = "llvmorg-${version}";
    hash = "sha256-fspqSReX+VD+Nl/Cfq+tDcdPtnQPV1IRopNDfd5VtUs=";
  };

  patches = [];

  sourceRoot = "source/libcxxabi";
}
