pkgs: attrs: with pkgs; with attrs; rec {
  version = "20.1.8";

  src = fetchFromGitHub {
    owner = "llvm";
    repo = "llvm-project";
    rev = "llvmorg-${version}";
    hash = "sha256-ysyB/EYxi2qE9fD5x/F2zI4vjn8UDoo1Z9ukiIrjFGw=";
  };

  sourceRoot = "source/compiler-rt";

  patches = [];
}
