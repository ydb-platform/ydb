pkgs: attrs: with pkgs; with attrs; rec {
  pname = "openmp";
  version = "22.1.2";

  src = fetchFromGitHub {
    owner = "llvm";
    repo = "llvm-project";
    rev = "llvmorg-${version}";
    hash = "sha256-z6YcxgDd3F3JwfU5Y/wMw5MK+ZPISI3KLwHwUaraTuw=";
  };

  buildInputs = [ pkgs.python3 ];

  sourceRoot = "source/openmp";
}
