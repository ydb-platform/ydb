pkgs: attrs: with pkgs; with attrs; rec {
  pname = "openmp";
  version = "22.1.0";

  src = fetchFromGitHub {
    owner = "llvm";
    repo = "llvm-project";
    rev = "llvmorg-${version}";
    hash = "sha256-tG0JuBfOyYCqGGS2N2zpJxhkb1CUnGtfZ5nBesQzK/A=";
  };

  buildInputs = [ pkgs.python3 ];

  sourceRoot = "source/openmp";
}
