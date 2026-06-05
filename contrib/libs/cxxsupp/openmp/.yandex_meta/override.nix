pkgs: attrs: with pkgs; with attrs; rec {
  pname = "openmp";
  version = "22.1.6";

  src = fetchFromGitHub {
    owner = "llvm";
    repo = "llvm-project";
    rev = "llvmorg-${version}";
    hash = "sha256-0on6nTlwzVTT0y3tjZ4ijt5qPQfY/o9Uwe2VYq2NZx8=";
  };

  buildInputs = [ pkgs.python3 ];

  sourceRoot = "source/openmp";
}
