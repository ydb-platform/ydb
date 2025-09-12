pkgs: attrs: with pkgs; with attrs; rec {
  pname = "openmp";
  version = "21.1.0";

  src = fetchFromGitHub {
    owner = "llvm";
    repo = "llvm-project";
    rev = "llvmorg-${version}";
    hash = "sha256-4DLEZuhREHMl2t0f1iqvXSRSE5VBMVxd94Tj4m8Yf9s=";
  };

  buildInputs = [ pkgs.python3 ];

  sourceRoot = "source/openmp";
}
