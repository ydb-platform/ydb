pkgs: attrs: with pkgs; with attrs; rec {
  pname = "openmp";
  version = "22.1.1";

  src = fetchFromGitHub {
    owner = "llvm";
    repo = "llvm-project";
    rev = "llvmorg-${version}";
    hash = "sha256-atodeQhxBLz4iDzi7rntR3rDmXS5K8EKAHf6XXotOhg=";
  };

  buildInputs = [ pkgs.python3 ];

  sourceRoot = "source/openmp";
}
