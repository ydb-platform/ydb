pkgs: attrs: with pkgs; with attrs; rec {
  pname = "openmp";
  version = "22.1.5";

  src = fetchFromGitHub {
    owner = "llvm";
    repo = "llvm-project";
    rev = "llvmorg-${version}";
    hash = "sha256-eunfMOH+HVpefZJ+CG7hXDoM+pi6iYvHpD3DoSAsjoE=";
  };

  buildInputs = [ pkgs.python3 ];

  sourceRoot = "source/openmp";
}
