pkgs: attrs: with pkgs; with attrs; rec {
  pname = "openmp";
  version = "22.1.4";

  src = fetchFromGitHub {
    owner = "llvm";
    repo = "llvm-project";
    rev = "llvmorg-${version}";
    hash = "sha256-7fo7XYmWAziRpwloM7HMBRAJh7X8aIEce+LilikXwug=";
  };

  buildInputs = [ pkgs.python3 ];

  sourceRoot = "source/openmp";
}
