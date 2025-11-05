pkgs: attrs: with pkgs; with attrs; rec {
  pname = "openmp";
  version = "21.1.3";

  src = fetchFromGitHub {
    owner = "llvm";
    repo = "llvm-project";
    rev = "llvmorg-${version}";
    hash = "sha256-zYoVXLfXY3CDbm0ZI0U1Mx5rM65ObhZg6VkU1YrGE0c=";
  };

  buildInputs = [ pkgs.python3 ];

  sourceRoot = "source/openmp";
}
