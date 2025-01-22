pkgs: attrs: with pkgs; with attrs; rec {
  version = "19.1.6";

  src = fetchFromGitHub {
    owner = "llvm";
    repo = "llvm-project";
    rev = "llvmorg-${version}";
    hash = "sha256-LD4nIjZTSZJtbgW6tZopbTF5Mq0Tenj2gbuPXhtOeUI=";
  };

  patches = [];

  sourceRoot = "source/libcxxabi";
}
