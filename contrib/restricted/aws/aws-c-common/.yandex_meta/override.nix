pkgs: attrs: with pkgs; with attrs; rec {
  version = "0.12.6";

  src = fetchFromGitHub {
    owner = "awslabs";
    repo = "aws-c-common";
    rev = "v${version}";
    hash = "sha256-RQNo+B9qdJurgFT9JKWIl7OZd2O9p4KfZdGGFSCnKZA=";
  };
}
