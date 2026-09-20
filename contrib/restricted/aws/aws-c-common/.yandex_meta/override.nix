pkgs: attrs: with pkgs; with attrs; rec {
  version = "1.0.0";

  src = fetchFromGitHub {
    owner = "awslabs";
    repo = "aws-c-common";
    rev = "v${version}";
    hash = "sha256-m3CZwGUDnI+DIrxMPGpjVszZ0UI2aSQ18afbscafW6o=";
  };
}
