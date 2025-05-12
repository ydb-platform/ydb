pkgs: attrs: with pkgs; with attrs; rec {
  version = "0.9.17";

  src = fetchFromGitHub {
    owner = "awslabs";
    repo = "aws-c-common";
    rev = "v${version}";
    hash = "sha256-Ee3wkgIOTsZn2PgHoaO5HqblXuOacuKm5vUwkl4Dg+4=";
  };
}
