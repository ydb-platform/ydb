pkgs: attrs: with pkgs; with attrs; rec {
  version = "0.2.18";

  src = fetchFromGitHub {
    owner = "awslabs";
    repo = "aws-c-compression";
    rev = "v${version}";
    hash = "sha256-Cf3MvoRWGAy+vlE59JSpTGOBl07dI4mbIaL1HIiLN/I=";
  };
}
