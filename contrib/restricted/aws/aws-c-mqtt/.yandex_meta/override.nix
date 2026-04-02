pkgs: attrs: with pkgs; with attrs; rec {
  version = "0.15.0";

  src = fetchFromGitHub {
    owner = "awslabs";
    repo = "aws-c-mqtt";
    rev = "v${version}";
    hash = "sha256-50b8TLQvaSaawKsGbm4fSCRoTfolAlF7ZwMdNmZ8wQo=";
  };
}
