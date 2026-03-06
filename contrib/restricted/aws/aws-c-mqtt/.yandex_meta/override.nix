pkgs: attrs: with pkgs; with attrs; rec {
  version = "0.14.0";

  src = fetchFromGitHub {
    owner = "awslabs";
    repo = "aws-c-mqtt";
    rev = "v${version}";
    hash = "sha256-8qlyOlzYucarGRoRnXmL56OFQt/mhfZlAxfQN3n4aDw=";
  };
}
