pkgs: attrs: with pkgs; with attrs; rec {
  version = "0.8.15";

  src = fetchFromGitHub {
    owner = "awslabs";
    repo = "aws-c-common";
    rev = "v${version}";
    hash = "sha256-AemFZZwfHdjqX/sXUw1fpusICOa3C7rT6Ofsz5bGYOQ=";
  };
}
