pkgs: attrs: with pkgs; with attrs; rec {
  version = "0.15.1";

  src = fetchFromGitHub {
    owner = "awslabs";
    repo = "aws-c-mqtt";
    rev = "v${version}";
    hash = "sha256-ItEXl3Un72FIRqbAgNDbBOGz4Z2IUJgwbEpzgbFebdw=";
  };
}
