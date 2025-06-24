pkgs: attrs: with pkgs; with attrs; rec {
  version = "0.13.1";

  src = fetchFromGitHub {
    owner = "awslabs";
    repo = "aws-c-mqtt";
    rev = "v${version}";
    hash = "sha256-bUASZ1B2zrKAoa0GBKA1Qvn3jFt/mzvop6Y6d/zD2Kc=";
  };
}
