pkgs: attrs: with pkgs; with attrs; rec {
  version = "0.13.0";

  src = fetchFromGitHub {
    owner = "awslabs";
    repo = "aws-c-mqtt";
    rev = "v${version}";
    hash = "sha256-mDtQ5H8PUExil2y70LaUPKx/PNqTscUnY6CQoYNfKY4=";
  };
}
