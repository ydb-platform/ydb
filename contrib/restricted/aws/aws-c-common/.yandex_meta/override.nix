pkgs: attrs: with pkgs; with attrs; rec {
  version = "0.14.4";

  src = fetchFromGitHub {
    owner = "awslabs";
    repo = "aws-c-common";
    rev = "v${version}";
    hash = "sha256-15nidQkcaWzkRSHuqWrD980cMSHcXP9snLjrgThNSdU=";
  };
}
