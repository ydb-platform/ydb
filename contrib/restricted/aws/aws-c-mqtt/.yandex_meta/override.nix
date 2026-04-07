pkgs: attrs: with pkgs; with attrs; rec {
  version = "0.15.2";

  src = fetchFromGitHub {
    owner = "awslabs";
    repo = "aws-c-mqtt";
    rev = "v${version}";
    hash = "sha256-24OqObewfaGO2sly9/R4FBePOezCvqpDlzfmHDhqBsE=";
  };
}
