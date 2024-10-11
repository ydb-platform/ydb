pkgs: attrs: with pkgs; with attrs; rec {
  version = "0.6.26";

  src = fetchFromGitHub {
    owner = "awslabs";
    repo = "aws-c-auth";
    rev = "v${version}";
    hash = "sha256-PvdkTw5JydJT0TbXLB2C9tk4T+ho+fAbaw4jU9m5KuU=";
  };
}
