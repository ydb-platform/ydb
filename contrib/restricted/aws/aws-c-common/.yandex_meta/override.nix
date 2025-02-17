pkgs: attrs: with pkgs; with attrs; rec {
  version = "0.8.23";

  src = fetchFromGitHub {
    owner = "awslabs";
    repo = "aws-c-common";
    rev = "v${version}";
    hash = "sha256-HkRaQnlasayg5Nu2KaEA18360rxAH/tdJ1iqzoi6i2E=";
  };
}
