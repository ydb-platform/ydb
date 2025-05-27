pkgs: attrs: with pkgs; with attrs; rec {
  version = "0.1.16";

  src = fetchFromGitHub {
    owner = "awslabs";
    repo = "aws-c-sdkutils";
    rev = "v${version}";
    hash = "sha256-ih7U2uP5FrBx6or1Rp/k+HWDE6evEZyNM//wsPxH9Qo=";
  };
}
