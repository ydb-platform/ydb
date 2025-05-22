pkgs: attrs: with pkgs; with attrs; rec {
  version = "0.6.12";

  src = fetchFromGitHub {
    owner = "awslabs";
    repo = "aws-c-cal";
    rev = "v${version}";
    hash = "sha256-aegK01wYdOc6RGNVf/dZKn1HkqQr+yEblcu6hnlMZE4=";
  };
}
