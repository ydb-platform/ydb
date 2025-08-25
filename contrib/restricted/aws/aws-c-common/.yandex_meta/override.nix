pkgs: attrs: with pkgs; with attrs; rec {
  version = "0.12.4";

  src = fetchFromGitHub {
    owner = "awslabs";
    repo = "aws-c-common";
    rev = "v${version}";
    hash = "sha256-hKCIPZlLPyH7D3Derk2onyqTzWGUtCx+f2+EKtAKlwA=";
  };
}
