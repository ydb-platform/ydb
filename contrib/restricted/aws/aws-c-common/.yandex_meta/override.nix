pkgs: attrs: with pkgs; with attrs; rec {
  version = "0.13.1";

  src = fetchFromGitHub {
    owner = "awslabs";
    repo = "aws-c-common";
    rev = "v${version}";
    hash = "sha256-pvVhrgynahEvBe+K9T/tbCEXzrGDt400Uwo6G2wGAwo=";
  };
}
