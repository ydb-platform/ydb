pkgs: attrs: with pkgs; with attrs; rec {
  version = "0.5.7";

  src = fetchFromGitHub {
    owner = "awslabs";
    repo = "aws-c-s3";
    rev = "v${version}";
    hash = "sha256-zzsRYhLgJfd02fPgoZBf7n6dTfbLHarc1aQa0fx/uck=";
  };
}
