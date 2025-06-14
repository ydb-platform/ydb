pkgs: attrs: with pkgs; with attrs; rec {
  version = "0.12.3";

  src = fetchFromGitHub {
    owner = "awslabs";
    repo = "aws-c-common";
    rev = "v${version}";
    hash = "sha256-mOn2JZnqWHZ0M6PYkkckCyDjCdp9kKup6CYEew8AgKw=";
  };
}
