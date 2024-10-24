pkgs: attrs: with pkgs; with attrs; rec {
  version = "0.1.14";

  src = fetchFromGitHub {
    owner = "awslabs";
    repo = "aws-checksums";
    rev = "v${version}";
    hash = "sha256-yoViXJuM9UQMcn8W0CcWkCXroBLXjAestr+oqWHi5hQ=";
  };
}
