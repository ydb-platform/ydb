pkgs: attrs: with pkgs; with attrs; rec {
  version = "0.2.8";

  src = fetchFromGitHub {
    owner = "awslabs";
    repo = "aws-c-s3";
    rev = "v${version}";
    hash = "sha256-kwYzsKdEy+e0GxqYcakcdwoaC2LLPZe8E7bZNrmqok0=";
  };
}
