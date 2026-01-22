pkgs: attrs: with pkgs; with attrs; rec {
  version = "0.2.8";

  src = fetchFromGitHub {
    owner = "awslabs";
    repo = "aws-checksums";
    rev = "v${version}";
    hash = "sha256-0lLvTJATCw0Yf3Z3Q3rN7jQPSc0QK3A0z3wk4qI7L+Q=";
  };
}
