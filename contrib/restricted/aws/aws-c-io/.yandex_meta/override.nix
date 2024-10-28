pkgs: attrs: with pkgs; with attrs; rec {
  version = "0.13.21";

  src = fetchFromGitHub {
    owner = "awslabs";
    repo = "aws-c-io";
    rev = "v${version}";
    hash = "sha256-YexLE75SJwzX+xZEXJWu1XGr+zSLnUYvYC0zWcOvU/0=";
  };
}
