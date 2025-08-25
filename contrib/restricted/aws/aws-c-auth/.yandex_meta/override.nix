pkgs: attrs: with pkgs; with attrs; rec {
  version = "0.7.18";

  src = fetchFromGitHub {
    owner = "awslabs";
    repo = "aws-c-auth";
    rev = "v${version}";
    hash = "sha256-JWYJz8ugYvXDvtJ5dRWVcA8F3PdjxO8aCc8l0jghYXg=";
  };
}
