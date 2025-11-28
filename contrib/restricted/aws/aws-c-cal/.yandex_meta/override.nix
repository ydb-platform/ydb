pkgs: attrs: with pkgs; with attrs; rec {
  version = "0.7.4";

  src = fetchFromGitHub {
    owner = "awslabs";
    repo = "aws-c-cal";
    rev = "v${version}";
    hash = "sha256-9VxOMXc2lbaJb43qQJk6Dz1uBJxfFJFTxYsOe/31AwY=";
  };
}
