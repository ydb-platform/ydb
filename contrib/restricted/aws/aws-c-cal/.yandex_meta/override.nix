pkgs: attrs: with pkgs; with attrs; rec {
  version = "0.5.26";

  src = fetchFromGitHub {
    owner = "awslabs";
    repo = "aws-c-cal";
    rev = "v${version}";
    hash = "sha256-X325dMH3mZBvRqZ540ZsmcJ/N5PbDxzGDf5Gi+XXP0c=";
  };
}
