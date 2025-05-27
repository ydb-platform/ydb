pkgs: attrs: with pkgs; with attrs; rec {
  version = "0.2.7";

  src = fetchFromGitHub {
    owner = "awslabs";
    repo = "aws-checksums";
    rev = "v${version}";
    hash = "sha256-dYDTDWZJJ0JlvkMfLS376uUt5QzSmbV0UNRC4aq35TY=";
  };
}
