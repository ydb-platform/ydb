pkgs: attrs: with pkgs; with attrs; rec {
  version = "0.2.9";

  src = fetchFromGitHub {
    owner = "awslabs";
    repo = "aws-checksums";
    rev = "v${version}";
    hash = "sha256-SqmVFz3wqMzrPlWaCDr+PiTj6J1J4to5lnuu6bsr3iU=";
  };
}
