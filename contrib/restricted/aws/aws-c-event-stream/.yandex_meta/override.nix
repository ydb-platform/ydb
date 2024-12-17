pkgs: attrs: with pkgs; with attrs; rec {
  version = "0.2.20";

  src = fetchFromGitHub {
    owner = "awslabs";
    repo = "aws-c-event-stream";
    rev = "v${version}";
    hash = "sha256-UDACkGqTtyLablSzePMmMk4iGpgfdtZU/SEv0RCSFfA=";
  };
}
