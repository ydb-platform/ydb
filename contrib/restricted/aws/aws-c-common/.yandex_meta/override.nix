pkgs: attrs: with pkgs; with attrs; rec {
  version = "0.14.1";

  src = fetchFromGitHub {
    owner = "awslabs";
    repo = "aws-c-common";
    rev = "v${version}";
    hash = "sha256-HRnMoWZ1VrIc3ZUQUNonB+bC5vOsdqOD/gN/ZIK4RIo=";
  };
}
