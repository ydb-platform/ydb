pkgs: attrs: with pkgs; with attrs; rec {
  version = "1.0.1";

  src = fetchFromGitHub {
    owner = "awslabs";
    repo = "aws-c-common";
    rev = "v${version}";
    hash = "sha256-QJauMpF3O6PMXlX5auWH9wgxpBSZ9AaQeqq/7GV+u74=";
  };
}
