pkgs: attrs: with pkgs; with attrs; rec {
  version = "0.13.3";

  src = fetchFromGitHub {
    owner = "awslabs";
    repo = "aws-c-mqtt";
    rev = "v${version}";
    hash = "sha256-Nf8c5iVl+NOPZFjsAPCMOGq2e7D8e7PafuMQh6t0DYw=";
  };
}
