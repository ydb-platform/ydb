pkgs: attrs: with pkgs; with attrs; rec {
  version = "0.10.4";

  src = fetchFromGitHub {
    owner = "awslabs";
    repo = "aws-c-mqtt";
    rev = "v${version}";
    hash = "sha256-i+ssZzHC8MPfyOaRqvjq0z7w772BJqIA6BwntW1fRek=";
  };
}
