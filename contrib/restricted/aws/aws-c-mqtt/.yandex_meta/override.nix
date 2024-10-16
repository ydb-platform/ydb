pkgs: attrs: with pkgs; with attrs; rec {
  version = "0.8.8";

  src = fetchFromGitHub {
    owner = "awslabs";
    repo = "aws-c-mqtt";
    rev = "v${version}";
    hash = "sha256-bt5Qjw+CqgTfi/Ibhc4AwmJxr22Q6m3ygpmeMhvQTT0=";
  };
}
