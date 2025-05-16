pkgs: attrs: with pkgs; with attrs; rec {
  version = "0.8.1";

  src = fetchFromGitHub {
    owner = "awslabs";
    repo = "aws-c-http";
    rev = "v${version}";
    hash = "sha256-S5ETVkdGTndt2GJBNL4DU5SycHAufsmN06xBDRMFVKo=";
  };
}
