pkgs: attrs: with pkgs; with attrs; rec {
  version = "0.14.7";

  src = fetchFromGitHub {
    owner = "awslabs";
    repo = "aws-c-io";
    rev = "v${version}";
    hash = "sha256-Z4o1vv/8FWp3S7GfLDsV0a8ih+IdJIUC0DL4klOXjnw=";
  };
}
