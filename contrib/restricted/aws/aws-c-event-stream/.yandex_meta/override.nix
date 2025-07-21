pkgs: attrs: with pkgs; with attrs; rec {
  version = "0.5.5";

  src = fetchFromGitHub {
    owner = "awslabs";
    repo = "aws-c-event-stream";
    rev = "v${version}";
    hash = "sha256-wVjpDKKwoksq5gFtvhH76c7ciP0XmMozhkWmzY6GwgU=";
  };
}
