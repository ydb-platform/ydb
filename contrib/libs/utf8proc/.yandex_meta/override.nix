self: super: with self; rec {
  version = "2.11.2";

  src = fetchFromGitHub {
    owner = "JuliaStrings";
    repo = "utf8proc";
    rev = "v${version}";
    hash = "sha256-/+/IrsLQ9ykuVOaItd2ZbX60pPlP2omvS1qJz51AnWA=";
  };
}
