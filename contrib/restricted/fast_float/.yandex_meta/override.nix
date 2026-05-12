self: super: with self; rec {
  name = "fast_float";
  version = "8.2.5";

  src = fetchFromGitHub {
    owner = "fastfloat";
    repo = "fast_float";
    rev = "v${version}";
    hash = "sha256-ZQm8kDMYdwjKugc2vBG5mwTqXa01u6hODQc/Tai2I9A=";
  };
}
