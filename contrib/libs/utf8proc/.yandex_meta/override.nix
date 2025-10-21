self: super: with self; rec {
  version = "2.11.0";

  src = fetchFromGitHub {
    owner = "JuliaStrings";
    repo = "utf8proc";
    rev = "v${version}";
    hash = "sha256-iNITnxA1cacOBRU/XV22yzjB0XUOCYsaLLLPYLa+AoA=";
  };
}
