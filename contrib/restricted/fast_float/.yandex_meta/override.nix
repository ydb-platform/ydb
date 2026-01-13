self: super: with self; rec {
  name = "fast_float";
  version = "8.2.0";

  src = fetchFromGitHub {
    owner = "fastfloat";
    repo = "fast_float";
    rev = "v${version}";
    hash = "sha256-opOCu8/jsoRGM89kPV+Xjw6IifWcRoQ4YMSNGa9cQZE=";
  };
}
