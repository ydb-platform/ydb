self: super: with self; rec {
  name = "fast_float";
  version = "8.2.1";

  src = fetchFromGitHub {
    owner = "fastfloat";
    repo = "fast_float";
    rev = "v${version}";
    hash = "sha256-TeCMWdeuYvQcuSwOAGd2RlOIHJ32WkNLI/u3+w8VZH8=";
  };
}
