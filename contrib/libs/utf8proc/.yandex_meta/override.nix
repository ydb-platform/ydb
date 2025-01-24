self: super: with self; rec {
  version = "2.10.0";

  src = fetchFromGitHub {
    owner = "JuliaStrings";
    repo = "utf8proc";
    rev = "v${version}";
    hash = "sha256-wmtMo6eBK/xxxkIeJfh5Yb293po9cKK+7WjqNPoxM9g=";
  };
}
