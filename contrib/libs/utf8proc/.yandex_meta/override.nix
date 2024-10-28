self: super: with self; rec {
  version = "2.9.0";

  src = fetchFromGitHub {
    owner = "JuliaStrings";
    repo = "utf8proc";
    rev = "v${version}";
    hash = "sha256-Sgh8vTbclUV+lFZdR29PtNUy8F+9L/OAXk647B+l2mg=";
  };
}
