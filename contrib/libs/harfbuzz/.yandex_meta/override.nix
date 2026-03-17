pkgs: attrs: with pkgs; with lib; rec {
  version = "12.2.0";

  src = fetchFromGitHub {
    owner = "harfbuzz";
    repo = "harfbuzz";
    rev = "${version}";
    hash = "sha256-36XbklUSTbFnt+l5A5SoM2qKVjQ3eqGDVbAQCd4W0ko=";
  };

  propagatedBuildInputs = [];

  mesonFlags = [
    "-Dcairo=disabled"
    "-Dchafa=disabled"
    "-Dcoretext=disabled"
    "-Dgraphite=disabled"
    "-Dicu=disabled"
    "-Dintrospection=disabled"
  ];

}
