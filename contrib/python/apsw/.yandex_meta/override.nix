pkgs: attrs: with pkgs; with pkgs.python310.pkgs; with attrs; rec {
  version = "3.40.1.0";

  # when fetched from pypi,
  # will unconditionally fetch sqlite3 code and enable its amalgamation
  # (that is, including sqlite3.c into the extension code).
  #
  # We would like to avoid this behavior, hence we are using fetchFromGitHub
  src = fetchFromGitHub {
    owner = "rogerbinns";
    repo = "apsw";
    rev = "${version}";
    hash = "sha256-abZ2qoRV7oVvZtfe/GFbzBztBOWuFejb+fYFi1a0BL4=";
  };

  patches = [ ./disable-check-sqlite-version.patch ];
}
