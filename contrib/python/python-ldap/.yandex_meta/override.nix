pkgs: attrs: with pkgs; with python310.pkgs; with attrs; rec {
  pname = "python-ldap";
  version = "3.4.4";

  src = fetchPypi {
    inherit pname version;
    hash = "sha256-ftsKzOxOA3eXcF86Bcvzap/eUNCMj2fyrvmaJij6uCg=";
  };

  patches = [];
}
