pkgs: attrs: with pkgs.python310.pkgs; with pkgs; with attrs; rec {
  pname = "pyodbc";
  version = "4.0.39";

  src = fetchPypi {
    inherit pname version;
    hash = "sha256-5Si7cN1tYpnuQphokl3whm4+kZx3K57/ecjheSDY8RY=";
  };
}
